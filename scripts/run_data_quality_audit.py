"""Capture/read frozen S3 sources, replay cleaning locally, never publish lake/DW.

Run --snapshot-only first if desired; --from-snapshot reuses content-verified data.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4
from urllib.parse import urlparse
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from processing.core.env import load_env_vars
from processing.quality.inventory import (ReadOnlyS3, snapshot_objects, parquet_inventory, classify,
                                          read_frame, write_json, sha256, now)
from processing.quality.profiles import daily_replay, postgres_profile, trade_replay, reconcile_candles


def local_inventory():
    import pyarrow.parquet as pq
    rows = []
    for parent in (ROOT / "data",):
        for path in sorted(parent.rglob("*.parquet")):
            if any(part in ("quality_audits", "checkpoints", "spark_temp") for part in path.relative_to(parent).parts):
                continue
            meta = pq.ParquetFile(path)
            rows.append(dict(path=str(path.relative_to(ROOT)), bytes=path.stat().st_size,
                             rows=meta.metadata.num_rows, sha256=sha256(path),
                             scope="forecast_archive" if "long_term" in path.parts else (
                                 "test_or_audit" if any(part.startswith("test_") for part in path.parts) else "local_archive")))
    return rows


def build_spark(directory):
    os.environ.setdefault("JAVA_HOME", r"C:\Program Files\Java\jre1.8.0_511")
    os.environ.setdefault("HADOOP_HOME", str(ROOT / "hadoop"))
    os.environ["PATH"] = str(ROOT / "hadoop/bin") + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["TZ"] = "UTC"
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master("local[2]").appName("ReadOnlyDataQualityReplay")
            .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2").config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1")
            .config("spark.local.dir", str(directory / "spark_tmp")).getOrCreate())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-only", action="store_true")
    parser.add_argument("--from-snapshot", type=Path)
    args = parser.parse_args()
    load_env_vars()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "Z-" + uuid4().hex[:8]
    directory = ROOT / "data/quality_audits" / run_id
    directory.mkdir(parents=True, exist_ok=False)
    print("[AUDIT RUN] " + str(directory), flush=True)
    result = dict(run_id=run_id, started_utc=now(), state="running", writes="Local audit directory only; S3/DB read-only")
    from storage.canonical_snapshot import load_descriptor
    canonical=load_descriptor()
    result["canonical_snapshot"]=canonical
    sources = ("processing/quality/inventory.py", "processing/quality/profiles.py", "scripts/run_data_quality_audit.py",
               "processing/batch/bronze_to_silver.py", "ingestion/historical/okx_daily_backfill.py", "configs/pipeline.yaml")
    result["source_sha256"] = {name: sha256(ROOT / name) for name in sources}
    write_json(directory / "run.json", result)
    try:
        if args.from_snapshot:
            original = args.from_snapshot.resolve()
            manifest = json.loads((original / "s3_inventory.json").read_text(encoding="utf-8"))
            downloaded = json.loads((original / "downloaded_objects.json").read_text(encoding="utf-8"))
            for item in downloaded:
                assert sha256(item["local"]) == item["sha256"], "Frozen snapshot checksum mismatch"
            result["snapshot_reused_from"] = str(original)
        else:
            import boto3
            from botocore.config import Config
            client = boto3.client("s3", region_name=os.getenv("AWS_REGION"),
                                 config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 2}))
            reader = ReadOnlyS3(client, os.environ["S3_BUCKET_NAME"], os.getenv("S3_RUN_PREFIX"))
            manifest = reader.inventory()
            write_json(directory / "s3_inventory.json", manifest)
            downloaded = snapshot_objects(reader, manifest, directory / "snapshot")
        write_json(directory / "s3_inventory.json", manifest)
        write_json(directory / "downloaded_objects.json", downloaded)
        result["s3_inventory_sha256"] = sha256(directory / "s3_inventory.json")
        result["snapshot_objects_sha256"] = sha256(directory / "downloaded_objects.json")
        if args.snapshot_only:
            result.update(state="snapshot_only", finished_utc=now(), snapshot_objects=len(downloaded))
            write_json(directory / "run.json", result)
            print("[SNAPSHOT READY] " + str(directory), flush=True)
            return directory
        datasets, frames = parquet_inventory(downloaded, as_of=manifest["started_utc"])
        write_json(directory / "s3_datasets.json", datasets)
        write_json(directory / "downloaded_objects.json", downloaded)
        objects = manifest["objects"]
        scopes = {}
        for scope in sorted({o["scope"] for o in objects}):
            items = [o for o in objects if o["scope"] == scope]
            scopes[scope] = dict(objects=len(items), bytes=sum(o["bytes"] for o in items),
                                 parquet_files=sum(o["key"].endswith(".parquet") for o in items))
        result["s3"] = dict(bucket=manifest["bucket"], inventory_window=[manifest["started_utc"], manifest["ended_utc"]],
                            objects=len(objects), bytes=sum(o["bytes"] for o in objects), scopes=scopes,
                            objects_after_cutoff=sum(o["after_cutoff"] for o in objects), datasets=datasets)
        primary=(urlparse(canonical["gold_uri"]).path.lstrip("/") if canonical else "crypto/gold/candles_1m")
        if not any(item["dataset"]==primary for item in datasets):
            raise ValueError("Active canonical version absent from this inventory; capture a new S3 snapshot")
        result["s3"]["effective_canonical_dataset"]=primary
        for item in datasets:
            item["canonical_role"]=("active" if item["dataset"]==primary else "legacy_archive" if item["dataset"]=="crypto/gold/candles_1m" else "other")
        result["local_files"] = local_inventory()
        result["daily_replay"] = daily_replay(ROOT, directory)
        import psycopg2
        def connect():
            return psycopg2.connect(host=os.getenv("POSTGRES_HOST", "127.0.0.1"), port=os.getenv("POSTGRES_PORT", "5433"),
                dbname=os.getenv("POSTGRES_DB", "crypto_dw"), user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", ""), connect_timeout=3,
                options="-c timezone=UTC -c statement_timeout=30000 -c default_transaction_read_only=on")
        result["postgres"], db_candles = postgres_profile(connect)
        db_candles.to_parquet(directory / "postgres_candles_snapshot.parquet", index=False)
        result["postgres"]["candles_snapshot_sha256"] = sha256(directory / "postgres_candles_snapshot.parquet")
        result["lake_dw_reconciliation"] = {}
        for dataset, frame in frames.items():
            if "window_start" in frame and "open" in frame:
                comparison, mismatches = reconcile_candles(frames[dataset], db_candles, result["postgres"]["candle_numeric_scales"])
                result["lake_dw_reconciliation"][dataset] = comparison
                mismatches.head(50).to_csv(directory / (dataset.replace("/", "_") + "_dw_mismatch_examples.csv"), index=False, encoding="utf-8-sig")
        result["trade_replays"] = {}
        spark = build_spark(directory)
        try:
            for dataset, frame in frames.items():
                if "/bronze/trades" not in dataset:
                    continue
                name = dataset.replace("/", "_")
                print(f"[CLEANING REPLAY] {dataset}: {len(frame)} rows", flush=True)
                profile, cleaned = trade_replay(spark, frame, directory / "trade_replays" / name)
                result["trade_replays"][dataset] = profile
                # Stored Silver can be compared only after checking keys/period.
                silver_name = dataset.replace("/bronze/trades", "/silver/trades")
                silver = frames.get(silver_name)
                if silver is not None:
                    source_ids, stored_ids = set(cleaned.trade_id), set(silver.trade_id)
                    profile["stored_silver_comparison"] = dict(dataset=silver_name, stored_rows=len(silver),
                        common_trade_ids=len(source_ids & stored_ids), only_replay_ids=len(source_ids - stored_ids),
                        only_stored_ids=len(stored_ids - source_ids),
                        note="Key coverage comparison; equal row counts alone do not prove same cleaning run")
                    common = cleaned.merge(silver, on="trade_id", suffixes=("_replay", "_stored"))
                    unequal = pd.Series(False, index=common.index)
                    fields = []
                    for field in ("price", "size", "side", "raw_timestamp", "trade_value"):
                        a, b = field + "_replay", field + "_stored"
                        if a in common and b in common:
                            fields.append(field)
                            if field == "side":
                                unequal |= common[a].ne(common[b])
                            else:
                                unequal |= ~np.isclose(common[a].astype(float), common[b].astype(float), rtol=1e-12, atol=1e-8, equal_nan=True)
                    profile["stored_silver_comparison"].update(compared_fields=fields, mismatching_common_trade_ids=int(unequal.sum()))
                else:
                    profile["stored_silver_comparison"] = {"status": "absent", "dataset": silver_name,
                        "note": "Local replay is evidence of cleaning this snapshot, not a published Silver batch"}
        finally:
            spark.stop()
        # S3 vs local active daily snapshot are immutable file comparisons.
        active_id = result["daily_replay"]["run_id"]
        result["daily_s3_matches_local"] = []
        for item in downloaded:
            if "/long_term/" + active_id + "/" in item["key"]:
                local = ROOT / "data/long_term/runs" / active_id / Path(item["key"]).name
                result["daily_s3_matches_local"].append(dict(key=item["key"], matches=sha256(local) == item["sha256"]))
        result["code_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip()
        assert all(sha256(ROOT / name) == digest for name, digest in result["source_sha256"].items()), "Audit implementation changed during this run"
        result["snapshot_objects_sha256"] = sha256(directory / "downloaded_objects.json")
        result.update(state="completed", finished_utc=now())
        write_json(directory / "run.json", result)
        print("[AUDIT COMPLETED] " + str(directory), flush=True)
        return directory
    except Exception as exc:
        result.update(state="failed", finished_utc=now(), error_type=type(exc).__name__)
        write_json(directory / "run.json", result)
        raise


if __name__ == "__main__":
    main()
