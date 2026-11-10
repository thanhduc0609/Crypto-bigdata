"""Snapshot and profile actual stored data. No S3/DB mutation capabilities."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
import re
from urllib.parse import unquote, urlparse

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


def now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, data):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str, allow_nan=False), encoding="utf-8")


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def classify(key):
    """Keep staging/test/checkpoint namespaces out of business totals."""
    parts = key.strip("/").split("/")
    if parts[0] == "checkpoints" or "checkpoints" in parts:
        return "checkpoint", "/".join(parts[:2])
    if any(p in ("tests", "test", "audit") or p.startswith("test_") for p in parts):
        return "test_or_audit", "/".join(parts[:5])
    if any("_staging_" in p or p.endswith("_staging") for p in parts):
        end = next(i for i, p in enumerate(parts) if "_staging" in p) + 1
        return "staging", "/".join(parts[:end])
    if "models" in parts:
        return "model", "/".join(parts[:parts.index("models") + 2])
    if "long_term" in parts:
        end = parts.index("long_term") + 2
        return "forecast_snapshot", "/".join(parts[:end])
    for layer in ("bronze", "silver", "gold", "speed", "streaming", "raw"):
        if layer in parts:
            end = parts.index(layer) + 2
            return ("run_scoped" if "local-runs" in parts else "shared"), "/".join(parts[:end])
    return "other", "/".join(parts[:2])


class ReadOnlyS3:
    def __init__(self, client, bucket, active_prefix=None):
        self.client, self.bucket = client, bucket
        self.active_prefix = active_prefix.strip("/") if active_prefix else None

    def inventory(self):
        started = now()
        rows = []
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket):
            for obj in page.get("Contents", []):
                scope, dataset = classify(obj["Key"])
                if scope == "run_scoped" and self.active_prefix and obj["Key"].startswith(self.active_prefix + "/"):
                    scope = "active_run"
                rows.append(dict(key=obj["Key"], bytes=obj["Size"], etag=obj["ETag"],
                                 modified_utc=obj["LastModified"].isoformat(), scope=scope, dataset=dataset,
                                 after_cutoff=obj["LastModified"].isoformat() > started))
        return dict(bucket=self.bucket, active_run_prefix=self.active_prefix, started_utc=started, ended_utc=now(),
                    scope="Current objects only; not version history; listing is not an atomic S3 snapshot", objects=rows)

    def download(self, item, destination):
        path = Path(destination) / item["key"]
        # Object keys are untrusted filesystem paths.
        root = Path(destination).resolve()
        if not path.resolve().is_relative_to(root) or "\\" in item["key"]:
            raise ValueError("Unsafe object key")
        response = self.client.get_object(Bucket=self.bucket, Key=item["key"], IfMatch=item["etag"])
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            digest, size = hashlib.sha256(), 0
            with path.open("xb") as handle:
                while chunk := response["Body"].read(1024 * 1024):
                    digest.update(chunk); size += len(chunk); handle.write(chunk)
            if size != item["bytes"]:
                raise ValueError("Snapshot object size changed")
            return dict(item, local=str(path), sha256=digest.hexdigest())
        finally:
            response["Body"].close()


def snapshot_objects(reader, manifest, destination, workers=8):
    wanted = [o for o in manifest["objects"] if not o["after_cutoff"] and (
        o["key"].endswith(".parquet") or re.search(r"/_spark_metadata/\d+(?:\.compact)?$", o["key"]))]
    # Catch an accidental broad/expensive scan before downloading content.
    if sum(o["bytes"] for o in wanted) > 2 * 1024**3:
        raise ValueError("Snapshot exceeds 2 GiB; split audit explicitly instead of silently sampling")
    result = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(reader.download, item, destination) for item in wanted]
        for index, future in enumerate(as_completed(futures), 1):
            result.append(future.result())
            if index % 100 == 0 or index == len(futures):
                print(f"[SNAPSHOT] {index}/{len(futures)} objects", flush=True)
    return sorted(result, key=lambda o: o["key"])


def committed_files(items):
    """Replay Spark file-sink metadata (latest compact + following batches).

    Unreferenced Parquet is inventoried, but is not passed off as committed data.
    A broken metadata chain aborts profiling rather than guessing completeness.
    """
    parquet = {o["key"] for o in items if o["key"].endswith(".parquet")}
    logs = {}
    for item in items:
        match = re.search(r"/_spark_metadata/(\d+)(\.compact)?$", item["key"])
        if match:
            logs[(int(match[1]), bool(match[2]))] = item
    if not logs:
        return parquet, dict(mode="directory_listing", committed_files=len(parquet), unreferenced_files=0)
    compact = max((batch for batch, is_compact in logs if is_compact), default=None)
    start = compact if compact is not None else 0
    end = max(batch for batch, _ in logs)
    active = set()
    for batch in range(start, end + 1):
        item = logs.get((batch, batch == compact)) or logs.get((batch, False))
        if item is None:
            raise ValueError(f"Missing Spark metadata batch {batch}")
        lines = Path(item["local"]).read_text(encoding="utf-8").splitlines()
        if not lines or lines[0] != "v1":
            raise ValueError("Unknown Spark sink metadata version")
        for line in lines[1:]:
            record = json.loads(line)
            key = unquote(urlparse(record["path"]).path.lstrip("/"))
            if record.get("action", "add") == "delete":
                active.discard(key)
            else:
                active.add(key)
    missing = active - parquet
    if missing:
        raise ValueError(f"{len(missing)} committed objects absent from snapshot")
    return active, dict(mode="spark_file_sink_log", first_batch=start, last_batch=end,
                       committed_files=len(active), unreferenced_files=len(parquet - active))


def read_frame(item):
    frame = pq.ParquetFile(item["local"]).read().to_pandas()
    # Spark partition columns need not be physically stored in Parquet files.
    for name, value in re.findall(r"(?:^|/)([A-Za-z_]+)=([^/]+)", item["key"]):
        if name not in frame:
            frame[name] = unquote(value)
    frame["_source_object"] = item["key"]
    frame["_source_sha256"] = item["sha256"]
    return frame


def continuity(values, seconds=60):
    stamps = pd.DatetimeIndex(pd.to_datetime(values, utc=True, errors="coerce")).dropna().unique().sort_values()
    if not len(stamps):
        return dict(observed=0, expected_between_bounds=0, missing=0, gaps=[], first=None, last=None)
    gaps = []
    for before, after in zip(stamps[:-1], stamps[1:]):
        distance = (after - before).total_seconds()
        if distance > seconds:
            gaps.append(dict(start=(before + pd.Timedelta(seconds=seconds)).isoformat(),
                             end=(after - pd.Timedelta(seconds=seconds)).isoformat(),
                             missing=int(distance // seconds - 1)))
    missing = sum(g["missing"] for g in gaps)
    return dict(first=stamps[0].isoformat(), last=stamps[-1].isoformat(), observed=len(stamps),
                expected_between_bounds=int((stamps[-1] - stamps[0]).total_seconds() // seconds + 1),
                missing=missing, gaps=gaps, coverage_pct=round(100 * len(stamps) / (len(stamps) + missing), 6))


def candle_profile(frame, symbol="BTC-USDT", as_of=None):
    """Diagnostic validator, distinct from a claim about past cleaning losses."""
    aliases = {"open_price": "open", "high_price": "high", "low_price": "low", "close_price": "close",
               "total_volume": "volume", "volume_btc": "volume"}
    frame = frame.rename(columns={k: v for k, v in aliases.items() if k in frame and v not in frame})
    timestamp = next((c for c in ("window_start", "bucket_time", "date") if c in frame), None)
    if timestamp is None or not all(c in frame for c in ("open", "high", "low", "close", "volume")):
        return {"rows": len(frame), "profile_status": "schema_not_candles", "columns": list(frame.columns)}
    parsed = pd.to_datetime(frame[timestamp], utc=True, errors="coerce")
    prices = frame[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
    flags = {
        "invalid_timestamp": parsed.isna(),
        "invalid_bucket_alignment": parsed.notna() & parsed.ne(parsed.dt.floor("D" if timestamp == "date" else "min")),
        "wrong_symbol": frame["symbol"].ne(symbol) if "symbol" in frame else pd.Series(False, index=frame.index),
        "non_finite_numeric": pd.Series(~np.isfinite(prices.to_numpy()).all(axis=1), index=frame.index),
        "invalid_ohlcv": ((prices[["open", "high", "low", "close"]] <= 0).any(axis=1) |
                          (prices.volume < 0) | (prices.low > prices[["open", "close"]].min(axis=1)) |
                          (prices.high < prices[["open", "close"]].max(axis=1)) | (prices.low > prices.high)),
    }
    if "window_end" in frame:
        end = pd.to_datetime(frame.window_end, utc=True, errors="coerce")
        flags["invalid_window_duration"] = end.isna() | ((end - parsed) != pd.Timedelta(minutes=1))
    if as_of is not None:
        flags["future_bucket"] = parsed > pd.Timestamp(as_of) + pd.Timedelta(seconds=5)
    for field in ("total_trade_value", "turnover_usd", "turnover_usdt"):
        if field in frame:
            values = pd.to_numeric(frame[field], errors="coerce")
            flags["invalid_" + field] = frame[field].notna() & (~np.isfinite(values) | (values < 0))
    bad = pd.Series(False, index=frame.index)
    for flag in flags.values():
        bad |= flag
    valid = frame.loc[~bad].copy()
    duplicate_count = int(parsed[~bad].duplicated().sum())
    unique = valid.assign(_time=parsed[~bad]).drop_duplicates("_time")
    result = dict(rows=len(frame), profile_status="full_scan", invalid_rows=int(bad.sum()),
                  invalid_reasons_overlapping={k: int(v.sum()) for k, v in flags.items()},
                  valid_before_dedup=len(valid), duplicate_rows=duplicate_count, unique_valid_rows=len(unique),
                  continuity=continuity(unique["_time"], 86400 if timestamp == "date" else 60),
                  null_counts={c: int(frame[c].isna().sum()) for c in frame.columns if not c.startswith("_")})
    if "is_closed" in frame:
        result["unclosed_rows"] = int(frame.is_closed.eq(False).sum())
        result["closed_rows"] = int(frame.is_closed.eq(True).sum())
        result["unknown_closed_rows"] = int(frame.is_closed.isna().sum())
    if "source" in frame:
        result["source_counts"] = {str(k): int(v) for k, v in frame.source.value_counts(dropna=False).items()}
    result["count_balance_valid"] = len(frame) == int(bad.sum()) + duplicate_count + len(unique)
    return result


def parquet_inventory(downloaded, as_of=None):
    groups = {}
    for item in downloaded:
        groups.setdefault(item["dataset"], []).append(item)
    results, business = [], {}
    for dataset, items in sorted(groups.items()):
        files = [o for o in items if o["key"].endswith(".parquet")]
        if not files:
            continue
        scope = items[0]["scope"]
        active, commit_info = committed_files(items) if scope in ("active_run", "run_scoped", "shared") else (
            {o["key"] for o in files}, {"mode": "directory_listing"})
        row = dict(dataset=dataset, scope=scope, parquet_files=len(files), bytes=sum(o["bytes"] for o in files),
                   physical_rows=0, logical_committed_rows=0, schemas=[], **commit_info)
        for item in files:
            meta = pq.ParquetFile(item["local"])
            item["rows"] = meta.metadata.num_rows
            row["physical_rows"] += item["rows"]
            if item["key"] in active:
                row["logical_committed_rows"] += item["rows"]
            schema = str(meta.schema_arrow)
            if schema not in row["schemas"]:
                row["schemas"].append(schema)
        if scope in ("shared", "active_run", "run_scoped"):
            frames = [read_frame(o) for o in files if o["key"] in active and o["rows"]]
            if frames:
                frame = pd.concat(frames, ignore_index=True)
                if "raw_payload" in frame or "trade_id" in frame:
                    row["kind"] = "raw_trades" if "raw_payload" in frame else "trades"
                    business[dataset] = frame
                else:
                    row["kind"] = "candles"
                    row["quality"] = candle_profile(frame, as_of=as_of)
                    business[dataset] = frame
        results.append(row)
    for row in results:
        files = [o for o in downloaded if o["dataset"] == row["dataset"] and o["key"].endswith(".parquet")]
        row["parquet_multiset_sha256"] = hashlib.sha256("\n".join(sorted(o["sha256"] for o in files)).encode()).hexdigest()
    for row in results:
        row["same_parquet_bytes_as"] = [other["dataset"] for other in results if other is not row and
            other["parquet_multiset_sha256"] == row["parquet_multiset_sha256"]]
    return results, business
