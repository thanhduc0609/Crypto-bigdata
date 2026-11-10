"""Append a new operational batch version; never mutate frozen ML snapshots."""
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import re
import uuid
from filelock import FileLock
from runtime.environment import ROOT,configure,run_prefix,state_directory


def now():return datetime.now(timezone.utc).isoformat()


def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_name(path.name+"."+uuid.uuid4().hex+".tmp")
    temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding="utf-8")
    os.replace(temporary,path)


def version_paths(run_id,mode,bucket=None):
    if not re.fullmatch(r"[0-9]{8}T[0-9]{12}Z-[a-f0-9]{8}",run_id):raise ValueError("Invalid batch run id")
    if mode=="s3":
        base=f"s3a://{bucket}/{run_prefix()}/medallion-runs/{run_id}"
    elif mode=="local":base=(ROOT/"data/medallion_runs"/run_id).as_posix()
    else:raise ValueError("Unknown storage mode")
    return dict(silver=base+"/silver/trades",gold=base+"/gold/candles_1m",manifest=base+"/manifest.json")


def canonical_digest():
    pointer=ROOT/"configs/canonical_snapshot.json"
    return hashlib.sha256(pointer.read_bytes()).hexdigest() if pointer.exists() else None


def run_batch(sync_to_postgres=True):
    state=configure(spark=True)
    with FileLock(str(state/"batch.lock"),timeout=0):
        from storage.s3 import get_storage_mode,get_s3_bucket,get_bronze_trades_path
        from processing.core.spark_session import create_spark_session
        from processing.batch.bronze_to_silver import process_bronze_to_silver
        from processing.batch.silver_to_gold import process_silver_to_gold
        mode=get_storage_mode();bucket=get_s3_bucket()
        run_id=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")+"-"+uuid.uuid4().hex[:8]
        paths=version_paths(run_id,mode,bucket)
        before=canonical_digest()
        manifest=dict(schema_version=1,run_id=run_id,started_at=now(),storage_mode=mode,
            status="running",paths=paths,bronze_path=get_bronze_trades_path(),canonical_pointer_sha256=before,
            semantics="Operational trade aggregation; authoritative REST candles retain precedence in DW")
        atomic_json(state/"batch-current.json",manifest)
        spark=None
        try:
            if mode=="s3":
                import boto3
                from botocore.config import Config
                client=boto3.client("s3",config=Config(connect_timeout=5,read_timeout=20,retries={"max_attempts":1}))
                prefix=run_prefix()+"/medallion-runs/"+run_id
                client.put_object(Bucket=bucket,Key=prefix+"/_owner.json",Body=json.dumps(dict(run_id=run_id)).encode(),IfNoneMatch="*")
                from runtime.bronze_capture import capture
                source=capture(client,bucket,manifest["bronze_path"],ROOT/"data/medallion_runs"/run_id,state/"bronze-cache")
                input_path=source["path"]
                manifest["source_capture"]=dict(commit=source["commit"],rows=source["rows"],
                    packed_sha256=source["packed_sha256"],source_manifest_sha256=hashlib.sha256(
                        (ROOT/"data/medallion_runs"/run_id/"source.json").read_bytes()).hexdigest())
            else:input_path=manifest["bronze_path"]
            spark=create_spark_session("VersionedMedallionBatch",master="local[2]",include_kafka=False,
                                       include_s3=mode=="s3",include_postgres=False)
            silver=process_bronze_to_silver(spark,bronze_path=input_path,silver_path=paths["silver"])
            manifest["silver"]=silver
            if silver["status"]!="success":
                manifest.update(status="no_data",gold={"status":"skipped"})
            else:
                gold=process_silver_to_gold(spark,sync_to_postgres=sync_to_postgres,
                    silver_path=paths["silver"],gold_path=paths["gold"])
                manifest["gold"]=gold
                if gold["status"]!="success":raise RuntimeError("Gold batch did not finish")
                # Verify the published output, not only a success log.
                from pyspark.sql.functions import min as min_,max as max_,sum as sum_,countDistinct
                published=spark.read.parquet(paths["gold"])
                row=published.agg(min_("window_start").alias("first"),max_("window_start").alias("last"),
                    sum_("trade_count").alias("trades"),countDistinct("window_start").alias("minutes")).first()
                if int(row.trades)!=silver["valid"] or int(row.minutes)!=gold["candles_count"]:
                    raise RuntimeError("Silver and Gold counts do not reconcile")
                manifest["verified_output"]=dict(silver_rows=silver["valid"],gold_rows=int(row.minutes),
                    summed_trade_count=int(row.trades),first_minute=row.first.isoformat(),last_minute=row.last.isoformat())
                manifest.update(status="success",dw_sync=bool(sync_to_postgres))
            if canonical_digest()!=before:raise RuntimeError("Frozen canonical pointer changed")
            manifest.update(completed_at=now(),frozen_canonical_unchanged=True)
            if mode=="s3" and manifest["status"]=="success":
                client.put_object(Bucket=bucket,Key=prefix+"/manifest.json",
                    Body=json.dumps(manifest,ensure_ascii=False).encode(),ContentType="application/json",IfNoneMatch="*")
            atomic_json(ROOT/"data/medallion_runs"/run_id/"manifest.json",manifest)
            if manifest["status"]=="success":atomic_json(state/"batch-last-success.json",manifest)
            atomic_json(state/"batch-current.json",manifest)
            return manifest
        except Exception as exc:
            manifest.update(status="failed",completed_at=now(),error_type=type(exc).__name__)
            atomic_json(state/"batch-current.json",manifest)
            raise
        finally:
            if spark:spark.stop()
