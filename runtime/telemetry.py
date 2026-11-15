"""Measured pipeline stages. HTTP serves a sanitized file, never probes S3 itself."""
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import time
from runtime.environment import ROOT,configure,state_directory,stream_checkpoints
from runtime.medallion import atomic_json


def load_json(path):
    try:return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError,ValueError):return None


def age(value,at=None):
    if not value:return None
    try:
        seconds=((at or datetime.now(timezone.utc))-datetime.fromisoformat(value.replace("Z","+00:00"))).total_seconds()
        return max(0,seconds) if seconds>=-5 else None
    except (ValueError,TypeError):return None


def checkpoint_offsets(directory):
    directory=Path(directory)
    commits=[p for p in (directory/"commits").glob("*") if p.name.isdigit()]
    if not commits:return dict(status="unverified",reason="No committed checkpoint batch")
    batch=max(int(p.name) for p in commits)
    path=directory/"offsets"/str(batch)
    lines=path.read_text(encoding="utf-8").splitlines()
    offsets={}
    for line in lines[1:]:
        try:value=json.loads(line)
        except ValueError:continue
        if isinstance(value,dict):
            for topic,partitions in value.items():
                if isinstance(partitions,dict) and all(str(k).isdigit() for k in partitions):
                    offsets[topic]={str(k):int(v) for k,v in partitions.items()}
    if not offsets:return dict(status="unverified",reason="No Kafka source offsets in checkpoint")
    return dict(status="verified",committed_batch=batch,offsets=offsets,
                checked_at=datetime.fromtimestamp((directory/"commits"/str(batch)).stat().st_mtime,timezone.utc).isoformat())


def kafka_probe():
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition
    topic=os.getenv("KAFKA_TOPIC_TRADES","crypto-trades")
    consumer=KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS","127.0.0.1:9092"),
        enable_auto_commit=False,group_id=None,request_timeout_ms=5000,
        session_timeout_ms=4000,consumer_timeout_ms=3000)
    try:
        parts=consumer.partitions_for_topic(topic)
        if not parts:raise ValueError("No topic partitions")
        end=consumer.end_offsets([TopicPartition(topic,p) for p in sorted(parts)])
        return dict(status="verified",topic=topic,end_offsets={str(k.partition):v for k,v in end.items()})
    finally:consumer.close(autocommit=False)


def db_probe():
    import psycopg2
    conn=psycopg2.connect(host=os.getenv("POSTGRES_HOST","127.0.0.1"),port=os.getenv("POSTGRES_PORT","5433"),
        dbname=os.getenv("POSTGRES_DB","crypto_dw"),user=os.getenv("POSTGRES_USER"),password=os.getenv("POSTGRES_PASSWORD"),
        connect_timeout=3,options="-c timezone=UTC -c statement_timeout=3000 -c default_transaction_read_only=on")
    try:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*),MAX(window_start) FROM warehouse.fact_crypto_1m_candles WHERE symbol='BTC-USDT'")
            count,latest=c.fetchone()
            c.execute("SELECT source,COUNT(*),MAX(window_start) FROM warehouse.fact_crypto_1m_candles WHERE symbol='BTC-USDT' GROUP BY source")
            sources=[dict(source=s,rows=n,last_minute=t.isoformat() if t else None) for s,n,t in c.fetchall()]
            c.execute("SELECT COUNT(*),MAX(feature_timestamp),COUNT(*) FILTER (WHERE actual_direction IS NOT NULL) FROM warehouse.fact_btc_predictions WHERE symbol='BTC-USDT'")
            predictions,feature,evaluated=c.fetchone()
            c.execute("SELECT COUNT(*)-COUNT(DISTINCT (symbol,window_start)) FROM warehouse.fact_crypto_1m_candles")
            duplicates=c.fetchone()[0]
        return dict(status="verified",candle_rows=count,latest_candle=latest.isoformat() if latest else None,
            sources=sources,duplicate_candle_keys=duplicates,predictions=predictions,
            latest_prediction_feature=feature.isoformat() if feature else None,evaluated_predictions=evaluated)
    finally:conn.close()


def collect():
    state=configure();now=datetime.now(timezone.utc).isoformat()
    previous=load_json(state/"telemetry.json") or {}
    stages={}
    for name,call in [("kafka",kafka_probe),("warehouse",db_probe)]:
        try:stages[name]=call()
        except Exception as exc:stages[name]=dict(status="error",error_type=type(exc).__name__)
    for name,folder in [("bronze","bronze"),("speed_dw","dw_candles")]:
        try:stages[name]=checkpoint_offsets(stream_checkpoints()/folder)
        except Exception as exc:stages[name]=dict(status="error",error_type=type(exc).__name__)
    batch=load_json(state/"batch-last-success.json")
    current=load_json(state/"batch-current.json")
    stages["batch"]=dict(status="verified" if batch else "unverified",run_id=batch.get("run_id") if batch else None,
        completed_at=batch.get("completed_at") if batch else None,storage_mode=batch.get("storage_mode") if batch else None,
        output=batch.get("verified_output") if batch else None,cleaning=batch.get("silver") if batch else None,
        frozen_canonical_unchanged=batch.get("frozen_canonical_unchanged") if batch else None,
        current_status=current.get("status") if current else None,dw_sync=batch.get("dw_sync") if batch else False,
        source_commit=batch.get("source_capture",{}).get("commit") if batch else None)
    scheduler=load_json(state/"scheduler.json")
    scheduler_age=age(scheduler.get("checked_at")) if scheduler else None
    stages["scheduler"]=dict(status="verified" if scheduler and scheduler.get("mode")=="watch" and scheduler_age is not None and scheduler_age<660 else "unverified",
        interval_seconds=scheduler.get("interval_seconds") if scheduler else None)
    kafka=stages["kafka"]
    if kafka.get("status")=="verified":
        old=previous.get("stages",{}).get("kafka",{}).get("end_offsets")
        kafka["new_records_since_previous_measurement"]=sum(kafka["end_offsets"].values())-sum(old.values()) if old else None
        for name in ["bronze","speed_dw"]:
            source=stages[name].get("offsets",{}).get(kafka["topic"],{})
            if source:stages[name]["lag_records"]=sum(max(0,end-source.get(part,0)) for part,end in kafka["end_offsets"].items())
    report=dict(schema_version=1,checked_at=now,stages=stages,
        notes="Web charts may query OKX directly; these independent measurements describe the pipeline.")
    atomic_json(state/"telemetry.json",report)
    return report


def public_status(path=None,at=None):
    report=load_json(path or state_directory()/"telemetry.json")
    if not isinstance(report,dict) or not isinstance(report.get("stages"),dict):return dict(status="unverified",message="Chưa có phép đo luồng dữ liệu; không suy ra từ biểu đồ web.",stages={})
    elapsed=age(report.get("checked_at"),at)
    report["measurement_age_seconds"]=elapsed
    report["status"]="current" if elapsed is not None and elapsed<90 else "stale"
    batch=report["stages"].get("batch",{})
    batch_age=age(batch.get("completed_at"),at)
    batch["age_seconds"]=batch_age
    if batch_age is not None and batch_age>900:batch["status"]="stale"
    return report
