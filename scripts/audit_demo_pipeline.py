"""Read-only trace of real Kafka -> committed Bronze -> Silver -> Gold -> DW/API."""
from datetime import datetime,timezone
import hashlib
import io
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlparse
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import boto3
from botocore.config import Config
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import psycopg2
import requests
from kafka import KafkaConsumer,TopicPartition
from runtime.environment import configure,ROOT,run_prefix,state_directory
from runtime.medallion import atomic_json


def read_s3_frame(client,uri):
    from storage.project_layout import resolve_uri
    import re
    uri=resolve_uri(uri)
    parsed=urlparse(uri);prefix=parsed.path.strip("/")+"/"
    if not re.fullmatch(re.escape(run_prefix())+r'/(silver/trades|gold/candles_1m)/[0-9]{8}T[0-9]{12}Z-[a-f0-9]{8}/',prefix):
        raise ValueError("Only versioned project Silver/Gold can be audited")
    frames=[];files=[]
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=parsed.netloc,Prefix=prefix):
        for obj in page.get("Contents",[]):
            if not obj["Key"].endswith(".parquet"):continue
            response=client.get_object(Bucket=parsed.netloc,Key=obj["Key"],IfMatch=obj["ETag"])
            try:body=response["Body"].read()
            finally:response["Body"].close()
            frames.append(pq.ParquetFile(io.BytesIO(body)).read().to_pandas())
            files.append(dict(key=obj["Key"],sha256=hashlib.sha256(body).hexdigest(),bytes=len(body)))
    if not frames:raise ValueError("Published layer is empty")
    return pd.concat(frames,ignore_index=True),files


def main():
    configure()
    run=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output=ROOT/"data/demo_audits"/run;output.mkdir(parents=True)
    manifest=json.loads((state_directory()/"batch-last-success.json").read_text())
    source_dir=ROOT/"data/medallion_runs"/manifest["run_id"]
    source=json.loads((source_dir/"source.json").read_text())
    client=boto3.client("s3",config=Config(connect_timeout=5,read_timeout=20,retries={"max_attempts":1}))
    silver,silver_files=read_s3_frame(client,manifest["paths"]["silver"])
    if not {"_source_object","_source_sha256"}.issubset(silver.columns):
        raise ValueError("Published Silver lacks lineage; complete a batch using the corrected Arrow footer")
    gold,gold_files=read_s3_frame(client,manifest["paths"]["gold"])
    closed=gold[gold.is_closed.eq(True)].sort_values("window_start")
    candle=closed.iloc[-1]
    minute=pd.Timestamp(candle.window_start).tz_localize("UTC") if pd.Timestamp(candle.window_start).tzinfo is None else pd.Timestamp(candle.window_start)
    minute_trades=silver[pd.to_datetime(silver.trade_time,utc=True).dt.floor("min").eq(minute)].copy()
    minute_trades=minute_trades.sort_values(["raw_timestamp","trade_id"])
    trade=minute_trades.iloc[-1]
    raw=pq.ParquetFile(source_dir/"bronze-input.parquet").read().to_pandas()
    selected=raw[(raw.kafka_offset==trade.kafka_offset)&(raw.kafka_partition==trade.kafka_partition)&(raw["_source_object"]==trade["_source_object"])].iloc[0]
    topic=str(selected.kafka_topic)
    consumer=KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS","127.0.0.1:9092"),
        group_id=None,enable_auto_commit=False,bootstrap_timeout_ms=3000,request_timeout_ms=7000,session_timeout_ms=6000)
    try:
        partition=TopicPartition(topic,int(selected.kafka_partition));consumer.assign([partition]);consumer.seek(partition,int(selected.kafka_offset))
        messages=consumer.poll(timeout_ms=5000,max_records=1).get(partition,[])
        if not messages:raise ValueError("Captured Kafka record could not be read back")
        message=messages[0]
        kafka_equal=message.offset==int(selected.kafka_offset) and message.value.decode("utf-8")==selected.raw_payload
    finally:consumer.close(autocommit=False)
    payload=json.loads(selected.raw_payload)
    checks=dict(kafka_record_matches_bronze=bool(kafka_equal),source_has_spark_commit_log=source["commit"]["mode"] in ('spark_file_sink_log','spark_file_sink_logs'),
        source_manifest_bound_to_batch=hashlib.sha256((source_dir/"source.json").read_bytes()).hexdigest()==manifest["source_capture"]["source_manifest_sha256"],
        bronze_packed_checksum_valid=hashlib.sha256((source_dir/"bronze-input.parquet").read_bytes()).hexdigest()==source["packed_sha256"],
        bronze_trade_matches_silver=str(payload["trade_id"])==str(trade.trade_id) and float(payload["price"])==float(trade.price),
        silver_trace_preserves_source_digest=selected["_source_sha256"]==trade["_source_sha256"],
        silver_rows_reconcile=len(silver)==manifest["silver"]["valid"],
        gold_trade_counts_reconcile=int(gold.trade_count.sum())==len(silver),
        gold_open_close_correct=bool(np.isclose(candle.open,minute_trades.price.iloc[0],rtol=1e-10,atol=1e-8) and np.isclose(candle.close,minute_trades.price.iloc[-1],rtol=1e-10,atol=1e-8)),
        gold_high_low_correct=bool(np.isclose(candle.high,minute_trades.price.max(),rtol=1e-10,atol=1e-8) and np.isclose(candle.low,minute_trades.price.min(),rtol=1e-10,atol=1e-8)),
        gold_volume_correct=bool(np.isclose(candle.volume,minute_trades["size"].sum(),rtol=1e-10,atol=1e-8)))
    conn=psycopg2.connect(host=os.getenv("POSTGRES_HOST"),port=os.getenv("POSTGRES_PORT"),dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),password=os.getenv("POSTGRES_PASSWORD"),connect_timeout=3,
        options="-c timezone=UTC -c statement_timeout=3000 -c default_transaction_read_only=on")
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT open,high,low,close,volume,source,is_closed FROM warehouse.fact_crypto_1m_candles WHERE symbol='BTC-USDT' AND window_start=%s",(minute.to_pydatetime(),))
            stored=cur.fetchone()
            checks["gold_minute_has_dw_record"]=stored is not None
            reconciliation=dict(minute=minute.isoformat(),dw_source=stored[5] if stored else None,
                note="REST confirmed candle takes precedence over partial stream aggregation; equality to stream Gold is not required")
            if stored and stored[5]=="historical_rest":
                response=requests.get("https://www.okx.com/api/v5/market/history-candles",params=dict(instId="BTC-USDT",bar="1m",after=int(minute.timestamp()*1000)+60000,limit=3),timeout=10)
                response.raise_for_status();rest=response.json()
                match=next(row for row in rest["data"] if int(row[0])==int(minute.timestamp()*1000))
                checks["dw_rest_record_matches_exchange"]=match[8]=="1" and all(abs(float(stored[i])-float(match[i+1]))<=1e-7 for i in range(5)) and bool(stored[6])
                reconciliation["okx_closed_candle"]=match
            else:checks["dw_stream_record_matches_gold"]=stored is not None and abs(float(stored[3])-float(candle.close))<=1e-7
            cur.execute("""SELECT p.prediction_id,p.current_price,p.actual_future_price,p.actual_direction,p.is_correct,
                                  p.predicted_direction,c.close,c.source,c.is_closed
                FROM warehouse.fact_btc_predictions p JOIN warehouse.fact_crypto_1m_candles c
                  ON c.symbol=p.symbol AND c.window_start=p.target_timestamp
                WHERE p.actual_direction IS NOT NULL AND p.prediction_timestamp>='2026-11-09T08:28:00Z'
                ORDER BY p.feature_timestamp DESC LIMIT 1""")
            evaluated=cur.fetchone()
            checks["prediction_has_real_closed_target"]=bool(evaluated and evaluated[7]=="historical_rest" and evaluated[8])
            checks["prediction_outcome_matches_dw_price"]=bool(evaluated and evaluated[3]==int(float(evaluated[6])>float(evaluated[1])) and abs(float(evaluated[2])-float(evaluated[6]))<=1e-7 and evaluated[4]==(evaluated[3]==evaluated[5]))
            cur.execute("SELECT COUNT(*)-COUNT(DISTINCT (symbol,window_start)) FROM warehouse.fact_crypto_1m_candles")
            checks["no_duplicate_dw_candle_keys"]=cur.fetchone()[0]==0
    conn.close()
    statuses={}
    for route in ["/api/health","/api/system/status","/api/ml/review","/api/ml/history?page=1&page_size=10","/api/forecast/long-term","/api/forecast/long-term/research","/static/vendor/lightweight-charts-4.1.3.js"]:
        response=requests.get("http://127.0.0.1:5001"+route,timeout=10);statuses[route]=response.status_code
    checks["all_required_http_endpoints_available"]=all(code==200 for code in statuses.values())
    checks["frozen_canonical_still_matches_batch"]=hashlib.sha256((ROOT/"configs/canonical_snapshot.json").read_bytes()).hexdigest()==manifest["canonical_pointer_sha256"]
    trace=dict(trade_id=str(trade.trade_id),topic=topic,kafka_partition=int(selected.kafka_partition),kafka_offset=int(selected.kafka_offset),
        raw_source_object=str(selected["_source_object"]),raw_source_sha256=str(selected["_source_sha256"]),gold_minute=minute.isoformat(),
        captured_trades_in_minute=len(minute_trades),evaluated_prediction_id=evaluated[0] if evaluated else None)
    result=dict(checked_at=datetime.now(timezone.utc).isoformat(),batch_run_id=manifest["run_id"],checks=checks,passed=all(checks.values()),
        trace=trace,reconciliation=reconciliation,http_status=statuses,silver_files=silver_files,gold_files=gold_files,
        limitations="Trace proves the captured Kafka sample and published batch, not completeness of all OKX trades or exchange events during downtime.")
    atomic_json(output/"pipeline-audit.json",result)
    print(json.dumps(result,ensure_ascii=False,indent=2),flush=True)
    if not result["passed"]:raise SystemExit(1)


if __name__=="__main__":main()
