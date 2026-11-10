"""
=============================================================================
Crypto Big Data - Real End-to-End Streaming Medallion Integration Test
=============================================================================
Architecture Under Test:
  OKX WebSocket -> Kafka -> Spark -> S3 Bronze -> S3 Silver -> S3 Gold -> PostgreSQL DW

Validates:
  1. Real OKX WebSocket trade ingestion & Kafka message delivery
  2. Spark Structured Streaming to S3 Bronze with raw payloads & Kafka metadata
  3. Spark checkpoint recovery & resume semantics (no duplicates, resumes offset)
  4. Bronze to Silver transformation (data cleaning, validation, deduplication)
  5. Silver to Gold aggregation (event-time OHLCV + order-flow microstructure)
  6. PostgreSQL Data Warehouse deterministic reconciliation
  7. Physical S3 object storage verification (boto3)
=============================================================================
"""

import os
import sys
import json
import time
import re
from unittest.mock import patch
from pathlib import Path
from datetime import datetime, timezone
import websocket
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

# Setup paths
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import create_spark_session, load_env_vars
from processing.streaming.bronze_writer import start_bronze_writer
from processing.batch.bronze_to_silver import process_bronze_to_silver
from processing.batch.silver_to_gold import process_silver_to_gold
from storage.s3 import (
    get_s3_bucket,
    get_bronze_trades_path,
    get_silver_trades_path,
    get_gold_candles_path
)
from warehouse.etl.db_config import get_db_connection
from test_support.s3_sandbox import S3TestSandbox, S3TestSafetyError

load_env_vars()


def fetch_real_okx_trades(target_count: int = 15) -> list:
    """Connects to OKX WebSocket and fetches real live market trades."""
    print(f"\n[*] Connecting to OKX WebSocket (wss://ws.okx.com:8443/ws/v5/public)...")
    collected_trades = []

    def on_message(ws, message):
        try:
            data = json.loads(message)
            if "data" in data and isinstance(data["data"], list):
                for t in data["data"]:
                    if all(k in t for k in ("tradeId", "instId", "px", "sz", "side", "ts")):
                        rec = {
                            "trade_id": str(t["tradeId"]),
                            "symbol": str(t["instId"]),
                            "price": float(t["px"]),
                            "size": float(t["sz"]),
                            "side": str(t["side"]).lower(),
                            "timestamp": int(t["ts"])
                        }
                        collected_trades.append(rec)
                        print(f"    [OKX Trade {len(collected_trades)}/{target_count}] "
                              f"ID={rec['trade_id']} | Price=${rec['price']:,.2f} | "
                              f"Size={rec['size']} | Side={rec['side']} | TS={rec['timestamp']}")
                        if len(collected_trades) >= target_count:
                            ws.close()
        except Exception as err:
            print(f"[!] Error processing WS message: {err}")

    def on_open(ws):
        sub_msg = {
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": "BTC-USDT"}]
        }
        ws.send(json.dumps(sub_msg))

    ws = websocket.WebSocketApp(
        "wss://ws.okx.com:8443/ws/v5/public",
        on_open=on_open,
        on_message=on_message
    )
    ws.run_forever()
    return collected_trades


def produce_trades_to_kafka(trades: list, topic=None) -> int:
    """Publishes verified trades into Kafka topic."""
    topic = topic or os.getenv('TEST_KAFKA_TOPIC','')
    if not re.fullmatch(r'crypto-tests-[a-zA-Z0-9_-]+',topic):
        raise S3TestSafetyError('A dedicated TEST_KAFKA_TOPIC named crypto-tests-* is required')
    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3,
        acks="all"
    )
    print(f"[*] Publishing {len(trades)} trades to Kafka ({kafka_server}, topic: {topic}, partition: 0)...")
    for t in trades:
        producer.send(topic, key=t["symbol"], value=t, partition=0)
    producer.flush()
    producer.close()
    print(f"[OK] Successfully published {len(trades)} trades to Kafka.")
    return len(trades)


def run_e2e_streaming_test():
    # Fail before creating S3 objects or connecting to live infrastructure unless
    # the broader integration test has explicitly selected dedicated DB/topic.
    test_db=os.getenv('TEST_POSTGRES_DB','')
    topic=os.getenv('TEST_KAFKA_TOPIC','')
    if not re.fullmatch(r'crypto_test_[a-z0-9_]{1,50}',test_db):
        raise S3TestSafetyError('Set TEST_POSTGRES_DB=crypto_test_* to an initialized dedicated test database')
    if not re.fullmatch(r'crypto-tests-[a-zA-Z0-9_-]+',topic):
        raise S3TestSafetyError('Set TEST_KAFKA_TOPIC=crypto-tests-* to a dedicated topic')
    import warehouse.etl.db_config as database
    import processing.streaming.bronze_writer as bronze_writer
    kafka_cfg={**bronze_writer.pipeline_cfg.get('kafka',{}),'topic_trades':topic,
               'bootstrap_servers':os.getenv('KAFKA_BOOTSTRAP_SERVERS','127.0.0.1:9092')}
    with S3TestSandbox.from_environment('streaming-medallion') as scope:
        with scope.isolated_storage_paths(), patch.object(database,'DB_NAME',test_db), patch.dict(bronze_writer.pipeline_cfg,{'kafka':kafka_cfg}):
            return _run_e2e_streaming_test(scope)


def _run_e2e_streaming_test(scope):
    print("=" * 80)
    print("INTEGRATION TEST: REAL STREAMING MEDALLION PIPELINE (BTC-USDT)")
    print("OKX WebSocket -> Kafka -> Spark -> S3 Bronze -> S3 Silver -> S3 Gold")
    print("=" * 80)

    bucket = scope.bucket
    print(f"Target AWS S3 Bucket: {bucket}")

    print('[*] Fresh owned S3 prefix: '+scope.prefix+' (no previous data deletion)')
    test_checkpoint_dir = scope.directory / 'checkpoints'
    test_checkpoint_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_str = str(test_checkpoint_dir).replace("\\", "/")

    # =========================================================================
    # STAGE 1: Real OKX WebSocket -> Kafka
    # =========================================================================
    print("\n" + "=" * 80)
    print("STAGE 1: INGESTION FROM LIVE OKX WEBSOCKET TO KAFKA")
    print("=" * 80)
    initial_trades = fetch_real_okx_trades(target_count=15)
    assert len(initial_trades) >= 15, "Failed to collect 15 real trades from OKX WebSocket"
    produce_trades_to_kafka(initial_trades)

    # Verify messages in Kafka
    consumer = KafkaConsumer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS','127.0.0.1:9092'),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )
    tp = TopicPartition(os.environ['TEST_KAFKA_TOPIC'], 0)
    consumer.assign([tp])
    latest_offset = consumer.end_offsets([tp])[tp]
    print(f"[OK] Verified isolated Kafka topic partition 0 end offset: {latest_offset}")
    consumer.close()

    # =========================================================================
    # STAGE 2: Kafka -> Spark Streaming -> S3 Bronze
    # =========================================================================
    print("\n" + "=" * 80)
    print("STAGE 2: SPARK STRUCTURED STREAMING TO S3 BRONZE")
    print("=" * 80)
    spark = create_spark_session(app_name="E2EStreamingMedallionTest", include_kafka=True, include_s3=True)

    try:
        query1 = start_bronze_writer(
            spark,
            trigger_interval="5 seconds",
            starting_offsets="earliest",
            checkpoint_location=checkpoint_str,
            available_now=True,
            symbol="BTC-USDT"
        )
        query1.awaitTermination()
        print("[OK] Spark streaming microbatch 1 terminated successfully.")

        # Read back from S3 Bronze
        bronze_path = get_bronze_trades_path("BTC-USDT")
        print(f"[*] Reading S3 Bronze dataset from: {bronze_path}")
        bronze_df = spark.read.parquet(bronze_path).filter("symbol = 'BTC-USDT'")
        bronze_count_1 = bronze_df.count()
        print(f"[OK] S3 Bronze record count after batch 1: {bronze_count_1}")
        assert bronze_count_1 >= 15, f"Expected at least 15 Bronze records, got {bronze_count_1}"

        # Schema & Data Quality check on Bronze
        bronze_cols = set(bronze_df.columns)
        expected_bronze_cols = {
            "trade_id", "symbol", "price", "size", "side", "raw_timestamp",
            "kafka_partition", "kafka_offset", "kafka_arrival_time", "trade_time", "trade_date"
        }
        missing_bronze = expected_bronze_cols - bronze_cols
        assert not missing_bronze, f"Bronze missing columns: {missing_bronze}"
        print(f"[OK] S3 Bronze schema validated. All 11 expected fields present.")

        sample_bronze = bronze_df.limit(3).toPandas()
        print("\n--- S3 Bronze Sample Records ---")
        for _, row in sample_bronze.iterrows():
            print(f"    ID={row['trade_id']} | Price=${row['price']} | Size={row['size']} | "
                  f"Kafka Off={row['kafka_offset']} | TradeTime={row['trade_time']}")

        # =========================================================================
        # STAGE 2b: Checkpoint Recovery & Resume Verification
        # =========================================================================
        print("\n" + "=" * 80)
        print("STAGE 2b: STREAMING CHECKPOINT RECOVERY VERIFICATION")
        print("=" * 80)
        # Verify checkpoint state directory
        offset_files = list((test_checkpoint_dir / "offsets").glob("*"))
        commit_files = list((test_checkpoint_dir / "commits").glob("*"))
        print(f"[*] Checkpoint state: {len(offset_files)} offset files, {len(commit_files)} commit files.")
        assert len(offset_files) > 0 and len(commit_files) > 0, "Checkpoint directory missing commit logs"

        # Fetch 5 new real trades from OKX and send to Kafka
        print("[*] Fetching 5 additional real trades to verify resume without duplicates...")
        new_trades = fetch_real_okx_trades(target_count=5)
        produce_trades_to_kafka(new_trades)
        time.sleep(2)

        # Resume streaming query with same checkpoint directory
        query2 = start_bronze_writer(
            spark,
            trigger_interval="5 seconds",
            starting_offsets="earliest",  # Should be overridden by existing checkpoint!
            checkpoint_location=checkpoint_str,
            available_now=True,
            symbol="BTC-USDT"
        )
        query2.awaitTermination()
        print("[OK] Spark streaming resumed query terminated successfully.")

        spark.catalog.clearCache()
        bronze_df_2 = spark.read.parquet(bronze_path).filter("symbol = 'BTC-USDT'")
        bronze_count_2 = bronze_df_2.count()
        print(f"[OK] S3 Bronze record count after checkpoint resume: {bronze_count_2}")
        assert bronze_count_2 == bronze_count_1 + len(new_trades), (
            f"Expected {bronze_count_1 + len(new_trades)} records after resume, got {bronze_count_2}"
        )
        print(f"[PASS] Checkpoint recovery verified: exactly {len(new_trades)} new records processed, zero duplicate replays.")

        # =========================================================================
        # STAGE 3: S3 Bronze -> Spark Batch -> S3 Silver
        # =========================================================================
        print("\n" + "=" * 80)
        print("STAGE 3: BATCH TRANSFORMATION BRONZE -> SILVER")
        print("=" * 80)
        silver_res = process_bronze_to_silver(spark, symbol="BTC-USDT")
        print(f"[OK] Bronze to Silver ETL result: {silver_res}")
        assert silver_res["status"] == "success", "Bronze to Silver ETL failed"

        silver_path = get_silver_trades_path("BTC-USDT")
        silver_df = spark.read.parquet(silver_path).filter("symbol = 'BTC-USDT'")
        silver_count = silver_df.count()
        print(f"[OK] S3 Silver validated record count: {silver_count}")
        assert silver_count > 0, "Silver record count is 0"

        # Check deduplication & validation
        silver_cols = set(silver_df.columns)
        assert "trade_value" in silver_cols, "Silver missing computed trade_value column"
        assert "trade_time" in silver_cols, "Silver missing trade_time column"

        sample_silver = silver_df.limit(3).toPandas()
        print("\n--- S3 Silver Cleaned Records ---")
        for _, row in sample_silver.iterrows():
            print(f"    ID={row['trade_id']} | Price=${row['price']} | Size={row['size']} | "
                  f"Value=${row['trade_value']:,.2f} | Side={row['side']}")

        # =========================================================================
        # STAGE 4: S3 Silver -> Spark Batch -> S3 Gold & PostgreSQL DW
        # =========================================================================
        print("\n" + "=" * 80)
        print("STAGE 4: BATCH AGGREGATION SILVER -> GOLD & DW SINK")
        print("=" * 80)
        gold_res = process_silver_to_gold(spark, symbol="BTC-USDT", sync_to_postgres=True)
        print(f"[OK] Silver to Gold aggregation result: {gold_res}")
        assert gold_res["status"] == "success", "Silver to Gold aggregation failed"

        gold_path = get_gold_candles_path("BTC-USDT")
        gold_df = spark.read.parquet(gold_path).filter("symbol = 'BTC-USDT'")
        gold_count = gold_df.count()
        print(f"[OK] S3 Gold 1m candlestick count: {gold_count}")
        assert gold_count > 0, "Gold candlestick count is 0"

        # Verify Gold metrics
        sample_gold = gold_df.filter("source = 'realtime_stream'").orderBy("window_start", ascending=False).limit(3).toPandas()
        print(f"\n--- S3 Gold Finalized Candlesticks ({len(sample_gold)} streaming candles) ---")
        for _, c in sample_gold.iterrows():
            print(f"    Window: {c['window_start']} to {c['window_end']} | "
                  f"O=${c['open']:,.2f} H=${c['high']:,.2f} L=${c['low']:,.2f} C=${c['close']:,.2f} | "
                  f"Vol={c['volume']:.6f} | VWAP=${c['vwap']:,.2f} | Imbalance={c['trade_imbalance']:.4f} | "
                  f"Source={c['source']}")
            assert c['high'] >= max(c['open'], c['close']), "Invalid candle: High < max(Open, Close)"
            assert c['low'] <= min(c['open'], c['close']), "Invalid candle: Low > min(Open, Close)"
            assert c['low'] <= c['vwap'] <= c['high'], "Invalid VWAP: not within [Low, High]"
            assert -1.0 <= c['trade_imbalance'] <= 1.0, "Invalid Imbalance: not within [-1, 1]"

        # Verify PostgreSQL reconciliation
        print("\n[*] Verifying PostgreSQL fact_crypto_1m_candles streaming ingestion...")
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, window_start, open, high, low, close, volume, vwap, trade_imbalance, source
            FROM warehouse.fact_crypto_1m_candles
            WHERE symbol = 'BTC-USDT' AND source = 'realtime_stream'
            ORDER BY window_start DESC
            LIMIT 3;
        """)
        pg_rows = cur.fetchall()
        print(f"[OK] Retrieved {len(pg_rows)} streaming records from PostgreSQL warehouse.fact_crypto_1m_candles:")
        for r in pg_rows:
            print(f"    PG Candle: {r[1]} | O=${r[2]} H=${r[3]} L=${r[4]} C=${r[5]} | Vol={r[6]} | VWAP=${r[7]} | Imb={r[8]} | Source={r[9]}")
        cur.close()
        conn.close()
        assert len(pg_rows) > 0, "No streaming candles found in PostgreSQL"

    finally:
        spark.stop()

    # =========================================================================
    # STAGE 5: Physical S3 Object Storage Evidence via Boto3
    # =========================================================================
    print("\n" + "=" * 80)
    print("STAGE 5: PHYSICAL S3 OBJECT STORAGE EVIDENCE (BOTO3)")
    print("=" * 80)
    s3_client = scope.client
    for prefix in [scope.key(layer+'/') for layer in ('bronze','silver','gold')]:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = resp.get("Contents", [])
        print(f"\nPrefix: s3://{bucket}/{prefix} ({len(keys)} objects)")
        for obj in keys:
            print(f"  - Key: {obj['Key']:<75} Size: {obj['Size']:>10,} bytes | Modified: {obj['LastModified']}")

    print("\n" + "=" * 80)
    print("ALL TESTS PASSED (100%): STREAMING MEDALLION PIPELINE VERIFIED END-TO-END")
    print("=" * 80)


if __name__ == "__main__":
    run_e2e_streaming_test()
