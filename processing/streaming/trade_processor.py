"""
=============================================================================
Crypto Big Data Streaming - Spark Clean, Transform, Whale Alert & Lakehouse
=============================================================================
Architecture:
  OKX WebSocket -> Kafka -> Spark Structured Streaming -> Data Cleaning 
  -> Real-time Whale Alerts & Fact Trades Sink (>= $10,000)
  -> 1m Tumbling Window Aggregations (OHLC, VWAP, Imbalance) 
  -> S3 / Local Parquet Lakehouse & PostgreSQL Data Warehouse
=============================================================================
"""

import os
import sys
from pathlib import Path

# Enable unbuffered stdout and stderr for instant real-time console streaming
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)

# Setup paths
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    date_format,
    window,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    count as spark_count,
    when,
    expr,
    lit
)

from processing.core.schemas import RAW_TRADE_SCHEMA
from processing.core.spark_session import create_spark_session, load_env_vars
from storage.s3 import (
    get_storage_mode,
    get_s3_bucket,
    get_bronze_trades_path,
    get_gold_candles_path,
    get_speed_candles_path,
    get_checkpoint_path
)
from warehouse.etl.streaming_sink import write_whales_batch, write_candles_batch

load_env_vars()

# =============================================================================
# CONFIGURATION
# =============================================================================
TARGET_SYMBOL = "BTC-USDT"
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRADES", os.getenv("KAFKA_TOPIC", "crypto-trades"))

S3_BUCKET = get_s3_bucket()
STORAGE_DESTINATION = get_storage_mode()

S3_RAW_PATH = get_bronze_trades_path(TARGET_SYMBOL)
S3_SPEED_PATH = get_speed_candles_path(TARGET_SYMBOL)

# Checkpoint Configuration (defaults to local for zero latency on Windows)
USE_S3_CHECKPOINTS = os.getenv("USE_S3_CHECKPOINTS", "false").lower() in ("true", "1")
checkpoint_base = project_root / "data" / "checkpoints"
checkpoint_base.mkdir(parents=True, exist_ok=True)

if USE_S3_CHECKPOINTS and STORAGE_DESTINATION == "s3":
    CHECKPOINT_RAW = f"s3a://{S3_BUCKET}/checkpoints/trades_raw"
    CHECKPOINT_PROCESSED = f"s3a://{S3_BUCKET}/checkpoints/trades_1m"
else:
    CHECKPOINT_RAW = str(checkpoint_base / "trades_raw").replace("\\", "/")
    CHECKPOINT_PROCESSED = str(checkpoint_base / "trades_1m").replace("\\", "/")

CHECKPOINT_WHALE = str(checkpoint_base / "whale_alerts").replace("\\", "/")
CHECKPOINT_DW_WHALES = str(checkpoint_base / "dw_whales").replace("\\", "/")
CHECKPOINT_DW_CANDLES = str(checkpoint_base / "dw_candles").replace("\\", "/")

ENABLE_DW_STREAMING = os.getenv("ENABLE_DW_STREAMING", "true").lower() in ("true", "1")

WATERMARK_DELAY = os.getenv("STREAMING_WATERMARK_DELAY", "2 minutes")
WINDOW_DURATION = os.getenv("STREAMING_WINDOW_DURATION", "1 minute")
TRIGGER_INTERVAL = os.getenv("STREAMING_TRIGGER_INTERVAL", "10 seconds")

# Large trade storage threshold (fact_crypto_trades >= $10k) and console alert (>= $500k)
LARGE_TRADE_STORAGE_THRESHOLD = 10000.0
WHALE_CONSOLE_ALERT_THRESHOLD = float(os.getenv("WHALE_ALERT_THRESHOLD_USD", "500000.0"))


def process_crypto_streams(spark: SparkSession):
    print("=" * 80)
    print("CRYPTO BIG DATA STREAMING - SPEED LAYER (CLEAN, AGGREGATE & WHALE ALERT)")
    print(f"Spark Version        : {spark.version}")
    print(f"Target Symbol        : {TARGET_SYMBOL}")
    print(f"Kafka Bootstrap      : {KAFKA_SERVER}")
    print(f"Kafka Topic          : {KAFKA_TOPIC}")
    print(f"Console Alert (> USD): ${WHALE_CONSOLE_ALERT_THRESHOLD:,.2f}")
    print(f"DW Storage (> USD)   : ${LARGE_TRADE_STORAGE_THRESHOLD:,.2f}")
    print(f"Storage Destination  : {STORAGE_DESTINATION.upper()}")
    print("=" * 80)

    # 1. INGESTION: Read raw Kafka stream
    kafka_raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. PARSE JSON & EXTRACT KAFKA TELEMETRY
    parsed_stream = (
        kafka_raw_stream
        .select(
            from_json(col("value").cast("string"), RAW_TRADE_SCHEMA).alias("trade"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_arrival_time")
        )
        .select(
            col("trade.trade_id").alias("trade_id"),
            col("trade.symbol").alias("symbol"),
            col("trade.price").alias("price"),
            col("trade.size").alias("size"),
            col("trade.side").alias("side"),
            col("trade.timestamp").alias("raw_timestamp"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_arrival_time")
        )
    )

    # 3. DATA CLEANING & WATERMARKING
    # Preserves millisecond precision via cast to timestamp; strictly filters BTC-USDT
    cleaned_trades_stream = (
        parsed_stream
        .filter(
            col("trade_id").isNotNull() &
            col("symbol").isNotNull() &
            (col("symbol") == TARGET_SYMBOL) &
            col("price").isNotNull() & (col("price") > 0.0) &
            col("size").isNotNull() & (col("size") > 0.0) &
            col("raw_timestamp").isNotNull() & (col("raw_timestamp") > 0) &
            col("side").isin("buy", "sell")
        )
        .withColumn("trade_time", (col("raw_timestamp") / 1000.0).cast("timestamp"))
        .withColumn("trade_value", col("price") * col("size"))
        .withColumn("trade_date", date_format(col("trade_time"), "yyyy-MM-dd"))
        .withWatermark("trade_time", WATERMARK_DELAY)
        .dropDuplicates(["trade_id", "trade_time"])
    )

    # 4. REAL-TIME WHALE & LARGE TRADE DETECTION
    # Persist all high-value trades >= $10,000 into DW; classify by tier
    large_trades_stream = (
        cleaned_trades_stream
        .filter(col("trade_value") >= LARGE_TRADE_STORAGE_THRESHOLD)
        .withColumn(
            "whale_tier",
            when(col("trade_value") >= 500000.0, "Mega Whale (>= $500K)")
            .when(col("trade_value") >= 100000.0, "Big Whale ($100K - $500K)")
            .when(col("trade_value") >= 50000.0, "Whale ($50K - $100K)")
            .otherwise("Large Order ($10K - $50K)")
        )
    )

    # High-priority alerts to console (Mega Whales >= $500,000)
    whale_console_stream = (
        large_trades_stream
        .filter(col("trade_value") >= WHALE_CONSOLE_ALERT_THRESHOLD)
        .select(
            col("trade_time"),
            col("symbol"),
            col("side"),
            col("price"),
            col("size"),
            col("trade_value"),
            col("whale_tier")
        )
    )

    # 5. 1-MINUTE CANDLESTICK AGGREGATIONS (NON-OVERLAPPING TUMBLING WINDOW)
    # Open/Close ordered strictly by (raw_timestamp, trade_id) for deterministic tie-break
    agg_1m_stream = (
        cleaned_trades_stream
        .groupBy(
            window(col("trade_time"), WINDOW_DURATION),
            col("symbol")
        )
        .agg(
            expr("min_by(price, struct(raw_timestamp, trade_id))").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            expr("max_by(price, struct(raw_timestamp, trade_id))").alias("close"),
            spark_sum("size").alias("volume"),
            spark_count("trade_id").alias("trade_count"),
            spark_sum("trade_value").alias("total_trade_value"),
            spark_sum(when(col("side") == "buy", col("size")).otherwise(0.0)).alias("buy_volume"),
            spark_sum(when(col("side") == "sell", col("size")).otherwise(0.0)).alias("sell_volume"),
            (spark_sum(col("price") * col("size")) / spark_sum("size")).alias("vwap")
        )
        .withColumn(
            "trade_imbalance",
            when(
                (col("buy_volume") + col("sell_volume")) > 0,
                (col("buy_volume") - col("sell_volume")) / (col("buy_volume") + col("sell_volume"))
            ).otherwise(0.0)
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("trade_count"),
            col("buy_volume"),
            col("sell_volume"),
            col("vwap"),
            col("trade_imbalance"),
            col("total_trade_value"),
            lit("realtime_stream").alias("source"),
            when(col("window.end") <= expr("current_timestamp()"), lit(True)).otherwise(lit(False)).alias("is_closed")
        )
    )

    queries = []

    # OUTPUT: Console Whale Alerts
    print("[OUTPUT] Activating Real-time Whale Console Alert Stream...")
    whale_query = (
        whale_console_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_WHALE)
        .start()
    )
    queries.append(whale_query)

    # OUTPUT: PostgreSQL Data Warehouse Sink
    if ENABLE_DW_STREAMING:
        print("[OUTPUT] Activating PostgreSQL Data Warehouse Sinks (fact_crypto_trades & fact_crypto_1m_candles)...")
        dw_whales_query = (
            large_trades_stream.writeStream
            .outputMode("append")
            .foreachBatch(write_whales_batch)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .option("checkpointLocation", CHECKPOINT_DW_WHALES)
            .start()
        )
        queries.append(dw_whales_query)

        dw_candles_query = (
            agg_1m_stream.writeStream
            .outputMode("update")
            .foreachBatch(write_candles_batch)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .option("checkpointLocation", CHECKPOINT_DW_CANDLES)
            .start()
        )
        queries.append(dw_candles_query)

    # OUTPUT: S3 / Local Parquet Lakehouse Sink
    if STORAGE_DESTINATION in ["s3", "local"]:
        print(f"[OUTPUT] Writing Raw Cleaned Stream -> Lakehouse: {S3_RAW_PATH}")
        raw_s3_query = (
            cleaned_trades_stream.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", S3_RAW_PATH)
            .option("checkpointLocation", CHECKPOINT_RAW)
            .partitionBy("symbol", "trade_date")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        queries.append(raw_s3_query)

        print(f"[OUTPUT] Writing Auxiliary Speed Layer 1m Stream -> Lakehouse: {S3_SPEED_PATH}")
        processed_s3_query = (
            agg_1m_stream.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", S3_SPEED_PATH)
            .option("checkpointLocation", CHECKPOINT_PROCESSED)
            .partitionBy("symbol")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        queries.append(processed_s3_query)

    print("\n>>> Speed Layer is actively streaming. Press CTRL + C to stop.\n")
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    spark = None
    try:
        spark = create_spark_session(app_name="CryptoStreamSpeedLayer")
        process_crypto_streams(spark)
    except KeyboardInterrupt:
        print("\nStreaming pipeline stopped by user.")
    finally:
        if spark:
            spark.stop()

