"""
=============================================================================
Crypto Big Data Streaming - Spark Clean, Transform, Whale Alert & S3 Pipeline
=============================================================================
Architecture:
  OKX WebSocket -> Kafka -> Spark Structured Streaming -> Data Cleaning 
  -> Real-time Whale Alerts (>= $10,000)
  -> 1m Window Aggregations (OHLC, VWAP, Imbalance) 
  -> AWS S3 (Raw & Processed Data Lake) / HDFS / Console
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
    to_timestamp,
    from_unixtime,
    date_format,
    window,
    first,
    last,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    count as spark_count,
    when
)

from processing.core.schemas import RAW_TRADE_SCHEMA
from processing.core.spark_session import create_spark_session, load_env_vars
from warehouse.etl.streaming_sink import write_whales_batch, write_candles_batch

load_env_vars()

# =============================================================================
# CONFIGURATION
# =============================================================================
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRADES", os.getenv("KAFKA_TOPIC", "crypto-trades"))

STORAGE_DESTINATION = os.getenv("STORAGE_DESTINATION", "s3").lower()
S3_RAW_PATH = os.getenv("S3_STREAMING_RAW_PATH", os.getenv("S3_RAW_PATH", f"s3a://{S3_BUCKET}/crypto/streaming/trades"))
if "${S3_BUCKET_NAME}" in S3_RAW_PATH:
    S3_RAW_PATH = S3_RAW_PATH.replace("${S3_BUCKET_NAME}", S3_BUCKET)

S3_PROCESSED_PATH = os.getenv("S3_STREAMING_CANDLES_PATH", os.getenv("S3_PROCESSED_PATH", f"s3a://{S3_BUCKET}/crypto/streaming/candles_1m"))
if "${S3_BUCKET_NAME}" in S3_PROCESSED_PATH:
    S3_PROCESSED_PATH = S3_PROCESSED_PATH.replace("${S3_BUCKET_NAME}", S3_BUCKET)
# Checkpoint Configuration
# Mặc định sử dụng local checkpoints để đạt tốc độ cao nhất (0ms latency), 
# không tốn chi phí gọi S3 API định kỳ và tránh lỗi atomic rename trên Windows.
USE_S3_CHECKPOINTS = os.getenv("USE_S3_CHECKPOINTS", "false").lower() in ("true", "1")

checkpoint_base = project_root / "data" / "checkpoints"
checkpoint_base.mkdir(parents=True, exist_ok=True)

if USE_S3_CHECKPOINTS:
    CHECKPOINT_RAW = f"s3a://{S3_BUCKET}/checkpoints/trades_raw"
    CHECKPOINT_PROCESSED = f"s3a://{S3_BUCKET}/checkpoints/trades_1m"
else:
    CHECKPOINT_RAW = str(checkpoint_base / "trades_raw").replace("\\", "/")
    CHECKPOINT_PROCESSED = str(checkpoint_base / "trades_1m").replace("\\", "/")

CHECKPOINT_WHALE = str(checkpoint_base / "whale_alerts").replace("\\", "/")
CHECKPOINT_DW_WHALES = str(checkpoint_base / "dw_whales").replace("\\", "/")
CHECKPOINT_DW_CANDLES = str(checkpoint_base / "dw_candles").replace("\\", "/")

ENABLE_DW_STREAMING = os.getenv("ENABLE_DW_STREAMING", "true").lower() in ("true", "1")

HDFS_RAW_PATH = os.getenv("HDFS_RAW_PATH", "hdfs://localhost:9000/crypto/raw/trades")
HDFS_PROCESSED_PATH = os.getenv("HDFS_PROCESSED_PATH", "hdfs://localhost:9000/crypto/processed/trades_1m")
HDFS_CHECKPOINT_RAW = os.getenv("HDFS_CHECKPOINT_RAW", "hdfs://localhost:9000/checkpoints/trades_raw")
HDFS_CHECKPOINT_PROCESSED = os.getenv("HDFS_CHECKPOINT_PROCESSED", "hdfs://localhost:9000/checkpoints/trades_1m")

WATERMARK_DELAY = os.getenv("STREAMING_WATERMARK_DELAY", "2 minutes")
WINDOW_DURATION = os.getenv("STREAMING_WINDOW_DURATION", "1 minute")
TRIGGER_INTERVAL = os.getenv("STREAMING_TRIGGER_INTERVAL", "10 seconds")
WHALE_ALERT_THRESHOLD = float(os.getenv("WHALE_ALERT_THRESHOLD_USD", "500000.0"))


def process_crypto_streams(spark: SparkSession):
    print("=" * 80)
    print("CRYPTO BIG DATA STREAMING - SPEED LAYER (CLEAN, AGGREGATE & WHALE ALERT)")
    print(f"Spark Version        : {spark.version}")
    print(f"Kafka Bootstrap      : {KAFKA_SERVER}")
    print(f"Kafka Topic          : {KAFKA_TOPIC}")
    print(f"Whale Alert (> USD)  : ${WHALE_ALERT_THRESHOLD:,.2f}")
    print(f"Storage Destination  : {STORAGE_DESTINATION.upper()}")
    print("=" * 80)

    # 1. INGESTION: Đọc raw stream từ Kafka
    kafka_raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. PARSE JSON
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
    cleaned_trades_stream = (
        parsed_stream
        .filter(
            col("trade_id").isNotNull() &
            col("symbol").isNotNull() &
            (col("symbol") == "BTC-USDT") &
            col("price").isNotNull() & (col("price") > 0.0) &
            col("size").isNotNull() & (col("size") > 0.0) &
            col("raw_timestamp").isNotNull() & (col("raw_timestamp") > 0) &
            col("side").isin("buy", "sell")
        )
        .withColumn("trade_time", to_timestamp(from_unixtime(col("raw_timestamp") / 1000)))
        .withColumn("trade_value", col("price") * col("size"))
        .withColumn("trade_date", date_format(col("trade_time"), "yyyy-MM-dd"))
        .withWatermark("trade_time", WATERMARK_DELAY)
        .dropDuplicates(["trade_id", "trade_time"])
    )

    # 4. REAL-TIME WHALE ALERT STREAM (Phát hiện giao dịch lớn tức thời)
    whale_trades_stream = (
        cleaned_trades_stream
        .filter(col("trade_value") >= WHALE_ALERT_THRESHOLD)
    )

    whale_stream = (
        whale_trades_stream
        .select(
            col("trade_time"),
            col("symbol"),
            col("side"),
            col("price"),
            col("size"),
            col("trade_value"),
            when(col("trade_value") >= 2000000.0, "🚨 MEGA WHALE (>= $2M)")
            .when(col("trade_value") >= 1000000.0, "⚡ BIG WHALE (>= $1M)")
            .otherwise(f"🐋 WHALE (>= ${int(WHALE_ALERT_THRESHOLD):,})").alias("whale_tier")
        )
    )

    # 5. 1-MINUTE CANDLESTICK & METRIC AGGREGATIONS
    agg_1m_stream = (
        cleaned_trades_stream
        .groupBy(
            window(col("trade_time"), WINDOW_DURATION),
            col("symbol")
        )
        .agg(
            first("price").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            last("price").alias("close"),
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
            col("total_trade_value")
        )
    )

    queries = []

    # OUTPUT: Whale Alerts ra Console
    print("[OUTPUT] Activating Real-time Whale Alert Stream...")
    whale_query = (
        whale_stream.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_WHALE)
        .start()
    )
    queries.append(whale_query)

    # OUTPUT: PostgreSQL Data Warehouse Sink (Chỉ lưu Nến 1m và Cá mập để giữ Postgres luôn nhẹ)
    if ENABLE_DW_STREAMING:
        print("[OUTPUT] Activating Real-time PostgreSQL Data Warehouse Sinks (1m Candles & Whale Alerts)...")
        dw_whales_query = (
            whale_trades_stream.writeStream
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

    # OUTPUT: S3 Data Lake
    if STORAGE_DESTINATION in ["s3", "all"]:
        print(f"[OUTPUT] Writing Raw Cleaned Stream -> S3: {S3_RAW_PATH}")
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

        print(f"[OUTPUT] Writing Processed 1m Aggregations -> S3: {S3_PROCESSED_PATH}")
        processed_s3_query = (
            agg_1m_stream.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", S3_PROCESSED_PATH)
            .option("checkpointLocation", CHECKPOINT_PROCESSED)
            .partitionBy("symbol")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        queries.append(processed_s3_query)

    # OUTPUT: HDFS
    if STORAGE_DESTINATION in ["hdfs", "all"]:
        print(f"[OUTPUT] Writing Raw Cleaned Stream -> HDFS: {HDFS_RAW_PATH}")
        raw_hdfs_query = (
            cleaned_trades_stream.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", HDFS_RAW_PATH)
            .option("checkpointLocation", HDFS_CHECKPOINT_RAW)
            .partitionBy("symbol")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        queries.append(raw_hdfs_query)

        print(f"[OUTPUT] Writing Processed 1m Aggregations -> HDFS: {HDFS_PROCESSED_PATH}")
        processed_hdfs_query = (
            agg_1m_stream.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", HDFS_PROCESSED_PATH)
            .option("checkpointLocation", HDFS_CHECKPOINT_PROCESSED)
            .partitionBy("symbol")
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        queries.append(processed_hdfs_query)

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
