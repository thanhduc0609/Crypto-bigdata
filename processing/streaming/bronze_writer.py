"""
=============================================================================
Crypto Big Data - Bronze Layer: Raw Kafka Trade Stream to Storage
=============================================================================
Consumes real-time BTC-USDT trades from Apache Kafka and appends them
into the Medallion Data Lake Bronze layer as Parquet with raw payloads
and Kafka metadata (partition, offset, arrival timestamp).
=============================================================================
"""

import os
import sys
from pathlib import Path
from typing import Optional

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
    date_format
)

from processing.core.schemas import RAW_TRADE_SCHEMA
from processing.core.spark_session import create_spark_session, load_env_vars
from processing.core.config import get_pipeline_config
from storage.s3 import get_bronze_trades_path, get_checkpoint_path

load_env_vars()
pipeline_cfg = get_pipeline_config()


def start_bronze_writer(
    spark: SparkSession,
    trigger_interval: str = "10 seconds",
    starting_offsets: str = "latest",
    checkpoint_location: Optional[str] = None,
    available_now: bool = False,
    symbol: Optional[str] = None
):
    """Starts the PySpark Structured Streaming job writing Kafka raw trades to Bronze."""
    kafka_server = pipeline_cfg.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
    topic = pipeline_cfg.get("kafka", {}).get("topic_trades", "crypto-trades")
    target_symbol = symbol or pipeline_cfg.get("pipeline", {}).get("target_symbol", "BTC-USDT")
    bronze_path = get_bronze_trades_path(target_symbol)
    checkpoint_path = checkpoint_location or get_checkpoint_path("bronze_trades")

    print("=" * 80)
    print("MEDALLION DATA LAKE - BRONZE LAYER (RAW TRADES)")
    print(f"Target Symbol   : {target_symbol}")
    print(f"Kafka Bootstrap : {kafka_server}")
    print(f"Kafka Topic     : {topic}")
    print(f"Starting Offsets: {starting_offsets}")
    print(f"Bronze S3 Path  : {bronze_path}")
    print(f"Checkpoint Path : {checkpoint_path}")
    print("=" * 80)

    # 1. Read Kafka raw byte stream
    raw_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Extract JSON payload and Kafka metadata
    bronze_df = (
        raw_kafka_df
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
        .filter(col("symbol") == target_symbol)
        .withColumn("trade_time", (col("raw_timestamp") / 1000.0).cast("timestamp"))
        .withColumn("trade_date", date_format(col("trade_time"), "yyyy-MM-dd"))
    )

    # 3. Append to Bronze Parquet
    writer = (
        bronze_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", bronze_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("symbol", "trade_date")
    )
    if available_now:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime=trigger_interval).start()

    print(f"[OK] Bronze Writer actively streaming into {bronze_path}")
    return query


if __name__ == "__main__":
    spark = create_spark_session(app_name="BronzeTradeWriter")
    try:
        q = start_bronze_writer(spark)
        q.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping Bronze writer...")
    finally:
        spark.stop()
