"""
=============================================================================
Crypto Big Data - Batch Layer: Bronze to Silver Transformation
=============================================================================
Transforms Bronze raw trade data into Silver clean data:
  - Validates schema and data types
  - Normalizes timestamps to UTC
  - Filters out corrupt/invalid prices (<= 0) and sizes (<= 0)
  - Eliminates duplicate records (deduplication on trade_id)
  - Computes trade_value = price * size
  - Tracks and isolates invalid records
  - Saves validated BTC trades to Silver Parquet partitioned by symbol & trade_date
=============================================================================
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    from_unixtime,
    date_format,
    isnan,
    from_json,
    trim,
    expr
)

from processing.core.schemas import RAW_TRADE_SCHEMA
from processing.core.spark_session import create_spark_session, load_env_vars
from processing.core.config import get_pipeline_config
from storage.s3 import get_bronze_trades_path, get_silver_trades_path

load_env_vars()
pipeline_cfg = get_pipeline_config()


def process_bronze_to_silver(
    spark: SparkSession,
    symbol: str = "BTC-USDT",
    trade_date: Optional[str] = None
) -> Dict[str, Any]:
    """
    Executes the Bronze to Silver ETL job for BTC trades.
    Returns metrics dictionary with counts of total, valid, and rejected records.
    """
    if symbol != "BTC-USDT":
        raise ValueError(f"Medallion Pipeline strictly processes 'BTC-USDT', but received: {symbol}")

    bronze_path = get_bronze_trades_path(symbol)
    silver_path = get_silver_trades_path(symbol)

    print("=" * 80)
    print("MEDALLION BATCH LAYER: BRONZE -> SILVER CLEANING")
    print(f"Symbol      : {symbol}")
    print(f"Bronze Path : {bronze_path}")
    print(f"Silver Path : {silver_path}")
    print("=" * 80)

    is_local = not bronze_path.startswith("s3a://") and not bronze_path.startswith("s3://")
    if is_local:
        local_p = Path(bronze_path)
        if not local_p.exists() or not any(local_p.rglob("*.parquet")):
            print(f"[INFO] Bronze path does not exist or has no parquet files ({bronze_path}). Initial run.")
            return {"total": 0, "valid": 0, "rejected": 0, "status": "no_data"}
        try:
            raw_df = spark.read.parquet(bronze_path)
        except Exception as read_err:
            print(f"[!] Critical error reading Bronze Parquet at {bronze_path}: {read_err}")
            raise read_err
    else:
        try:
            raw_df = spark.read.parquet(bronze_path)
        except Exception as s3_err:
            err_msg = str(s3_err).lower()
            if "path does not exist" in err_msg or "nosuchkey" in err_msg:
                print(f"[INFO] S3 Bronze path does not exist yet ({bronze_path}). Initial run.")
                return {"total": 0, "valid": 0, "rejected": 0, "status": "no_data"}
            print(f"[!] Critical error reading S3 Bronze at {bronze_path}: {s3_err}")
            raise s3_err

    if trade_date and "trade_date" in raw_df.columns:
        raw_df = raw_df.filter(col("trade_date") == trade_date)

    total_records = raw_df.count()
    if total_records == 0:
        print("[!] No records found in Bronze layer to process.")
        return {"total": 0, "valid": 0, "rejected": 0, "status": "empty"}

    # 1. Parse JSON payload if raw Kafka payload exists, otherwise use tabular columns
    if "raw_payload" in raw_df.columns:
        parsed_df = (
            raw_df
            .withColumn("parsed", from_json(col("raw_payload"), RAW_TRADE_SCHEMA))
            .select(
                col("parsed.trade_id").alias("trade_id"),
                col("parsed.symbol").alias("symbol"),
                col("parsed.price").alias("price"),
                col("parsed.size").alias("size"),
                col("parsed.side").alias("side"),
                col("parsed.timestamp").alias("raw_timestamp"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("kafka_timestamp").alias("kafka_arrival_time")
            )
        )
    else:
        parsed_df = raw_df

    # 2. Strict Silver Validation Conditions (reject NaN, Inf, non-BTC, negative/zero prices, empty trade_ids)
    is_valid_condition = (
        col("trade_id").isNotNull() & (trim(col("trade_id")) != "") &
        col("symbol").isNotNull() & (col("symbol") == "BTC-USDT") &
        col("price").isNotNull() & (~isnan(col("price"))) & (col("price") > 0.0) & (col("price") < float("inf")) &
        col("size").isNotNull() & (~isnan(col("size"))) & (col("size") > 0.0) & (col("size") < float("inf")) &
        col("raw_timestamp").isNotNull() & (col("raw_timestamp") > 0) &
        col("side").isNotNull() & col("side").isin("buy", "sell")
    )

    valid_df = parsed_df.filter(is_valid_condition.isNotNull() & is_valid_condition)
    invalid_df = parsed_df.filter(is_valid_condition.isNull() | (~is_valid_condition))

    rejected_count = invalid_df.count()
    if rejected_count > 0:
        print(f"[WARN] Detected and isolated {rejected_count} invalid records from Bronze (excluded from Silver).")

    # 3. Standardization & Deduplication
    cleaned_df = (
        valid_df
        .withColumn("trade_time", (col("raw_timestamp") / 1000.0).cast("timestamp"))
        .withColumn("trade_value", col("price") * col("size"))
        .withColumn("trade_date", date_format(col("trade_time"), "yyyy-MM-dd"))
        .dropDuplicates(["trade_id"])
    )

    valid_count = cleaned_df.count()

    # 3. Write to Silver Parquet with Dynamic Partition Overwrite (idempotent; only touched partitions replaced)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        cleaned_df.write
        .mode("overwrite")
        .partitionBy("symbol", "trade_date")
        .parquet(silver_path)
    )

    print(f"[SUCCESS] Silver transformation complete:")
    print(f"  - Total Ingested : {total_records:,}")
    print(f"  - Valid Cleaned  : {valid_count:,}")
    print(f"  - Rejected       : {rejected_count:,}")
    print(f"  - Output Path    : {silver_path}")

    return {
        "total": total_records,
        "valid": valid_count,
        "rejected": rejected_count,
        "status": "success"
    }


if __name__ == "__main__":
    spark = create_spark_session(app_name="BronzeToSilverBatch", include_kafka=False)
    try:
        process_bronze_to_silver(spark)
    finally:
        spark.stop()
