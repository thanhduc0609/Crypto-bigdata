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
    date_format
)

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
    bronze_path = get_bronze_trades_path(symbol)
    silver_path = get_silver_trades_path(symbol)

    print("=" * 80)
    print("MEDALLION BATCH LAYER: BRONZE -> SILVER CLEANING")
    print(f"Symbol      : {symbol}")
    print(f"Bronze Path : {bronze_path}")
    print(f"Silver Path : {silver_path}")
    print("=" * 80)

    try:
        raw_df = spark.read.parquet(bronze_path)
    except Exception as e:
        print(f"[!] Warning: Could not read Bronze path ({e}). Returning empty result.")
        return {"total": 0, "valid": 0, "rejected": 0, "status": "no_data"}

    if symbol:
        raw_df = raw_df.filter(col("symbol") == symbol)
    if trade_date:
        raw_df = raw_df.filter(col("trade_date") == trade_date)

    total_records = raw_df.count()
    if total_records == 0:
        print("[!] No records found in Bronze layer to process.")
        return {"total": 0, "valid": 0, "rejected": 0, "status": "empty"}

    # 1. Validation Conditions
    is_valid_condition = (
        col("trade_id").isNotNull() &
        (col("symbol") == symbol) &
        col("price").isNotNull() & (col("price") > 0.0) &
        col("size").isNotNull() & (col("size") > 0.0) &
        col("raw_timestamp").isNotNull() & (col("raw_timestamp") > 0) &
        col("side").isin("buy", "sell")
    )

    valid_df = raw_df.filter(is_valid_condition)
    invalid_df = raw_df.filter(~is_valid_condition)

    rejected_count = invalid_df.count()
    if rejected_count > 0:
        print(f"[WARN] Detected and isolated {rejected_count} invalid records from Bronze.")

    # 2. Standardization & Deduplication
    cleaned_df = (
        valid_df
        .withColumn("trade_time", to_timestamp(from_unixtime(col("raw_timestamp") / 1000)))
        .withColumn("trade_value", col("price") * col("size"))
        .withColumn("trade_date", date_format(col("trade_time"), "yyyy-MM-dd"))
        .dropDuplicates(["trade_id"])
    )

    valid_count = cleaned_df.count()

    # 3. Write to Silver Parquet
    (
        cleaned_df.write
        .mode("append")
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
