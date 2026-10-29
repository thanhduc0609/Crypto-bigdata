"""
=============================================================================
Crypto Big Data - Batch Layer: Silver to Gold 1m OHLCV Aggregation
=============================================================================
Aggregates validated Silver BTC trade events into 1-minute Candlesticks:
  - Exact event-time ordering for Open and Close prices
  - High, Low, Close, Volume, Trade Count, Total Trade Value
  - Buy Volume, Sell Volume, Volume-Weighted Average Price (VWAP)
  - Order Flow Trade Imbalance
Saves output to Gold Parquet Lakehouse and PostgreSQL fact_crypto_1m_candles.
=============================================================================
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    count as spark_count,
    when,
    expr,
    lit
)

from processing.core.spark_session import create_spark_session, load_env_vars
from processing.core.config import get_pipeline_config
from storage.s3 import get_silver_trades_path, get_gold_candles_path
from warehouse.etl.streaming_sink import write_candles_batch

load_env_vars()
pipeline_cfg = get_pipeline_config()


def process_silver_to_gold(
    spark: SparkSession,
    symbol: str = "BTC-USDT",
    sync_to_postgres: bool = True
) -> Dict[str, Any]:
    """
    Transforms Silver trades into 1-minute Gold OHLCV + microstructure candles.
    """
    silver_path = get_silver_trades_path(symbol)
    gold_path = get_gold_candles_path(symbol)

    print("=" * 80)
    print("MEDALLION BATCH LAYER: SILVER -> GOLD 1M OHLCV AGGREGATION")
    print(f"Symbol      : {symbol}")
    print(f"Silver Path : {silver_path}")
    print(f"Gold Path   : {gold_path}")
    print("=" * 80)

    try:
        trades_df = spark.read.parquet(silver_path)
    except Exception as e:
        print(f"[!] Warning: Could not read Silver path ({e}). Returning empty result.")
        return {"candles_count": 0, "status": "no_data"}

    if symbol:
        trades_df = trades_df.filter(col("symbol") == symbol)

    if trades_df.count() == 0:
        print("[!] No trades found in Silver layer to aggregate.")
        return {"candles_count": 0, "status": "empty"}

    # Event-time aggregation with tumbling 1-minute window
    # In PySpark 3.5+, expr("min_by(price, raw_timestamp)") guarantees exact event-time Open/Close
    candles_df = (
        trades_df
        .groupBy(
            window(col("trade_time"), "1 minute"),
            col("symbol")
        )
        .agg(
            expr("min_by(price, raw_timestamp)").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            expr("max_by(price, raw_timestamp)").alias("close"),
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
            lit("realtime_stream").alias("source")
        )
        .orderBy("window_start")
    )

    candle_count = candles_df.count()

    # 1. Write to Gold Parquet
    (
        candles_df.write
        .mode("append")
        .partitionBy("symbol")
        .parquet(gold_path)
    )

    print(f"[SUCCESS] Gold 1-minute candles generated: {candle_count:,} candles written to {gold_path}")

    # 2. Sync to PostgreSQL Warehouse if requested
    if sync_to_postgres and candle_count > 0:
        print("[*] Synchronizing Gold candles to PostgreSQL warehouse.fact_crypto_1m_candles...")
        try:
            write_candles_batch(candles_df, batch_id=0)
            print("[OK] Successfully synchronized to PostgreSQL Data Warehouse.")
        except Exception as e:
            print(f"[!] Warning syncing to Postgres: {e}")

    return {
        "candles_count": candle_count,
        "status": "success",
        "gold_path": gold_path
    }


if __name__ == "__main__":
    spark = create_spark_session(app_name="SilverToGoldBatch", include_kafka=False)
    try:
        process_silver_to_gold(spark)
    finally:
        spark.stop()
