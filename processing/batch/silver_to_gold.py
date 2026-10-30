"""
=============================================================================
Crypto Big Data - Batch Layer: Silver to Gold 1m OHLCV Aggregation
=============================================================================
Aggregates validated Silver BTC trade events into 1-minute Candlesticks:
  - Exact event-time ordering for Open and Close prices
  - High, Low, Close, Volume, Trade Count, Total Trade Value
  - Buy Volume, Sell Volume, Volume-Weighted Average Price (VWAP)
  - Order Flow Trade Imbalance
Saves output to Gold Parquet Lakehouse and synchronizes canonical chosen
records to PostgreSQL fact_crypto_1m_candles.
=============================================================================
"""

import os
import sys
import time
import uuid
import shutil
from pathlib import Path
from typing import Dict, Any, Optional

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    window,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    count as spark_count,
    when,
    expr,
    lit,
    row_number
)

from processing.core.spark_session import create_spark_session, load_env_vars
from processing.core.config import get_pipeline_config
from storage.s3 import get_silver_trades_path, get_gold_candles_path
from storage.gold_lock import GoldLock, GoldLockError
from warehouse.etl.streaming_sink import write_candles_batch

load_env_vars()
pipeline_cfg = get_pipeline_config()


class PublishState:
    STAGING_STARTED = "STAGING_STARTED"
    STAGING_COMPLETED = "STAGING_COMPLETED"
    BACKUP_STARTED = "BACKUP_STARTED"
    BACKUP_COMPLETED = "BACKUP_COMPLETED"
    PUBLISH_STARTED = "PUBLISH_STARTED"
    PUBLISH_COMPLETED = "PUBLISH_COMPLETED"
    ROLLBACK_STARTED = "ROLLBACK_STARTED"
    ROLLBACK_COMPLETED = "ROLLBACK_COMPLETED"
    ROLLBACK_FAILED = "ROLLBACK_FAILED"


def process_silver_to_gold(
    spark: SparkSession,
    symbol: str = "BTC-USDT",
    sync_to_postgres: bool = True
) -> Dict[str, Any]:
    """
    Transforms Silver trades into 1-minute Gold OHLCV + microstructure candles.
    Enforces:
      - BTC-USDT only.
      - Exclusive OS-level GoldLock acquired BEFORE reading existing Gold state.
      - Canonical Reconciliation shared rule:
          1. source='historical_rest' strictly > 'realtime_stream'
          2. is_closed=TRUE strictly > is_closed=FALSE
          3. For streaming: higher trade_count takes precedence over degraded stream
          4. Deterministic tie-breaking on batch_priority (newer batch)
      - Staging publish pattern with explicit state machine (backup before publish,
        safe rollback, preservation of recovery copies on rollback failure).
      - Synchronizes the exact CANONICAL chosen records to PostgreSQL DW with retry.
    """
    if symbol != "BTC-USDT":
        raise ValueError(f"Medallion Pipeline strictly processes 'BTC-USDT', but received: {symbol}")

    silver_path = get_silver_trades_path(symbol)
    gold_path = get_gold_candles_path(symbol)

    print("=" * 80)
    print("MEDALLION BATCH LAYER: SILVER -> GOLD 1M OHLCV AGGREGATION")
    print(f"Symbol      : {symbol}")
    print(f"Silver Path : {silver_path}")
    print(f"Gold Path   : {gold_path}")
    print("=" * 80)

    is_local = not silver_path.startswith("s3a://") and not silver_path.startswith("s3://")
    if is_local:
        local_s = Path(silver_path)
        if not local_s.exists() or not any(local_s.rglob("*.parquet")):
            print(f"[INFO] Silver path does not exist or has no parquet files ({silver_path}). Initial run.")
            return {"candles_count": 0, "status": "no_data"}
        try:
            trades_df = spark.read.parquet(silver_path)
        except Exception as read_err:
            print(f"[!] Critical error reading Silver Parquet at {silver_path}: {read_err}")
            raise read_err
    else:
        try:
            trades_df = spark.read.parquet(silver_path)
        except Exception as s3_err:
            err_msg = str(s3_err).lower()
            if "path does not exist" in err_msg or "nosuchkey" in err_msg:
                print(f"[INFO] S3 Silver path does not exist yet ({silver_path}). Initial run.")
                return {"candles_count": 0, "status": "no_data"}
            print(f"[!] Critical error reading S3 Silver at {silver_path}: {s3_err}")
            raise s3_err

    if symbol:
        trades_df = trades_df.filter(col("symbol") == symbol)

    if trades_df.count() == 0:
        print("[!] No trades found in Silver layer to aggregate.")
        return {"candles_count": 0, "status": "empty"}

    # Determine latest event time in this dataset to accurately determine closed vs in-flight windows
    max_trade_time = trades_df.select(spark_max("trade_time")).collect()[0][0]

    # Event-time aggregation with tumbling 1-minute window
    # In PySpark 3.5+, expr("min_by(price, struct(raw_timestamp, trade_id))") guarantees exact event-time Open/Close
    candles_df = (
        trades_df
        .groupBy(
            window(col("trade_time"), "1 minute"),
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
        .withColumn(
            "is_closed",
            when(col("window.end") <= lit(max_trade_time), lit(True)).otherwise(lit(False))
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
            col("is_closed")
        )
        .orderBy("window_start")
    )

    candle_count = candles_df.count()

    # Collect affected window starts for selective canonical DW sync
    affected_window_starts = [
        row.window_start for row in candles_df.select("window_start").distinct().collect()
    ]

    # ACQUIRE EXCLUSIVE GOLD LOCK BEFORE READING EXISTING GOLD
    # This prevents the Read-Modify-Write race condition across concurrent jobs
    with GoldLock(gold_path, timeout=60.0):
        # 1. Read existing Gold safely
        is_local_gold = not gold_path.startswith("s3a://") and not gold_path.startswith("s3://")
        existing_gold = None

        if is_local_gold:
            local_gold_p = Path(gold_path)
            if local_gold_p.exists() and any(local_gold_p.rglob("*.parquet")):
                try:
                    existing_gold = spark.read.parquet(gold_path).filter(col("symbol") == symbol)
                except Exception as read_err:
                    print(f"[!] Critical error reading existing Gold Parquet at {gold_path}: {read_err}")
                    raise read_err
        else:
            try:
                existing_gold = spark.read.parquet(gold_path).filter(col("symbol") == symbol)
            except Exception as s3_err:
                err_msg = str(s3_err).lower()
                if "path does not exist" in err_msg or "filenotfoundexception" in err_msg or "nosuchkey" in err_msg:
                    existing_gold = None
                else:
                    print(f"[!] Critical error reading S3 Gold at {gold_path}: {s3_err}")
                    raise s3_err

        # 2. Unified Canonical Reconciliation:
        # Precedence:
        #   a. Authoritative source: 'historical_rest' (score 2) strictly > 'realtime_stream' (score 1)
        #   b. Closure status: closed (is_closed=True) strictly > unclosed
        #   c. Streaming trade completeness: higher trade_count prioritized over degraded stream
        #   d. Newly ingested batch (batch_priority=1) breaks remaining ties deterministically
        if existing_gold is not None:
            source_score = (
                when(col("source") == "historical_rest", lit(2))
                .when(col("source") == "realtime_stream", lit(1))
                .otherwise(lit(0))
            )
            w = Window.partitionBy("symbol", "window_start").orderBy(
                source_score.desc(),
                col("is_closed").desc(),
                col("trade_count").desc_nulls_last(),
                col("batch_priority").desc()
            )
            combined = (
                candles_df.withColumn("batch_priority", lit(1))
                .unionByName(existing_gold.withColumn("batch_priority", lit(0)), allowMissingColumns=True)
            )
            final_gold = (
                combined
                .withColumn("rn", row_number().over(w))
                .filter(col("rn") == 1)
                .drop("rn", "batch_priority")
                .orderBy("window_start")
            )
        else:
            final_gold = candles_df

        # 3. Safe Staging & Publish pattern with explicit State Machine
        unique_run_id = f"{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"
        staging_path = f"{gold_path}_staging_{unique_run_id}"

        publish_state = PublishState.STAGING_STARTED
        (
            final_gold.write
            .mode("overwrite")
            .partitionBy("symbol")
            .parquet(staging_path)
        )

        staging_verify = spark.read.parquet(staging_path)
        if staging_verify.isEmpty():
            raise IOError(f"Staging write produced empty dataset at {staging_path}; aborting Gold publish.")
        publish_state = PublishState.STAGING_COMPLETED

        if is_local_gold:
            target_dir = Path(gold_path)
            staging_dir = Path(staging_path)
            backup_dir = Path(f"{gold_path}_backup_{unique_run_id}")

            try:
                # Step A: BACKUP existing Gold if present
                if target_dir.exists() and any(target_dir.iterdir()):
                    publish_state = PublishState.BACKUP_STARTED
                    shutil.copytree(str(target_dir), str(backup_dir))
                    publish_state = PublishState.BACKUP_COMPLETED

                # Step B: PUBLISH (Replace target_dir with staging)
                publish_state = PublishState.PUBLISH_STARTED
                target_dir.mkdir(parents=True, exist_ok=True)
                for item in staging_dir.iterdir():
                    dest = target_dir / item.name
                    if dest.exists():
                        if dest.is_dir():
                            shutil.rmtree(dest)
                        else:
                            dest.unlink()
                    shutil.move(str(item), str(dest))
                publish_state = PublishState.PUBLISH_COMPLETED
            except Exception as pub_err:
                print(f"[!] Error during Gold publish: {pub_err}. State={publish_state}. Executing rollback...")
                if publish_state in (PublishState.PUBLISH_STARTED, PublishState.BACKUP_COMPLETED):
                    publish_state = PublishState.ROLLBACK_STARTED
                    try:
                        if backup_dir.exists():
                            if target_dir.exists():
                                shutil.rmtree(target_dir, ignore_errors=True)
                            shutil.copytree(str(backup_dir), str(target_dir))
                            publish_state = PublishState.ROLLBACK_COMPLETED
                            print(f"[OK] Rollback successful: Restored Gold dataset from {backup_dir}.")
                    except Exception as rb_err:
                        publish_state = PublishState.ROLLBACK_FAILED
                        msg = (
                            f"FATAL: Gold publish and rollback both failed! "
                            f"Disaster recovery copy preserved at: {backup_dir}. "
                            f"Staging at: {staging_dir}. Error: {rb_err}"
                        )
                        print(f"[CRITICAL] {msg}")
                        raise RuntimeError(msg) from pub_err
                raise pub_err
            finally:
                # Clean up staging unless rollback failed
                if publish_state != PublishState.ROLLBACK_FAILED and staging_dir.exists():
                    shutil.rmtree(staging_dir, ignore_errors=True)
                # NEVER delete backup if publish did not succeed!
                if publish_state == PublishState.PUBLISH_COMPLETED and backup_dir.exists():
                    shutil.rmtree(backup_dir, ignore_errors=True)
        else:
            # S3 non-atomic publish guard
            staging_df = spark.read.parquet(staging_path)
            staging_df.write.mode("overwrite").partitionBy("symbol").parquet(gold_path)
            publish_state = PublishState.PUBLISH_COMPLETED

        print(f"[SUCCESS] Gold 1-minute candles generated: {candle_count:,} new candles processed; saved to {gold_path}")

        # 4. Sync to PostgreSQL Warehouse:
        # Materialize and sync the exact CANONICAL chosen records for the affected windows, NOT raw candles_df!
        if sync_to_postgres and candle_count > 0:
            print("[*] Synchronizing canonical Gold candles to PostgreSQL warehouse.fact_crypto_1m_candles...")
            # Read from the newly published snapshot to avoid stale lazy lineage dependencies
            published_gold = spark.read.parquet(gold_path).filter(col("symbol") == symbol)
            canonical_sync_df = published_gold.filter(col("window_start").isin(affected_window_starts))

            write_candles_batch(canonical_sync_df, batch_id=0, max_retries=3)
            print("[OK] Successfully synchronized canonical Gold records to PostgreSQL Data Warehouse.")

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
