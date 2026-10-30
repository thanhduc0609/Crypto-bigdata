"""
=============================================================================
Crypto Big Data - Entrypoint: Medallion Batch Pipeline (Silver -> Gold -> DW)
=============================================================================
Executes canonical Medallion transformation in strict dependency order:
  1. Bronze -> Silver: Parse raw Kafka payloads, filter BTC-only, validate schema,
     eliminate NaN/Inf/invalid prices, deduplicate on trade_id, dynamic partition overwrite.
  2. Silver -> Gold: Tumbling 1m window event-time OHLCV aggregation, VWAP,
     order-flow imbalance, deterministic tie-breaking, safe staging & publish,
     reconciliation with existing Gold and sync to PostgreSQL DW.
=============================================================================
"""

import sys
import argparse
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import create_spark_session
from processing.batch.bronze_to_silver import process_bronze_to_silver
from processing.batch.silver_to_gold import process_silver_to_gold


def run_batch_medallion_pipeline(symbol: str = "BTC-USDT", sync_to_postgres: bool = True):
    if symbol != "BTC-USDT":
        raise ValueError(f"Project scope is strictly dedicated to 'BTC-USDT', but received: {symbol}")

    print("=" * 80)
    print("STARTING MEDALLION BATCH TRANSFORMATION PIPELINE")
    print(f"Target Symbol       : {symbol}")
    print(f"PostgreSQL Sync     : {sync_to_postgres}")
    print("=" * 80)

    spark = None
    try:
        spark = create_spark_session(app_name="MedallionBatchPipeline", include_kafka=False)

        # Step 1: Bronze -> Silver
        print("\n>>> STEP 1: Transforming Bronze Raw Archive -> Silver Clean Data...")
        silver_res = process_bronze_to_silver(spark, symbol=symbol)
        print(f"[OK] Silver step finished: {silver_res}")

        if silver_res.get("status") in ["no_data", "empty"]:
            print(f"[INFO] Bronze layer has no new records to process ({silver_res.get('status')}). Skipping Gold aggregation.")
            return {"silver": silver_res, "gold": {"status": "skipped"}, "status": "skipped"}

        # Step 2: Silver -> Gold -> DW
        print("\n>>> STEP 2: Aggregating Silver Clean Data -> Gold 1m OHLCV & DW Sync...")
        gold_res = process_silver_to_gold(spark, symbol=symbol, sync_to_postgres=sync_to_postgres)
        print(f"[OK] Gold step finished: {gold_res}")

        if gold_res.get("status") != "success":
            print(f"[!] Gold step failed with status: {gold_res.get('status')}")
            return {"silver": silver_res, "gold": gold_res, "status": "failed"}

        print("\n" + "=" * 80)
        print("MEDALLION BATCH PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        return {"silver": silver_res, "gold": gold_res, "status": "success"}
    except Exception as e:
        print(f"\n[!] Critical error in Medallion Batch Pipeline: {e}")
        raise e
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Medallion Batch Transformation Pipeline")
    parser.add_argument("--symbol", type=str, default="BTC-USDT", help="Trading symbol (BTC-USDT)")
    parser.add_argument("--no-db", action="store_true", help="Skip syncing Gold candles to PostgreSQL DW")
    args = parser.parse_args()

    try:
        result = run_batch_medallion_pipeline(symbol=args.symbol, sync_to_postgres=not args.no_db)
        if result.get("status") == "failed":
            sys.exit(1)
    except Exception:
        sys.exit(1)
