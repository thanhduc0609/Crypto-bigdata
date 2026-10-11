"""
=============================================================================
Crypto Big Data - S3 Data Lake to PostgreSQL Data Warehouse ETL Pipeline
=============================================================================
Architecture:
  AWS S3 Parquet (Data Lake) -> PySpark / Batch Loader -> PostgreSQL (Data Mart)
=============================================================================
"""

import os
import sys
import argparse
from pathlib import Path

# UTF-8 stdout configuration for Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Setup JAVA_HOME and HADOOP_HOME for Spark
os.environ['HADOOP_HOME'] = r'C:\hadoop'
java_path = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot'
if os.path.exists(java_path):
    os.environ['JAVA_HOME'] = java_path
    os.environ['PATH'] = f"{java_path}\\bin;" + os.environ.get('PATH', '')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from warehouse.etl.db_config import (
    load_env_file,
    get_db_connection,
    init_warehouse_schema,
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

load_env_file(project_root / ".env")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from processing.core.spark_session import create_spark_session, ensure_java_and_hadoop


def get_spark_session() -> SparkSession:
    ensure_java_and_hadoop()
    return create_spark_session(
        app_name="S3ToWarehouseETL",
        include_kafka=False,
        include_s3=True,
        include_postgres=True
    )


def load_raw_trades_to_dw(spark: SparkSession, bucket: str, limit: int = None):
    s3_paths = [
        f"s3a://{bucket}/crypto/silver/trades",
        f"s3a://{bucket}/crypto/raw/trades"
    ]
    df = None
    target_path = None
    for p in s3_paths:
        try:
            print(f"[*] Kiem tra nguon Raw Trades: {p}")
            t_df = spark.read.parquet(p)
            if t_df.take(1):
                df = t_df
                target_path = p
                break
        except Exception:
            continue

    if df is None:
        print("[!] Khong tim thay du lieu Raw Trades tren S3.")
        return

    print(f"\n[ETL] Dang doc Raw Trades tu S3: {target_path}")
    try:
        if limit:
            df = df.limit(limit)

        total_rows = df.count()
        print(f"[*] Tong so ban ghi tim thay: {total_rows}")
        if total_rows == 0:
            return

        dw_df = df.select(
            col("trade_id").cast("string"),
            col("symbol").cast("string"),
            col("price").cast("double"),
            col("size").cast("double"),
            col("side").cast("string"),
            col("trade_value").cast("double"),
            col("trade_time"),
            col("trade_date").cast("date"),
            col("kafka_partition").cast("int"),
            col("kafka_offset").cast("long")
        )

        conn = get_db_connection()
        if conn:
            from psycopg2.extras import execute_batch
            rows = dw_df.collect()
            query = """
                INSERT INTO warehouse.fact_crypto_trades (
                    trade_id, symbol, price, size, side, trade_value,
                    trade_time, trade_date, kafka_partition, kafka_offset
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_id) DO NOTHING;
            """
            data = [
                (
                    r["trade_id"], r["symbol"], r["price"], r["size"],
                    r["side"], r["trade_value"], r["trade_time"],
                    r["trade_date"], r["kafka_partition"], r["kafka_offset"]
                )
                for r in rows
            ]
            with conn.cursor() as cur:
                execute_batch(cur, query, data, page_size=1000)
                conn.commit()
            conn.close()
            print(f"[OK] Nap thanh cong & idempotent {len(rows)} ban ghi vao 'warehouse.fact_crypto_trades'!")

    except Exception as e:
        print(f"[!] Loi khi nap Raw Trades: {e}")


def load_1m_candles_to_dw(spark: SparkSession, bucket: str):
    s3_paths = [
        f"s3a://{bucket}/crypto/silver/candles",
        f"s3a://{bucket}/crypto/batch/historical_candles_1m",
        f"s3a://{bucket}/crypto/processed/trades_1m"
    ]
    df = None
    target_path = None
    for p in s3_paths:
        try:
            print(f"[*] Kiem tra nguon nen 1m: {p}")
            t_df = spark.read.parquet(p)
            if t_df.take(1):
                df = t_df
                target_path = p
                break
        except Exception:
            continue

    if df is None:
        print("[*] Khong tim thay du lieu nen 1m tren S3.")
        return

    print(f"\n[ETL] Dang doc 1-Minute Aggregations tu S3: {target_path}")
    try:
        total_rows = df.count()
        print(f"[*] Tong so nen 1m tim thay: {total_rows}")
        if total_rows == 0:
            return

        turnover_col = col("turnover_usd") if "turnover_usd" in df.columns else col("total_trade_value")

        dw_df = df.select(
            col("symbol").cast("string"),
            col("window_start"),
            col("window_end"),
            col("open").cast("double"),
            col("high").cast("double"),
            col("low").cast("double"),
            col("close").cast("double"),
            col("volume").cast("double"),
            col("trade_count").cast("long"),
            col("buy_volume").cast("double"),
            col("sell_volume").cast("double"),
            col("vwap").cast("double"),
            col("trade_imbalance").cast("double"),
            turnover_col.cast("double").alias("total_trade_value")
        )

        conn = get_db_connection()
        if conn:
            from psycopg2.extras import execute_batch
            rows = dw_df.collect()
            query = """
                INSERT INTO warehouse.fact_crypto_1m_candles (
                    symbol, window_start, window_end, open, high, low, close,
                    volume, trade_count, buy_volume, sell_volume, vwap,
                    trade_imbalance, total_trade_value
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, window_start) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    trade_count = EXCLUDED.trade_count,
                    buy_volume = EXCLUDED.buy_volume,
                    sell_volume = EXCLUDED.sell_volume,
                    vwap = EXCLUDED.vwap,
                    trade_imbalance = EXCLUDED.trade_imbalance,
                    total_trade_value = EXCLUDED.total_trade_value;
            """
            data = [
                (
                    r["symbol"], r["window_start"], r["window_end"],
                    r["open"], r["high"], r["low"], r["close"],
                    r["volume"], r["trade_count"], r["buy_volume"],
                    r["sell_volume"], r["vwap"], r["trade_imbalance"],
                    r["total_trade_value"]
                )
                for r in rows
            ]
            with conn.cursor() as cur:
                execute_batch(cur, query, data, page_size=1000)
                conn.commit()
            conn.close()
            print(f"[OK] Nap thanh cong & idempotent {len(rows)} nen vao 'warehouse.fact_crypto_1m_candles'!")

    except Exception as e:
        print(f"[!] Loi khi nap 1m Candles vao DW: {e}")


def main():
    parser = argparse.ArgumentParser(description="S3 to PostgreSQL Data Warehouse ETL")
    parser.add_argument("--init-db", action="store_true", help="Khoi tao Schema DDL & Views trong PostgreSQL")
    parser.add_argument("--type", choices=["1m", "raw", "all"], default="all", help="Loai du lieu nap vao DW")
    parser.add_argument("--limit", type=int, default=None, help="Gioi han so dong nap (danh cho raw trades)")

    args = parser.parse_args()
    bucket = os.getenv("S3_BUCKET_NAME", "").strip()

    print("=" * 80)
    print("  [ETL] S3 DATA LAKE -> POSTGRESQL DATA WAREHOUSE PIPELINE")
    print("=" * 80)

    if args.init_db:
        init_warehouse_schema()

    if not bucket:
        print("[ERROR] S3_BUCKET_NAME chua duoc cau hinh trong .env!")
        sys.exit(1)

    spark = get_spark_session()
    try:
        if args.type in ["1m", "all"]:
            load_1m_candles_to_dw(spark, bucket)
        if args.type in ["raw", "all"]:
            load_raw_trades_to_dw(spark, bucket, limit=args.limit)
    finally:
        spark.stop()
        print("\n" + "=" * 80)
        print("ETL PIPELINE HOAN TAT.")
        print("=" * 80)


if __name__ == "__main__":
    main()
