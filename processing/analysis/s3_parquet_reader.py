"""
=============================================================================
Crypto Big Data - S3 Parquet Streaming Data Lake Explorer & CSV Tool
=============================================================================
Chuong trinh doc va phan tich du lieu Parquet tu AWS S3 Data Lake (Streaming):
  - Xem du lieu tong hop nen 1 phut (1m Candlestick / Aggregates)
  - Xem du lieu giao dich tho da lam sach (Cleaned Raw Trades)
  - Ho tro cap giao dich: BTC-USDT
  - Thong ke tong hop nhanh (Volume, VWAP, Price Range, Turnover USD)
  - Xuat du lieu S3 ra file CSV cuc bo de phan tich tren Excel / Power BI
=============================================================================
"""

import os
import sys
import argparse
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    round as spark_round
)
from processing.core.spark_session import create_spark_session, load_env_vars

load_env_vars()


def get_s3_paths(bucket: str):
    raw_p = os.getenv("S3_STREAMING_RAW_PATH", os.getenv("S3_RAW_PATH", f"s3a://{bucket}/crypto/streaming/trades"))
    if "${S3_BUCKET_NAME}" in raw_p:
        raw_p = raw_p.replace("${S3_BUCKET_NAME}", bucket)
    c_p = os.getenv("S3_STREAMING_CANDLES_PATH", os.getenv("S3_PROCESSED_PATH", f"s3a://{bucket}/crypto/streaming/candles_1m"))
    if "${S3_BUCKET_NAME}" in c_p:
        c_p = c_p.replace("${S3_BUCKET_NAME}", bucket)
    return {
        "raw": raw_p,
        "1m": c_p,
        "legacy_raw": f"s3a://{bucket}/crypto/raw/trades",
        "legacy_1m": f"s3a://{bucket}/crypto/processed/trades_1m"
    }


def load_parquet_with_fallback(spark: SparkSession, primary_path: str, fallback_path: str):
    try:
        return spark.read.parquet(primary_path)
    except Exception:
        return spark.read.parquet(fallback_path)


def show_1m_aggregates(spark: SparkSession, paths: dict, symbol: str = None, limit: int = 20):
    print("\n" + "=" * 80)
    print(f"  [1M CANDLESTICK STREAMING AGGREGATES] Path: {paths['1m']}")
    if symbol:
        print(f"  Filter Symbol: {symbol}")
    print("=" * 80)

    try:
        df = load_parquet_with_fallback(spark, paths["1m"], paths["legacy_1m"])
        if symbol:
            df = df.filter(col("symbol") == symbol)
        
        total_rows = df.count()
        print(f"[*] Tong so ban ghi tim thay: {total_rows}")
        
        if total_rows > 0:
            df.orderBy(col("window_start").desc()).show(limit, truncate=False)
            
            print("\n--- THONG KE TONG HOP ---")
            df.groupBy("symbol").agg(
                spark_round(spark_min("low"), 2).alias("min_price"),
                spark_round(spark_max("high"), 2).alias("max_price"),
                spark_round(spark_sum("volume"), 4).alias("total_volume"),
                spark_sum("trade_count").alias("total_trades"),
                spark_round(spark_sum("total_trade_value"), 2).alias("turnover_usd")
            ).show(truncate=False)
    except Exception as e:
        print(f"[!] Loi khi doc du lieu nen 1m tren S3: {e}")


def show_raw_trades(spark: SparkSession, paths: dict, symbol: str = None, limit: int = 20, min_value: float = None):
    print("\n" + "=" * 80)
    print(f"  [CLEANED RAW TRADES STREAM] Path: {paths['raw']}")
    if symbol:
        print(f"  Filter Symbol: {symbol}")
    if min_value:
        print(f"  Filter Min Value: >= ${min_value:,.2f}")
    print("=" * 80)

    try:
        df = load_parquet_with_fallback(spark, paths["raw"], paths["legacy_raw"])
        if symbol:
            df = df.filter(col("symbol") == symbol)
        if min_value:
            df = df.filter(col("trade_value") >= min_value)
            
        total_rows = df.count()
        print(f"[*] Tong so ban ghi tim thay: {total_rows}")
        
        if total_rows > 0:
            display_cols = ["trade_id", "symbol", "side", "price", "size", "trade_value", "trade_time"]
            available_cols = [c for c in display_cols if c in df.columns]
            df.select(available_cols).orderBy(col("trade_time").desc()).show(limit, truncate=False)
    except Exception as e:
        print(f"[!] Loi khi doc Raw Trades tren S3: {e}")


def export_to_local_csv(spark: SparkSession, primary_path: str, fallback_path: str, output_path: str, symbol: str = None):
    print(f"\n[*] Dang xuat du lieu tu S3 sang CSV: {output_path}")
    try:
        df = load_parquet_with_fallback(spark, primary_path, fallback_path)
        if symbol:
            df = df.filter(col("symbol") == symbol)
            
        out_file = Path(output_path).resolve()
        out_file.parent.mkdir(parents=True, exist_ok=True)
        
        pandas_df = df.toPandas()
        pandas_df.to_csv(str(out_file), index=False, encoding="utf-8-sig")
        print(f"[OK] Da xuat {len(pandas_df)} dong ra file CSV thanh cong: {out_file}")
    except Exception as e:
        print(f"[!] Loi khi xuat CSV: {e}")


def interactive_menu(spark: SparkSession, bucket: str):
    paths = get_s3_paths(bucket)
    while True:
        print("\n" + "=" * 65)
        print("   CRYPTO S3 DATA LAKE - STREAMING EXPLORER & CSV EXPORTER")
        print(f"   Bucket: s3://{bucket}")
        print("=" * 65)
        print("1. Xem nến 1 phút thời gian thực (1m Streaming Candles)")
        print("2. Xem giao dịch thô đã làm sạch (Cleaned Raw Trades)")
        print("3. Xem danh sách giao dịch cá mập (Whale Trades >= $10K)")
        print("4. Xuất nến 1 phút ra CSV (data/exports/streaming_candles_1m_*.csv)")
        print("5. Xuất Raw Trades ra CSV (data/exports/streaming_raw_trades_*.csv)")
        print("0. Thoát")
        print("=" * 65)
        
        choice = input("Chon thao tac (0-5): ").strip()
        if choice == "0":
            break
        elif choice == "1":
            sym = input("Nhap symbol (Mac dinh BTC-USDT): ").strip().upper() or "BTC-USDT"
            show_1m_aggregates(spark, paths, symbol=sym if sym else None)
        elif choice == "2":
            sym = input("Nhap symbol (Enter de lay het): ").strip().upper()
            show_raw_trades(spark, paths, symbol=sym if sym else None)
        elif choice == "3":
            sym = input("Nhap symbol (Enter de lay het): ").strip().upper()
            threshold = input("Nhap nguong USD (mac dinh 10000): ").strip()
            val = float(threshold) if threshold else 10000.0
            show_raw_trades(spark, paths, symbol=sym if sym else None, min_value=val)
        elif choice == "4":
            sym = input("Nhap symbol (Enter de xuat het): ").strip().upper()
            export_to_local_csv(
                spark, paths["1m"], paths["legacy_1m"],
                f"data/exports/streaming_candles_1m_{sym or 'all'}.csv",
                symbol=sym if sym else None
            )
        elif choice == "5":
            sym = input("Nhap symbol (Enter de xuat het): ").strip().upper()
            export_to_local_csv(
                spark, paths["raw"], paths["legacy_raw"],
                f"data/exports/streaming_raw_trades_{sym or 'all'}.csv",
                symbol=sym if sym else None
            )


def main():
    parser = argparse.ArgumentParser(description="Crypto S3 Parquet Streaming Data Lake Explorer")
    parser.add_argument("--type", choices=["1m", "raw"], default=None, help="Loai du lieu S3")
    parser.add_argument("--symbol", type=str, default=None, help="Loc theo coin symbol")
    parser.add_argument("--limit", type=int, default=20, help="So dong hien thi")
    parser.add_argument("--export", type=str, default=None, help="Duong dan xuat file CSV cuc bo")
    parser.add_argument("--min-value", type=float, default=None, help="Loc gia tri toi thieu USD")

    args = parser.parse_args()
    bucket = os.getenv("S3_BUCKET_NAME", "").strip()

    if not bucket:
        print("[ERROR] S3_BUCKET_NAME chua duoc cau hinh trong .env!")
        sys.exit(1)

    spark = create_spark_session(app_name="CryptoS3StreamingReader", include_kafka=False)
    paths = get_s3_paths(bucket)

    try:
        if args.type == "1m":
            if args.export:
                export_to_local_csv(spark, paths["1m"], paths["legacy_1m"], args.export, args.symbol)
            else:
                show_1m_aggregates(spark, paths, args.symbol, args.limit)
        elif args.type == "raw":
            if args.export:
                export_to_local_csv(spark, paths["raw"], paths["legacy_raw"], args.export, args.symbol)
            else:
                show_raw_trades(spark, paths, args.symbol, args.limit, args.min_value)
        else:
            interactive_menu(spark, bucket)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
