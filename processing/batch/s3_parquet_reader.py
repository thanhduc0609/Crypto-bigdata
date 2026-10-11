"""
=============================================================================
Crypto Big Data - Batch Layer: S3 Parquet Data Lake Reader & Analytics Tool
=============================================================================
Chuong trinh doc va phan tich du lieu Parquet tu AWS S3 Data Lake
Ho tro:
  - Xem du lieu tong hop nen 1 phut (1m Candlestick / Aggregates)
  - Xem du lieu giao dich thô da lam sach (Cleaned Raw Trades)
  - Loc theo cap giao dich (Symbol: BTC-USDT, ETH-USDT, SOL-USDT)
  - Tinh toan thong ke tong hop nhanh (Volume, VWAP, Price Range, Buy/Sell Ratio)
  - Xuat du lieu ra file CSV cuc bo de phan tich tren Power BI / Excel
  - Chay truy van SQL tuy y (Spark SQL)
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
from pyspark.sql.functions import col, min as spark_min, max as spark_max, sum as spark_sum, count as spark_count, round as spark_round
from processing.core.spark_session import create_spark_session, load_env_vars

load_env_vars()


def get_s3_paths(bucket: str):
    return {
        "bronze": f"s3a://{bucket}/crypto/bronze/batch/okx/candles",
        "silver": f"s3a://{bucket}/crypto/silver/candles",
        "gold_1d": f"s3a://{bucket}/crypto/gold/candles_1d",
        "gold_1w": f"s3a://{bucket}/crypto/gold/candles_1w",
        "gold_1mo": f"s3a://{bucket}/crypto/gold/candles_1mo",
        "gold_1y": f"s3a://{bucket}/crypto/gold/candles_1y",
        "1m": f"s3a://{bucket}/crypto/silver/candles",
        "raw": f"s3a://{bucket}/crypto/raw/trades",
        "daily": f"s3a://{bucket}/crypto/gold/candles_1d",
        "weekly": f"s3a://{bucket}/crypto/gold/candles_1w",
        "monthly": f"s3a://{bucket}/crypto/gold/candles_1mo",
        "yearly": f"s3a://{bucket}/crypto/gold/candles_1y"
    }


def show_1m_aggregates(spark: SparkSession, s3_path: str, symbol: str = None, limit: int = 20):
    print("\n" + "=" * 80)
    print(f"  [1M CANDLESTICK AGGREGATES] Path: {s3_path}")
    if symbol:
        print(f"  Filter Symbol: {symbol}")
    print("=" * 80)

    try:
        df = spark.read.parquet(s3_path)
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
        print(f"[!] Loi khi doc du lieu: {e}")


def show_raw_trades(spark: SparkSession, s3_path: str, symbol: str = None, limit: int = 20, min_value: float = None):
    print("\n" + "=" * 80)
    print(f"  [CLEANED RAW TRADES] Path: {s3_path}")
    if symbol:
        print(f"  Filter Symbol: {symbol}")
    if min_value:
        print(f"  Filter Min Value: >= ${min_value:,.2f}")
    print("=" * 80)

    try:
        df = spark.read.parquet(s3_path)
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
        print(f"[!] Loi khi doc du lieu: {e}")


def export_to_local_csv(spark: SparkSession, s3_path: str, output_path: str, symbol: str = None):
    print(f"\n[*] Dang xuat du lieu tu S3 sang CSV: {output_path}")
    try:
        try:
            df = spark.read.parquet(s3_path)
        except Exception:
            # Fallback path if using crypto/batch/ instead of crypto/processed/
            alt_path = s3_path.replace("/crypto/processed/", "/crypto/batch/") if "/crypto/processed/" in s3_path else s3_path.replace("/crypto/batch/", "/crypto/processed/")
            df = spark.read.parquet(alt_path)

        if symbol:
            df = df.filter(col("symbol") == symbol)
            
        out_file = Path(output_path).resolve()
        out_file.parent.mkdir(parents=True, exist_ok=True)
        
        pandas_df = df.toPandas()
        pandas_df.to_csv(str(out_file), index=False, encoding="utf-8-sig")
        print(f"[OK] Da xuat {len(pandas_df)} dong ra file CSV thanh cong: {out_file}")
    except Exception as e:
        if "Path does not exist" in str(e) or "PATH_NOT_FOUND" in str(e):
            print(f"[!] Thu muc tren S3 chua ton tai ({s3_path}).")
            print("    -> Goi y: Hay chay '.\\scripts\\setup_env.ps1 -BatchAnalytics' de PySpark tong hop du lieu va tu dong xuat file CSV.")
        else:
            print(f"[!] Loi khi xuat CSV: {e}")


def show_timeframe_summary(spark: SparkSession, s3_path: str, timeframe_label: str, symbol: str = None, limit: int = 20):
    print("\n" + "=" * 80)
    print(f"  [{timeframe_label.upper()} MARKET SUMMARY] Path: {s3_path}")
    if symbol:
        print(f"  Filter Symbol: {symbol}")
    print("=" * 80)

    try:
        df = spark.read.parquet(s3_path)
        if symbol:
            df = df.filter(col("symbol") == symbol)
        
        total_rows = df.count()
        print(f"[*] Tong so ban ghi tim thay: {total_rows}")
        if total_rows > 0:
            sort_col = "bucket_time" if "bucket_time" in df.columns else ("trade_date" if "trade_date" in df.columns else "window_start")
            if sort_col in df.columns:
                df.orderBy(col(sort_col).desc()).show(limit, truncate=False)
            else:
                df.show(limit, truncate=False)
    except Exception as e:
        if "Path does not exist" in str(e) or "PATH_NOT_FOUND" in str(e):
            print(f"[!] Thu muc tong hop {timeframe_label} tren S3 chua duoc tao ({s3_path}).")
            print("    -> Goi y: Hay chay '.\\scripts\\setup_env.ps1 -TimeframeAgg' de tao du lieu.")
        else:
            print(f"[!] Loi khi doc du lieu {timeframe_label}: {e}")


def interactive_menu(spark: SparkSession, bucket: str):
    paths = get_s3_paths(bucket)
    while True:
        print("\n" + "=" * 65)
        print("   CRYPTO S3 DATA LAKE - BATCH EXPLORER & CSV EXPORTER")
        print(f"   Bucket: s3://{bucket}")
        print("=" * 65)
        print("1. Xem nến 1 phút (1m Candlestick / Aggregates)")
        print("2. Xem giao dịch thô đã làm sạch (Cleaned Raw Trades)")
        print("3. Xem danh sách giao dịch cá mập (Whale Trades >= $10K)")
        print("4. Xem nến tổng hợp Ngày (Daily Market Summary)")
        print("5. Xem nến tổng hợp Tuần (Weekly Market Summary)")
        print("6. Xem nến tổng hợp Tháng (Monthly Market Summary)")
        print("-" * 65)
        print("7. Xuất nến 1 phút ra CSV (data/exports/candles_1m_*.csv)")
        print("8. Xuất Raw Trades đã làm sạch ra CSV (data/exports/cleaned_raw_trades_*.csv)")
        print("9. Xuất nến Ngày/Tuần/Tháng ra CSV (data/exports/summary_*_*.csv)")
        print("0. Thoát")
        print("=" * 65)
        
        choice = input("Chon thao tac (0-9): ").strip()
        if choice == "0":
            break
        elif choice == "1":
            sym = input("Nhap symbol (BTC-USDT, ETH-USDT, SOL-USDT hoac Enter de lay het): ").strip().upper()
            show_1m_aggregates(spark, paths["1m"], symbol=sym if sym else None)
        elif choice == "2":
            sym = input("Nhap symbol (Enter de lay het): ").strip().upper()
            show_raw_trades(spark, paths["raw"], symbol=sym if sym else None)
        elif choice == "3":
            sym = input("Nhap symbol (Enter de lay het): ").strip().upper()
            threshold = input("Nhap nguong USD (mac dinh 10000): ").strip()
            val = float(threshold) if threshold else 10000.0
            show_raw_trades(spark, paths["raw"], symbol=sym if sym else None, min_value=val)
        elif choice == "4":
            sym = input("Nhap symbol (Enter de lay het): ").strip().upper()
            show_timeframe_summary(spark, paths["daily"], "Daily", symbol=sym if sym else None)
        elif choice == "5":
            sym = input("Nhap symbol (Enter de lay het): ").strip().upper()
            show_timeframe_summary(spark, paths["weekly"], "Weekly", symbol=sym if sym else None)
        elif choice == "6":
            sym = input("Nhap symbol (Enter de lay het): ").strip().upper()
            show_timeframe_summary(spark, paths["monthly"], "Monthly", symbol=sym if sym else None)
        elif choice == "7":
            sym = input("Nhap symbol (Enter de xuat het): ").strip().upper()
            export_to_local_csv(spark, paths["1m"], f"data/exports/candles_1m_{sym or 'all'}.csv", symbol=sym if sym else None)
        elif choice == "8":
            sym = input("Nhap symbol (Enter de xuat het): ").strip().upper()
            export_to_local_csv(spark, paths["raw"], f"data/exports/cleaned_raw_trades_{sym or 'all'}.csv", symbol=sym if sym else None)
        elif choice == "9":
            tf_choice = input("Chon khung thoi gian (1: Ngay [mac dinh], 2: Tuan, 3: Thang): ").strip()
            tf_key = "weekly" if tf_choice == "2" else ("monthly" if tf_choice == "3" else "daily")
            sym = input("Nhap symbol (Enter de xuat het): ").strip().upper()
            export_to_local_csv(spark, paths[tf_key], f"data/exports/summary_{tf_key}_{sym or 'all'}.csv", symbol=sym if sym else None)


def main():
    parser = argparse.ArgumentParser(description="Crypto S3 Parquet Data Lake Reader")
    parser.add_argument("--type", choices=["bronze", "silver", "gold_1d", "gold_1w", "gold_1mo", "gold_1y", "1m", "raw", "daily", "weekly", "monthly", "yearly"], default=None, help="Loai du lieu Medallion hoac Legacy")
    parser.add_argument("--symbol", type=str, default=None, help="Loc theo coin symbol")
    parser.add_argument("--limit", type=int, default=20, help="So dong hien thi")
    parser.add_argument("--export", type=str, default=None, help="Duong dan xuat file CSV cuc bo")
    parser.add_argument("--min-value", type=float, default=None, help="Loc gia tri toi thieu USD")

    args = parser.parse_args()
    bucket = os.getenv("S3_BUCKET_NAME", "").strip()

    if not bucket:
        print("[ERROR] S3_BUCKET_NAME chua duoc cau hinh trong .env!")
        sys.exit(1)

    spark = create_spark_session(app_name="CryptoS3BatchReader", include_kafka=False)
    paths = get_s3_paths(bucket)

    try:
        if args.type in ["1m", "silver"]:
            if args.export:
                export_to_local_csv(spark, paths[args.type], args.export, args.symbol)
            else:
                show_1m_aggregates(spark, paths[args.type], args.symbol, args.limit)
        elif args.type == "bronze":
            try:
                df = spark.read.parquet(paths["bronze"])
                if args.symbol:
                    df = df.filter(col("symbol") == args.symbol)
                print(f"\n[BRONZE DATA LAKE] Path: {paths['bronze']}")
                print(f"[*] Tong so ban ghi: {df.count()}")
                df.show(args.limit, truncate=False)
            except Exception as e:
                print(f"[!] Loi khi doc Bronze: {e}")
        elif args.type == "raw":
            if args.export:
                export_to_local_csv(spark, paths["raw"], args.export, args.symbol)
            else:
                show_raw_trades(spark, paths["raw"], args.symbol, args.limit, args.min_value)
        elif args.type in ["daily", "weekly", "monthly", "yearly", "gold_1d", "gold_1w", "gold_1mo", "gold_1y"]:
            if args.export:
                export_to_local_csv(spark, paths[args.type], args.export, args.symbol)
            else:
                show_timeframe_summary(spark, paths[args.type], args.type.upper(), args.symbol, args.limit)
        else:
            interactive_menu(spark, bucket)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
