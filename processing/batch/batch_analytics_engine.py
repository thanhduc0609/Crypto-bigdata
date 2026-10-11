"""
=============================================================================
Crypto Big Data - Batch Analytics Engine (Silver -> Gold Medallion Pipeline)
=============================================================================
Module tổng hợp và phân tích dữ liệu Batch theo kiến trúc Medallion (Gold Layer):
  - Đọc từ Silver Data Lake: s3a://<bucket>/crypto/silver/candles (hoặc fallback legacy)
  - Phân tích đa khung thời gian:
      + Ngày (1D):   s3a://<bucket>/crypto/gold/candles_1d/
      + Tuần (1W):   s3a://<bucket>/crypto/gold/candles_1w/
      + Tháng (1MO): s3a://<bucket>/crypto/gold/candles_1mo/
      + Năm (1Y):    s3a://<bucket>/crypto/gold/candles_1y/
  - Tính toán chỉ số định lượng nâng cao chuẩn xác:
      + Open = first valid open price theo thời gian (min_by)
      + High = maximum high price
      + Low = minimum low price
      + Close = last valid close price theo thời gian (max_by)
      + Volume = sum of volume
      + Turnover USD & VWAP chuẩn hóa
      + Tỷ suất sinh lời (Return %) & Độ biến động giá (Volatility %)
      + Order flow imbalance (Buy/Sell Imbalance)
  - Hỗ trợ đa cặp coin độc lập (--symbol, --all-symbols)
  - Bảo toàn tương thích ngược: Xuất file CSV ra data/exports/ và lưu legacy S3 paths
=============================================================================
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    date_trunc,
    min_by,
    max_by,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    count as spark_count,
    round as spark_round,
    when
)
from processing.core.schemas import CANDLE_1M_SCHEMA
from processing.core.spark_session import create_spark_session, load_env_vars

load_env_vars()
SUPPORTED_SYMBOLS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]


def get_batch_input_dataframe(spark: SparkSession, bucket: str, symbols: list = None):
    """
    Đọc dữ liệu đầu vào cho Batch Analytics theo thứ tự ưu tiên:
      1. Silver Candles: s3a://{bucket}/crypto/silver/candles
      2. Legacy Batch Candles: s3a://{bucket}/crypto/batch/historical_candles_1m
      3. Processed Candles: s3a://{bucket}/crypto/processed/trades_1m
      4. Streaming Candles: s3a://{bucket}/crypto/streaming/candles_1m
      5. Fallback sang Raw Trades nếu chưa có nến
    """
    input_paths_1m = [
        f"s3a://{bucket}/crypto/silver/candles",
        f"s3a://{bucket}/crypto/batch/historical_candles_1m",
        f"s3a://{bucket}/crypto/processed/trades_1m",
        f"s3a://{bucket}/crypto/streaming/candles_1m"
    ]

    for p in input_paths_1m:
        try:
            print(f"[*] Kiem tra nguon du lieu nen 1m: {p}")
            df = spark.read.schema(CANDLE_1M_SCHEMA).parquet(p)
            if symbols:
                df = df.filter(col("symbol").isin(symbols))
            if df.take(1):
                count = df.count()
                print(f"[OK] Tim thay {count:,} nen 1m tai: {p}")
                return df, "1m"
        except Exception:
            continue

    # Fallback sang Raw Trades
    print("\n[!] Khong tim thay nen 1m. Kich hoat che do tu dong fallback sang Raw Trades...")
    raw_paths = [
        f"s3a://{bucket}/crypto/silver/trades",
        f"s3a://{bucket}/crypto/raw/trades",
        f"s3a://{bucket}/crypto/streaming/trades"
    ]
    for p in raw_paths:
        try:
            print(f"[*] Kiem tra nguon Raw Trades: {p}")
            df = spark.read.parquet(p)
            if symbols:
                df = df.filter(col("symbol").isin(symbols))
            if df.take(1):
                count = df.count()
                print(f"[OK] Tim thay {count:,} giao dich tho tai: {p}")
                return df, "raw"
        except Exception:
            continue

    return None, None


def aggregate_from_1m(df_1m, timeframe: str):
    """
    Tổng hợp đa khung thời gian từ nến 1 phút với logic tất định (deterministic).
    Open = nến đầu tiên theo thời gian UTC (min_by)
    Close = nến cuối cùng theo thời gian UTC (max_by)
    """
    # Xác định cột turnover_usd hoặc total_trade_value an toàn theo schema đầu vào
    if "turnover_usd" in df_1m.columns:
        turnover_col = col("turnover_usd")
    elif "total_trade_value" in df_1m.columns:
        turnover_col = col("total_trade_value")
    else:
        turnover_col = col("volume") * col("close")

    return (
        df_1m
        .withColumn("bucket_time", date_trunc(timeframe, col("window_start")))
        .groupBy("symbol", "bucket_time")
        .agg(
            min_by(col("open"), col("window_start")).alias("open"),
            spark_max("high").alias("high"),
            spark_min("low").alias("low"),
            max_by(col("close"), col("window_start")).alias("close"),
            spark_round(spark_sum("volume"), 4).alias("total_volume"),
            spark_sum("trade_count").alias("total_trades"),
            spark_round(spark_sum(turnover_col), 2).alias("turnover_usd"),
            spark_round(spark_sum("buy_volume"), 4).alias("buy_volume"),
            spark_round(spark_sum("sell_volume"), 4).alias("sell_volume")
        )
        .withColumn(
            "return_pct",
            spark_round(((col("close") - col("open")) / col("open")) * 100, 2)
        )
        .withColumn(
            "volatility_pct",
            spark_round(((col("high") - col("low")) / col("low")) * 100, 2)
        )
        .withColumn(
            "vwap",
            spark_round(col("turnover_usd") / when(col("total_volume") > 0, col("total_volume")).otherwise(1.0), 2)
        )
        .withColumn(
            "trade_imbalance",
            spark_round(
                when(
                    (col("buy_volume") + col("sell_volume")) > 0,
                    (col("buy_volume") - col("sell_volume")) / (col("buy_volume") + col("sell_volume"))
                ).otherwise(0.0),
                4
            )
        )
        .orderBy(col("bucket_time").desc(), col("symbol"))
    )


def aggregate_from_raw(df_raw, timeframe: str):
    """Tổng hợp đa khung thời gian trực tiếp từ Raw Trades"""
    time_col = "trade_time" if "trade_time" in df_raw.columns else "timestamp"
    val_col = "trade_value" if "trade_value" in df_raw.columns else (col("price") * col("size"))

    return (
        df_raw
        .withColumn("bucket_time", date_trunc(timeframe, col(time_col)))
        .groupBy("symbol", "bucket_time")
        .agg(
            min_by(col("price"), col(time_col)).alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            max_by(col("price"), col(time_col)).alias("close"),
            spark_round(spark_sum("size"), 4).alias("total_volume"),
            spark_count("price").alias("total_trades"),
            spark_round(spark_sum(val_col), 2).alias("turnover_usd"),
            spark_round(spark_sum(when(col("side") == "buy", col("size")).otherwise(0.0)), 4).alias("buy_volume"),
            spark_round(spark_sum(when(col("side") == "sell", col("size")).otherwise(0.0)), 4).alias("sell_volume")
        )
        .withColumn(
            "return_pct",
            spark_round(((col("close") - col("open")) / col("open")) * 100, 2)
        )
        .withColumn(
            "volatility_pct",
            spark_round(((col("high") - col("low")) / col("low")) * 100, 2)
        )
        .withColumn(
            "vwap",
            spark_round(col("turnover_usd") / when(col("total_volume") > 0, col("total_volume")).otherwise(1.0), 2)
        )
        .withColumn(
            "trade_imbalance",
            spark_round(
                when(
                    (col("buy_volume") + col("sell_volume")) > 0,
                    (col("buy_volume") - col("sell_volume")) / (col("buy_volume") + col("sell_volume"))
                ).otherwise(0.0),
                4
            )
        )
        .orderBy(col("bucket_time").desc(), col("symbol"))
    )


def process_and_export_timeframe(
    spark: SparkSession,
    df_source,
    source_type: str,
    bucket: str,
    timeframe: str,
    symbols: list = None,
    export_csv: bool = True,
    upload_s3_csv: bool = False
):
    """
    Xử lý tính toán chỉ số cho một khung thời gian (day/week/month/year),
    lưu Parquet lên Gold Medallion S3 và xuất file CSV cục bộ.
    """
    unit_map = {
        "day": ("summary_daily", "candles_1d", "NGAY (DAILY)"),
        "week": ("summary_weekly", "candles_1w", "TUAN (WEEKLY)"),
        "month": ("summary_monthly", "candles_1mo", "THANG (MONTHLY)"),
        "year": ("summary_yearly", "candles_1y", "NAM (YEARLY)")
    }
    legacy_folder, gold_folder, label = unit_map[timeframe]

    print(f"\n{'='*75}")
    print(f"  [GOLD LAYER ANALYTICS] TONG HOP THEO {label}")
    print(f"  Nguon du lieu: {source_type.upper()}")
    print(f"{'='*75}")

    if source_type == "1m":
        df_agg = aggregate_from_1m(df_source, timeframe)
    else:
        df_agg = aggregate_from_raw(df_source, timeframe)

    if symbols and len(symbols) == 1:
        df_agg = df_agg.filter(col("symbol") == symbols[0])

    # Hiển thị kết quả mẫu trên console
    df_agg.show(10, truncate=False)

    # 1. Lưu Gold Parquet vào S3 Data Lake (Partitioned by symbol only to avoid small files)
    s3_path_gold = f"s3a://{bucket}/crypto/gold/{gold_folder}"
    s3_path_legacy_processed = f"s3a://{bucket}/crypto/processed/{legacy_folder}"
    s3_path_legacy_batch = f"s3a://{bucket}/crypto/batch/{legacy_folder}"

    print(f"[*] Dang luu Gold Parquet vao S3: {s3_path_gold}")
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        (
            df_agg.write
            .mode("overwrite")
            .partitionBy("symbol")
            .parquet(s3_path_gold)
        )
        print(f"[OK] Da luu Gold Parquet vao {s3_path_gold}")

        # Đồng bộ thêm vào các đường dẫn legacy để không làm vỡ các module cũ
        (
            df_agg.write
            .mode("overwrite")
            .partitionBy("symbol")
            .parquet(s3_path_legacy_processed)
        )
        (
            df_agg.write
            .mode("overwrite")
            .partitionBy("symbol")
            .parquet(s3_path_legacy_batch)
        )
    except Exception as e:
        print(f"[!] Canh bao khi luu Parquet vao S3: {e}")

    # 2. Xuất file CSV cục bộ (data/exports/)
    if export_csv:
        symbol_suffix = symbols[0].lower() if (symbols and len(symbols) == 1) else "all"
        csv_filename = f"{legacy_folder}_{symbol_suffix}.csv"
        csv_out_path = project_root / "data" / "exports" / csv_filename
        csv_out_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"[*] Dang xuat file CSV cuc bo: {csv_out_path}")
        pandas_df = df_agg.toPandas()
        pandas_df.to_csv(str(csv_out_path), index=False, encoding="utf-8-sig")
        print(f"[SUCCESS] Da xuat {len(pandas_df)} ban ghi ra file CSV: {csv_out_path}")

        # Đồng bộ file summary_daily_all.csv nếu là chạy all
        if symbol_suffix != "all" and not (project_root / "data" / "exports" / f"{legacy_folder}_all.csv").exists():
            pandas_df.to_csv(str(project_root / "data" / "exports" / f"{legacy_folder}_all.csv"), index=False, encoding="utf-8-sig")

        # 3. Tùy chọn: Upload trực tiếp file CSV lên AWS S3
        if upload_s3_csv:
            s3_csv_path = f"s3a://{bucket}/crypto/exports/csv/{legacy_folder}_{symbol_suffix}"
            print(f"[*] Dang upload CSV truc tiep len S3: {s3_csv_path}")
            try:
                (
                    df_agg.coalesce(1).write
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(s3_csv_path)
                )
                print(f"[SUCCESS] Da upload CSV len S3 thanh cong tai: {s3_csv_path}")
            except Exception as e:
                print(f"[!] Loi khi upload CSV len S3: {e}")

    return df_agg


def run_batch_analytics_pipeline(
    timeframes: list,
    symbols: list = None,
    export_csv: bool = True,
    upload_s3_csv: bool = False
):
    """Chạy toàn bộ pipeline phân tích dữ liệu Batch Medallion"""
    bucket = os.getenv("S3_BUCKET_NAME", "").strip()
    if not bucket:
        print("[ERROR] S3_BUCKET_NAME chua duoc cau hinh trong .env!")
        return

    print("=" * 80)
    print("  CRYPTO BIG DATA - BATCH ANALYTICS ENGINE (SILVER -> GOLD PIPELINE)")
    print(f"  Bucket          : s3://{bucket}")
    print(f"  Target Frames   : {', '.join(timeframes)}")
    print(f"  Target Symbols  : {', '.join(symbols) if symbols else 'ALL'}")
    print(f"  Export Local CSV: {export_csv} (data/exports/)")
    print(f"  Upload S3 CSV   : {upload_s3_csv}")
    print("=" * 80)

    spark = create_spark_session(app_name="CryptoBatchAnalyticsEngineMedallion", include_kafka=False)

    try:
        df_source, source_type = get_batch_input_dataframe(spark, bucket, symbols=symbols)
        if df_source is None:
            print("\n[!] KHONG TIM THAY DU LIEU NAO TREN S3.")
            print("    -> Vui long chay: .\\scripts\\setup_env.ps1 -Backfill")
            return

        for tf in timeframes:
            process_and_export_timeframe(
                spark=spark,
                df_source=df_source,
                source_type=source_type,
                bucket=bucket,
                timeframe=tf,
                symbols=symbols,
                export_csv=export_csv,
                upload_s3_csv=upload_s3_csv
            )

        print("\n" + "=" * 80)
        print("[HOAN TAT] TOAN BO CAC TIEN TRINH PHAN TICH GOLD BATCH HOAN THANH CONG!")
        print(f"           Kiem tra S3 Gold tai: s3://{bucket}/crypto/gold/")
        print("=" * 80)

    except Exception as e:
        print(f"[ERROR] Loi trong qua trinh phan tich Batch: {e}")
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Crypto Batch Analytics Engine (Medallion Architecture)")
    parser.add_argument(
        "--timeframe",
        choices=["day", "week", "month", "year", "all"],
        default="all",
        help="Khung thoi gian can tong hop (mac dinh: all - Day, Week, Month, Year)"
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="Loc theo cap giao dich (BTC-USDT, ETH-USDT, SOL-USDT)"
    )
    parser.add_argument(
        "--all-symbols",
        action="store_true",
        help="Xu ly cho tat ca cac cap coin"
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Danh sach cac cap coin cach nhau boi dau phay"
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Khong xuat file CSV cuc bo"
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Dong thoi upload file CSV len S3"
    )

    args = parser.parse_args()
    if args.timeframe == "all":
        timeframes = ["day", "week", "month", "year"]
    else:
        timeframes = [args.timeframe]

    if args.symbol:
        syms = [args.symbol.strip().upper()]
    elif args.all_symbols:
        syms = list(SUPPORTED_SYMBOLS)
    elif args.symbols:
        syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        syms = None  # Process all available

    run_batch_analytics_pipeline(
        timeframes=timeframes,
        symbols=syms,
        export_csv=not args.no_csv,
        upload_s3_csv=args.upload_s3
    )


if __name__ == "__main__":
    main()
