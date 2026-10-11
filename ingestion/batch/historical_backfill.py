"""
=============================================================================
Crypto Big Data - Ingestion Layer (Batch)
Historical Candlestick Backfill Tool (OKX REST API -> AWS S3 Medallion Data Lake)
=============================================================================
Thu thập nến 1 phút lịch sử (1m Candlesticks) từ OKX REST API và nạp vào:
  - Bronze Data Lake: s3a://<bucket>/crypto/bronze/batch/okx/candles/ (Raw JSON payload + metadata)
  - Silver Data Lake: s3a://<bucket>/crypto/silver/candles/ (Standardized, deduplicated, Parquet)
  - Legacy Data Lake: s3a://<bucket>/crypto/batch/historical_candles_1m/ (Tuong thich nguoc)

Hỗ trợ:
  - Đa cặp coin: BTC-USDT, ETH-USDT, SOL-USDT độc lập (--symbol, --all-symbols)
  - Incremental Batch Ingestion (--incremental): Tự động phát hiện watermark mới nhất
  - Idempotent rerun: Khử trùng lặp theo (symbol, window_start)
=============================================================================
"""

import os
import sys
import time
import json
import csv
import argparse
import tempfile
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Setup paths
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
from pyspark.sql.functions import col, max as spark_max
from processing.core.schemas import CANDLE_1M_SCHEMA, BRONZE_CANDLE_SCHEMA, SILVER_CANDLE_SCHEMA
from processing.core.spark_session import create_spark_session, load_env_vars

load_env_vars()

OKX_REST_URL = os.getenv("OKX_REST_URL", "https://www.okx.com")
SUPPORTED_SYMBOLS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]


def fetch_okx_candles(symbol: str, bar: str = "1m", after: int = None, limit: int = 300):
    """
    Gọi OKX REST API lấy nến lịch sử.
    Endpoint: /api/v5/market/history-candles
    OKX hỗ trợ tối đa 300 nến cho mỗi request.
    after: Lấy các nến xảy ra trước timestamp này (phân trang lùi về quá khứ).
    """
    url = f"{OKX_REST_URL}/api/v5/market/history-candles?instId={symbol}&bar={bar}&limit={min(limit, 300)}"
    if after:
        url += f"&after={after}"

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CryptoDataBot/2.0"}
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        data = json.loads(response.read().decode("utf-8"))
        if data.get("code") == "0":
            return data.get("data", [])
        else:
            print(f"[!] OKX API Error ({symbol}): {data.get('msg')}")
            return []


def get_latest_candle_timestamp(spark: SparkSession, bucket: str, symbol: str):
    """
    Tìm timestamp nến mới nhất đã có trên S3 Data Lake của symbol này
    để hỗ trợ nạp dữ liệu gia tăng (Incremental Ingestion).
    """
    check_paths = [
        f"s3a://{bucket}/crypto/silver/candles",
        f"s3a://{bucket}/crypto/batch/historical_candles_1m"
    ]
    for p in check_paths:
        try:
            df = spark.read.parquet(p).filter(col("symbol") == symbol)
            if df.take(1):
                max_ts_val = df.select(spark_max("window_start")).collect()[0][0]
                if max_ts_val:
                    if isinstance(max_ts_val, str):
                        return datetime.fromisoformat(max_ts_val.replace("Z", "+00:00")).astimezone(timezone.utc)
                    elif isinstance(max_ts_val, datetime):
                        return max_ts_val.astimezone(timezone.utc) if max_ts_val.tzinfo else max_ts_val.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def collect_historical_candles_for_symbol(
    symbol: str,
    days: int = 7,
    max_records: int = None,
    start_time: datetime = None
):
    """
    Tải nến lịch sử của 1 cặp coin từ start_time (hoặc lùi 'days' ngày) đến hiện tại.
    Trả về cả raw Bronze payloads và standardized Silver records.
    """
    now = datetime.now(timezone.utc)
    if start_time is not None:
        target_start_time = start_time
        calc_days = max(1, (now - target_start_time).days + 1)
    else:
        target_start_time = now - timedelta(days=days)
        calc_days = days

    target_start_ts = int(target_start_time.timestamp() * 1000)
    expected_candles = max_records if max_records else (calc_days * 1440)

    print(f"\n[*] Thu thap du lieu lich su: {symbol}")
    print(f"    Khoang thoi gian : Tu {target_start_time.strftime('%Y-%m-%d %H:%M')} den {now.strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"    Muc tieu ly thuyet: {expected_candles:,} nen 1m")

    silver_records = []
    bronze_records = []
    after_ts = None
    batch_count = 0
    now_utc_str = now.strftime("%Y-%m-%d %H:%M:%S")

    while True:
        try:
            candles = fetch_okx_candles(symbol=symbol, bar="1m", after=after_ts, limit=300)
            if not candles:
                break

            for c in candles:
                ts = int(c[0])
                if ts < target_start_ts:
                    break

                # 1. Bronze raw payload preservation
                bronze_record = {
                    "symbol": symbol,
                    "ts": ts,
                    "raw_json": json.dumps(c),
                    "source": "OKX_REST_API",
                    "ingestion_time": now_utc_str
                }
                bronze_records.append(bronze_record)

                # 2. Silver standardized record
                o = float(c[1])
                h = float(c[2])
                l = float(c[3])
                cl = float(c[4])
                vol = float(c[5])
                vol_ccy = float(c[6]) if len(c) > 6 and c[6] else (vol * cl)

                w_start = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
                w_end = datetime.fromtimestamp((ts + 60000) / 1000.0, tz=timezone.utc)
                vwap = (vol_ccy / vol) if vol > 0 else (o + h + l + cl) / 4.0

                silver_record = {
                    "symbol": symbol,
                    "window_start": w_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "window_end": w_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": cl,
                    "volume": vol,
                    "turnover_usd": round(vol_ccy, 4),
                    "vwap": round(vwap, 4),
                    "trade_count": 1,
                    "buy_volume": round(vol / 2.0, 4),
                    "sell_volume": round(vol / 2.0, 4),
                    "trade_imbalance": 0.0,
                    "total_trade_value": round(vol_ccy, 4),
                    "source": "OKX_REST_API",
                    "ingestion_time": now_utc_str
                }
                silver_records.append(silver_record)

            oldest_candle_ts = int(candles[-1][0])
            after_ts = oldest_candle_ts
            batch_count += 1

            if max_records and len(silver_records) >= max_records:
                silver_records = silver_records[:max_records]
                bronze_records = bronze_records[:max_records]
                break

            if oldest_candle_ts < target_start_ts:
                break

            if batch_count % 3 == 0 or len(silver_records) >= expected_candles:
                pct = min(100.0, (len(silver_records) / max(1, expected_candles)) * 100.0)
                oldest_str = datetime.fromtimestamp(oldest_candle_ts / 1000.0, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
                print(f"    -> Da thu thap: {len(silver_records):,} / {expected_candles:,} nen ({pct:.1f}%) | Moc cu nhat: {oldest_str} UTC")

            time.sleep(0.12)

        except Exception as e:
            print(f"[!] Loi khi goi API cho {symbol}: {e}. Nghi 2s roi thu lai...")
            time.sleep(2)

    print(f"[OK] Hoan tat thu thap {symbol}: Tong cong {len(silver_records):,} nen 1m.")
    return bronze_records, silver_records


def save_medallion_s3(spark: SparkSession, bronze_records: list, silver_records: list, bucket: str, symbols: list):
    """
    Lưu trữ dữ liệu vào Medallion Data Lake trên AWS S3:
      - Bronze: s3a://{bucket}/crypto/bronze/batch/okx/candles/ (Partition by symbol)
      - Silver: s3a://{bucket}/crypto/silver/candles/ (Partition by symbol, deduplicated)
      - Legacy: s3a://{bucket}/crypto/batch/historical_candles_1m/ (Tuong thich nguoc)
    """
    if not silver_records:
        print("[!] Khong co ban ghi nao de ghi len S3.")
        return

    # Kích hoạt partition dynamic overwrite để khi ghi một symbol không xoá các symbol khác
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # 1. Ghi Bronze Data Lake
    print(f"\n[*] [BRONZE LAYER] Dang ghi {len(bronze_records):,} ban ghi Raw JSON...")
    bronze_path = f"s3a://{bucket}/crypto/bronze/batch/okx/candles"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp_bronze:
        for b in bronze_records:
            tmp_bronze.write(json.dumps(b) + "\n")
        tmp_bronze_file = tmp_bronze.name

    try:
        df_bronze = spark.read.schema(BRONZE_CANDLE_SCHEMA).json(tmp_bronze_file.replace("\\", "/"))
        (
            df_bronze.write
            .mode("overwrite")
            .partitionBy("symbol")
            .parquet(bronze_path)
        )
        print(f"[OK] Da luu Bronze Data Lake tai: {bronze_path}")
    except Exception as e:
        print(f"[!] Canh bao khi ghi Bronze S3: {e}")
    finally:
        if os.path.exists(tmp_bronze_file):
            try:
                os.remove(tmp_bronze_file)
            except Exception:
                pass

    # 2. Ghi Silver Data Lake
    print(f"\n[*] [SILVER LAYER] Dang xu ly lam sach & khu trung lap cho {len(silver_records):,} nen...")
    silver_path = f"s3a://{bucket}/crypto/silver/candles"
    legacy_path = f"s3a://{bucket}/crypto/batch/historical_candles_1m"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp_silver:
        for s in silver_records:
            tmp_silver.write(json.dumps(s) + "\n")
        tmp_silver_file = tmp_silver.name

    try:
        df_new_silver = spark.read.schema(CANDLE_1M_SCHEMA).json(tmp_silver_file.replace("\\", "/"))

        # Kiểm tra nếu partition đã có dữ liệu cũ thì hợp nhất và khử trùng lặp
        try:
            df_existing = spark.read.schema(CANDLE_1M_SCHEMA).parquet(silver_path)
            # Chỉ lấy các symbol đang xử lý để hợp nhất
            df_filtered_existing = df_existing.filter(col("symbol").isin(symbols))
            df_final_silver = df_filtered_existing.unionByName(df_new_silver, allowMissingColumns=True)
            df_final_silver = df_final_silver.dropDuplicates(["symbol", "window_start"])
        except Exception:
            df_final_silver = df_new_silver.dropDuplicates(["symbol", "window_start"])

        (
            df_final_silver.write
            .mode("overwrite")
            .partitionBy("symbol")
            .parquet(silver_path)
        )
        print(f"[OK] Da luu Silver Data Lake tai: {silver_path}")

        # Đồng bộ đường dẫn legacy để tương thích ngược 100%
        (
            df_final_silver.write
            .mode("overwrite")
            .partitionBy("symbol")
            .parquet(legacy_path)
        )
        print(f"[OK] Da dong bo Legacy Data Lake tai: {legacy_path}")

    except Exception as e:
        print(f"[!] Loi khi ghi Silver S3: {e}")
    finally:
        if os.path.exists(tmp_silver_file):
            try:
                os.remove(tmp_silver_file)
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="Crypto Historical Candlestick Backfill (OKX -> S3 Medallion Data Lake)")
    parser.add_argument("--symbol", type=str, default=None, help="Chi dinh 1 cap coin cu the (vi du: BTC-USDT)")
    parser.add_argument("--all-symbols", action="store_true", help="Chay toan bo cac cap coin ho tro (BTC-USDT, ETH-USDT, SOL-USDT)")
    parser.add_argument("--symbols", type=str, default=None, help="Danh sach coin cach nhau boi dau phay (tuong thich cu)")
    parser.add_argument("--days", type=int, default=7, help="So ngay lich su can lay (mac dinh: 7 ngay)")
    parser.add_argument("--incremental", action="store_true", help="Nap du lieu gia tang tu watermark moi nhat tren S3")
    parser.add_argument("--max-records", type=int, default=None, help="Gioi han toi da so ban ghi tren moi coin")
    parser.add_argument("--local-preview", action="store_true", help="Chi in ra xem truoc, khong ghi len S3")
    parser.add_argument("--export-csv", type=str, default=None, help="Xuat ket qua thu thap ra file CSV cuc bo")
    parser.add_argument("--no-s3", action="store_true", help="Bo qua ghi len S3 (dung khi chi muon xuat CSV)")

    args = parser.parse_args()

    # Xác định danh sách symbols
    if args.symbol:
        symbols = [args.symbol.strip().upper()]
    elif args.all_symbols:
        symbols = list(SUPPORTED_SYMBOLS)
    elif args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = list(SUPPORTED_SYMBOLS)

    bucket = os.getenv("S3_BUCKET_NAME", "").strip()

    print("=" * 80)
    print("  CRYPTO HISTORICAL BACKFILL (OKX -> AWS S3 MEDALLION ARCHITECTURE)")
    print(f"  Target Symbols   : {', '.join(symbols)}")
    print(f"  Mode             : {'INCREMENTAL' if args.incremental else 'FULL BACKFILL'}")
    print(f"  Lookback Window  : {args.days} days")
    print(f"  Target S3 Bucket : s3://{bucket}")
    if args.export_csv:
        print(f"  Export CSV       : {args.export_csv}")
    print("=" * 80)

    if not bucket and not args.local_preview and not args.no_s3 and not args.export_csv:
        print("[ERROR] S3_BUCKET_NAME chua duoc cau hinh trong .env!")
        sys.exit(1)

    spark = None
    if not (args.local_preview or args.no_s3):
        spark = create_spark_session(app_name="CryptoHistoricalBackfillMedallion", include_kafka=False)

    all_bronze_records = []
    all_silver_records = []

    try:
        for sym in symbols:
            start_time = None
            if args.incremental and spark and bucket:
                latest_ts = get_latest_candle_timestamp(spark, bucket, sym)
                if latest_ts:
                    now = datetime.now(timezone.utc)
                    if latest_ts >= (now - timedelta(minutes=2)):
                        print(f"[*] Symbol {sym} da dat moc thoi gian moi nhat ({latest_ts.strftime('%Y-%m-%d %H:%M')} UTC). Bo qua thu thap.")
                        continue
                    start_time = latest_ts
                    print(f"[*] [INCREMENTAL] Phat hien moc watermark cua {sym}: {start_time.strftime('%Y-%m-%d %H:%M')} UTC")

            bronze_recs, silver_recs = collect_historical_candles_for_symbol(
                symbol=sym,
                days=args.days,
                max_records=args.max_records,
                start_time=start_time
            )
            all_bronze_records.extend(bronze_recs)
            all_silver_records.extend(silver_recs)

        print(f"\n[*] TONG SO NEN 1M THU THAP DUOC: {len(all_silver_records):,}")

        # Xuất CSV cục bộ nếu được chỉ định
        if args.export_csv and all_silver_records:
            csv_path = Path(args.export_csv)
            if not csv_path.is_absolute():
                csv_path = project_root / csv_path
            csv_path.parent.mkdir(parents=True, exist_ok=True)

            fieldnames = list(all_silver_records[0].keys())
            with open(csv_path, mode="w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_silver_records)
            print(f"[SUCCESS] Da xuat thanh cong {len(all_silver_records):,} dong ra CSV: {csv_path}")

        if args.local_preview or args.no_s3 or not bucket:
            print("[*] Hoan tat che do cuc bo, khong ghi len S3.")
            return

        if spark and all_silver_records:
            save_medallion_s3(spark, all_bronze_records, all_silver_records, bucket, symbols)

    finally:
        if spark:
            spark.stop()
        print("\n" + "=" * 80)
        print("HISTORICAL BACKFILL PIPELINE HOAN TAT.")
        print("=" * 80)


if __name__ == "__main__":
    main()
