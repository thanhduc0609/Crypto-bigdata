"""
=============================================================================
Crypto Big Data - Test AWS S3 Connection & Initialize Streaming Lake Folders
=============================================================================
Script kiểm tra kết nối AWS S3 và tự động khởi tạo cây thư mục Streaming:
  - s3a://<S3_BUCKET>/crypto/streaming/trades/ (phân vùng theo symbol)
  - s3a://<S3_BUCKET>/crypto/streaming/candles_1m/ (phân vùng theo symbol)
=============================================================================
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

try:
    sys.setrecursionlimit(50000)
except Exception:
    pass

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import create_spark_session, load_env_vars

load_env_vars()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "")

print("=" * 75, flush=True)
print("TEST AWS S3 CONNECTION & INITIALIZE STREAMING DATA LAKE FOLDERS", flush=True)
print(f"Bucket     : {S3_BUCKET}", flush=True)
print(f"Region     : {AWS_REGION}", flush=True)
print(f"Access Key : {AWS_ACCESS_KEY[:4]}***" if AWS_ACCESS_KEY else "Access Key : (Not Configured)", flush=True)
print("=" * 75, flush=True)

if not S3_BUCKET or not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    print("[ERROR] Please configure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and S3_BUCKET_NAME in .env file!", flush=True)
    sys.exit(1)

spark = create_spark_session(app_name="InitS3StreamingLake", include_kafka=False)

try:
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # -------------------------------------------------------------------------
    # 1. KHỞI TẠO CẤU TRÚC: crypto/streaming/trades/
    # -------------------------------------------------------------------------
    raw_sql = f"""
    SELECT 
        'trade_init_btc' AS trade_id, 'BTC-USDT' AS symbol, 86500.0 AS price, 0.05 AS size, 
        'buy' AS side, 4325.0 AS trade_value, '{today_str}' AS trade_date, {now_ts}L AS raw_timestamp
    """
    raw_df = spark.sql(raw_sql)

    trades_path = os.getenv("S3_RAW_PATH", f"s3a://{S3_BUCKET}/crypto/streaming/trades")
    if "${S3_BUCKET_NAME}" in trades_path:
        trades_path = trades_path.replace("${S3_BUCKET_NAME}", S3_BUCKET)
    print(f"\n[1/2] Dang tao cay thu muc Raw Trades Parquet: {trades_path} ...", flush=True)
    raw_df.write.mode("append").partitionBy("symbol").parquet(trades_path)
    print(f" [OK] Da tao thanh cong thu muc: crypto/streaming/trades/ (phan vung symbol)", flush=True)

    # -------------------------------------------------------------------------
    # 2. KHỞI TẠO CẤU TRÚC: crypto/streaming/candles_1m/
    # -------------------------------------------------------------------------
    candle_sql = f"""
    SELECT 
        'BTC-USDT' AS symbol, 
        to_timestamp('{today_str} 14:00:00') AS window_start, 
        to_timestamp('{today_str} 14:01:00') AS window_end, 
        86450.0 AS open, 86520.0 AS high, 86400.0 AS low, 86500.0 AS close, 
        15.2 AS volume, 120L AS trade_count, 86490.0 AS vwap, 0.15 AS trade_imbalance, 1314648.0 AS total_trade_value
    """
    candle_df = spark.sql(candle_sql)

    candles_path = os.getenv("S3_PROCESSED_PATH", f"s3a://{S3_BUCKET}/crypto/streaming/candles_1m")
    if "${S3_BUCKET_NAME}" in candles_path:
        candles_path = candles_path.replace("${S3_BUCKET_NAME}", S3_BUCKET)
    print(f"\n[2/2] Dang tao cay thu muc 1m Candles Parquet: {candles_path} ...", flush=True)
    candle_df.write.mode("append").partitionBy("symbol").parquet(candles_path)
    print(f" [OK] Da tao thanh cong thu muc: crypto/streaming/candles_1m/ (phan vung symbol)", flush=True)

    # -------------------------------------------------------------------------
    # 3. KIỂM TRA ĐỌC LẠI
    # -------------------------------------------------------------------------
    print("\n--- KIEM TRA DOC DU LIEU TU S3 DATA LAKE ---", flush=True)
    read_trades = spark.read.parquet(trades_path)
    print(f"[*] Tong so ban ghi Raw Trades tren S3 : {read_trades.count()}", flush=True)

    read_candles = spark.read.parquet(candles_path)
    print(f"[*] Tong so ban ghi Nen 1m tren S3     : {read_candles.count()}", flush=True)

    print("\n" + "=" * 75, flush=True)
    print("[SUCCESS] KHOI TAO CAY THU MUC S3 DATA LAKE THANH CONG RUC RO!", flush=True)
    print(f"Vao AWS S3 Console kiem tra bucket [{S3_BUCKET}], ban se thay:", flush=True)
    print(f"  * crypto/streaming/trades/", flush=True)
    print(f"  * crypto/streaming/candles_1m/", flush=True)
    print("=" * 75, flush=True)

except Exception as e:
    print(f"\n[ERROR] Loi khi thao tac tren S3: {e}", flush=True)
finally:
    spark.stop()
