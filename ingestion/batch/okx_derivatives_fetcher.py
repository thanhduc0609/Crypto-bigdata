"""
=============================================================================
Crypto Big Data - Ingestion & Cleansing Layer (Batch Derivatives)
OKX Derivatives Data Fetcher & Cleanser (Funding Rate, Open Interest)
=============================================================================
Pipeline bao gồm:
1. Ingestion: Thu thập dữ liệu thô từ OKX REST API v5 (Funding Rate, Open Interest)
2. Bronze Layer: Lưu trữ nguyên trạng raw API payload lên AWS S3:
     - s3://<bucket>/crypto/bronze/batch/okx/funding_rates/symbol=<SYM>/
     - s3://<bucket>/crypto/bronze/batch/okx/open_interest/symbol=<SYM>/
3. Data Cleansing & Silver Layer:
     - Chuẩn hóa Symbol (bỏ hậu tố -SWAP)
     - Khử khuyết thiếu, kẹp ngưỡng anomaly
     - Chuyển đổi timestamp Unix millisecond sang UTC
     - Lưu trữ Silver Parquet/JSON lên AWS S3:
         - s3://<bucket>/crypto/silver/funding_rates/symbol=<SYM>/
         - s3://<bucket>/crypto/silver/open_interest/symbol=<SYM>/
4. Warehouse & Dashboard Persistence:
     - UPSERT vào PostgreSQL Data Warehouse (fact_crypto_funding_rates, fact_crypto_open_interest)
     - Xuất dữ liệu JSON/CSV vào data/exports/ phục vụ Web Dashboard & tương thích ngược
=============================================================================
"""

import os
import sys
import time
import json
import csv
import argparse
import tempfile
import subprocess
import urllib.request
from pathlib import Path
from datetime import datetime, timezone
import psycopg2

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from processing.core.spark_session import load_env_vars

load_env_vars()

OKX_REST_URL = os.getenv("OKX_REST_URL", "https://www.okx.com")
SUPPORTED_SYMBOLS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]

# Giới hạn vật lý tối đa của OKX Funding Rate (±0.375% thông thường, kẹp ±0.75% cho anomaly guard)
OKX_FUNDING_RATE_MAX = 0.0075
OKX_FUNDING_RATE_MIN = -0.0075


def get_dw_connection():
    """Tạo kết nối tới PostgreSQL Data Warehouse"""
    try:
        host = os.getenv("POSTGRES_HOST") or os.getenv("DW_HOST", "127.0.0.1")
        port = int(os.getenv("POSTGRES_PORT") or os.getenv("DW_PORT", "5433"))
        db = os.getenv("POSTGRES_DB") or os.getenv("DW_DB", "crypto_dw")
        user = os.getenv("POSTGRES_USER") or os.getenv("DW_USER", "postgres")
        pwd = os.getenv("POSTGRES_PASSWORD") or os.getenv("DW_PASSWORD", "password123")
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=pwd,
            connect_timeout=5
        )
        return conn
    except Exception as e:
        print(f"  [!] Khong the ket noi PostgreSQL Data Warehouse: {e}")
        return None


def fetch_json(url: str):
    """Gửi HTTP GET Request tới OKX REST API với timeout và custom User-Agent"""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CryptoBigData/2.0"}
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def safe_float(val, default=0.0):
    """Chuyển đổi an toàn sang float, xử lý chuỗi rỗng '' hoặc None từ OKX REST API"""
    if val is None or val == "":
        return float(default)
    try:
        return float(val)
    except (ValueError, TypeError):
        return float(default)


def safe_int(val, default=0):
    """Chuyển đổi an toàn sang int, xử lý chuỗi rỗng '' hoặc None từ OKX REST API"""
    if val is None or val == "":
        return int(default)
    try:
        return int(val)
    except (ValueError, TypeError):
        return int(default)


# -----------------------------------------------------------------------------
# DATA CLEANSING & TRANSFORMATION FUNCTIONS
# -----------------------------------------------------------------------------

def clean_funding_rate_record(raw_item: dict, inst_id: str) -> dict:
    """
    Quy trình Làm sạch (Cleansing) cho Funding Rate:
    1. Chuẩn hóa Symbol: Loại bỏ hậu tố '-SWAP'
    2. Imputation: Bù đắp 'nextFundingRate' bằng 'fundingRate' hiện tại nếu rỗng
    3. Outlier Clamping: Kẹp giá trị vào khoảng cho phép [-0.0075, 0.0075]
    4. Timestamp Standardizer: Đổi millisecond epoch sang datetime UTC
    5. Feature Engineering: Tính % 8h, APY năm và Market Regime
    """
    symbol = inst_id.replace("-SWAP", "").strip()

    raw_fr = safe_float(raw_item.get("fundingRate"), 0.0001)
    raw_next_fr = safe_float(raw_item.get("nextFundingRate"), raw_fr)

    # 1. Outlier Guard & Clamping
    is_outlier = False
    if raw_fr > OKX_FUNDING_RATE_MAX or raw_fr < OKX_FUNDING_RATE_MIN:
        is_outlier = True
    clean_fr = max(min(raw_fr, OKX_FUNDING_RATE_MAX), OKX_FUNDING_RATE_MIN)
    clean_next_fr = max(min(raw_next_fr, OKX_FUNDING_RATE_MAX), OKX_FUNDING_RATE_MIN)

    # 2. Timestamp Cleansing
    raw_ft = safe_int(raw_item.get("fundingTime"), 0)
    raw_nft = safe_int(raw_item.get("nextFundingTime"), 0)

    now_utc = datetime.now(timezone.utc)
    if raw_ft > 0:
        funding_time = datetime.fromtimestamp(raw_ft / 1000.0, tz=timezone.utc)
    else:
        funding_time = now_utc

    if raw_nft > 0:
        next_funding_time = datetime.fromtimestamp(raw_nft / 1000.0, tz=timezone.utc)
    else:
        next_funding_time = datetime.fromtimestamp(funding_time.timestamp() + 28800, tz=timezone.utc)

    # 3. Derived Metrics & Sentiment Classification
    fr_pct = clean_fr * 100.0
    next_fr_pct = clean_next_fr * 100.0
    projected_apy_pct = clean_fr * 3 * 365 * 100.0

    if clean_fr > 0.0001:
        market_regime = "BULLISH_OVERHEATED"
        regime_desc = "Phe Long áp đảo (Long trả phí cho Short)"
    elif clean_fr < -0.0001:
        market_regime = "BEARISH_PANIC"
        regime_desc = "Phe Short áp đảo (Short trả phí cho Long)"
    else:
        market_regime = "NEUTRAL_BALANCED"
        regime_desc = "Thị trường cân bằng"

    return {
        "symbol": symbol,
        "instrument": inst_id,
        "funding_time": funding_time,
        "next_funding_time": next_funding_time,
        "funding_rate": clean_fr,
        "funding_rate_pct": fr_pct,
        "predicted_funding_rate": clean_next_fr,
        "predicted_funding_rate_pct": next_fr_pct,
        "projected_apy_pct": projected_apy_pct,
        "market_regime": market_regime,
        "regime_desc": regime_desc,
        "is_outlier": is_outlier,
        "exchange": "OKX",
        "collected_at": now_utc
    }


def clean_open_interest_record(raw_item: dict, inst_id: str) -> dict:
    """
    Quy trình Làm sạch (Cleansing) cho Open Interest:
    1. Chuẩn hóa Symbol: Loại bỏ '-SWAP'
    2. Sanity Check: OI contracts >= 0 và OI USD >= 0
    3. Timestamp Standardizer: Millisecond epoch -> UTC datetime
    """
    symbol = inst_id.replace("-SWAP", "").strip()
    oi_contracts = max(safe_float(raw_item.get("oi"), 0.0), 0.0)
    oi_usd = max(safe_float(raw_item.get("oiCcy"), 0.0), 0.0)

    raw_ts = safe_int(raw_item.get("ts"), 0)
    now_utc = datetime.now(timezone.utc)
    if raw_ts > 0:
        snapshot_time = datetime.fromtimestamp(raw_ts / 1000.0, tz=timezone.utc)
    else:
        snapshot_time = now_utc

    avg_contract_val = (oi_usd / oi_contracts) if oi_contracts > 0 else 0.0

    return {
        "symbol": symbol,
        "instrument": inst_id,
        "snapshot_time": snapshot_time,
        "oi_contracts": oi_contracts,
        "oi_usd": oi_usd,
        "avg_contract_val_usd": avg_contract_val,
        "exchange": "OKX",
        "collected_at": now_utc
    }


# -----------------------------------------------------------------------------
# PERSISTENCE LAYER: WAREHOUSE, LOCAL EXPORTS & S3 MEDALLION
# -----------------------------------------------------------------------------

def save_to_warehouse(funding_records: list, oi_records: list):
    """
    Ghi dữ liệu đã làm sạch vào PostgreSQL Data Warehouse bằng cơ chế UPSERT:
    - fact_crypto_funding_rates: Khử trùng lặp theo (symbol, funding_time)
    - fact_crypto_open_interest: Khử trùng lặp theo (symbol, snapshot_time)
    """
    conn = get_dw_connection()
    if not conn:
        print("  [!] Bo qua nap DW vi khong co ket noi DB.")
        return 0, 0

    inserted_funding = 0
    inserted_oi = 0

    try:
        cur = conn.cursor()

        # 1. Upsert Funding Rates
        for f in funding_records:
            cur.execute("""
                INSERT INTO warehouse.fact_crypto_funding_rates (
                    symbol, funding_time, next_funding_time, funding_rate, 
                    predicted_funding_rate, exchange, collected_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, funding_time) DO UPDATE SET
                    predicted_funding_rate = EXCLUDED.predicted_funding_rate,
                    collected_at = EXCLUDED.collected_at;
            """, (
                f["symbol"],
                f["funding_time"],
                f["next_funding_time"],
                f["funding_rate"],
                f["predicted_funding_rate"],
                f["exchange"],
                f["collected_at"]
            ))
            inserted_funding += 1

        # 2. Upsert Open Interest
        for oi in oi_records:
            cur.execute("""
                INSERT INTO warehouse.fact_crypto_open_interest (
                    symbol, snapshot_time, oi_contracts, oi_usd, exchange, collected_at
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, snapshot_time) DO UPDATE SET
                    oi_contracts = EXCLUDED.oi_contracts,
                    oi_usd = EXCLUDED.oi_usd,
                    collected_at = EXCLUDED.collected_at;
            """, (
                oi["symbol"],
                oi["snapshot_time"],
                oi["oi_contracts"],
                oi["oi_usd"],
                oi["exchange"],
                oi["collected_at"]
            ))
            inserted_oi += 1

        conn.commit()
        conn.close()
        print(f"  [OK] Da UPSERT thanh cong vao PostgreSQL DW: {inserted_funding} Funding Rates, {inserted_oi} Open Interest records.")
    except Exception as e:
        print(f"  [!] Loi khi ghi PostgreSQL DW: {e}")
        if conn:
            conn.rollback()
            conn.close()

    return inserted_funding, inserted_oi


def export_to_datalake(funding_records: list, oi_records: list):
    """Lưu trữ dữ liệu phái sinh đã làm sạch ra Data Lake (data/exports) định dạng JSON & CSV"""
    export_dir = project_root / "data" / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)

    # 1. Export JSON cho Web Dashboard & API
    combined_json = {}
    for f in funding_records:
        sym = f["symbol"]
        matching_oi = next((o for o in oi_records if o["symbol"] == sym), None)
        combined_json[sym] = {
            "symbol": sym,
            "instrument": f["instrument"],
            "funding_rate": f["funding_rate"],
            "funding_rate_pct": round(f["funding_rate_pct"], 6),
            "predicted_funding_rate": f["predicted_funding_rate"],
            "predicted_funding_rate_pct": round(f["predicted_funding_rate_pct"], 6),
            "projected_apy_pct": round(f["projected_apy_pct"], 2),
            "market_regime": f["market_regime"],
            "regime_desc": f["regime_desc"],
            "funding_time": f["funding_time"].isoformat(),
            "next_funding_time": f["next_funding_time"].isoformat(),
            "oi_contracts": matching_oi["oi_contracts"] if matching_oi else 0.0,
            "oi_usd": matching_oi["oi_usd"] if matching_oi else 0.0,
            "updated_at": f["collected_at"].isoformat()
        }

    json_path = export_dir / "derivatives_latest.json"
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(combined_json, jf, indent=2, ensure_ascii=False)

    # 2. Export CSV Lịch sử Funding Rates
    csv_funding_path = export_dir / "fact_crypto_funding_rates.csv"
    with open(csv_funding_path, "w", newline="", encoding="utf-8") as cf:
        writer = csv.writer(cf)
        writer.writerow(["symbol", "funding_time", "next_funding_time", "funding_rate", "predicted_funding_rate", "projected_apy_pct", "market_regime", "exchange", "collected_at"])
        for f in funding_records:
            writer.writerow([
                f["symbol"],
                f["funding_time"].isoformat(),
                f["next_funding_time"].isoformat(),
                f["funding_rate"],
                f["predicted_funding_rate"],
                round(f["projected_apy_pct"], 2),
                f["market_regime"],
                f["exchange"],
                f["collected_at"].isoformat()
            ])

    # 3. Export CSV Open Interest
    csv_oi_path = export_dir / "fact_crypto_open_interest.csv"
    with open(csv_oi_path, "w", newline="", encoding="utf-8") as co:
        writer = csv.writer(co)
        writer.writerow(["symbol", "snapshot_time", "oi_contracts", "oi_usd", "avg_contract_val_usd", "exchange", "collected_at"])
        for o in oi_records:
            writer.writerow([
                o["symbol"],
                o["snapshot_time"].isoformat(),
                o["oi_contracts"],
                o["oi_usd"],
                round(o["avg_contract_val_usd"], 4),
                o["exchange"],
                o["collected_at"].isoformat()
            ])

    print(f"  [OK] Da xuat du lieu cuc bo: {json_path.name}, {csv_funding_path.name}, {csv_oi_path.name}")


def save_derivatives_to_s3(raw_funding_records: list, raw_oi_records: list, clean_funding_records: list, clean_oi_records: list, bucket: str):
    """
    Lưu trữ phái sinh vào S3 Medallion Architecture:
      - Bronze: s3://<bucket>/crypto/bronze/batch/okx/funding_rates/symbol=<SYM>/ & open_interest/symbol=<SYM>/
      - Silver: s3://<bucket>/crypto/silver/funding_rates/symbol=<SYM>/ & open_interest/symbol=<SYM>/
    """
    if not bucket:
        return

    print(f"\n[*] [MEDALLION S3] Dang luu phai sinh len S3 Data Lake (s3://{bucket})...")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # 1. Bronze Funding Rates
        for r in raw_funding_records:
            sym = r["symbol"]
            b_file = tmp_path / f"bronze_fr_{sym}.jsonl"
            with open(b_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(r) + "\n")
            s3_dest = f"s3://{bucket}/crypto/bronze/batch/okx/funding_rates/symbol={sym}/data.jsonl"
            try:
                subprocess.run(["aws", "s3", "cp", str(b_file), s3_dest], capture_output=True, check=True)
            except Exception as e:
                print(f"  [!] Khong the upload Bronze FR len S3: {e}")

        # 2. Bronze Open Interest
        for r in raw_oi_records:
            sym = r["symbol"]
            b_file = tmp_path / f"bronze_oi_{sym}.jsonl"
            with open(b_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(r) + "\n")
            s3_dest = f"s3://{bucket}/crypto/bronze/batch/okx/open_interest/symbol={sym}/data.jsonl"
            try:
                subprocess.run(["aws", "s3", "cp", str(b_file), s3_dest], capture_output=True, check=True)
            except Exception as e:
                print(f"  [!] Khong the upload Bronze OI len S3: {e}")

        # 3. Silver Funding Rates
        for f in clean_funding_records:
            sym = f["symbol"]
            s_file = tmp_path / f"silver_fr_{sym}.jsonl"
            record_copy = dict(f)
            record_copy["funding_time"] = f["funding_time"].isoformat()
            record_copy["next_funding_time"] = f["next_funding_time"].isoformat()
            record_copy["collected_at"] = f["collected_at"].isoformat()
            with open(s_file, "a", encoding="utf-8") as out:
                out.write(json.dumps(record_copy) + "\n")
            s3_dest = f"s3://{bucket}/crypto/silver/funding_rates/symbol={sym}/data.jsonl"
            try:
                subprocess.run(["aws", "s3", "cp", str(s_file), s3_dest], capture_output=True, check=True)
            except Exception as e:
                print(f"  [!] Khong the upload Silver FR len S3: {e}")

        # 4. Silver Open Interest
        for o in clean_oi_records:
            sym = o["symbol"]
            s_file = tmp_path / f"silver_oi_{sym}.jsonl"
            record_copy = dict(o)
            record_copy["snapshot_time"] = o["snapshot_time"].isoformat()
            record_copy["collected_at"] = o["collected_at"].isoformat()
            with open(s_file, "a", encoding="utf-8") as out:
                out.write(json.dumps(record_copy) + "\n")
            s3_dest = f"s3://{bucket}/crypto/silver/open_interest/symbol={sym}/data.jsonl"
            try:
                subprocess.run(["aws", "s3", "cp", str(s_file), s3_dest], capture_output=True, check=True)
            except Exception as e:
                print(f"  [!] Khong the upload Silver OI len S3: {e}")

    print("  [OK] Da cap nhat Bronze & Silver Derivatives len AWS S3 thanh cong.")


# -----------------------------------------------------------------------------
# MAIN PIPELINE EXECUTION
# -----------------------------------------------------------------------------

def run_batch_fetch(symbols: list = None, save_s3: bool = True, save_dw: bool = True):
    symbols = symbols or SUPPORTED_SYMBOLS
    bucket = os.getenv("S3_BUCKET_NAME", "").strip()

    print("=" * 80)
    print("CRYPTO BIG DATA - DERIVATIVES INGESTION & CLEANSING PIPELINE (OKX)")
    print("Target Symbols     :", ", ".join(symbols))
    print("Cleansing Rules    : Symbol strip, Imputation, Anomaly clamping, UTC Timestamp")
    print(f"Storage Targets    : PostgreSQL DW & AWS S3 ({'s3://' + bucket if bucket else 'N/A'})")
    print("=" * 80)

    clean_funding_records = []
    clean_oi_records = []
    raw_funding_records = []
    raw_oi_records = []
    now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for sym in symbols:
        inst_id = f"{sym}-SWAP"
        print(f"\n[*] Dang cao du lieu tho cho: {sym} ({inst_id})")

        # 1. Cao Funding Rate
        f_url = f"{OKX_REST_URL}/api/v5/public/funding-rate?instId={inst_id}"
        f_raw = fetch_json(f_url)
        if f_raw.get("code") == "0" and f_raw.get("data"):
            raw_item = f_raw["data"][0]
            raw_funding_records.append({
                "symbol": sym,
                "instrument": inst_id,
                "raw_json": json.dumps(raw_item),
                "source": "OKX_REST_API",
                "ingestion_time": now_utc_str
            })
            clean_f = clean_funding_rate_record(raw_item, inst_id)
            clean_funding_records.append(clean_f)
            print(f"  [CLEAN] Funding Rate   : {clean_f['funding_rate']:.6f} ({clean_f['funding_rate_pct']:+.4f}%/8h) | Next: {clean_f['predicted_funding_rate_pct']:+.4f}%")
            print(f"  [CLEAN] Phan loai trang thai: {clean_f['market_regime']} ({clean_f['regime_desc']}) | APY: {clean_f['projected_apy_pct']:+.2f}%")
        else:
            print(f"  [!] Khong the lay funding rate cho {inst_id}")

        # 2. Cao Open Interest
        oi_url = f"{OKX_REST_URL}/api/v5/public/open-interest?instType=SWAP&instId={inst_id}"
        oi_raw = fetch_json(oi_url)
        if oi_raw.get("code") == "0" and oi_raw.get("data"):
            raw_item = oi_raw["data"][0]
            raw_oi_records.append({
                "symbol": sym,
                "instrument": inst_id,
                "raw_json": json.dumps(raw_item),
                "source": "OKX_REST_API",
                "ingestion_time": now_utc_str
            })
            clean_oi = clean_open_interest_record(raw_item, inst_id)
            clean_oi_records.append(clean_oi)
            print(f"  [CLEAN] Open Interest  : {clean_oi['oi_contracts']:,.2f} contracts (${clean_oi['oi_usd']:,.2f} USD)")
        else:
            print(f"  [!] Khong the lay open interest cho {inst_id}")

    # Ghi nhận vào Data Lake cục bộ
    print(f"\n[*] [BUOC 1/3] Luu tru du lieu ra cuc bo (CSV, JSON)...")
    export_to_datalake(clean_funding_records, clean_oi_records)

    # Ghi nhận lên S3 Medallion Data Lake
    if save_s3 and bucket:
        print(f"\n[*] [BUOC 2/3] Luu tru Bronze & Silver phai sinh len S3 Data Lake...")
        save_derivatives_to_s3(raw_funding_records, raw_oi_records, clean_funding_records, clean_oi_records, bucket)
    else:
        print(f"\n[*] [BUOC 2/3] Bo qua luu S3 (save_s3={save_s3}, bucket={bucket}).")

    # Nạp dữ liệu vào PostgreSQL Data Warehouse
    if save_dw:
        print(f"\n[*] [BUOC 3/3] Nap du lieu da lam sach vao PostgreSQL Data Warehouse...")
        save_to_warehouse(clean_funding_records, clean_oi_records)
    else:
        print(f"\n[*] [BUOC 3/3] Bo qua nap DW (--no-dw duoc bat).")

    print("\n" + "=" * 80)
    print("[HOAN TAT] TOAN BO PIPELINE CAO & LAM SACH DU LIEU PHAI SINH THANH CONG!")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="OKX Derivatives Fetcher & Medallion Storage")
    parser.add_argument("--symbol", type=str, default=None, help="Chay cho 1 symbol cu the (vi du: BTC-USDT)")
    parser.add_argument("--all-symbols", action="store_true", help="Chay cho tat ca symbols mac dinh")
    parser.add_argument("--symbols", type=str, default=None, help="Danh sach symbols ngan cach boi dau phay")
    parser.add_argument("--no-s3", action="store_true", help="Khong luu vao S3 Data Lake")
    parser.add_argument("--no-dw", action="store_true", help="Khong nap vao PostgreSQL Data Warehouse")

    args = parser.parse_args()

    if args.symbol:
        syms = [args.symbol.strip().upper()]
    elif args.symbols:
        syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        syms = list(SUPPORTED_SYMBOLS)

    run_batch_fetch(symbols=syms, save_s3=not args.no_s3, save_dw=not args.no_dw)


if __name__ == "__main__":
    main()
