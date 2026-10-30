"""
=============================================================================
Crypto Big Data - Historical BTC-USDT 1m Candlestick Backfill Module
=============================================================================
Downloads historical 1-minute OHLCV candles from the OKX REST API:
  - Documented endpoint: GET /api/v5/market/history-candles
  - Pagination using 'after' parameter (walking backward in time)
  - Rate limiting (compliant with OKX 20 req/2s public limit)
  - Exponential backoff on rate limits (429) and network timeouts
  - Resumable: persists progress in checkpoint state JSON
  - Validates candles: rejects incomplete bars (confirm == '0'), invalid prices/volumes
  - Eliminates duplicate timestamps
  - Detects missing minute intervals (timestamp gaps)
  - Preserves exact UTC timestamps
  - Saves raw historical candles to Parquet Data Lake (local and S3)
  - Normalizes and reconciles with Gold candles in PostgreSQL without fake order flow
=============================================================================
"""

import os
import sys
import time
import json
import random
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd

# Setup project root
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import load_env_vars
from storage.s3 import (
    get_storage_mode,
    get_s3_bucket,
    get_raw_historical_candles_path,
    get_gold_candles_path
)
from warehouse.etl.streaming_sink import write_historical_candles_batch

load_env_vars()

OKX_REST_BASE = os.getenv("OKX_REST_URL", "https://www.okx.com")
DEFAULT_SYMBOL = "BTC-USDT"
DEFAULT_BAR = "1m"
DEFAULT_LIMIT = 100  # Documented safe limit for OKX history-candles endpoint


class OKXHistoricalBackfill:
    def __init__(
        self,
        symbol: str = DEFAULT_SYMBOL,
        days: int = 90,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        rate_limit_delay: float = 0.12,  # ~8 req/sec (safe under OKX 20 req/2s limit)
        max_retries: int = 5,
        batch_size: int = DEFAULT_LIMIT,
        lake_dir: Optional[Path] = None,
        progress_file: Optional[Path] = None,
        sync_to_postgres: bool = True,
        sync_to_s3: bool = False
    ):
        self.symbol = symbol
        self.bar = DEFAULT_BAR
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.sync_to_postgres = sync_to_postgres
        self.sync_to_s3 = sync_to_s3

        # Configure time window
        now_utc = datetime.now(timezone.utc)
        if end_date:
            self.end_date = end_date.astimezone(timezone.utc) if end_date.tzinfo else end_date.replace(tzinfo=timezone.utc)
        else:
            self.end_date = now_utc

        if start_date:
            self.start_date = start_date.astimezone(timezone.utc) if start_date.tzinfo else start_date.replace(tzinfo=timezone.utc)
        else:
            self.start_date = self.end_date - timedelta(days=days)

        self.start_ms = int(self.start_date.timestamp() * 1000)
        self.end_ms = int(self.end_date.timestamp() * 1000)

        # Storage paths
        data_dir = project_root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        if lake_dir is None:
            self.raw_lake_dir = project_root / "data" / "lake" / "raw" / "historical_candles_1m" / self.symbol
            self.gold_lake_dir = project_root / "data" / "lake" / "gold" / "candles_1m" / f"symbol={self.symbol}"
        else:
            self.raw_lake_dir = lake_dir / "raw" / "historical_candles_1m" / self.symbol
            self.gold_lake_dir = lake_dir / "gold" / "candles_1m" / f"symbol={self.symbol}"

        self.raw_lake_dir.mkdir(parents=True, exist_ok=True)
        self.gold_lake_dir.mkdir(parents=True, exist_ok=True)

        if progress_file is None:
            self.progress_file = data_dir / f"backfill_progress_{self.symbol.replace('-', '_')}.json"
        else:
            self.progress_file = progress_file

        self.summary_file = data_dir / f"backfill_summary_{self.symbol.replace('-', '_')}.json"

    def _fetch_page(self, after_ms: Optional[int] = None) -> List[List[str]]:
        """
        Fetches a single page of candles from OKX /api/v5/market/history-candles.
        Handles rate limits (429) and network errors with exponential backoff.
        """
        url = f"{OKX_REST_BASE}/api/v5/market/history-candles?instId={self.symbol}&bar={self.bar}&limit={self.batch_size}"
        if after_ms is not None:
            url += f"&after={after_ms}"

        for attempt in range(1, self.max_retries + 1):
            try:
                time.sleep(self.rate_limit_delay)
                req = urllib.request.Request(url, headers={
                    "User-Agent": "CryptoBigData-HistoricalBackfill/2.0",
                    "Accept": "application/json"
                })
                with urllib.request.urlopen(req, timeout=15) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))

                code = payload.get("code")
                if code == "0":
                    return payload.get("data", [])
                elif code == "50011":  # OKX rate limit error code
                    wait = (2 ** attempt) + random.uniform(0.5, 1.5)
                    print(f"  [429 Rate Limit] OKX rate limit hit. Backing off for {wait:.2f}s (Attempt {attempt}/{self.max_retries})...")
                    time.sleep(wait)
                else:
                    msg = payload.get("msg", "Unknown error")
                    print(f"  [API Error] Code: {code}, Msg: {msg}. Retrying...")
                    time.sleep(1.0)
            except urllib.error.HTTPError as http_err:
                if http_err.code == 429:
                    wait = (2 ** attempt) + random.uniform(0.5, 1.5)
                    print(f"  [HTTP 429] Rate limit reached. Backing off for {wait:.2f}s...")
                    time.sleep(wait)
                else:
                    print(f"  [HTTP {http_err.code}] {http_err.reason}. Attempt {attempt}/{self.max_retries}")
                    time.sleep(2.0)
            except Exception as e:
                print(f"  [Network Error] {e}. Attempt {attempt}/{self.max_retries}")
                time.sleep(2.0)

        raise RuntimeError(f"Failed to fetch history candles after {self.max_retries} attempts at after={after_ms}")

    def get_missing_intervals(self, existing_ts_set: set) -> List[Tuple[int, int]]:
        """
        Scans from self.start_ms to self.end_ms in 60s steps to identify all
        contiguous missing intervals [interval_start_ms, interval_end_ms].
        """
        s_ms = (self.start_ms // 60000) * 60000
        e_ms = (self.end_ms // 60000) * 60000

        missing_intervals = []
        cur_gap_start = None
        cur_gap_end = None

        for t in range(s_ms, e_ms + 60000, 60000):
            if t not in existing_ts_set:
                if cur_gap_start is None:
                    cur_gap_start = t
                cur_gap_end = t
            else:
                if cur_gap_start is not None:
                    missing_intervals.append((cur_gap_start, cur_gap_end))
                    cur_gap_start = None
                    cur_gap_end = None

        if cur_gap_start is not None:
            missing_intervals.append((cur_gap_start, cur_gap_end))

        return missing_intervals

    def load_progress(self) -> Optional[Dict[str, Any]]:
        """Loads previous checkpoint state if available."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"[!] Warning reading progress file: {e}")
        return None

    def save_progress(self, state: Dict[str, Any]):
        """Persists current download progress state."""
        try:
            with open(self.progress_file, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"[!] Warning saving progress file: {e}")

    def run_backfill(self, resume: bool = True) -> Dict[str, Any]:
        """
        Executes full historical download walking backwards from end_date to start_date.
        """
        print("=" * 80)
        print("HISTORICAL CANDLESTICK BACKFILL PIPELINE (OKX REST -> MEDALLION LAKE)")
        print(f"Symbol           : {self.symbol}")
        print(f"Bar Interval     : {self.bar}")
        print(f"Start Window     : {self.start_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"End Window       : {self.end_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        total_expected_candles = int((self.end_ms - self.start_ms) / (60 * 1000))
        print(f"Target Duration  : {(self.end_date - self.start_date).total_seconds() / 86400:.1f} days (~{total_expected_candles:,} 1m candles)")
        print(f"Raw Lake Path    : {self.raw_lake_dir}")
        print(f"Gold Lake Path   : {self.gold_lake_dir}")
        print("=" * 80)

        # Check existing checkpoint
        checkpoint = self.load_progress() if resume else None
        collected_records: Dict[int, Dict[str, Any]] = {}
        rejected_count = 0
        duplicate_count = 0

        # Load existing local raw candles if any to avoid re-downloading
        existing_parquet_files = list(self.raw_lake_dir.glob("*.parquet"))
        if existing_parquet_files and resume:
            try:
                df_exist = pd.read_parquet(self.raw_lake_dir)
                for _, row in df_exist.iterrows():
                    ts_ms = int(row["window_start"].timestamp() * 1000)
                    if self.start_ms <= ts_ms <= self.end_ms:
                        collected_records[ts_ms] = row.to_dict()
                print(f"[*] Resumed with {len(collected_records):,} existing valid candles from local lake.")
            except Exception as e:
                print(f"[!] Note loading existing parquet: {e}")

        # Detect missing intervals within requested window
        missing_intervals = self.get_missing_intervals(set(collected_records.keys()))
        total_missing = sum((gap_end - gap_start) // 60000 + 1 for gap_start, gap_end in missing_intervals)

        t0 = time.time()
        calls_count = 0
        new_candles_count = 0

        if not missing_intervals or total_missing == 0:
            print(f"[*] All {len(collected_records):,} candles in requested window are already present in local lake.")
            print("[*] 0 missing minutes detected. Skipping unnecessary API downloads (Incremental sync up-to-date).")
        else:
            print(f"[*] Identified {len(missing_intervals)} missing interval(s) ({total_missing:,} missing minutes to download).")

            # Iterate through each missing interval from latest to earliest (OKX pages backward)
            for gap_idx, (gap_start_ms, gap_end_ms) in enumerate(reversed(missing_intervals), 1):
                gap_start_dt = datetime.fromtimestamp(gap_start_ms / 1000, tz=timezone.utc)
                gap_end_dt = datetime.fromtimestamp(gap_end_ms / 1000, tz=timezone.utc)
                gap_minutes = (gap_end_ms - gap_start_ms) // 60000 + 1
                print(f"\n[*] Processing Gap {gap_idx}/{len(missing_intervals)}: {gap_start_dt} to {gap_end_dt} UTC ({gap_minutes:,} candles)")

                # Check if checkpoint resumes inside this gap
                current_after_ms = gap_end_ms + 60000
                if checkpoint and "last_cursor_ms" in checkpoint and resume:
                    candidate_cursor = int(checkpoint["last_cursor_ms"])
                    if gap_start_ms <= candidate_cursor <= gap_end_ms + 60000:
                        current_after_ms = candidate_cursor
                        print(f"  [*] Resuming Gap {gap_idx} from checkpoint cursor: {datetime.fromtimestamp(current_after_ms/1000, tz=timezone.utc)}")

                while True:
                    raw_batch = self._fetch_page(after_ms=current_after_ms)
                    calls_count += 1

                    if not raw_batch:
                        print(f"  [*] Reached end of data for Gap {gap_idx}.")
                        break

                    batch_min_ts = float("inf")
                    for item in raw_batch:
                        ts_ms = int(item[0])
                        batch_min_ts = min(batch_min_ts, ts_ms)

                        # 1. Reject incomplete candles (confirm == "0")
                        confirm = str(item[8]) if len(item) > 8 else "1"
                        if confirm == "0":
                            rejected_count += 1
                            continue

                        # 2. Check time boundary for this gap
                        if ts_ms < gap_start_ms:
                            continue
                        if ts_ms > gap_end_ms:
                            continue

                        # 3. Check duplicates
                        if ts_ms in collected_records:
                            duplicate_count += 1
                            continue

                        # 4. Validate numeric values
                        try:
                            open_p = float(item[1])
                            high_p = float(item[2])
                            low_p = float(item[3])
                            close_p = float(item[4])
                            vol = float(item[5])
                            turnover = float(item[7]) if len(item) > 7 and item[7] else None

                            if open_p <= 0 or high_p <= 0 or low_p <= 0 or close_p <= 0 or vol < 0:
                                rejected_count += 1
                                continue

                            if high_p < low_p:
                                rejected_count += 1
                                continue

                            window_start = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                            window_end = window_start + timedelta(minutes=1)

                            collected_records[ts_ms] = {
                                "symbol": self.symbol,
                                "window_start": window_start,
                                "window_end": window_end,
                                "open": open_p,
                                "high": high_p,
                                "low": low_p,
                                "close": close_p,
                                "volume": vol,
                                "total_trade_value": turnover,
                                "trade_count": None,
                                "buy_volume": None,
                                "sell_volume": None,
                                "vwap": None,
                                "trade_imbalance": None,
                                "source": "historical_rest",
                                "is_closed": True,
                                "created_at": datetime.now(timezone.utc)
                            }
                            new_candles_count += 1
                        except (ValueError, TypeError):
                            rejected_count += 1
                            continue

                    # Update cursor
                    current_after_ms = int(raw_batch[-1][0])

                    # Checkpoint progress periodically
                    if calls_count % 15 == 0:
                        elapsed = time.time() - t0
                        current_time_str = datetime.fromtimestamp(current_after_ms/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
                        print(
                            f"  [Progress] Gap {gap_idx} | Downloaded {len(collected_records):,} total candles | "
                            f"Cursor: {current_time_str} UTC | "
                            f"Speed: {new_candles_count / max(elapsed, 0.1):.1f} candles/s"
                        )
                        self.save_progress({
                            "symbol": self.symbol,
                            "target_start_utc": str(self.start_date),
                            "target_end_utc": str(self.end_date),
                            "last_cursor_ms": current_after_ms,
                            "last_cursor_utc": current_time_str,
                            "total_collected": len(collected_records),
                            "status": "in_progress",
                            "updated_at": datetime.now(timezone.utc).isoformat()
                        })

                    # Termination condition for this gap
                    if batch_min_ts <= gap_start_ms:
                        print(f"  [OK] Gap {gap_idx} filled ({gap_start_dt.strftime('%Y-%m-%d %H:%M')} UTC reached).")
                        break

        total_collected = len(collected_records)
        elapsed_total = time.time() - t0
        print("\n" + "=" * 80)
        print(f"HISTORICAL DOWNLOAD COMPLETED in {elapsed_total:.1f}s")
        print(f"  - Total Valid Candles   : {total_collected:,}")
        print(f"  - Incomplete Rejected   : {rejected_count:,}")
        print(f"  - Duplicates Dropped    : {duplicate_count:,}")
        print(f"  - Total API Calls       : {calls_count:,}")
        print("=" * 80)

        if total_collected == 0:
            raise RuntimeError("No historical candles collected.")

        # Sort chronologically
        sorted_records = [collected_records[ts] for ts in sorted(collected_records.keys())]
        df_candles = pd.DataFrame(sorted_records)

        # 5. Detect missing intervals (timestamp continuity check)
        df_candles = df_candles.sort_values("window_start").reset_index(drop=True)
        timestamps = df_candles["window_start"].values
        missing_intervals = []
        for i in range(len(timestamps) - 1):
            t1 = pd.to_datetime(timestamps[i])
            t2 = pd.to_datetime(timestamps[i + 1])
            diff_sec = (t2 - t1).total_seconds()
            if diff_sec > 60:
                missing_intervals.append({
                    "gap_start": str(t1),
                    "gap_end": str(t2),
                    "missing_minutes": int(diff_sec / 60) - 1
                })

        total_missing_minutes = sum(g["missing_minutes"] for g in missing_intervals)
        coverage_pct = (total_collected / max(total_collected + total_missing_minutes, 1)) * 100
        print(f"  - Missing Intervals     : {len(missing_intervals)} gaps detected ({total_missing_minutes:,} missing minutes)")
        print(f"  - Timestamp Continuity  : {coverage_pct:.2f}% data density")

        # 6. Save to Raw Medallion Lakehouse (Parquet partitioned by year_month)
        print("\n[*] Saving raw historical candles to Parquet Data Lake...")
        df_candles["year_month"] = df_candles["window_start"].dt.strftime("%Y-%m")
        for ym, group in df_candles.groupby("year_month"):
            ym_file = self.raw_lake_dir / f"btc_1m_{ym}.parquet"
            group.drop(columns=["year_month"]).to_parquet(ym_file, index=False)

        # 7. Save to Canonical Gold Lakehouse (Parquet)
        gold_file = self.gold_lake_dir / "canonical_btc_1m.parquet"
        df_candles.drop(columns=["year_month"], errors="ignore").to_parquet(gold_file, index=False)
        print(f"[OK] Saved canonical Gold candles to: {gold_file}")

        # 8. Sync to PostgreSQL Data Warehouse if requested
        if self.sync_to_postgres:
            print("[*] Synchronizing canonical historical candles to PostgreSQL Data Warehouse...")
            try:
                # Batch sync in chunks of 5000 to keep memory and transactions optimal
                chunk_size = 5000
                total_synced = 0
                for start_idx in range(0, len(sorted_records), chunk_size):
                    chunk = sorted_records[start_idx:start_idx + chunk_size]
                    write_historical_candles_batch(chunk)
                    total_synced += len(chunk)
                print(f"[OK] Successfully synchronized {total_synced:,} historical candles to PostgreSQL!")
            except Exception as e:
                print(f"[!] Warning syncing to PostgreSQL: {e}")

        # 9. Sync to AWS S3 if explicitly enabled
        if self.sync_to_s3:
            bucket = get_s3_bucket()
            print(f"[*] Synchronizing historical candles to AWS S3 (s3://{bucket})...")
            try:
                import boto3
                ak = os.getenv("AWS_ACCESS_KEY_ID")
                sk = os.getenv("AWS_SECRET_ACCESS_KEY")
                reg = os.getenv("AWS_REGION", "ap-southeast-1")
                s3_client = boto3.client("s3", region_name=reg, aws_access_key_id=ak, aws_secret_access_key=sk)

                s3_key = f"crypto/gold/candles_1m/symbol={self.symbol}/canonical_btc_1m.parquet"
                s3_client.upload_file(str(gold_file), bucket, s3_key)
                print(f"[OK] Uploaded canonical Gold Parquet to S3: s3://{bucket}/{s3_key}")
            except Exception as s3_err:
                print(f"[!] S3 upload warning: {s3_err}")

        # 10. Save final summary
        summary = {
            "symbol": self.symbol,
            "candle_interval": self.bar,
            "target_start_utc": str(self.start_date),
            "target_end_utc": str(self.end_date),
            "actual_start_utc": str(df_candles["window_start"].iloc[0]),
            "actual_end_utc": str(df_candles["window_start"].iloc[-1]),
            "total_valid_candles": total_collected,
            "incomplete_candles_rejected": rejected_count,
            "duplicate_timestamps_dropped": duplicate_count,
            "missing_intervals_count": len(missing_intervals),
            "total_missing_minutes": total_missing_minutes,
            "data_continuity_coverage_pct": round(coverage_pct, 2),
            "storage_locations": {
                "raw_parquet": str(self.raw_lake_dir),
                "gold_parquet": str(gold_file),
                "postgres_table": "warehouse.fact_crypto_1m_candles"
            },
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat()
        }
        with open(self.summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        # Mark progress as complete
        self.save_progress({
            "status": "completed",
            "symbol": self.symbol,
            "total_candles": total_collected,
            "completed_at": datetime.now(timezone.utc).isoformat()
        })

        return summary


def run_historical_backfill(days: int = 90, symbol: str = "BTC-USDT"):
    backfill = OKXHistoricalBackfill(symbol=symbol, days=days)
    return backfill.run_backfill()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="BTC-USDT Historical Candlestick Backfill from OKX REST API")
    parser.add_argument("--symbol", type=str, default="BTC-USDT", help="Trading pair symbol (default: BTC-USDT)")
    parser.add_argument("--days", type=int, default=90, help="Number of historical days to backfill (default: 90)")
    parser.add_argument("--no-db", action="store_true", help="Skip syncing to PostgreSQL Data Warehouse")
    parser.add_argument("--s3", action="store_true", help="Upload canonical parquet to AWS S3")
    args = parser.parse_args()

    backfill = OKXHistoricalBackfill(
        symbol=args.symbol,
        days=args.days,
        sync_to_postgres=not args.no_db,
        sync_to_s3=args.s3
    )
    backfill.run_backfill()
