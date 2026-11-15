"""
=============================================================================
Crypto Big Data - Entrypoint: Run BTC Historical Data Backfill
=============================================================================
Downloads historical 1-minute BTC-USDT candles from OKX REST API,
validates, normalizes into canonical Gold schema, and syncs to Data Warehouse.
=============================================================================
"""

import sys
import argparse
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ingestion.historical.okx_historical_backfill import OKXHistoricalBackfill

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run BTC Historical Data Backfill (OKX REST -> Gold Medallion)")
    parser.add_argument("--days", type=int, default=90, help="Days of 1m historical candles to backfill (default: 90)")
    parser.add_argument("--symbol", type=str, default="BTC-USDT", help="Symbol (default: BTC-USDT)")
    parser.add_argument("--no-db", action="store_true", help="Skip syncing to PostgreSQL")
    parser.add_argument("--s3", action="store_true", help="Upload to AWS S3")
    args = parser.parse_args()

    engine = OKXHistoricalBackfill(
        symbol=args.symbol,
        days=args.days,
        sync_to_postgres=not args.no_db,
        sync_to_s3=args.s3
    )
    engine.run_backfill()
