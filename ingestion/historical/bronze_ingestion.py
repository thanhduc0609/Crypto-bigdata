"""
=============================================================================
Crypto Big Data - Medallion Bronze Layer: Historical Ingestion
=============================================================================
Entry point for historical Bronze data ingestion from OKX REST API.
Delegates to OKXHistoricalBackfill to fetch validated 1-minute historical
candles and store them into the Bronze / Raw Lakehouse partition.
=============================================================================
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ingestion.historical.okx_historical_backfill import OKXHistoricalBackfill


def run_bronze_ingestion(symbol: str = "BTC-USDT", days: int = 30, sync_to_postgres: bool = True, sync_to_s3: bool = False):
    """Executes Bronze historical ingestion."""
    backfill = OKXHistoricalBackfill(
        symbol=symbol,
        days=days,
        sync_to_postgres=sync_to_postgres,
        sync_to_s3=sync_to_s3
    )
    return backfill.run_backfill()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Medallion Bronze Historical Ingestion for BTC-USDT")
    parser.add_argument("--symbol", type=str, default="BTC-USDT", help="Symbol (default: BTC-USDT)")
    parser.add_argument("--days", type=int, default=30, help="Number of historical days to backfill (default: 30)")
    parser.add_argument("--no-db", action="store_true", help="Skip syncing to DW")
    parser.add_argument("--s3", action="store_true", help="Upload to AWS S3")
    args = parser.parse_args()

    run_bronze_ingestion(
        symbol=args.symbol,
        days=args.days,
        sync_to_postgres=not args.no_db,
        sync_to_s3=args.s3
    )

