"""
Versioned project Silver/Gold medallion batch and DW synchronization.
Supports both single-pass execution and continuous interval watch loop.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from runtime.medallion import run_batch, atomic_json, now
from runtime.environment import state_directory


def run_batch_medallion_pipeline(symbol='BTC-USDT', sync_to_postgres=True):
    """Executes a single batch medallion cycle (Bronze -> Silver -> Gold -> DW)."""
    if symbol != 'BTC-USDT':
        raise ValueError('Only BTC-USDT is supported')
    return run_batch(sync_to_postgres=sync_to_postgres)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--symbol", default="BTC-USDT", help="Trading pair symbol (default: BTC-USDT)")
    p.add_argument("--watch", action="store_true", help="Run continuously at a fixed interval")
    p.add_argument("--interval", type=int, default=300, help="Interval in seconds between runs in watch mode (default: 300)")
    p.add_argument("--no-db", action="store_true", help="Skip Data Warehouse synchronization")
    args = p.parse_args()

    if args.symbol != "BTC-USDT":
        p.error("Only BTC-USDT is supported")

    if args.watch:
        if args.interval < 60:
            p.error("Interval must be at least 60 seconds")
        while True:
            start = time.monotonic()
            atomic_json(state_directory() / "scheduler.json", dict(
                mode="watch", status="running", checked_at=now(), interval_seconds=args.interval
            ))
            try:
                result = run_batch_medallion_pipeline(args.symbol, not args.no_db)
                print(json.dumps(result, ensure_ascii=False), flush=True)
            except Exception as exc:
                print("BATCH_FAILED " + type(exc).__name__, flush=True)
            deadline = start + args.interval
            while time.monotonic() < deadline:
                atomic_json(state_directory() / "scheduler.json", dict(
                    mode="watch", status="waiting", checked_at=now(), interval_seconds=args.interval
                ))
                time.sleep(min(10, max(0.1, deadline - time.monotonic())))
    else:
        result = run_batch_medallion_pipeline(args.symbol, not args.no_db)
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

