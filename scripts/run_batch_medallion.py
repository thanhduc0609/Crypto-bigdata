"""Compatibility CLI for versioned project Silver/Gold and DW synchronization."""
import argparse
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from runtime.medallion import run_batch


def run_batch_medallion_pipeline(symbol='BTC-USDT',sync_to_postgres=True):
    if symbol!='BTC-USDT':raise ValueError('Only BTC-USDT is supported')
    return run_batch(sync_to_postgres=sync_to_postgres)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbol',default='BTC-USDT');parser.add_argument('--no-db',action='store_true')
    args=parser.parse_args()
    result=run_batch_medallion_pipeline(args.symbol,not args.no_db)
    print(result)
