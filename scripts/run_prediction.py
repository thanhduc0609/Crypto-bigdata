"""
=============================================================================
Crypto Big Data - Entrypoint: Run BTC Price Direction Prediction
=============================================================================
"""

import sys
import time
import argparse
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ml.predict import run_single_prediction

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BTC 5m Direction Inference Service")
    parser.add_argument("--once", action="store_true", help="Run a single prediction and exit")
    parser.add_argument("--interval", type=int, default=60, help="Interval in seconds between predictions (default: 60)")
    args = parser.parse_args()

    print("=" * 70)
    print("BTC-USDT 5-MINUTE PRICE DIRECTION INFERENCE SERVICE")
    print(f"Mode: {'Single Pass' if args.once else f'Continuous Loop (Every {args.interval}s)'}")
    print("=" * 70)

    if args.once:
        run_single_prediction()
    else:
        print("Starting inference loop (Ctrl+C to stop)...\n")
        try:
            while True:
                run_single_prediction()
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nInference service stopped by user.")
