"""
=============================================================================
Crypto Big Data - Entrypoint: Train XGBoost Model
=============================================================================
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ml.realtime.train import train_btc_xgboost

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    train_btc_xgboost()
