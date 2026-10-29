"""
=============================================================================
Crypto Big Data - Entrypoint: Train XGBoost Model
=============================================================================
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ml.train import train_btc_xgboost

if __name__ == "__main__":
    train_btc_xgboost()
