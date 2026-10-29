"""
=============================================================================
Crypto Big Data - Entrypoint: Build Features & Dataset
=============================================================================
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ml.dataset import create_chronological_splits

if __name__ == "__main__":
    print("[RUN] Executing BTC-USDT Feature Engineering & Dataset Build...")
    data = create_chronological_splits()
    m = data["metadata"]
    print(f"\n[SUCCESS] Feature Dataset Built:")
    print(f"  - Total Labeled Samples: {m['total_samples']}")
    print(f"  - Features ({m['n_features']}): {', '.join(m['feature_names'])}")
    print(f"  - Train Partition      : {m['train_samples']} samples ({m['train_period']})")
    print(f"  - Val Partition        : {m['val_samples']} samples ({m['val_period']})")
    print(f"  - Test Partition       : {m['test_samples']} samples ({m['test_period']})")
