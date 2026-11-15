"""
=============================================================================
Crypto Big Data - Entrypoint: Run Ingestion (OKX BTC WebSocket -> Kafka)
=============================================================================
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ingestion.streaming.okx_producer import run_producer

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    print("[RUN] Starting OKX BTC-USDT Streaming Producer...")
    run_producer()

