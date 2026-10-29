"""
=============================================================================
Crypto Big Data - Entrypoint: Run Streaming Processing Engine
=============================================================================
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import create_spark_session
from processing.streaming.trade_processor import process_crypto_streams

if __name__ == "__main__":
    print("[RUN] Launching PySpark Structured Streaming Speed Layer...")
    spark = None
    try:
        spark = create_spark_session(app_name="CryptoStreamSpeedLayer")
        process_crypto_streams(spark)
    except KeyboardInterrupt:
        print("\nStreaming engine stopped by user.")
    finally:
        if spark:
            spark.stop()
