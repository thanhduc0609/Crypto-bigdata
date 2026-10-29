"""
=============================================================================
Crypto Big Data - Entrypoint: Run Ingestion (OKX BTC WebSocket -> Kafka)
=============================================================================
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ingestion.streaming.okx_producer import (
    get_kafka_producer,
    OKX_WS_URL,
    on_open,
    on_message,
    on_error,
    on_close
)
import websocket

if __name__ == "__main__":
    print("[RUN] Starting OKX BTC-USDT Streaming Producer...")
    producer = get_kafka_producer()
    try:
        ws = websocket.WebSocketApp(
            OKX_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
    except KeyboardInterrupt:
        print("\nStopping ingestion producer...")
    finally:
        if producer:
            producer.flush()
            producer.close()
            print("Kafka producer flushed and closed.")
