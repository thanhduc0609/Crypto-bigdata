"""
=============================================================================
Crypto Big Data - Streaming Kafka Consumer Helper (BTC Focus)
=============================================================================
"""

import os
import sys
import json
from pathlib import Path
from kafka import KafkaConsumer

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.config import get_pipeline_config

pipeline_cfg = get_pipeline_config()


def get_btc_consumer(group_id: str = "btc-processor-group", auto_offset_reset: str = "latest"):
    """Instantiates a KafkaConsumer configured for BTC trade stream."""
    kafka_server = pipeline_cfg.get("kafka", {}).get("bootstrap_servers", "localhost:9092")
    topic = pipeline_cfg.get("kafka", {}).get("topic_trades", "crypto-trades")
    
    return KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None
    )


if __name__ == "__main__":
    consumer = get_btc_consumer()
    print("Listening to BTC-USDT trades on Kafka (Ctrl+C to stop)...")
    try:
        for msg in consumer:
            if msg.key == "BTC-USDT" or (isinstance(msg.value, dict) and msg.value.get("symbol") == "BTC-USDT"):
                print(f"[BTC CONSUMER] Offset={msg.offset} | P={msg.partition} | {msg.value}")
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()
