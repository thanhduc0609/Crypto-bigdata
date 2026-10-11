"""
=============================================================================
Crypto Big Data - Test Kafka Consumer
=============================================================================
"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path
from kafka import KafkaConsumer

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import load_env_vars

load_env_vars()

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_TRADES", os.getenv("KAFKA_TOPIC", "crypto-trades"))
CONSUMER_GROUP = "crypto-test-consumer"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    key_deserializer=lambda key: key.decode("utf-8") if key else None,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("=" * 85)
print("KAFKA TEST CONSUMER")
print(f"Kafka Server  : {KAFKA_SERVER}")
print(f"Topic         : {TOPIC}")
print(f"Consumer Group: {CONSUMER_GROUP}")
print("=" * 85)
print("\nWaiting for crypto trades (Press Ctrl+C to stop)...\n")

try:
    for message in consumer:
        trade = message.value
        trade_time = datetime.fromtimestamp(trade["timestamp"] / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(
            f"[CONSUMER] "
            f"P{message.partition} | "
            f"Offset: {message.offset:<8} | "
            f"{trade['symbol']:<9} | "
            f"{trade['side']:<4} | "
            f"Price: {trade['price']:<12} | "
            f"Size: {trade['size']:<10} | "
            f"{trade_time}"
        )
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    print("Kafka Consumer closed.")
