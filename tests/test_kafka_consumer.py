"""Opt-in read-only Kafka smoke test with a deadline; safe to import offline."""
import argparse
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=30)
    parser.add_argument('--max-messages', type=int, default=10)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 300 or args.max_messages < 1:
        parser.error('Use 1–300 seconds and a positive message count')
    from kafka import KafkaConsumer
    from processing.core.env import load_env_vars
    load_env_vars()
    consumer = KafkaConsumer(os.getenv('KAFKA_TOPIC_TRADES', 'crypto-trades'),
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        group_id=None, enable_auto_commit=False, auto_offset_reset='latest', consumer_timeout_ms=1000,
        value_deserializer=lambda value: json.loads(value.decode('utf-8')))
    count = 0
    deadline = time.monotonic() + args.seconds
    try:
        while time.monotonic() < deadline and count < args.max_messages:
            for message in consumer:
                count += 1
                print(json.dumps(dict(partition=message.partition, offset=message.offset, trade=message.value)))
                if count >= args.max_messages or time.monotonic() >= deadline:
                    break
    finally:
        consumer.close()
    print('Messages observed:', count)
    return 0 if count else 1


if __name__ == '__main__':
    sys.exit(main())
