"""
=============================================================================
Crypto Big Data - Ingestion Layer (Streaming)
OKX WebSocket -> Apache Kafka Producer
=============================================================================
"""

import json
import os
import sys
from pathlib import Path
import websocket
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions

# Setup project root
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import load_env_vars

load_env_vars()

# =============================================================================
# CONFIGURATION
# =============================================================================
OKX_WS_URL = os.getenv("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_TRADES", os.getenv("KAFKA_TOPIC", "crypto-trades"))

# Mapping mỗi coin vào một partition cố định
PARTITION_MAP = {
    "BTC-USDT": 0,
    "ETH-USDT": 1,
    "SOL-USDT": 2
}

def ensure_topic_partitions():
    """Tự động đảm bảo topic tồn tại và có tối thiểu 3 partitions"""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER, request_timeout_ms=5000)
        topics = admin_client.list_topics()
        if TOPIC not in topics:
            print(f"[*] Topic '{TOPIC}' chua ton tai. Dang tao moi voi 3 partitions...")
            admin_client.create_topics([NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)])
            print(f"[OK] Da tao topic '{TOPIC}' thanh cong (3 partitions)!")
        else:
            topic_metadata = admin_client.describe_topics([TOPIC])
            parts = len(topic_metadata[0].get("partitions", []))
            if parts < 3:
                print(f"[*] Topic '{TOPIC}' hien chi co {parts} partition. Dang nang len 3 partitions...")
                admin_client.create_partitions({TOPIC: NewPartitions(total_count=3)})
                print(f"[OK] Da nang so partition cua '{TOPIC}' len 3 thanh cong!")
        admin_client.close()
    except Exception as e:
        print(f"[!] Warning khi kiem tra topic partitions: {e}")

# =============================================================================
# KAFKA PRODUCER
# =============================================================================
def get_kafka_producer():
    ensure_topic_partitions()
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
        acks="all"
    )

producer = None

# =============================================================================
# OKX WEBSOCKET CALLBACKS
# =============================================================================
def on_open(ws):
    print("=" * 70)
    print("Connected to OKX WebSocket")
    # Active subscription: Dedicated exclusively to BTC-USDT for academic ML pipeline
    subscribe_message = {
        "op": "subscribe",
        "args": [
            {"channel": "trades", "instId": "BTC-USDT"}
        ]
    }
    ws.send(json.dumps(subscribe_message))

    print("Active Subscription:")
    print("  BTC-USDT -> Partition 0 (Dedicated Pipeline)")
    print("=" * 70)


def on_message(ws, message):
    try:
        data = json.loads(message)
        if "data" not in data or not isinstance(data["data"], list):
            return

        for trade in data["data"]:
            if not isinstance(trade, dict):
                continue
            # Detect and reject malformed records
            if not all(k in trade for k in ("tradeId", "instId", "px", "sz", "side", "ts")):
                print(f"[WARN] Malformed trade skipped: {trade}")
                continue

            try:
                price = float(trade["px"])
                size = float(trade["sz"])
                ts = int(trade["ts"])
                side = str(trade["side"]).lower()
                if price <= 0.0 or size <= 0.0 or ts <= 0 or side not in ("buy", "sell"):
                    continue
            except (ValueError, TypeError) as val_err:
                print(f"[WARN] Invalid data types in trade: {val_err}")
                continue

            record = {
                "trade_id": str(trade["tradeId"]),
                "symbol": str(trade["instId"]),
                "price": price,
                "size": size,
                "side": side,
                "timestamp": ts
            }

            symbol = record["symbol"]
            partition = PARTITION_MAP.get(symbol, 0)

            try:
                producer.send(
                    TOPIC,
                    key=symbol,
                    value=record,
                    partition=partition
                )
            except Exception:
                producer.send(
                    TOPIC,
                    key=symbol,
                    value=record
                )

            print(
                f"[PRODUCER] "
                f"P{partition} | "
                f"{symbol:<9} | "
                f"{record['side']:<4} | "
                f"Price: {record['price']:<12.2f} | "
                f"Size: {record['size']:<8.4f}"
            )
    except Exception as error:
        print(f"[ERROR] Failed to process message: {error}")


def on_error(ws, error):
    print(f"[WEBSOCKET ERROR] {error}")


def on_close(ws, close_status_code, close_msg):
    print("\n" + "=" * 70)
    print(f"Disconnected from OKX | Code: {close_status_code} | Msg: {close_msg}")
    print("=" * 70)


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    print("=" * 70)
    print("OKX -> KAFKA STREAMING PRODUCER")
    print(f"Kafka Server : {KAFKA_SERVER}")
    print(f"Kafka Topic  : {TOPIC}")
    print("=" * 70)

    try:
        producer = get_kafka_producer()
        ws = websocket.WebSocketApp(
            OKX_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        if producer:
            producer.flush()
            producer.close()
            print("Kafka Producer closed.")
