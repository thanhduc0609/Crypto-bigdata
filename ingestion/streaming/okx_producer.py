import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Optional
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
# CONFIGURATION (Strictly BTC-USDT Only)
# =============================================================================
OKX_WS_URL = os.getenv("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_TRADES", os.getenv("KAFKA_TOPIC", "crypto-trades"))
TARGET_SYMBOL = "BTC-USDT"
BTC_PARTITION = 0

_global_producer: Optional[KafkaProducer] = None
_stop_requested = False


def ensure_topic_partitions(num_partitions: int = 1):
    """Ensures the Kafka topic exists and has at least partition 0 for BTC-USDT."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER, request_timeout_ms=5000)
        topics = admin_client.list_topics()
        if TOPIC not in topics:
            print(f"[*] Topic '{TOPIC}' does not exist. Creating topic with {num_partitions} partition(s)...")
            admin_client.create_topics([NewTopic(name=TOPIC, num_partitions=num_partitions, replication_factor=1)])
            print(f"[OK] Topic '{TOPIC}' created successfully.")
        else:
            topic_metadata = admin_client.describe_topics([TOPIC])
            parts = len(topic_metadata[0].get("partitions", []))
            if parts < num_partitions:
                print(f"[*] Topic '{TOPIC}' has {parts} partition(s). Expanding to {num_partitions}...")
                admin_client.create_partitions({TOPIC: NewPartitions(total_count=num_partitions)})
        admin_client.close()
    except Exception as e:
        print(f"[!] Note on Kafka topic inspection: {e}")


def get_kafka_producer() -> KafkaProducer:
    """Returns or creates the singleton KafkaProducer instance."""
    global _global_producer
    if _global_producer is None:
        ensure_topic_partitions(num_partitions=1)
        _global_producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            retries=5,
            acks="all"
        )
    return _global_producer


def set_kafka_producer(producer: Optional[KafkaProducer]):
    """Explicitly sets or overrides the producer instance (useful for dependency injection and tests)."""
    global _global_producer
    _global_producer = producer


# =============================================================================
# OKX WEBSOCKET CALLBACKS
# =============================================================================
def on_open(ws):
    print("=" * 70)
    print("Connected to OKX WebSocket")
    subscribe_message = {
        "op": "subscribe",
        "args": [
            {"channel": "trades", "instId": TARGET_SYMBOL}
        ]
    }
    ws.send(json.dumps(subscribe_message))
    print(f"Active Subscription: {TARGET_SYMBOL} -> Kafka [{TOPIC}] Partition {BTC_PARTITION}")
    print("=" * 70)


def _on_send_success(record_metadata, symbol: str, side: str, price: float, size: float):
    print(
        f"[PRODUCER DELIVERED] "
        f"P{record_metadata.partition} O{record_metadata.offset} | "
        f"{symbol:<9} | "
        f"{side:<4} | "
        f"Price: {price:<10.2f} | "
        f"Size: {size:<8.4f}"
    )


def _on_send_error(ex, trade_id: str):
    print(f"[ERROR] Failed to deliver trade {trade_id} to Kafka: {ex}")


def on_message(ws, message):
    try:
        data = json.loads(message)
        if "data" not in data or not isinstance(data["data"], list):
            return

        producer_instance = getattr(ws, "producer", None) or _global_producer
        if producer_instance is None:
            try:
                producer_instance = get_kafka_producer()
            except Exception as pe:
                print(f"[ERROR] Kafka producer unavailable: {pe}")
                return

        for trade in data["data"]:
            if not isinstance(trade, dict):
                continue

            # Verify required fields
            if not all(k in trade for k in ("tradeId", "instId", "px", "sz", "side", "ts")):
                print(f"[WARN] Malformed trade skipped (missing keys): {trade}")
                continue

            symbol = str(trade.get("instId", "")).strip().upper()
            if symbol != TARGET_SYMBOL:
                # Strictly reject non-BTC symbols
                continue

            try:
                price = float(trade["px"])
                size = float(trade["sz"])
                ts = int(trade["ts"])
                side = str(trade["side"]).strip().lower()

                # Validate numeric boundaries, NaN and Infinity
                if (math.isnan(price) or math.isinf(price) or price <= 0.0 or
                    math.isnan(size) or math.isinf(size) or size <= 0.0 or
                    ts <= 0 or side not in ("buy", "sell")):
                    print(f"[WARN] Invalid numeric/side value in trade skipped: {trade}")
                    continue
            except (ValueError, TypeError) as val_err:
                print(f"[WARN] Invalid data type in trade ({val_err}): {trade}")
                continue

            trade_id = str(trade["tradeId"])
            record = {
                "trade_id": trade_id,
                "symbol": symbol,
                "price": price,
                "size": size,
                "side": side,
                "timestamp": ts
            }

            try:
                future = producer_instance.send(
                    TOPIC,
                    key=symbol,
                    value=record,
                    partition=BTC_PARTITION
                )
                future.add_callback(_on_send_success, symbol, side, price, size)
                future.add_errback(_on_send_error, trade_id)
            except Exception as send_err:
                print(f"[ERROR] Failed to queue message to Kafka: {send_err}")

    except Exception as error:
        print(f"[ERROR] Failed to process incoming WebSocket message: {error}")


def on_error(ws, error):
    print(f"[WEBSOCKET ERROR] {error}")


def on_close(ws, close_status_code, close_msg):
    print("\n" + "=" * 70)
    print(f"Disconnected from OKX | Code: {close_status_code} | Msg: {close_msg}")
    print("=" * 70)


def run_producer(auto_reconnect: bool = True, reconnect_delay: int = 5):
    """
    Runs the OKX WebSocket client loop with heartbeat ping and automatic reconnection.
    """
    global _stop_requested
    _stop_requested = False

    producer_instance = get_kafka_producer()

    print("=" * 70)
    print("OKX -> KAFKA STREAMING PRODUCER (BTC-USDT DEDICATED)")
    print(f"Kafka Server : {KAFKA_SERVER}")
    print(f"Kafka Topic  : {TOPIC} (Partition {BTC_PARTITION})")
    print(f"OKX WS URL   : {OKX_WS_URL}")
    print("=" * 70)

    try:
        while not _stop_requested:
            ws = websocket.WebSocketApp(
                OKX_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.producer = producer_instance

            # Heartbeat: ping every 20 seconds, timeout 10 seconds
            ws.run_forever(ping_interval=20, ping_timeout=10)

            if not auto_reconnect or _stop_requested:
                break

            print(f"[*] Connection dropped. Reconnecting to OKX in {reconnect_delay} seconds...")
            time.sleep(reconnect_delay)

    except KeyboardInterrupt:
        print("\nStopping streaming producer on user interrupt...")
    finally:
        _stop_requested = True
        if producer_instance:
            try:
                producer_instance.flush(timeout=5)
                producer_instance.close(timeout=5)
                print("Kafka Producer flushed and closed cleanly.")
            except Exception as e:
                print(f"[!] Note closing producer: {e}")


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    run_producer()
