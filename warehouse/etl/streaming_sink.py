"""
=============================================================================
Crypto Big Data - Real-time Streaming PostgreSQL Sink (Data Mart Serving)
=============================================================================
Thuc hien ghi du lieu streaming nhe vao PostgreSQL Data Warehouse:
  1. Ghi nến 1 phut (fact_crypto_1m_candles): Chi ~3 dong/phut, khong lam nang Postgres.
  2. Ghi cac giao dich ca map (fact_crypto_trades): Chi luu lenh >= $10,000.
=============================================================================
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from warehouse.etl.db_config import get_db_connection


def write_whales_batch(batch_df, batch_id):
    """Ghi micro-batch lenh ca map vao warehouse.fact_crypto_trades"""
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    if not rows:
        return

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO warehouse.fact_crypto_trades 
            (trade_id, symbol, price, size, side, trade_value, trade_time, trade_date, kafka_partition, kafka_offset)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_id) DO NOTHING;
        """

        records = [
            (
                str(r["trade_id"]),
                str(r["symbol"]),
                float(r["price"]),
                float(r["size"]),
                str(r["side"]),
                float(r["trade_value"]),
                r["trade_time"],
                r["trade_date"],
                int(r["kafka_partition"]) if r["kafka_partition"] is not None else None,
                int(r["kafka_offset"]) if r["kafka_offset"] is not None else None
            )
            for r in rows
        ]

        cur.executemany(insert_sql, records)
        conn.commit()
        cur.close()
        print(f"[DW SINK] Da nap {len(records)} lenh ca map vao PostgreSQL (fact_crypto_trades)!")
    except Exception as e:
        print(f"[!] Loi khi nap Ca map vao PostgreSQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def write_candles_batch(batch_df, batch_id):
    """Ghi nến 1 phut vao warehouse.fact_crypto_1m_candles (3 dong/phut)"""
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    if not rows:
        return

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO warehouse.fact_crypto_1m_candles 
            (symbol, window_start, window_end, open, high, low, close, volume, 
             trade_count, buy_volume, sell_volume, vwap, trade_imbalance, total_trade_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, window_start) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                trade_count = EXCLUDED.trade_count,
                buy_volume = EXCLUDED.buy_volume,
                sell_volume = EXCLUDED.sell_volume,
                vwap = EXCLUDED.vwap,
                trade_imbalance = EXCLUDED.trade_imbalance,
                total_trade_value = EXCLUDED.total_trade_value;
        """

        records = [
            (
                str(r["symbol"]),
                r["window_start"],
                r["window_end"],
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["volume"]),
                int(r["trade_count"]),
                float(r["buy_volume"]),
                float(r["sell_volume"]),
                float(r["vwap"]),
                float(r["trade_imbalance"]),
                float(r["total_trade_value"])
            )
            for r in rows
        ]

        cur.executemany(upsert_sql, records)
        conn.commit()
        cur.close()
        print(f"[DW SINK] Da nap/cap nhat {len(records)} nến 1m vao PostgreSQL (fact_crypto_1m_candles)!")
    except Exception as e:
        print(f"[!] Loi khi nap Nến 1m vao PostgreSQL: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
