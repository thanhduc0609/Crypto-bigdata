"""
=============================================================================
Crypto Big Data - Real-time Streaming PostgreSQL Sink (Data Mart Serving)
=============================================================================
Thực hiện ghi dữ liệu streaming nhẹ vào PostgreSQL Data Warehouse:
  1. Ghi nến 1 phút (fact_crypto_1m_candles): ~3 dòng/phút, cập nhật upsert (ON CONFLICT).
  2. Ghi các giao dịch cá mập/lệnh lớn (fact_crypto_trades): Lưu lệnh >= $10,000 kèm telemetry độ trễ.
=============================================================================
"""

import sys
from pathlib import Path

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from warehouse.etl.db_config import get_db_connection


def compute_whale_tier(val: float) -> str:
    """Phân loại cấp bậc lệnh cá mập theo giá trị USD"""
    if val >= 500000.0:
        return "Mega Whale (>= $500K)"
    elif val >= 100000.0:
        return "Big Whale ($100K - $500K)"
    elif val >= 50000.0:
        return "Whale ($50K - $100K)"
    return "Large Order ($10K - $50K)"


def write_whales_batch(batch_df, batch_id):
    """Ghi micro-batch lệnh cá mập vào warehouse.fact_crypto_trades kèm telemetry độ trễ"""
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
            (trade_id, symbol, price, size, side, trade_value, trade_time, trade_date, kafka_partition, kafka_offset, kafka_arrival_time, whale_tier)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_id) DO NOTHING;
        """

        records = []
        for r in rows:
            r_dict = r.asDict() if hasattr(r, "asDict") else {}
            val = float(r["trade_value"])
            tier = r_dict.get("whale_tier") or compute_whale_tier(val)
            arrival = r_dict.get("kafka_arrival_time")

            k_part = r_dict.get("kafka_partition")
            k_off = r_dict.get("kafka_offset")

            records.append((
                str(r["trade_id"]),
                str(r["symbol"]),
                float(r["price"]),
                float(r["size"]),
                str(r["side"]),
                val,
                r["trade_time"],
                r["trade_date"],
                int(k_part) if k_part is not None else None,
                int(k_off) if k_off is not None else None,
                arrival,
                tier
            ))

        cur.executemany(insert_sql, records)
        conn.commit()
        cur.close()
        print(f"[DW SINK] Da nap {len(records)} lenh ca map vao PostgreSQL (fact_crypto_trades)!")
    except Exception as e:
        print(f"[!] Loi khi nap Ca map vao PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


def write_candles_batch(batch_df, batch_id):
    """Ghi nến 1 phút vào warehouse.fact_crypto_1m_candles (~3 dòng/phút)"""
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
             trade_count, buy_volume, sell_volume, vwap, trade_imbalance, total_trade_value, source, is_closed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, window_start) DO UPDATE SET
                open = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.open
                    ELSE EXCLUDED.open 
                END,
                high = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.high
                    ELSE EXCLUDED.high 
                END,
                low = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.low
                    ELSE EXCLUDED.low 
                END,
                close = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.close
                    ELSE EXCLUDED.close 
                END,
                volume = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.volume
                    ELSE EXCLUDED.volume 
                END,
                total_trade_value = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.total_trade_value
                    ELSE EXCLUDED.total_trade_value 
                END,
                trade_count = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.trade_count
                    ELSE EXCLUDED.trade_count 
                END,
                buy_volume = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.buy_volume
                    ELSE EXCLUDED.buy_volume 
                END,
                sell_volume = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.sell_volume
                    ELSE EXCLUDED.sell_volume 
                END,
                vwap = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.vwap
                    ELSE EXCLUDED.vwap 
                END,
                trade_imbalance = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN warehouse.fact_crypto_1m_candles.trade_imbalance
                    ELSE EXCLUDED.trade_imbalance 
                END,
                source = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN 'historical_rest'
                    ELSE EXCLUDED.source 
                END,
                is_closed = CASE 
                    WHEN warehouse.fact_crypto_1m_candles.source = 'historical_rest' 
                    THEN TRUE
                    WHEN warehouse.fact_crypto_1m_candles.is_closed = TRUE AND EXCLUDED.is_closed = FALSE
                    THEN TRUE
                    ELSE EXCLUDED.is_closed 
                END;
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
                int(r["trade_count"]) if r["trade_count"] is not None else None,
                float(r["buy_volume"]) if r["buy_volume"] is not None else None,
                float(r["sell_volume"]) if r["sell_volume"] is not None else None,
                float(r["vwap"]) if r["vwap"] is not None else None,
                float(r["trade_imbalance"]) if r["trade_imbalance"] is not None else None,
                float(r["total_trade_value"]) if r["total_trade_value"] is not None else None,
                r.asDict().get("source", "realtime_stream") if hasattr(r, "asDict") else "realtime_stream",
                r.asDict().get("is_closed", True) if hasattr(r, "asDict") else True
            )
            for r in rows
        ]

        cur.executemany(upsert_sql, records)
        conn.commit()
        cur.close()
        print(f"[DW SINK] Da nap/cap nhat {len(records)} nen 1m vao PostgreSQL (fact_crypto_1m_candles)!")
    except Exception as e:
        print(f"[!] Loi khi nap Nen 1m vao PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


def write_historical_candles_batch(records: list) -> int:
    """
    Persists historical 1m candles into warehouse.fact_crypto_1m_candles with source='historical_rest'.
    Deterministic reconciliation rule:
      - Historical REST candles represent authoritative exchange-confirmed ground truth.
      - If existing record was 'realtime_stream', the official REST candle updates OHLCV, volume,
        turnover, sets source='historical_rest' and is_closed=TRUE, and resets micro-orderflow fields
        to NULL to avoid presenting partial streaming stats as full REST stats.
      - Guarantees identical canonical outcome regardless of arrival order (REST -> stream vs stream -> REST).
    """
    if not records:
        return 0

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO warehouse.fact_crypto_1m_candles 
            (symbol, window_start, window_end, open, high, low, close, volume, 
             trade_count, buy_volume, sell_volume, vwap, trade_imbalance, total_trade_value, source, is_closed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'historical_rest', TRUE)
            ON CONFLICT (symbol, window_start) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                total_trade_value = EXCLUDED.total_trade_value,
                trade_count = NULL,
                buy_volume = NULL,
                sell_volume = NULL,
                vwap = NULL,
                trade_imbalance = NULL,
                source = 'historical_rest',
                is_closed = TRUE;
        """

        formatted_records = [
            (
                r["symbol"],
                r["window_start"],
                r["window_end"],
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["volume"]),
                r.get("trade_count"),
                r.get("buy_volume"),
                r.get("sell_volume"),
                r.get("vwap"),
                r.get("trade_imbalance"),
                r.get("total_trade_value")
            )
            for r in records
        ]

        cur.executemany(upsert_sql, formatted_records)
        conn.commit()
        cur.close()
        return len(records)
    except Exception as e:
        print(f"[!] Error writing historical candles to PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

