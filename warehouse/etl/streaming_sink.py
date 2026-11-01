"""
=============================================================================
Crypto Big Data - Real-time Streaming PostgreSQL Sink (Data Mart Serving)
=============================================================================
Thực hiện ghi dữ liệu streaming nhẹ vào PostgreSQL Data Warehouse:
  1. Ghi nến 1 phút (fact_crypto_1m_candles): áp dụng quy tắc Canonical Reconciliation dùng chung.
  2. Ghi các giao dịch cá mập/lệnh lớn (fact_crypto_trades): Lưu lệnh >= $10,000 kèm telemetry độ trễ.
=============================================================================
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Any, Union

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


def ensure_utc_datetime(dt: Any) -> Optional[datetime]:
    """
    Ensures datetime is UTC-aware.
    Converts naive datetimes into UTC-aware datetimes without timezone shift.
    """
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            # Naive datetime from PySpark deserialization representing UTC event time
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(dt, (int, float)):
        ts = dt / 1000.0 if dt > 1e11 else float(dt)
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(dt, str):
        try:
            parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def compute_whale_tier(val: float) -> str:
    """Phân loại cấp bậc lệnh cá mập theo giá trị USD"""
    if val >= 500000.0:
        return "Mega Whale (>= $500K)"
    elif val >= 100000.0:
        return "Big Whale ($100K - $500K)"
    elif val >= 50000.0:
        return "Whale ($50K - $100K)"
    return "Large Order ($10K - $50K)"


def write_whales_batch(batch_df, batch_id: int = 0, conn=None, table_name: str = "warehouse.fact_crypto_trades", close_conn: bool = True):
    """Ghi micro-batch lệnh cá mập vào warehouse.fact_crypto_trades kèm telemetry độ trễ"""
    if hasattr(batch_df, "isEmpty") and batch_df.isEmpty():
        return 0

    if hasattr(batch_df, "collect"):
        rows = batch_df.collect()
    elif isinstance(batch_df, list):
        rows = batch_df
    elif hasattr(batch_df, "to_dict"):
        rows = batch_df.to_dict(orient="records")
    else:
        rows = list(batch_df)

    if not rows:
        return 0

    local_conn = False
    active_conn = conn
    if active_conn is None:
        active_conn = get_db_connection()
        local_conn = True

    try:
        cur = active_conn.cursor()
        insert_sql = f"""
            INSERT INTO {table_name} 
            (trade_id, symbol, price, size, side, trade_value, trade_time, trade_date, kafka_partition, kafka_offset, kafka_arrival_time, whale_tier)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_id) DO NOTHING;
        """

        records = []
        for r in rows:
            r_dict = r if isinstance(r, dict) else (r.asDict() if hasattr(r, "asDict") else {})
            val = float(r["trade_value"] if not isinstance(r, dict) else r_dict["trade_value"])
            tier = r_dict.get("whale_tier") or compute_whale_tier(val)
            arrival = r_dict.get("kafka_arrival_time")

            k_part = r_dict.get("kafka_partition")
            k_off = r_dict.get("kafka_offset")

            trade_id = str(r["trade_id"] if not isinstance(r, dict) else r_dict["trade_id"])
            symbol = str(r["symbol"] if not isinstance(r, dict) else r_dict["symbol"])
            price = float(r["price"] if not isinstance(r, dict) else r_dict["price"])
            size = float(r["size"] if not isinstance(r, dict) else r_dict["size"])
            side = str(r["side"] if not isinstance(r, dict) else r_dict["side"])
            raw_trade_time = r["trade_time"] if not isinstance(r, dict) else r_dict["trade_time"]
            trade_time = ensure_utc_datetime(raw_trade_time)
            trade_date = r["trade_date"] if not isinstance(r, dict) else r_dict["trade_date"]
            if trade_date is None and trade_time is not None:
                trade_date = trade_time.strftime("%Y-%m-%d")
            arrival = ensure_utc_datetime(arrival)

            records.append((
                trade_id,
                symbol,
                price,
                size,
                side,
                val,
                trade_time,
                trade_date,
                int(k_part) if k_part is not None else None,
                int(k_off) if k_off is not None else None,
                arrival,
                tier
            ))

        cur.executemany(insert_sql, records)
        active_conn.commit()
        cur.close()
        print(f"[DW SINK] Da nap {len(records)} lenh ca map vao PostgreSQL ({table_name})!")
        return len(records)
    except Exception as e:
        print(f"[!] Loi khi nap Ca map vao PostgreSQL: {e}")
        if active_conn:
            active_conn.rollback()
        raise e
    finally:
        if local_conn and close_conn and active_conn:
            active_conn.close()


def build_upsert_candles_sql(table_name: str = "warehouse.fact_crypto_1m_candles") -> str:
    """
    Constructs the canonical reconciliation SQL upsert query for 1-minute candles.
    Rules:
      1. REST authoritative precedence:
         - Existing 'historical_rest' row CANNOT be overwritten by incoming 'realtime_stream'.
         - Incoming 'historical_rest' row OVERWRITES existing 'realtime_stream', updates OHLCV/turnover,
           sets source='historical_rest', is_closed=TRUE, and resets micro-orderflow fields to NULL.
      2. Same-source streaming precedence:
         - A closed candle (is_closed=TRUE) CANNOT be demoted by an unclosed candle (is_closed=FALSE).
         - For identical closure status, a record with strictly higher trade_count is preserved
           over a degraded/partial stream.
         - For equal trade_count (or REST updates), incoming record updates deterministically.
    """
    return f"""
        INSERT INTO {table_name} 
        (symbol, window_start, window_end, open, high, low, close, volume, 
         trade_count, buy_volume, sell_volume, vwap, trade_imbalance, total_trade_value, source, is_closed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, window_start) DO UPDATE SET
            open = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.open
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.open
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.open
                ELSE EXCLUDED.open 
            END,
            high = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.high
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.high
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.high
                ELSE EXCLUDED.high 
            END,
            low = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.low
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.low
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.low
                ELSE EXCLUDED.low 
            END,
            close = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.close
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.close
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.close
                ELSE EXCLUDED.close 
            END,
            volume = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.volume
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.volume
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.volume
                ELSE EXCLUDED.volume 
            END,
            total_trade_value = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.total_trade_value
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.total_trade_value
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.total_trade_value
                ELSE EXCLUDED.total_trade_value 
            END,
            trade_count = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.trade_count
                WHEN EXCLUDED.source = 'historical_rest' THEN NULL
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.trade_count
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.trade_count
                ELSE EXCLUDED.trade_count 
            END,
            buy_volume = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.buy_volume
                WHEN EXCLUDED.source = 'historical_rest' THEN NULL
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.buy_volume
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.buy_volume
                ELSE EXCLUDED.buy_volume 
            END,
            sell_volume = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.sell_volume
                WHEN EXCLUDED.source = 'historical_rest' THEN NULL
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.sell_volume
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.sell_volume
                ELSE EXCLUDED.sell_volume 
            END,
            vwap = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.vwap
                WHEN EXCLUDED.source = 'historical_rest' THEN NULL
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.vwap
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.vwap
                ELSE EXCLUDED.vwap 
            END,
            trade_imbalance = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.trade_imbalance
                WHEN EXCLUDED.source = 'historical_rest' THEN NULL
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.trade_imbalance
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.trade_imbalance
                ELSE EXCLUDED.trade_imbalance 
            END,
            source = CASE 
                WHEN {table_name}.source = 'historical_rest' AND EXCLUDED.source <> 'historical_rest' THEN {table_name}.source
                WHEN EXCLUDED.source = 'historical_rest' THEN 'historical_rest'
                WHEN {table_name}.source = EXCLUDED.source AND {table_name}.is_closed = TRUE AND EXCLUDED.is_closed = FALSE THEN {table_name}.source
                WHEN {table_name}.source = 'realtime_stream' AND EXCLUDED.source = 'realtime_stream' 
                     AND {table_name}.is_closed = EXCLUDED.is_closed 
                     AND COALESCE({table_name}.trade_count, 0) > COALESCE(EXCLUDED.trade_count, 0) THEN {table_name}.source
                ELSE EXCLUDED.source 
            END,
            is_closed = CASE 
                WHEN {table_name}.source = 'historical_rest' THEN TRUE
                WHEN EXCLUDED.source = 'historical_rest' THEN TRUE
                WHEN {table_name}.is_closed = TRUE THEN TRUE
                ELSE EXCLUDED.is_closed 
            END;
    """


def _normalize_candle_records(rows) -> list:
    """Normalizes heterogenous rows (dict, Spark Row, pandas row) into standard 16-tuple for SQL upsert."""
    records = []
    for r in rows:
        if isinstance(r, dict):
            symbol = str(r["symbol"])
            w_start = r["window_start"]
            w_end = r["window_end"]
            open_p = float(r["open"])
            high_p = float(r["high"])
            low_p = float(r["low"])
            close_p = float(r["close"])
            vol = float(r["volume"])
            tc = int(r["trade_count"]) if r.get("trade_count") is not None else None
            bv = float(r["buy_volume"]) if r.get("buy_volume") is not None else None
            sv = float(r["sell_volume"]) if r.get("sell_volume") is not None else None
            vw = float(r["vwap"]) if r.get("vwap") is not None else None
            ti = float(r["trade_imbalance"]) if r.get("trade_imbalance") is not None else None
            tv = float(r["total_trade_value"]) if r.get("total_trade_value") is not None else None
            source = str(r.get("source", "realtime_stream"))
            is_closed = bool(r.get("is_closed", True))
        else:
            r_dict = r.asDict() if hasattr(r, "asDict") else {}
            symbol = str(r["symbol"])
            w_start = r["window_start"]
            w_end = r["window_end"]
            open_p = float(r["open"])
            high_p = float(r["high"])
            low_p = float(r["low"])
            close_p = float(r["close"])
            vol = float(r["volume"])
            tc = int(r["trade_count"]) if r_dict.get("trade_count") is not None else None
            bv = float(r["buy_volume"]) if r_dict.get("buy_volume") is not None else None
            sv = float(r["sell_volume"]) if r_dict.get("sell_volume") is not None else None
            vw = float(r["vwap"]) if r_dict.get("vwap") is not None else None
            ti = float(r["trade_imbalance"]) if r_dict.get("trade_imbalance") is not None else None
            tv = float(r["total_trade_value"]) if r_dict.get("total_trade_value") is not None else None
            source = str(r_dict.get("source", "realtime_stream"))
            is_closed = bool(r_dict.get("is_closed", True))

        records.append((
            symbol, ensure_utc_datetime(w_start), ensure_utc_datetime(w_end), open_p, high_p, low_p, close_p, vol,
            tc, bv, sv, vw, ti, tv, source, is_closed
        ))
    return records


def write_candles_batch(
    batch_df: Any, 
    batch_id: int = 0, 
    conn=None, 
    table_name: str = "warehouse.fact_crypto_1m_candles", 
    close_conn: bool = True,
    max_retries: int = 3
) -> int:
    """
    Ghi micro-batch nến 1 phút vào PostgreSQL Data Warehouse.
    Hỗ trợ cả Spark DataFrame, list of dicts, pandas DataFrame.
    Bảo đảm:
      - Sử dụng SQL Canonical Reconciliation thống nhất với Gold Layer.
      - Idempotent upsert: chạy lại không tạo bản ghi trùng.
      - Hỗ trợ dependency injection conn/table_name cho isolated testing.
      - Cơ chế retry lũy tiến nếu gặp lỗi mạng tạm thời khi dùng connection mặc định.
    """
    if hasattr(batch_df, "isEmpty") and batch_df.isEmpty():
        return 0

    if hasattr(batch_df, "collect"):
        rows = batch_df.collect()
    elif isinstance(batch_df, list):
        rows = batch_df
    elif hasattr(batch_df, "to_dict"):
        rows = batch_df.to_dict(orient="records")
    else:
        rows = list(batch_df)

    if not rows:
        return 0

    records = _normalize_candle_records(rows)
    upsert_sql = build_upsert_candles_sql(table_name)

    # Retry loop if using internal connection
    attempts = max_retries if conn is None else 1
    last_err = None

    for attempt in range(attempts):
        local_conn = False
        active_conn = conn
        try:
            if active_conn is None:
                active_conn = get_db_connection()
                local_conn = True

            cur = active_conn.cursor()
            cur.executemany(upsert_sql, records)
            active_conn.commit()
            cur.close()
            print(f"[DW SINK] Da nap/cap nhat {len(records)} nen 1m vao PostgreSQL ({table_name})!")
            return len(records)
        except Exception as e:
            last_err = e
            if active_conn:
                try:
                    active_conn.rollback()
                except Exception:
                    pass
            if attempt < attempts - 1 and conn is None:
                wait_s = 0.5 * (2 ** attempt)
                print(f"[!] DW sync attempt {attempt + 1} failed ({e}). Retrying in {wait_s:.1f}s...")
                time.sleep(wait_s)
            else:
                print(f"[!] Loi khi nap Nen 1m vao PostgreSQL ({table_name}): {e}")
                raise last_err
        finally:
            if local_conn and close_conn and active_conn:
                try:
                    active_conn.close()
                except Exception:
                    pass

    return 0


def write_historical_candles_batch(
    records: list, 
    conn=None, 
    table_name: str = "warehouse.fact_crypto_1m_candles", 
    close_conn: bool = True
) -> int:
    """
    Persists historical 1m candles into PostgreSQL DW with source='historical_rest'.
    Reuses the exact same canonical reconciliation upsert logic to guarantee single source of truth.
    """
    if not records:
        return 0

    # Ensure source and closure flags are canonical REST
    formatted_records = []
    for r in records:
        item = dict(r) if isinstance(r, dict) else (r.asDict() if hasattr(r, "asDict") else {})
        item["source"] = "historical_rest"
        item["is_closed"] = True
        formatted_records.append(item)

    return write_candles_batch(
        formatted_records, 
        batch_id=0, 
        conn=conn, 
        table_name=table_name, 
        close_conn=close_conn
    )
