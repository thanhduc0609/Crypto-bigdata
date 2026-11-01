"""
=============================================================================
Crypto Big Data - Web Overview Service (Tab 1)
=============================================================================
Service xử lý nghiệp vụ dữ liệu Tổng quan và Biểu đồ nến:
  - Giá gần nhất và thống kê biến động 24h thực tế (OKX REST / DW).
  - Nến 1m, 1h, 1d kèm tính toán VWAP.
  - Phân định rõ nguồn dữ liệu live_okx vs postgres_dw.
=============================================================================
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable


def format_iso_utc(dt):
    """Chuyển đổi datetime sang chuỗi ISO UTC chuẩn ending with Z"""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(dt)


def get_overview_data(
    source_pref: str,
    symbol: str,
    okx_rest_url: str,
    get_db_conn: Callable[[], Any],
    fetch_okx_json: Callable[[str], Optional[Dict[str, Any]]]
) -> tuple[Dict[str, Any], int]:
    """
    Cung cấp thông tin giá gần nhất và thống kê 24h thực tế.
    Biến động 24h chỉ trả về khi có đủ dữ liệu khoảng 24h (>= 23h).
    """
    if source_pref != "dw":
        ticker_url = f"{okx_rest_url}/api/v5/market/ticker?instId={symbol}"
        res = fetch_okx_json(ticker_url)
        if res and res.get("code") == "0" and res.get("data"):
            item = res["data"][0]
            last_price = float(item["last"])
            open_24h = float(item["open24h"]) if item.get("open24h") else None
            change_24h = round(((last_price - open_24h) / open_24h * 100), 2) if open_24h and open_24h > 0 else None
            high_24h = float(item["high24h"]) if item.get("high24h") else None
            low_24h = float(item["low24h"]) if item.get("low24h") else None
            vol_24h = float(item["vol24h"]) if item.get("vol24h") else None
            turnover_24h = float(item["volCcy24h"]) if item.get("volCcy24h") else None
            data_ts = datetime.fromtimestamp(int(item["ts"]) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            return {
                "status": "success",
                "symbol": symbol,
                "current_price": last_price,
                "change_24h": change_24h,
                "high_24h": high_24h,
                "low_24h": low_24h,
                "volume_24h": vol_24h,
                "turnover_24h": turnover_24h,
                "data_timestamp": data_ts,
                "source": "live_okx"
            }, 200
        else:
            return {
                "status": "error",
                "message": "Không thể kết nối đến OKX REST API để lấy dữ liệu Ticker",
                "source": "live_okx"
            }, 503

    # Nguồn phụ: PostgreSQL Data Warehouse
    conn = get_db_conn()
    if not conn:
        return {
            "status": "error",
            "message": "Không thể kết nối PostgreSQL Data Warehouse",
            "source": "postgres_dw"
        }, 500

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT close, window_start
                FROM warehouse.fact_crypto_1m_candles
                WHERE symbol = %s
                ORDER BY window_start DESC
                LIMIT 1;
            """, (symbol,))
            latest_row = cur.fetchone()
            if not latest_row:
                return {
                    "status": "empty",
                    "symbol": symbol,
                    "current_price": None,
                    "change_24h": None,
                    "message": "Chưa có dữ liệu nến trong Data Warehouse",
                    "source": "postgres_dw"
                }, 200

            current_price = float(latest_row[0])
            data_ts = format_iso_utc(latest_row[1])

            cur.execute("""
                SELECT 
                    MIN(window_start),
                    MAX(window_start),
                    MAX(high),
                    MIN(low),
                    SUM(volume),
                    SUM(total_trade_value)
                FROM warehouse.fact_crypto_1m_candles
                WHERE symbol = %s AND window_start >= NOW() - INTERVAL '24 hours';
            """, (symbol,))
            agg_row = cur.fetchone()

            change_24h = None
            high_24h = None
            low_24h = None
            vol_24h = None
            turnover_24h = None

            if agg_row and agg_row[0] and agg_row[1]:
                min_time = agg_row[0]
                max_time = agg_row[1]
                timespan_seconds = (max_time - min_time).total_seconds()

                if timespan_seconds >= 23 * 3600:
                    high_24h = float(agg_row[2]) if agg_row[2] is not None else None
                    low_24h = float(agg_row[3]) if agg_row[3] is not None else None
                    vol_24h = float(agg_row[4]) if agg_row[4] is not None else None
                    turnover_24h = float(agg_row[5]) if agg_row[5] is not None else None

                    cur.execute("""
                        SELECT open
                        FROM warehouse.fact_crypto_1m_candles
                        WHERE symbol = %s AND window_start >= NOW() - INTERVAL '24 hours'
                        ORDER BY window_start ASC
                        LIMIT 1;
                    """, (symbol,))
                    open_row = cur.fetchone()
                    if open_row and open_row[0] > 0:
                        open_val = float(open_row[0])
                        change_24h = round(((current_price - open_val) / open_val * 100), 2)

            return {
                "status": "success",
                "symbol": symbol,
                "current_price": current_price,
                "change_24h": change_24h,
                "high_24h": high_24h,
                "low_24h": low_24h,
                "volume_24h": vol_24h,
                "turnover_24h": turnover_24h,
                "data_timestamp": data_ts,
                "source": "postgres_dw"
            }, 200
    except Exception as e:
        return {
            "status": "error",
            "message": f"Lỗi truy vấn tổng quan từ Data Warehouse: {e}",
            "source": "postgres_dw"
        }, 500
    finally:
        conn.close()


def get_candles_data(
    source_pref: str,
    symbol: str,
    timeframe: str,
    limit: int,
    okx_rest_url: str,
    get_db_conn: Callable[[], Any],
    fetch_okx_json: Callable[[str], Optional[Dict[str, Any]]]
) -> tuple[Dict[str, Any], int]:
    """
    Trả về danh sách nến OHLCV, VWAP và vi cấu trúc nến.
    Chỉ trả về các trường thật, không tự suy đoán hoặc chia đôi volume.
    """
    if source_pref != "dw":
        bar_map = {"1m": "1m", "1h": "1H", "1d": "1D"}
        bar = bar_map.get(timeframe, "1m")
        url = f"{okx_rest_url}/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
        data = fetch_okx_json(url)

        if data and data.get("code") == "0" and data.get("data"):
            candles = []
            for c in reversed(data["data"]):
                ts_sec = int(c[0]) // 1000
                o = float(c[1])
                h = float(c[2])
                l = float(c[3])
                cl = float(c[4])
                vol = float(c[5])
                turnover = float(c[7]) if len(c) > 7 and c[7] else (vol * cl)
                is_closed = (c[8] == "1") if len(c) > 8 else True
                vwap = round(turnover / vol, 2) if vol > 0 else None

                candles.append({
                    "time": ts_sec,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": cl,
                    "volume": vol,
                    "vwap": vwap,
                    "buy_volume": None,     # Nguồn REST không có microstructure
                    "sell_volume": None,
                    "trade_imbalance": None,
                    "total_trade_value": turnover,
                    "is_closed": is_closed
                })

            data_ts = datetime.fromtimestamp(candles[-1]["time"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if candles else None
            return {
                "status": "success",
                "symbol": symbol,
                "timeframe": timeframe,
                "source": "live_okx",
                "candles": candles,
                "data_timestamp": data_ts
            }, 200
        else:
            return {
                "status": "error",
                "message": "Không thể tải nến từ OKX REST API",
                "source": "live_okx"
            }, 503

    # Nguồn phụ: PostgreSQL Data Warehouse
    conn = get_db_conn()
    if not conn:
        return {
            "status": "error",
            "message": "Không thể kết nối PostgreSQL Data Warehouse",
            "source": "postgres_dw"
        }, 500

    try:
        with conn.cursor() as cur:
            if timeframe == "1h":
                sql = """
                    SELECT 
                        EXTRACT(EPOCH FROM hour_bucket)::bigint AS time,
                        open_price, high_price, low_price, close_price, total_volume,
                        hourly_vwap, buy_volume, sell_volume, avg_trade_imbalance, total_turnover_usd,
                        TRUE AS is_closed
                    FROM warehouse.vw_hourly_market_summary
                    WHERE symbol = %s
                    ORDER BY hour_bucket DESC
                    LIMIT %s;
                """
            elif timeframe == "1d":
                sql = """
                    SELECT 
                        EXTRACT(EPOCH FROM (trade_date::timestamp AT TIME ZONE 'UTC'))::bigint AS time,
                        open_price, high_price, low_price, close_price, total_volume,
                        daily_vwap, buy_volume, sell_volume, NULL AS avg_trade_imbalance, total_turnover_usd,
                        TRUE AS is_closed
                    FROM warehouse.vw_daily_market_summary
                    WHERE symbol = %s
                    ORDER BY trade_date DESC
                    LIMIT %s;
                """
            else:
                sql = """
                    SELECT 
                        EXTRACT(EPOCH FROM window_start)::bigint AS time,
                        open, high, low, close, volume,
                        vwap, buy_volume, sell_volume, trade_imbalance, total_trade_value,
                        is_closed
                    FROM warehouse.fact_crypto_1m_candles
                    WHERE symbol = %s
                    ORDER BY window_start DESC
                    LIMIT %s;
                """

            cur.execute(sql, (symbol, limit))
            rows = cur.fetchall()

            candles = []
            if rows:
                for r in reversed(rows):
                    candles.append({
                        "time": int(r[0]),
                        "open": float(r[1]),
                        "high": float(r[2]),
                        "low": float(r[3]),
                        "close": float(r[4]),
                        "volume": float(r[5]),
                        "vwap": float(r[6]) if r[6] is not None else None,
                        "buy_volume": float(r[7]) if r[7] is not None else None,
                        "sell_volume": float(r[8]) if r[8] is not None else None,
                        "trade_imbalance": float(r[9]) if r[9] is not None else None,
                        "total_trade_value": float(r[10]) if r[10] is not None else None,
                        "is_closed": bool(r[11])
                    })

            data_ts = datetime.fromtimestamp(candles[-1]["time"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if candles else None
            return {
                "status": "success",
                "symbol": symbol,
                "timeframe": timeframe,
                "source": "postgres_dw",
                "candles": candles,
                "data_timestamp": data_ts
            }, 200
    except Exception as e:
        return {
            "status": "error",
            "message": f"Lỗi truy vấn nến từ Data Warehouse: {e}",
            "source": "postgres_dw"
        }, 500
    finally:
        conn.close()

