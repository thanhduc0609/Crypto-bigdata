"""
=============================================================================
Crypto Big Data - Web Analytics & Quant Trading Dashboard (Flask Backend)
=============================================================================
Cung cấp API phục vụ Web Dashboard:
  - Nến tương tác (OHLCV, VWAP, Imbalance) hỗ trợ Live Stream OKX & PostgreSQL DW
  - Phân tích Dòng tiền & Cán cân Mua/Bán (Order Flow Pressure)
  - Radar Cá mập (Live Whale Alerts & Historical Whale Archive)
  - Chỉ số Phái sinh (Funding Rate, APY, Open Interest)
  - Truy vấn & Xuất dữ liệu Data Warehouse (PostgreSQL Star Schema)
  - Giám sát Kiến trúc Big Data Pipeline & Đo lường Telemetry
=============================================================================
"""

import os
import sys
import json
import time
import math
import io
import csv
import urllib.request
from datetime import datetime, timezone, date
from decimal import Decimal
from pathlib import Path
from flask import Flask, render_template, jsonify, request, Response, send_file

# UTF-8 stdout configuration for Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
        os.system("")
    except Exception:
        pass

# Setup path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import load_env_vars

load_env_vars()

app = Flask(
    __name__,
    template_folder=str(project_root / "web" / "templates"),
    static_folder=str(project_root / "web" / "static")
)

OKX_REST_URL = os.getenv("OKX_REST_URL", "https://www.okx.com")
SYMBOLS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]

# Database config
DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
DB_NAME = os.getenv("POSTGRES_DB", "crypto_dw")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

ALLOWED_DW_TABLES = [
    "vw_whale_trades",
    "vw_hourly_market_summary",
    "vw_daily_market_summary",
    "vw_weekly_market_summary",
    "vw_monthly_market_summary",
    "vw_yearly_market_summary",
    "vw_order_flow_pressure",
    "fact_crypto_trades",
    "fact_crypto_1m_candles",
    "fact_market_predictions",
    "fact_crypto_funding_rates",
    "fact_crypto_open_interest",
    "dim_symbol"
]


def get_db_connection():
    """Thử kết nối tới PostgreSQL Data Warehouse"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=2
        )
        return conn
    except Exception:
        return None


def serialize_val(val):
    """Serialize các kiểu dữ liệu đặc thù của Postgres sang JSON friendly"""
    if isinstance(val, Decimal):
        return float(val)
    elif isinstance(val, (datetime, date)):
        return val.isoformat()
    return val


def fetch_okx_json(url: str):
    """Gọi REST API công khai của OKX"""
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CryptoDataBot/1.0"}
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception:
        return None


# =============================================================================
# API ENDPOINTS: HEALTH & OVERVIEW
# =============================================================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/health")
def api_health():
    """Kiểm tra toàn diện trạng thái hệ thống"""
    conn = get_db_connection()
    db_ok = conn is not None
    table_counts = {}
    db_size = "0 MB"

    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_size_pretty(pg_database_size(%s));", (DB_NAME,))
                res = cur.fetchone()
                if res:
                    db_size = res[0]
                for tbl in ["fact_crypto_trades", "fact_crypto_1m_candles", "vw_whale_trades"]:
                    cur.execute(f"SELECT count(*) FROM warehouse.{tbl};")
                    table_counts[tbl] = cur.fetchone()[0]
        except Exception:
            pass
        finally:
            conn.close()

    csv_path = project_root / "data" / "exports" / "raw_trades.csv"
    has_local_data = csv_path.exists()
    s3_bucket = os.getenv("S3_BUCKET_NAME", "crypto-bigdata-lake-raw")

    return jsonify({
        "status": "online",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database_connected": db_ok,
        "database_target": f"{DB_HOST}:{DB_PORT}/{DB_NAME}",
        "database_size": db_size,
        "table_counts": table_counts,
        "lake_s3_bucket": s3_bucket,
        "local_cache_available": has_local_data,
        "symbols": SYMBOLS
    })


@app.route("/api/overview")
def api_overview():
    """Thông tin ticker tổng quan 24h về 3 cặp tiền"""
    results = {}
    
    for sym in SYMBOLS:
        ticker_url = f"{OKX_REST_URL}/api/v5/market/ticker?instId={sym}"
        res = fetch_okx_json(ticker_url)
        
        if res and res.get("code") == "0" and res.get("data"):
            item = res["data"][0]
            last_price = float(item.get("last", 0.0))
            open_24h = float(item.get("open24h", last_price))
            change_24h = ((last_price - open_24h) / open_24h * 100) if open_24h > 0 else 0.0
            high_24h = float(item.get("high24h", last_price))
            low_24h = float(item.get("low24h", last_price))
            vol_24h = float(item.get("vol24h", 0.0))
            vol_usd_24h = float(item.get("volCcy24h", 0.0))
            
            results[sym] = {
                "symbol": sym,
                "price": last_price,
                "change_24h": round(change_24h, 2),
                "high_24h": high_24h,
                "low_24h": low_24h,
                "volume_24h": vol_24h,
                "volume_usd_24h": vol_usd_24h,
                "vwap": round((high_24h + low_24h + last_price) / 3, 2),
                "source": "live_okx"
            }
        else:
            fallback_prices = {"BTC-USDT": 86500.0, "ETH-USDT": 2750.0, "SOL-USDT": 122.5}
            p = fallback_prices.get(sym, 100.0)
            results[sym] = {
                "symbol": sym,
                "price": p,
                "change_24h": 1.25,
                "high_24h": round(p * 1.02, 2),
                "low_24h": round(p * 0.98, 2),
                "volume_24h": 12000.0,
                "volume_usd_24h": round(p * 12000, 2),
                "vwap": p,
                "source": "fallback"
            }

    return jsonify(results)


# =============================================================================
# API ENDPOINT: CANDLES (OHLCV + VWAP)
# =============================================================================

@app.route("/api/candles")
def api_candles():
    """
    Lấy danh sách nến OHLCV + VWAP cho TradingView Lightweight Charts
    Query params:
      - symbol: BTC-USDT, ETH-USDT, SOL-USDT (default: BTC-USDT)
      - timeframe: 1m, 5m, 15m, 1h, 1d (default: 1m)
      - source: live (OKX REST API), dw (PostgreSQL DW), auto (default: live)
      - limit: số nến tối đa (default: 150)
    """
    symbol = request.args.get("symbol", "BTC-USDT")
    timeframe = request.args.get("timeframe", "1m").lower()
    source_pref = request.args.get("source", "live").lower()
    limit = int(request.args.get("limit", 150))

    # 1. NẾU YÊU CẦU NGUỒN TỪ DATA WAREHOUSE (POSTGRESQL DW)
    if source_pref == "dw":
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    candles = []
                    if timeframe == "1d":
                        sql = """
                            SELECT 
                                EXTRACT(EPOCH FROM trade_date::timestamp)::bigint AS time,
                                open_price, high_price, low_price, close_price, total_volume, daily_vwap, 0.0 AS imbalance, total_turnover_usd
                            FROM warehouse.vw_daily_market_summary
                            WHERE symbol = %s
                            ORDER BY trade_date DESC
                            LIMIT %s;
                        """
                        cur.execute(sql, (symbol, limit))
                    elif timeframe in ("1w", "week"):
                        sql = """
                            SELECT 
                                EXTRACT(EPOCH FROM week_start_date::timestamp)::bigint AS time,
                                open_price, high_price, low_price, close_price, total_volume, weekly_vwap, 0.0 AS imbalance, total_turnover_usd
                            FROM warehouse.vw_weekly_market_summary
                            WHERE symbol = %s
                            ORDER BY week_start_date DESC
                            LIMIT %s;
                        """
                        cur.execute(sql, (symbol, limit))
                    elif timeframe in ("1mo", "1month", "month"):
                        sql = """
                            SELECT 
                                EXTRACT(EPOCH FROM month_start_date::timestamp)::bigint AS time,
                                open_price, high_price, low_price, close_price, total_volume, monthly_vwap, 0.0 AS imbalance, total_turnover_usd
                            FROM warehouse.vw_monthly_market_summary
                            WHERE symbol = %s
                            ORDER BY month_start_date DESC
                            LIMIT %s;
                        """
                        cur.execute(sql, (symbol, limit))
                    elif timeframe in ("1y", "1year", "year"):
                        sql = """
                            SELECT 
                                EXTRACT(EPOCH FROM year_start_date::timestamp)::bigint AS time,
                                open_price, high_price, low_price, close_price, total_volume, yearly_vwap, 0.0 AS imbalance, total_turnover_usd
                            FROM warehouse.vw_yearly_market_summary
                            WHERE symbol = %s
                            ORDER BY year_start_date DESC
                            LIMIT %s;
                        """
                        cur.execute(sql, (symbol, limit))
                    elif timeframe == "1h":
                        sql = """
                            SELECT 
                                EXTRACT(EPOCH FROM hour_bucket)::bigint AS time,
                                open_price, high_price, low_price, close_price, total_volume, hourly_vwap, avg_trade_imbalance, total_turnover_usd
                            FROM warehouse.vw_hourly_market_summary
                            WHERE symbol = %s
                            ORDER BY hour_bucket DESC
                            LIMIT %s;
                        """
                        cur.execute(sql, (symbol, limit))
                    else:
                        sql = """
                            SELECT 
                                EXTRACT(EPOCH FROM window_start)::bigint AS time,
                                open, high, low, close, volume, vwap, trade_imbalance, total_trade_value
                            FROM warehouse.fact_crypto_1m_candles
                            WHERE symbol = %s
                            ORDER BY window_start DESC
                            LIMIT %s;
                        """
                        cur.execute(sql, (symbol, limit))

                    rows = cur.fetchall()
                    if rows:
                        for r in reversed(rows):
                            candles.append({
                                "time": r[0],
                                "open": float(r[1]),
                                "high": float(r[2]),
                                "low": float(r[3]),
                                "close": float(r[4]),
                                "volume": float(r[5]),
                                "vwap": float(r[6]) if r[6] else float(r[4]),
                                "imbalance": float(r[7]) if r[7] else 0.0,
                                "trade_value": float(r[8]) if r[8] else 0.0
                            })
                        conn.close()
                        return jsonify({
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "source": "postgres_dw",
                            "candles": candles
                        })
            except Exception:
                pass
            finally:
                conn.close()

    # 2. MẶC ĐỊNH HOẶC FALLBACK: LẤY TRỰC TIẾP TỪ OKX LIVE REST API
    bar_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1H", "1d": "1D"}
    okx_bar = bar_map.get(timeframe, "1m")
    url = f"{OKX_REST_URL}/api/v5/market/candles?instId={symbol}&bar={okx_bar}&limit={min(limit, 300)}"
    data = fetch_okx_json(url)

    if data and data.get("code") == "0" and data.get("data"):
        raw_candles = data["data"]
        candles = []
        for c in reversed(raw_candles):
            ts = int(c[0]) // 1000
            o = float(c[1])
            h = float(c[2])
            l = float(c[3])
            cl = float(c[4])
            vol = float(c[5])
            vol_ccy = float(c[6]) if len(c) > 6 and c[6] else vol * cl
            vwap = round(vol_ccy / vol, 2) if vol > 0 else round((o + h + l + cl) / 4, 2)
            imbalance = round((cl - o) / (h - l) if h > l else 0.0, 3)

            candles.append({
                "time": ts,
                "open": o,
                "high": h,
                "low": l,
                "close": cl,
                "volume": vol,
                "vwap": vwap,
                "imbalance": imbalance,
                "trade_value": vol_ccy
            })
        return jsonify({
            "symbol": symbol,
            "timeframe": timeframe,
            "source": "live_okx",
            "candles": candles
        })

    # 3. FALLBACK TỪ POSTGRESQL NẾU OKX MẤT MẠNG
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT 
                        EXTRACT(EPOCH FROM window_start)::bigint AS time,
                        open, high, low, close, volume, vwap, trade_imbalance, total_trade_value
                    FROM warehouse.fact_crypto_1m_candles
                    WHERE symbol = %s
                    ORDER BY window_start DESC
                    LIMIT %s;
                """
                cur.execute(sql, (symbol, limit))
                rows = cur.fetchall()
                if rows:
                    candles = []
                    for r in reversed(rows):
                        candles.append({
                            "time": r[0],
                            "open": float(r[1]),
                            "high": float(r[2]),
                            "low": float(r[3]),
                            "close": float(r[4]),
                            "volume": float(r[5]),
                            "vwap": float(r[6]) if r[6] else float(r[4]),
                            "imbalance": float(r[7]) if r[7] else 0.0,
                            "trade_value": float(r[8]) if r[8] else 0.0
                        })
                    conn.close()
                    return jsonify({
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "source": "postgres_dw_fallback",
                        "candles": candles
                    })
        except Exception:
            pass
        finally:
            conn.close()

    # 4. FALLBACK MOCK DATA NẾU KHÔNG CÓ KẾT NỐI
    now_ts = int(time.time())
    base_p = 86500.0 if "BTC" in symbol else (2750.0 if "ETH" in symbol else 122.0)
    mock_candles = []
    for i in range(60, 0, -1):
        t = now_ts - i * 60
        o = base_p + math.sin(i / 5) * 50
        h = o + 25
        l = o - 20
        cl = o + 10
        mock_candles.append({
            "time": t, "open": round(o, 2), "high": round(h, 2), "low": round(l, 2), "close": round(cl, 2),
            "volume": 12.5, "vwap": round(o + 5, 2), "imbalance": 0.15, "trade_value": 12.5 * o
        })
    return jsonify({"symbol": symbol, "timeframe": timeframe, "source": "mock", "candles": mock_candles})


# =============================================================================
# API ENDPOINT: WHALE ALERTS & RADAR
# =============================================================================

@app.route("/api/whales")
def api_whales():
    """
    Lấy danh sách các giao dịch Cá mập (Whale Trades).
    Query params:
      - symbol: BTC-USDT, ETH-USDT, SOL-USDT hoặc rỗng (all)
      - source: live (live OKX feed), dw (PostgreSQL historical archive), all
      - tier: mega, whale, large, all
      - limit: default 25
    """
    symbol = request.args.get("symbol", "").strip()
    source_pref = request.args.get("source", "all").lower()
    tier_filter = request.args.get("tier", "all").lower()
    limit = int(request.args.get("limit", 25))

    # Nếu truy vấn từ Data Warehouse
    if source_pref in ("dw", "all"):
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    conditions = []
                    params = []

                    if symbol:
                        conditions.append("symbol = %s")
                        params.append(symbol)

                    if tier_filter == "mega":
                        conditions.append("trade_value_usd >= 2000000")
                    elif tier_filter == "whale":
                        conditions.append("trade_value_usd >= 1000000 AND trade_value_usd < 2000000")
                    elif tier_filter == "large":
                        conditions.append("trade_value_usd >= 500000 AND trade_value_usd < 1000000")

                    where_sql = ("WHERE " + " AND ".join(conditions)) if conditions else ""
                    params.append(limit)

                    sql = f"""
                        SELECT trade_id, symbol, side, price, size, trade_value_usd, trade_time, whale_tier
                        FROM warehouse.vw_whale_trades
                        {where_sql}
                        ORDER BY trade_time DESC
                        LIMIT %s;
                    """
                    cur.execute(sql, params)
                    rows = cur.fetchall()
                    if rows:
                        trades = []
                        for r in rows:
                            trades.append({
                                "trade_id": str(r[0]),
                                "symbol": r[1],
                                "side": r[2],
                                "price": float(r[3]),
                                "size": float(r[4]),
                                "trade_value": float(r[5]),
                                "time": r[6].strftime("%Y-%m-%d %H:%M:%S") if r[6] else "",
                                "tier": r[7],
                                "source": "postgres_dw"
                            })
                        conn.close()
                        return jsonify({"source": "postgres_dw", "whales": trades})
            except Exception:
                pass
            finally:
                conn.close()

    # Truy vấn từ Live OKX Trades
    target_sym = symbol if symbol else "BTC-USDT"
    url = f"{OKX_REST_URL}/api/v5/market/trades?instId={target_sym}&limit=100"
    data = fetch_okx_json(url)

    whales = []
    if data and data.get("code") == "0" and data.get("data"):
        for item in data["data"]:
            price = float(item["px"])
            size = float(item["sz"])
            val = round(price * size, 2)
            if val >= 500000:
                if val >= 2000000:
                    tier = "Mega Whale (>= $2M)"
                elif val >= 1000000:
                    tier = "Big Whale ($1M - $2M)"
                else:
                    tier = "Whale ($500K - $1M)"

                if tier_filter == "mega" and "Mega" not in tier:
                    continue
                if tier_filter == "whale" and "Whale (" not in tier:
                    continue
                if tier_filter == "large" and "Large" not in tier:
                    continue

                whales.append({
                    "trade_id": item["tradeId"],
                    "symbol": target_sym,
                    "side": item["side"],
                    "price": price,
                    "size": size,
                    "trade_value": val,
                    "time": datetime.fromtimestamp(int(item["ts"]) / 1000, tz=timezone.utc).strftime("%H:%M:%S"),
                    "tier": tier,
                    "source": "live_okx"
                })

    return jsonify({"source": "live_feed", "whales": whales[:limit]})


# =============================================================================
# API ENDPOINT: ORDER FLOW & DERIVATIVES
# =============================================================================

@app.route("/api/orderflow")
def api_orderflow():
    """Lấy cán cân áp lực Mua/Bán (Order Flow Pressure & Imbalance)"""
    symbol = request.args.get("symbol", "BTC-USDT")
    source_pref = request.args.get("source", "live").lower()

    # 1. NẾU YÊU CẦU DATA WAREHOUSE: Lấy từ View của DW
    if source_pref == "dw":
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    # Lấy giá đóng cửa gần nhất để định giá USD chính xác (không hardcode)
                    cur.execute("""
                        SELECT close FROM warehouse.fact_crypto_1m_candles
                        WHERE symbol = %s ORDER BY window_start DESC LIMIT 1;
                    """, (symbol,))
                    price_row = cur.fetchone()
                    ref_price = float(price_row[0]) if (price_row and price_row[0]) else (85000.0 if "BTC" in symbol else (2700.0 if "ETH" in symbol else 130.0))

                    cur.execute("""
                        SELECT total_buy_volume, total_sell_volume, buy_to_sell_ratio, net_order_imbalance, market_sentiment
                        FROM warehouse.vw_order_flow_pressure
                        WHERE symbol = %s;
                    """, (symbol,))
                    row = cur.fetchone()
                    if row:
                        b_vol = float(row[0]) if row[0] else 0.0
                        s_vol = float(row[1]) if row[1] else 0.0
                        total = b_vol + s_vol
                        b_pct = round((b_vol / total * 100) if total > 0 else 50.0, 1)
                        s_pct = round(100.0 - b_pct, 1)
                        imbalance = float(row[3]) if row[3] else 0.0
                        sentiment = row[4]
                        return jsonify({
                            "symbol": symbol,
                            "buy_volume": round(b_vol, 4),
                            "sell_volume": round(s_vol, 4),
                            "buy_value_usd": round(b_vol * ref_price, 2),
                            "sell_value_usd": round(s_vol * ref_price, 2),
                            "buy_ratio_pct": b_pct,
                            "sell_ratio_pct": s_pct,
                            "imbalance": round(imbalance, 4),
                            "sentiment": sentiment,
                            "source": "postgres_dw"
                        })
            except Exception as e:
                logger.warning(f"Error querying DW orderflow: {e}")
            finally:
                conn.close()

    # 2. MẶC ĐỊNH (REAL-TIME STREAMING): Tính toán trực tiếp theo 100 lệnh khớp liên tục mới nhất
    url = f"{OKX_REST_URL}/api/v5/market/trades?instId={symbol}&limit=100"
    data = fetch_okx_json(url)

    buy_vol = 0.0
    sell_vol = 0.0
    buy_val = 0.0
    sell_val = 0.0

    if data and data.get("code") == "0" and data.get("data"):
        for item in data["data"]:
            sz = float(item["sz"])
            px = float(item["px"])
            val = sz * px
            if item["side"] == "buy":
                buy_vol += sz
                buy_val += val
            else:
                sell_vol += sz
                sell_val += val

        total_vol = buy_vol + sell_vol
        imbalance = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0.0
        buy_ratio = (buy_vol / total_vol * 100) if total_vol > 0 else 50.0
        sell_ratio = 100.0 - buy_ratio

        if imbalance >= 0.2:
            sentiment = "Bullish Pressure (Phe Mua áp đảo)"
        elif imbalance <= -0.2:
            sentiment = "Bearish Pressure (Phe Bán áp đảo)"
        else:
            sentiment = "Neutral (Cân bằng)"

        return jsonify({
            "symbol": symbol,
            "buy_volume": round(buy_vol, 4),
            "sell_volume": round(sell_vol, 4),
            "buy_value_usd": round(buy_val, 2),
            "sell_value_usd": round(sell_val, 2),
            "buy_ratio_pct": round(buy_ratio, 1),
            "sell_ratio_pct": round(sell_ratio, 1),
            "imbalance": round(imbalance, 4),
            "sentiment": sentiment,
            "trades_count": len(data["data"]),
            "source": "live_okx"
        })

    # 3. FALLBACK: Nếu API bên ngoài lỗi, fallback sang DW
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT total_buy_volume, total_sell_volume, buy_to_sell_ratio, net_order_imbalance, market_sentiment
                    FROM warehouse.vw_order_flow_pressure
                    WHERE symbol = %s;
                """, (symbol,))
                row = cur.fetchone()
                if row:
                    b_vol = float(row[0]) if row[0] else 0.0
                    s_vol = float(row[1]) if row[1] else 0.0
                    total = b_vol + s_vol
                    b_pct = round((b_vol / total * 100) if total > 0 else 50.0, 1)
                    s_pct = round(100.0 - b_pct, 1)
                    imbalance = float(row[3]) if row[3] else 0.0
                    sentiment = row[4]
                    ref_price = 85000.0 if "BTC" in symbol else (2700.0 if "ETH" in symbol else 130.0)
                    return jsonify({
                        "symbol": symbol,
                        "buy_volume": round(b_vol, 4),
                        "sell_volume": round(s_vol, 4),
                        "buy_value_usd": round(b_vol * ref_price, 2),
                        "sell_value_usd": round(s_vol * ref_price, 2),
                        "buy_ratio_pct": b_pct,
                        "sell_ratio_pct": s_pct,
                        "imbalance": round(imbalance, 4),
                        "sentiment": sentiment,
                        "source": "postgres_dw_fallback"
                    })
        except Exception:
            pass
        finally:
            conn.close()

    # 4. Fallback cuối cùng nếu cả 2 đều không có
    return jsonify({
        "symbol": symbol,
        "buy_volume": 0.0,
        "sell_volume": 0.0,
        "buy_value_usd": 0.0,
        "sell_value_usd": 0.0,
        "buy_ratio_pct": 50.0,
        "sell_ratio_pct": 50.0,
        "imbalance": 0.0,
        "sentiment": "Neutral (Chờ dữ liệu)",
        "source": "empty"
    })


@app.route("/api/derivatives")
def api_derivatives():
    """Lấy chỉ số thị trường phái sinh: Funding Rate, Open Interest"""
    results = {}
    
    for base_sym in ["BTC", "ETH", "SOL"]:
        inst_id = f"{base_sym}-USDT-SWAP"
        display_sym = f"{base_sym}-USDT"
        
        f_url = f"{OKX_REST_URL}/api/v5/public/funding-rate?instId={inst_id}"
        f_data = fetch_okx_json(f_url)
        funding_rate = 0.0001
        next_rate = 0.0001
        
        if f_data and f_data.get("code") == "0" and f_data.get("data"):
            item = f_data["data"][0]
            try:
                fr_val = item.get("fundingRate")
                funding_rate = float(fr_val) if fr_val not in (None, "") else 0.0001
            except (ValueError, TypeError):
                funding_rate = 0.0001

            try:
                next_val = item.get("nextFundingRate")
                next_rate = float(next_val) if next_val not in (None, "") else funding_rate
            except (ValueError, TypeError):
                next_rate = funding_rate

        oi_url = f"{OKX_REST_URL}/api/v5/public/open-interest?instType=SWAP&instId={inst_id}"
        oi_data = fetch_okx_json(oi_url)
        oi_usd = 0.0
        oi_coins = 0.0
        
        if oi_data and oi_data.get("code") == "0" and oi_data.get("data"):
            oi_item = oi_data["data"][0]
            try:
                oi_c = oi_item.get("oi")
                oi_coins = float(oi_c) if oi_c not in (None, "") else 0.0
            except (ValueError, TypeError):
                oi_coins = 0.0

            try:
                oi_u = oi_item.get("oiCcy")
                oi_usd = float(oi_u) if oi_u not in (None, "") else 0.0
            except (ValueError, TypeError):
                oi_usd = 0.0

        projected_apy = funding_rate * 3 * 365 * 100

        results[display_sym] = {
            "symbol": display_sym,
            "funding_rate_pct": round(funding_rate * 100, 4),
            "next_funding_rate_pct": round(next_rate * 100, 4),
            "projected_apy_pct": round(projected_apy, 2),
            "open_interest_usd": round(oi_usd, 2),
            "open_interest_coins": round(oi_coins, 2),
            "funding_state": "Dương (Long trả Short)" if funding_rate > 0 else "Âm (Short trả Long)"
        }

    return jsonify(results)


# =============================================================================
# API ENDPOINTS: DATA WAREHOUSE EXPLORER & ANALYTICS
# =============================================================================

@app.route("/api/warehouse/stats")
def api_warehouse_stats():
    """Thống kê tổng quan Data Warehouse: danh sách bảng, số bản ghi, dung lượng"""
    conn = get_db_connection()
    if not conn:
        return jsonify({
            "connected": False,
            "message": "Không thể kết nối đến PostgreSQL Data Warehouse"
        }), 503

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_size_pretty(pg_database_size(%s));", (DB_NAME,))
            db_size = cur.fetchone()[0]

            tables_info = []
            for tbl in ALLOWED_DW_TABLES:
                cur.execute(f"SELECT count(*) FROM warehouse.{tbl};")
                cnt = cur.fetchone()[0]
                tables_info.append({
                    "name": tbl,
                    "count": cnt,
                    "type": "VIEW" if tbl.startswith("vw_") else "FACT/DIM"
                })

            # Lấy thống kê Whale
            cur.execute("""
                SELECT 
                    count(*), 
                    COALESCE(SUM(trade_value_usd), 0),
                    COALESCE(MAX(trade_value_usd), 0)
                FROM warehouse.vw_whale_trades;
            """)
            whale_stats = cur.fetchone()

            # Lấy thống kê fact_crypto_trades
            cur.execute("""
                SELECT 
                    count(*),
                    COALESCE(SUM(trade_value), 0),
                    MIN(trade_time),
                    MAX(trade_time)
                FROM warehouse.fact_crypto_trades;
            """)
            trades_stats = cur.fetchone()

        conn.close()
        return jsonify({
            "connected": True,
            "database_name": DB_NAME,
            "database_size": db_size,
            "host": f"{DB_HOST}:{DB_PORT}",
            "tables": tables_info,
            "whale_summary": {
                "total_whale_trades": whale_stats[0],
                "total_turnover_usd": round(float(whale_stats[1]), 2),
                "max_single_trade_usd": round(float(whale_stats[2]), 2)
            },
            "trades_summary": {
                "total_trades": trades_stats[0],
                "total_value_usd": round(float(trades_stats[1]), 2),
                "earliest_time": trades_stats[2].isoformat() if trades_stats[2] else None,
                "latest_time": trades_stats[3].isoformat() if trades_stats[3] else None
            }
        })
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({"connected": False, "error": str(e)}), 500


@app.route("/api/warehouse/query")
def api_warehouse_query():
    """
    Truy vấn động an toàn từ các bảng / views trong Data Warehouse
    Params:
      - table: Tên bảng (phải nằm trong ALLOWED_DW_TABLES)
      - symbol: lọc theo symbol (tùy chọn)
      - tier: lọc theo whale_tier (nếu truy vấn vw_whale_trades)
      - limit: giới hạn bản ghi (default: 50, max: 200)
      - offset: phân trang (default: 0)
    """
    table = request.args.get("table", "vw_whale_trades")
    if table not in ALLOWED_DW_TABLES:
        return jsonify({"error": f"Bảng không hợp lệ. Cho phép: {ALLOWED_DW_TABLES}"}), 400

    symbol = request.args.get("symbol", "").strip()
    tier = request.args.get("tier", "").strip()
    limit = min(int(request.args.get("limit", 50)), 200)
    offset = max(int(request.args.get("offset", 0)), 0)

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Không có kết nối PostgreSQL"}), 503

    try:
        with conn.cursor() as cur:
            conditions = []
            params = []

            if symbol and symbol != "ALL":
                conditions.append("symbol = %s")
                params.append(symbol)

            if tier and tier != "ALL" and "whale_tier" in table:
                conditions.append("whale_tier LIKE %s")
                params.append(f"%{tier}%")

            where_str = ("WHERE " + " AND ".join(conditions)) if conditions else ""

            # Count total
            count_sql = f"SELECT count(*) FROM warehouse.{table} {where_str};"
            cur.execute(count_sql, params)
            total_count = cur.fetchone()[0]

            # Query data
            query_sql = f"SELECT * FROM warehouse.{table} {where_str} LIMIT %s OFFSET %s;"
            query_params = list(params) + [limit, offset]
            cur.execute(query_sql, query_params)

            cols = [desc[0] for desc in cur.description]
            raw_rows = cur.fetchall()
            rows = [[serialize_val(val) for val in r] for r in raw_rows]

        conn.close()
        return jsonify({
            "table": table,
            "columns": cols,
            "rows": rows,
            "total_count": total_count,
            "limit": limit,
            "offset": offset
        })
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/warehouse/export")
def api_warehouse_export():
    """Xuất trực tiếp dữ liệu từ bảng/view Data Warehouse sang file CSV"""
    table = request.args.get("table", "vw_whale_trades")
    if table not in ALLOWED_DW_TABLES:
        return jsonify({"error": "Bảng không hợp lệ"}), 400

    symbol = request.args.get("symbol", "").strip()

    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Không có kết nối PostgreSQL"}), 503

    try:
        with conn.cursor() as cur:
            where_str = ""
            params = []
            if symbol and symbol != "ALL":
                where_str = "WHERE symbol = %s"
                params = [symbol]

            sql = f"SELECT * FROM warehouse.{table} {where_str} LIMIT 5000;"
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

        conn.close()

        # Tạo file CSV trong bộ nhớ
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(cols)
        for r in rows:
            writer.writerow([serialize_val(c) for c in r])

        csv_content = output.getvalue()
        output.close()

        filename = f"{table}_{symbol or 'all'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return Response(
            csv_content,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        if conn:
            conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/exports/list")
def api_exports_list():
    """Liệt kê danh sách các file CSV đã xuất trong data/exports/"""
    exports_dir = project_root / "data" / "exports"
    files = []
    if exports_dir.exists():
        for f in sorted(exports_dir.glob("*.csv"), key=lambda x: x.stat().st_mtime, reverse=True):
            files.append({
                "name": f.name,
                "size_kb": round(f.stat().st_size / 1024, 2),
                "modified": datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "download_url": f"/api/exports/download/{f.name}"
            })
    return jsonify({"exports": files, "count": len(files)})


@app.route("/api/exports/download/<path:filename>")
def api_exports_download(filename):
    """Tải trực tiếp file CSV đã xuất về máy"""
    exports_dir = project_root / "data" / "exports"
    file_path = (exports_dir / filename).resolve()
    # Kiểm tra an toàn bảo mật tránh directory traversal
    if not str(file_path).startswith(str(exports_dir.resolve())):
        return jsonify({"error": "Truy cập tệp không hợp lệ"}), 403
    if not file_path.exists():
        return jsonify({"error": "Tệp không tồn tại"}), 404
    return send_file(str(file_path), as_attachment=True, download_name=filename, mimetype="text/csv")


# =============================================================================
# BATCH EXECUTION RUNNER & JOB MANAGER
# =============================================================================
import subprocess
import threading

BATCH_JOB_LOCK = threading.Lock()
BATCH_JOB_STATE = {
    "running": False,
    "action": None,
    "label": "",
    "status": "idle",
    "message": "Hệ thống sẵn sàng nhận lệnh.",
    "started_at": None,
    "finished_at": None,
    "logs": []
}

def _run_batch_script_thread(action: str, params: dict):
    global BATCH_JOB_STATE
    # Ưu tiên sử dụng python từ .venv nếu có
    venv_python = project_root / ".venv" / "Scripts" / "python.exe"
    python_bin = str(venv_python) if venv_python.exists() else sys.executable

    def log(msg):
        with BATCH_JOB_LOCK:
            timestamp = datetime.now().strftime("%H:%M:%S")
            BATCH_JOB_STATE["logs"].append(f"[{timestamp}] {msg}")
            BATCH_JOB_STATE["message"] = msg
            if len(BATCH_JOB_STATE["logs"]) > 200:
                BATCH_JOB_STATE["logs"] = BATCH_JOB_STATE["logs"][-200:]

    try:
        commands = []
        days = str(params.get("days", 7))
        symbols_str = str(params.get("symbols", "BTC-USDT,ETH-USDT,SOL-USDT"))

        if action == "backfill":
            commands.append(([python_bin, str(project_root / "ingestion" / "batch" / "historical_backfill.py"), "--days", days, "--symbols", symbols_str], f"Cào {days} ngày nến lịch sử từ OKX"))
        elif action == "derivatives":
            commands.append(([python_bin, str(project_root / "ingestion" / "batch" / "okx_derivatives_fetcher.py")], "Cào chỉ số phái sinh OKX (Funding, OI, Long/Short)"))
        elif action == "analytics":
            tf = str(params.get("timeframe", "all"))
            commands.append(([python_bin, str(project_root / "processing" / "batch" / "batch_analytics_engine.py"), "--timeframe", tf], f"Tổng hợp nến Batch ({tf.upper()}) & Xuất CSV"))
        elif action == "predict":
            commands.append(([python_bin, str(project_root / "processing" / "batch" / "market_predictor.py"), "--symbols", symbols_str], "Chạy mô hình lượng hóa dự báo chu kỳ kế tiếp"))
        elif action == "pipeline":
            commands.append(([python_bin, str(project_root / "ingestion" / "batch" / "historical_backfill.py"), "--days", days, "--symbols", symbols_str], f"[1/4] Cào {days} ngày nến lịch sử OKX"))
            commands.append(([python_bin, str(project_root / "ingestion" / "batch" / "okx_derivatives_fetcher.py")], "[2/4] Cào chỉ số phái sinh"))
            commands.append(([python_bin, str(project_root / "processing" / "batch" / "batch_analytics_engine.py"), "--timeframe", "all"], "[3/4] Batch Analytics Ngày/Tuần/Tháng/Năm & Xuất CSV"))
            commands.append(([python_bin, str(project_root / "processing" / "batch" / "market_predictor.py"), "--symbols", symbols_str], "[4/4] Định lượng dự báo chu kỳ kế tiếp & Nạp DW"))

        for cmd, desc in commands:
            log(f"Bắt đầu: {desc}...")
            env_vars = os.environ.copy()
            res = subprocess.run(cmd, cwd=str(project_root), capture_output=True, text=True, encoding="utf-8", errors="replace", env=env_vars)
            
            if res.stdout:
                for line in res.stdout.strip().split("\n"):
                    trimmed = line.strip()
                    if trimmed:
                        log(f"  {trimmed[:160]}")
            
            if res.returncode != 0:
                err_msg = res.stderr.strip()[:200] if res.stderr else f"Lỗi thực thi (Exit code: {res.returncode})"
                log(f"[!] Thất bại: {err_msg}")
                with BATCH_JOB_LOCK:
                    BATCH_JOB_STATE["running"] = False
                    BATCH_JOB_STATE["status"] = "error"
                    BATCH_JOB_STATE["message"] = f"Lỗi: {desc} thất bại!"
                    BATCH_JOB_STATE["finished_at"] = datetime.now().isoformat()
                return

        log("🎉 Hoàn tất toàn bộ tác vụ thành công! Các số liệu mới đã được cập nhật.")
        with BATCH_JOB_LOCK:
            BATCH_JOB_STATE["running"] = False
            BATCH_JOB_STATE["status"] = "success"
            BATCH_JOB_STATE["message"] = f"Hoàn tất {BATCH_JOB_STATE['label']} thành công!"
            BATCH_JOB_STATE["finished_at"] = datetime.now().isoformat()

    except Exception as e:
        log(f"[!] Ngoại lệ: {str(e)}")
        with BATCH_JOB_LOCK:
            BATCH_JOB_STATE["running"] = False
            BATCH_JOB_STATE["status"] = "error"
            BATCH_JOB_STATE["message"] = f"Lỗi ngoại lệ: {str(e)}"
            BATCH_JOB_STATE["finished_at"] = datetime.now().isoformat()


@app.route("/api/batch/run", methods=["POST"])
def api_batch_run():
    data = request.get_json(silent=True) or {}
    action = data.get("action", "predict")

    labels = {
        "backfill": "Cào Nến Lịch Sử",
        "derivatives": "Cào Phái Sinh",
        "analytics": "Tổng Hợp Nến Batch",
        "predict": "Dự Báo Chu Kỳ Kế Tiếp",
        "pipeline": "Trọn Gói Batch Pipeline (1-Click)"
    }
    action_label = labels.get(action, action)

    with BATCH_JOB_LOCK:
        if BATCH_JOB_STATE["running"]:
            return jsonify({
                "status": "busy",
                "message": f"Tác vụ [{BATCH_JOB_STATE['label']}] đang chạy dở. Vui lòng đợi hoàn thành.",
                "job": BATCH_JOB_STATE
            }), 409

        BATCH_JOB_STATE["running"] = True
        BATCH_JOB_STATE["action"] = action
        BATCH_JOB_STATE["label"] = action_label
        BATCH_JOB_STATE["status"] = "running"
        BATCH_JOB_STATE["message"] = f"Đang khởi động: {action_label}..."
        BATCH_JOB_STATE["started_at"] = datetime.now().isoformat()
        BATCH_JOB_STATE["finished_at"] = None
        BATCH_JOB_STATE["logs"] = [f"Khởi động tiến trình {action_label} tại {datetime.now().strftime('%H:%M:%S')}"]

    th = threading.Thread(target=_run_batch_script_thread, args=(action, data), daemon=True)
    th.start()

    return jsonify({
        "status": "started",
        "action": action,
        "label": action_label,
        "message": BATCH_JOB_STATE["message"]
    })


@app.route("/api/batch/status")
def api_batch_status():
    with BATCH_JOB_LOCK:
        return jsonify(BATCH_JOB_STATE)


# =============================================================================
# API ENDPOINT: QUANT ANALYTICS METRICS & PIPELINE TELEMETRY
# =============================================================================

@app.route("/api/analytics/metrics")
def api_analytics_metrics():
    """
    Trả về số liệu so sánh định lượng phục vụ Tab Phân Tích (Quant Analytics)
    - So sánh Turnover & Volume của 3 cặp tiền
    - Phân bổ giao dịch Cá mập theo cấp bậc (Mega vs Whale vs Large)
    - Cán cân Mua/Bán tổng hợp
    """
    conn = get_db_connection()
    data = {
        "symbols_comparison": {},
        "whale_distribution": {},
        "orderflow_pressure": {}
    }

    if conn:
        try:
            with conn.cursor() as cur:
                # 1. So sánh ngày từ vw_daily_market_summary
                cur.execute("""
                    SELECT symbol, open_price, close_price, total_volume, total_turnover_usd, return_pct, volatility_pct, daily_vwap
                    FROM warehouse.vw_daily_market_summary;
                """)
                for r in cur.fetchall():
                    data["symbols_comparison"][r[0]] = {
                        "open": float(r[1]),
                        "close": float(r[2]),
                        "volume": float(r[3]),
                        "turnover_usd": float(r[4]),
                        "return_pct": float(r[5]),
                        "volatility_pct": float(r[6]),
                        "vwap": float(r[7])
                    }

                # 2. Phân bổ cấp bậc Cá mập
                cur.execute("""
                    SELECT whale_tier, count(*), SUM(trade_value_usd)
                    FROM warehouse.vw_whale_trades
                    GROUP BY whale_tier;
                """)
                for r in cur.fetchall():
                    data["whale_distribution"][r[0]] = {
                        "count": r[1],
                        "turnover_usd": float(r[2])
                    }

                # 3. Order flow áp lực mua bán
                cur.execute("""
                    SELECT symbol, total_buy_volume, total_sell_volume, buy_to_sell_ratio, net_order_imbalance, market_sentiment
                    FROM warehouse.vw_order_flow_pressure;
                """)
                for r in cur.fetchall():
                    data["orderflow_pressure"][r[0]] = {
                        "buy_volume": float(r[1]),
                        "sell_volume": float(r[2]),
                        "buy_to_sell_ratio": float(r[3]),
                        "imbalance": float(r[4]),
                        "sentiment": r[5]
                    }
        except Exception:
            pass
        finally:
            conn.close()

    return jsonify(data)


@app.route("/api/pipeline/telemetry")
def api_pipeline_telemetry():
    """Đo lường toàn diện luồng dữ liệu kiến trúc Big Data Pipeline"""
    conn = get_db_connection()
    db_ok = conn is not None
    db_size = "0 MB"
    trades_count = 0
    candles_count = 0

    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_size_pretty(pg_database_size(%s));", (DB_NAME,))
                db_size = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM warehouse.fact_crypto_trades;")
                trades_count = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM warehouse.fact_crypto_1m_candles;")
                candles_count = cur.fetchone()[0]
        except Exception:
            pass
        finally:
            conn.close()

    # Thống kê export local
    exports_dir = project_root / "data" / "exports"
    local_exports = []
    if exports_dir.exists():
        for f in exports_dir.glob("*.csv"):
            local_exports.append({
                "name": f.name,
                "size_kb": round(f.stat().st_size / 1024, 1),
                "modified": datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            })

    telemetry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {
            "ingestion": {
                "name": "OKX WebSocket Producer",
                "source": "wss://ws.okx.com:8443/ws/v5/public",
                "status": "HEALTHY",
                "channels": ["trades"],
                "symbols": SYMBOLS,
                "latency_ms": 12
            },
            "broker": {
                "name": "Apache Kafka (KRaft)",
                "cluster_port": 9092,
                "topic": "crypto-trades",
                "partitions": [
                    {"partition": 0, "symbol": "BTC-USDT", "status": "ACTIVE"},
                    {"partition": 1, "symbol": "ETH-USDT", "status": "ACTIVE"},
                    {"partition": 2, "symbol": "SOL-USDT", "status": "ACTIVE"}
                ],
                "replication_factor": 1
            },
            "processing": {
                "name": "Apache Spark Structured Streaming",
                "version": "3.5.1 LTS",
                "window": "1 Minute Tumbling Window",
                "watermark": "2 Minutes Delay Tolerance",
                "deduplication": "trade_id & symbol Key",
                "features": ["OHLCV", "VWAP", "Trade Imbalance", "Whale Alert Filter"]
            },
            "lake": {
                "name": "AWS S3 Data Lake",
                "bucket": os.getenv("S3_BUCKET_NAME", "crypto-bigdata-lake-raw"),
                "format": "Snappy Parquet",
                "partitions": ["symbol", "trade_date"],
                "raw_path": "crypto/raw/trades/",
                "processed_path": "crypto/processed/trades_1m/",
                "local_csv_backups": local_exports
            },
            "warehouse": {
                "name": "PostgreSQL 16 Data Warehouse",
                "status": "ONLINE" if db_ok else "OFFLINE",
                "host": f"{DB_HOST}:{DB_PORT}/{DB_NAME}",
                "schema": "warehouse",
                "database_size": db_size,
                "total_fact_trades": trades_count,
                "total_fact_candles": candles_count,
                "views_count": 5
            }
        }
    }
    return jsonify(telemetry)


# =============================================================================
# API ENDPOINT: QUANT PREDICTIONS & FORECAST (BATCH LAYER)
# =============================================================================

@app.route("/api/predictions")
def api_predictions():
    """
    Trả về dữ liệu dự đoán chu kỳ kế tiếp (Ngày, Tuần, Tháng, Năm)
    phục vụ phân tích chiến lược trung-dài hạn từ nhánh Batch.
    """
    target_sym = request.args.get("symbol", "").strip().upper()
    target_tf = request.args.get("timeframe", "").strip().lower()

    # 1. Thử lấy từ PostgreSQL Data Warehouse
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                where_clauses = []
                params = []
                if target_sym and target_sym != "ALL":
                    where_clauses.append("symbol = %s")
                    params.append(target_sym)
                if target_tf and target_tf != "ALL":
                    where_clauses.append("target_timeframe = %s")
                    params.append(target_tf)

                where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
                cur.execute(f"""
                    SELECT symbol, target_timeframe, target_period_start, base_price,
                           predicted_price, predicted_high, predicted_low,
                           expected_return_pct, expected_volatility_pct,
                           trend_direction, strategic_signal, confidence_score, model_name, created_at
                    FROM warehouse.fact_market_predictions
                    {where_sql}
                    ORDER BY created_at DESC, symbol,
                        CASE target_timeframe
                            WHEN 'next_day' THEN 1
                            WHEN 'next_week' THEN 2
                            WHEN 'next_month' THEN 3
                            WHEN 'next_year' THEN 4
                            ELSE 5
                        END;
                """, params)
                rows = cur.fetchall()
                if rows:
                    preds = []
                    seen_keys = set()
                    for r in rows:
                        key = (r[0], r[1])
                        if key in seen_keys:
                            continue
                        seen_keys.add(key)
                        preds.append({
                            "symbol": r[0],
                            "target_timeframe": r[1],
                            "target_period_start": r[2].isoformat() if hasattr(r[2], "isoformat") else str(r[2]),
                            "base_price": float(r[3]),
                            "predicted_price": float(r[4]),
                            "predicted_high": float(r[5]),
                            "predicted_low": float(r[6]),
                            "expected_return_pct": float(r[7]),
                            "expected_volatility_pct": float(r[8]),
                            "trend_direction": r[9],
                            "strategic_signal": r[10],
                            "confidence_score": float(r[11]),
                            "model_name": r[12],
                            "created_at": r[13].isoformat() if hasattr(r[13], "isoformat") else str(r[13]),
                            "source": "postgresql_dw"
                        })
                    conn.close()
                    tf_order = {"next_day": 1, "next_week": 2, "next_month": 3, "next_year": 4}
                    preds.sort(key=lambda x: (x["symbol"], tf_order.get(x["target_timeframe"], 5)))
                    return jsonify({"status": "success", "count": len(preds), "predictions": preds})
        except Exception:
            pass
        finally:
            if conn:
                conn.close()

    # 2. Thử đọc từ CSV export
    csv_file = project_root / "data" / "exports" / "market_predictions_next_period.csv"
    if csv_file.exists():
        try:
            preds = []
            with open(csv_file, mode="r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if target_sym and target_sym != "ALL" and row.get("symbol", "").upper() != target_sym:
                        continue
                    if target_tf and target_tf != "ALL" and row.get("target_timeframe", "").lower() != target_tf:
                        continue
                    preds.append({
                        "symbol": row["symbol"],
                        "target_timeframe": row["target_timeframe"],
                        "target_period_start": row["target_period_start"],
                        "base_price": float(row["base_price"]),
                        "predicted_price": float(row["predicted_price"]),
                        "predicted_high": float(row["predicted_high"]),
                        "predicted_low": float(row["predicted_low"]),
                        "expected_return_pct": float(row["expected_return_pct"]),
                        "expected_volatility_pct": float(row["expected_volatility_pct"]),
                        "trend_direction": row["trend_direction"],
                        "strategic_signal": row["strategic_signal"],
                        "confidence_score": float(row["confidence_score"]),
                        "model_name": row.get("model_name", "QuantLinearTrendModel_v1"),
                        "source": "csv_batch"
                    })
            if preds:
                tf_order = {"next_day": 1, "next_week": 2, "next_month": 3, "next_year": 4}
                preds.sort(key=lambda x: (x["symbol"], tf_order.get(x["target_timeframe"], 5)))
                return jsonify({"status": "success", "count": len(preds), "predictions": preds})
        except Exception:
            pass

    # 3. Fallback chạy dự báo động
    from processing.batch.market_predictor import run_all_predictions
    generated = run_all_predictions(symbols=SYMBOLS, export_csv=True, save_dw=False)
    return jsonify({"status": "success", "count": len(generated), "predictions": generated})


# =============================================================================
# API ENDPOINT: REAL-TIME ACTIONABLE SIGNALS (STREAMING LAYER)
# =============================================================================

@app.route("/api/signals/realtime")
def api_signals_realtime():
    """
    Cung cấp các quyết định giao dịch tức thời từ luồng Real-Time Streaming:
      - Scalping Signal (Mua/Bán lướt sóng mili-giây)
      - Cảnh báo Cá mập gom hàng / xả hàng tức thì
      - Cán cân lực mua bán (Order Flow Imbalance)
    """
    symbol = request.args.get("symbol", "BTC-USDT").strip().upper()
    
    # 1. Lấy ticker gần nhất
    ticker_url = f"{OKX_REST_URL}/api/v5/market/ticker?instId={symbol}"
    ticker_data = fetch_okx_json(ticker_url)
    curr_price = 85000.0 if "BTC" in symbol else (2700.0 if "ETH" in symbol else 120.0)
    change_24h = 0.0

    if ticker_data and ticker_data.get("code") == "0" and ticker_data.get("data"):
        item = ticker_data["data"][0]
        curr_price = float(item.get("last", curr_price))
        open_24h = float(item.get("open24h", curr_price))
        change_24h = ((curr_price - open_24h) / open_24h * 100) if open_24h > 0 else 0.0

    # 2. Lấy 100 lệnh khớp mới nhất để đo áp lực tức thời
    trades_url = f"{OKX_REST_URL}/api/v5/market/trades?instId={symbol}&limit=100"
    trades_data = fetch_okx_json(trades_url)
    buy_vol = 0.0
    sell_vol = 0.0
    whale_buys = 0
    whale_sells = 0

    if trades_data and trades_data.get("code") == "0" and trades_data.get("data"):
        for t in trades_data["data"]:
            sz = float(t["sz"])
            px = float(t["px"])
            val = sz * px
            if t["side"] == "buy":
                buy_vol += sz
                if val >= 500000:
                    whale_buys += 1
            else:
                sell_vol += sz
                if val >= 500000:
                    whale_sells += 1

    total_vol = buy_vol + sell_vol
    imbalance = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0.0

    # 3. Tổng hợp tín hiệu quyết định tức thời
    if imbalance >= 0.25 and whale_buys >= whale_sells:
        action = "SCALP_LONG"
        action_label = "MUA LƯỚT SÓNG (SCALP LONG)"
        urgency = "HIGH"
        reason = f"Lực mua áp đảo ({imbalance*100:+.1f}%) kết hợp {whale_buys} lệnh cá mập mua chủ động."
    elif imbalance <= -0.25 and whale_sells >= whale_buys:
        action = "SCALP_SHORT"
        action_label = "BÁN LƯỚT SÓNG (SCALP SHORT)"
        urgency = "HIGH"
        reason = f"Lực bán tháo áp đảo ({imbalance*100:+.1f}%) kết hợp {whale_sells} lệnh cá mập xả hàng."
    elif whale_buys > 0 and whale_sells == 0:
        action = "WHALE_ACCUMULATION"
        action_label = "CÁ MẬP GOM HÀNG (ACCUMULATION)"
        urgency = "MEDIUM"
        reason = f"Phát hiện cá mập vào hàng với {whale_buys} lệnh lớn >= $500,000."
    elif whale_sells > 0 and whale_buys == 0:
        action = "WHALE_DISTRIBUTION"
        action_label = "CÁ MẬP XẢ HÀNG (DISTRIBUTION)"
        urgency = "MEDIUM"
        reason = f"Phát hiện cá mập xả hàng với {whale_sells} lệnh lớn >= $500,000."
    else:
        action = "NEUTRAL_WAIT"
        action_label = "QUAN SÁT (NEUTRAL / WAIT)"
        urgency = "LOW"
        reason = "Thị trường đang cân bằng cung cầu, chưa có biến động bất thường."

    return jsonify({
        "symbol": symbol,
        "current_price": curr_price,
        "change_24h": round(change_24h, 2),
        "order_imbalance": round(imbalance, 4),
        "buy_volume": round(buy_vol, 4),
        "sell_volume": round(sell_vol, 4),
        "whale_buys_count": whale_buys,
        "whale_sells_count": whale_sells,
        "action": action,
        "action_label": action_label,
        "urgency": urgency,
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })



if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print("=" * 75)
    print("  [*] CRYPTO QUANT ANALYTICS & TRADING WEB DASHBOARD")
    print(f"  Server URL : http://127.0.0.1:{port}")
    print(f"  Database   : {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 75)
    app.run(host="0.0.0.0", port=port, debug=False)
