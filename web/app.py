"""
=============================================================================
Crypto Big Data - Web Analytics & ML Prediction Dashboard (Flask Backend)
=============================================================================
Cung cấp API phục vụ Web Dashboard theo kiến trúc tối giản, rõ ràng và dữ liệu thật:
  - Tab 1: Tổng quan (Overview) - Nến 1m OHLCV, VWAP, Vi cấu trúc Mua/Bán (BTC-USDT)
  - Tab 2: Giao dịch lớn (Large Trades) - Lọc theo ngưỡng và khoảng thời gian thực
  - Tab 3: Dự báo (Prediction) - Kết quả dự báo hướng giá +5m (XGBoost) và lịch sử đối chiếu
=============================================================================
"""

import os
import sys
import json
import math
import io
import csv
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from flask import Flask, render_template, jsonify, request, Response

# Setup stdout utf-8 encoding on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
        os.system("")
    except Exception:
        pass

# Setup project root
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from web.orderflow_realtime import get_orderflow_tracker
from ml.prediction_worker import get_prediction_worker

def load_env_vars():
    """Đọc file .env nếu có"""
    env_path = project_root / ".env"
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip("'\"")
            if k not in os.environ or os.environ[k].startswith("${"):
                os.environ[k] = v

load_env_vars()

app = Flask(
    __name__,
    template_folder=str(project_root / "web" / "templates"),
    static_folder=str(project_root / "web" / "static")
)

OKX_REST_URL = os.getenv("OKX_REST_URL", "https://www.okx.com")
TARGET_SYMBOL = "BTC-USDT"

# Database config
DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
DB_NAME = os.getenv("POSTGRES_DB", "crypto_dw")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# Khởi động dịch vụ nền: Realtime Orderflow Tracker (60s sliding window) & Prediction Worker
orderflow_tracker = get_orderflow_tracker(symbol=TARGET_SYMBOL, window_seconds=60)
orderflow_tracker.start()

prediction_worker = get_prediction_worker(interval_seconds=15)
prediction_worker.start()


def get_db_connection():
    """Tạo kết nối tới PostgreSQL Data Warehouse"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=3
        )
        return conn
    except Exception as e:
        print(f"[!] Lỗi kết nối PostgreSQL ({DB_HOST}:{DB_PORT}/{DB_NAME}): {e}")
        return None


def fetch_okx_json(url: str):
    """Gửi HTTP request đến OKX REST API với timeout và User-Agent thật"""
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json"
            }
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                return json.loads(response.read().decode("utf-8"))
    except Exception as e:
        print(f"[!] Lỗi gọi OKX REST ({url}): {e}")
        return None
    return None


def format_iso_utc(dt):
    """Chuyển đổi datetime sang chuỗi ISO UTC chuẩn"""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(dt)


# =============================================================================
# ROUTE: MAIN APPLICATION PAGE
# =============================================================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/health")
def api_health():
    """Kiểm tra tình trạng kết nối DB và thời gian máy chủ"""
    conn = get_db_connection()
    db_ok = conn is not None
    table_counts = {}
    if conn:
        try:
            with conn.cursor() as cur:
                for tbl in ["fact_crypto_trades", "fact_crypto_1m_candles", "fact_btc_predictions"]:
                    cur.execute(f"SELECT count(*) FROM warehouse.{tbl};")
                    table_counts[tbl] = cur.fetchone()[0]
        except Exception:
            pass
        finally:
            conn.close()

    return jsonify({
        "status": "online",
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "database_connected": db_ok,
        "database_target": f"{DB_HOST}:{DB_PORT}/{DB_NAME}",
        "table_counts": table_counts,
        "symbol": TARGET_SYMBOL
    })


# =============================================================================
# TAB 1: TỔNG QUAN (OVERVIEW & CANDLES)
# =============================================================================

@app.route("/api/overview")
def api_overview():
    """
    Cung cấp thông tin giá gần nhất và thống kê 24h thực tế.
    Biến động 24h chỉ trả về khi có đủ dữ liệu khoảng 24h.
    Nguồn cố định: OKX REST (live). Hỗ trợ source=dw khi kiểm thử.
    """
    source_pref = request.args.get("source", "live").lower()

    if source_pref != "dw":
        ticker_url = f"{OKX_REST_URL}/api/v5/market/ticker?instId={TARGET_SYMBOL}"
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

            return jsonify({
                "status": "success",
                "symbol": TARGET_SYMBOL,
                "current_price": last_price,
                "change_24h": change_24h,
                "high_24h": high_24h,
                "low_24h": low_24h,
                "volume_24h": vol_24h,
                "turnover_24h": turnover_24h,
                "data_timestamp": data_ts,
                "source": "live_okx"
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Không thể kết nối đến OKX REST API để lấy dữ liệu Ticker",
                "source": "live_okx"
            }), 503

    # Nguồn phụ phục vụ kiểm thử: PostgreSQL Data Warehouse
    conn = get_db_connection()
    if not conn:
        return jsonify({
            "status": "error",
            "message": "Không thể kết nối PostgreSQL Data Warehouse",
            "source": "postgres_dw"
        }), 500

    try:
        with conn.cursor() as cur:
            # Lấy nến 1m gần nhất
            cur.execute("""
                SELECT close, window_start
                FROM warehouse.fact_crypto_1m_candles
                WHERE symbol = %s
                ORDER BY window_start DESC
                LIMIT 1;
            """, (TARGET_SYMBOL,))
            latest_row = cur.fetchone()
            if not latest_row:
                return jsonify({
                    "status": "empty",
                    "symbol": TARGET_SYMBOL,
                    "current_price": None,
                    "change_24h": None,
                    "message": "Chưa có dữ liệu nến trong Data Warehouse",
                    "source": "postgres_dw"
                })

            current_price = float(latest_row[0])
            data_ts = format_iso_utc(latest_row[1])

            # Kiểm tra khoảng thời gian thực tế trong 24h qua
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
            """, (TARGET_SYMBOL,))
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

                # Chỉ tính biến động 24h nếu dữ liệu bao phủ ít nhất 23 giờ
                if timespan_seconds >= 23 * 3600:
                    high_24h = float(agg_row[2]) if agg_row[2] is not None else None
                    low_24h = float(agg_row[3]) if agg_row[3] is not None else None
                    vol_24h = float(agg_row[4]) if agg_row[4] is not None else None
                    turnover_24h = float(agg_row[5]) if agg_row[5] is not None else None

                    # Tìm giá mở cửa nến 24h trước
                    cur.execute("""
                        SELECT open
                        FROM warehouse.fact_crypto_1m_candles
                        WHERE symbol = %s AND window_start >= NOW() - INTERVAL '24 hours'
                        ORDER BY window_start ASC
                        LIMIT 1;
                    """, (TARGET_SYMBOL,))
                    open_row = cur.fetchone()
                    if open_row and open_row[0] > 0:
                        open_val = float(open_row[0])
                        change_24h = round(((current_price - open_val) / open_val * 100), 2)

            return jsonify({
                "status": "success",
                "symbol": TARGET_SYMBOL,
                "current_price": current_price,
                "change_24h": change_24h,
                "high_24h": high_24h,
                "low_24h": low_24h,
                "volume_24h": vol_24h,
                "turnover_24h": turnover_24h,
                "data_timestamp": data_ts,
                "source": "postgres_dw"
            })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi truy vấn tổng quan từ Data Warehouse: {e}",
            "source": "postgres_dw"
        }), 500
    finally:
        conn.close()


@app.route("/api/candles")
def api_candles():
    """
    Trả về danh sách nến OHLCV, VWAP và vi cấu trúc nến.
    Chỉ trả về các trường thật, không tự suy đoán hoặc chia đôi volume.
    Nguồn cố định: OKX REST (live). Hỗ trợ source=dw khi kiểm thử.
    Query params:
      - source: live (mặc định) | dw (PostgreSQL Data Warehouse)
      - timeframe: 1m (mặc định) | 1h | 1d
      - limit: số nến (mặc định: 150)
    """
    source_pref = request.args.get("source", "live").lower()
    timeframe = request.args.get("timeframe", "1m").lower()
    limit = min(int(request.args.get("limit", 150)), 300)

    if timeframe not in ("1m", "1h", "1d"):
        timeframe = "1m"

    if source_pref != "dw":
        bar_map = {"1m": "1m", "1h": "1H", "1d": "1D"}
        bar = bar_map.get(timeframe, "1m")
        url = f"{OKX_REST_URL}/api/v5/market/candles?instId={TARGET_SYMBOL}&bar={bar}&limit={limit}"
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
            return jsonify({
                "status": "success",
                "symbol": TARGET_SYMBOL,
                "timeframe": timeframe,
                "source": "live_okx",
                "candles": candles,
                "data_timestamp": data_ts
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Không thể tải nến từ OKX REST API",
                "source": "live_okx"
            }), 503

    # Nguồn phụ phục vụ kiểm thử: PostgreSQL Data Warehouse
    conn = get_db_connection()
    if not conn:
        return jsonify({
            "status": "error",
            "message": "Không thể kết nối PostgreSQL Data Warehouse",
            "source": "postgres_dw"
        }), 500

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

            cur.execute(sql, (TARGET_SYMBOL, limit))
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
            return jsonify({
                "status": "success",
                "symbol": TARGET_SYMBOL,
                "timeframe": timeframe,
                "source": "postgres_dw",
                "candles": candles,
                "data_timestamp": data_ts
            })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi truy vấn nến từ Data Warehouse: {e}",
            "source": "postgres_dw"
        }), 500
    finally:
        conn.close()


# =============================================================================
# REALTIME ORDER FLOW (60S SLIDING WINDOW)
# =============================================================================

@app.route("/api/orderflow/realtime")
def api_orderflow_realtime():
    """
    Trả về chỉ số cán cân mua/bán trong cửa sổ 60s thời gian thực (shared OKX WebSocket).
    """
    try:
        metrics = orderflow_tracker.get_orderflow_metrics()
        return jsonify(metrics)
    except Exception as e:
        return jsonify({
            "status": "error",
            "state": "error",
            "message": f"Lỗi đọc chỉ số cán cân mua/bán: {e}"
        }), 500


# =============================================================================
# TAB 2: GIAO DỊCH LỚN (LARGE TRADES & ARCHIVE)
# =============================================================================

@app.route("/api/trades/large")
def api_large_trades():
    """
    Trả về danh sách các giao dịch lớn từ PostgreSQL Data Warehouse thỏa điều kiện lọc.
    Thống kê tổng số và tổng giá trị được tính toán trên toàn bộ tập kết quả.
    """
    time_range = request.args.get("time_range", "all").lower()
    try:
        min_value = float(request.args.get("min_value", 10000.0))
    except ValueError:
        min_value = 10000.0

    page = max(1, int(request.args.get("page", 1)))
    page_size = min(max(10, int(request.args.get("page_size", 25))), 100)
    offset = (page - 1) * page_size

    # Nguồn dữ liệu cố định cho Giao dịch lớn: PostgreSQL Data Warehouse
    conn = get_db_connection()
    if not conn:
        return jsonify({
            "status": "error",
            "message": "Không thể kết nối PostgreSQL Data Warehouse",
            "source": "postgres_dw"
        }), 500

    try:
        with conn.cursor() as cur:
            where_conditions = ["symbol = %s", "trade_value >= %s"]
            params = [TARGET_SYMBOL, min_value]

            if time_range == "1h":
                where_conditions.append("trade_time >= NOW() - INTERVAL '1 hour'")
            elif time_range == "24h":
                where_conditions.append("trade_time >= NOW() - INTERVAL '24 hours'")
            elif time_range == "7d":
                where_conditions.append("trade_time >= NOW() - INTERVAL '7 days'")

            where_sql = "WHERE " + " AND ".join(where_conditions)

            # 1. Thống kê toàn bộ tập kết quả thỏa điều kiện
            count_sql = f"""
                SELECT COUNT(*), COALESCE(SUM(trade_value), 0.0), MAX(trade_time)
                FROM warehouse.fact_crypto_trades
                {where_sql};
            """
            cur.execute(count_sql, params)
            agg = cur.fetchone()
            total_count = agg[0]
            total_value = float(agg[1])
            data_ts = format_iso_utc(agg[2])

            # 2. Truy vấn dữ liệu có phân trang
            query_sql = f"""
                SELECT trade_id, trade_time, side, price, size, trade_value
                FROM warehouse.fact_crypto_trades
                {where_sql}
                ORDER BY trade_time DESC, trade_id DESC
                LIMIT %s OFFSET %s;
            """
            cur.execute(query_sql, params + [page_size, offset])
            rows = cur.fetchall()

            trades = []
            for r in rows:
                trades.append({
                    "trade_id": str(r[0]),
                    "trade_time": format_iso_utc(r[1]),
                    "side": r[2].upper(),
                    "price": float(r[3]),
                    "size": float(r[4]),
                    "trade_value": float(r[5])
                })

            total_pages = math.ceil(total_count / page_size) if total_count > 0 else 1

            return jsonify({
                "status": "success",
                "source": "postgres_dw",
                "total_count": total_count,
                "total_value": round(total_value, 2),
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "trades": trades,
                "data_timestamp": data_ts
            })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi truy vấn giao dịch lớn từ Data Warehouse: {e}",
            "source": "postgres_dw"
        }), 500
    finally:
        conn.close()


@app.route("/api/trades/large/export")
def api_large_trades_export():
    """Xuất file CSV danh sách giao dịch lớn đúng theo bộ lọc hiện tại"""
    time_range = request.args.get("time_range", "all").lower()
    try:
        min_value = float(request.args.get("min_value", 10000.0))
    except ValueError:
        min_value = 10000.0

    conn = get_db_connection()
    if not conn:
        return Response("Không thể kết nối PostgreSQL Data Warehouse", status=500, mimetype="text/plain")

    try:
        with conn.cursor() as cur:
            where_conditions = ["symbol = %s", "trade_value >= %s"]
            params = [TARGET_SYMBOL, min_value]

            if time_range == "1h":
                where_conditions.append("trade_time >= NOW() - INTERVAL '1 hour'")
            elif time_range == "24h":
                where_conditions.append("trade_time >= NOW() - INTERVAL '24 hours'")
            elif time_range == "7d":
                where_conditions.append("trade_time >= NOW() - INTERVAL '7 days'")

            where_sql = "WHERE " + " AND ".join(where_conditions)
            cur.execute(f"""
                SELECT trade_id, trade_time, side, price, size, trade_value
                FROM warehouse.fact_crypto_trades
                {where_sql}
                ORDER BY trade_time DESC, trade_id DESC
                LIMIT 5000;
            """, params)
            rows = cur.fetchall()

            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["trade_id", "trade_time_utc", "side", "price_usdt", "size_btc", "value_usdt"])
            for r in rows:
                writer.writerow([
                    str(r[0]),
                    format_iso_utc(r[1]),
                    r[2].upper(),
                    f"{float(r[3]):.2f}",
                    f"{float(r[4]):.6f}",
                    f"{float(r[5]):.2f}"
                ])

            csv_data = output.getvalue()
            return Response(
                csv_data,
                mimetype="text/csv; charset=utf-8",
                headers={
                    "Content-Disposition": f"attachment; filename=btc_large_trades_{time_range}_{int(min_value)}.csv"
                }
            )
    except Exception as e:
        return Response(f"Lỗi xuất CSV: {e}", status=500, mimetype="text/plain")
    finally:
        conn.close()


# Alias for backward compatibility
@app.route("/api/whales")
def api_whales_alias():
    return api_large_trades()


# =============================================================================
# TAB 3: DỰ BÁO (MACHINE LEARNING PREDICTION & EVALUATION)
# =============================================================================

@app.route("/api/ml/prediction")
def api_ml_prediction():
    """
    Trả về kết quả dự báo gần nhất từ warehouse.fact_btc_predictions.
    Chỉ đọc dữ liệu thật, không tự ý chạy suy luận khi chỉ xem trang.
    """
    conn = get_db_connection()
    if not conn:
        return jsonify({
            "status": "error",
            "message": "Không thể kết nối PostgreSQL Data Warehouse",
            "source": "postgres_dw"
        }), 500

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    prediction_id, symbol, feature_timestamp, prediction_timestamp, target_timestamp,
                    predicted_direction, probability_up, probability_not_up,
                    model_version, model_name, current_price, horizon_minutes,
                    actual_direction, actual_future_price, is_correct
                FROM warehouse.fact_btc_predictions
                WHERE symbol = %s
                ORDER BY feature_timestamp DESC
                LIMIT 1;
            """, (TARGET_SYMBOL,))
            row = cur.fetchone()

            if not row:
                return jsonify({
                    "status": "empty",
                    "prediction": None,
                    "message": "Chưa có dự báo nào trong cơ sở dữ liệu. Nhấn 'Chạy dự báo mới' để thực hiện suy luận.",
                    "source": "postgres_dw"
                })

            now_utc = datetime.now(timezone.utc)
            target_ts = row[4].astimezone(timezone.utc) if row[4] else None

            # Xác định trạng thái đối chiếu
            if target_ts and target_ts > now_utc:
                eval_status = "PENDING_TARGET"     # Chưa đến thời điểm đối chiếu
            elif row[12] is None:
                eval_status = "MISSING_ACTUAL"     # Đã qua thời điểm nhưng chưa có nến đối chiếu
            else:
                eval_status = "EVALUATED"

            pred = {
                "prediction_id": row[0],
                "symbol": row[1],
                "feature_timestamp": format_iso_utc(row[2]),
                "prediction_timestamp": format_iso_utc(row[3]),
                "target_timestamp": format_iso_utc(row[4]),
                "predicted_direction": row[5],
                "direction_label": "UP" if row[5] == 1 else "NOT_UP",
                "probability_up": round(float(row[6]), 4) if row[6] is not None else None,
                "probability_not_up": round(float(row[7]), 4) if row[7] is not None else None,
                "model_version": row[8],
                "model_name": row[9],
                "current_price": float(row[10]) if row[10] is not None else None,
                "horizon_minutes": row[11],
                "actual_direction": row[12],
                "actual_direction_label": "UP" if row[12] == 1 else ("NOT_UP" if row[12] == 0 else None),
                "actual_future_price": float(row[13]) if row[13] is not None else None,
                "is_correct": row[14],
                "eval_status": eval_status,
                "source": "postgres_dw"
            }

            return jsonify({
                "status": "success",
                "prediction": pred,
                "data_timestamp": pred["feature_timestamp"]
            })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi truy vấn dự báo từ Data Warehouse: {e}",
            "source": "postgres_dw"
        }), 500
    finally:
        conn.close()


@app.route("/api/ml/history")
def api_ml_history():
    """
    Trả về lịch sử dự báo dạng bảng có phân trang từ warehouse.fact_btc_predictions.
    Query params:
      - page: trang (mặc định: 1)
      - page_size: số bản ghi mỗi trang (mặc định: 10)
    """
    page = max(1, int(request.args.get("page", 1)))
    page_size = min(max(5, int(request.args.get("page_size", 10))), 50)
    offset = (page - 1) * page_size

    conn = get_db_connection()
    if not conn:
        return jsonify({
            "status": "error",
            "message": "Không thể kết nối PostgreSQL Data Warehouse",
            "source": "postgres_dw"
        }), 500

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM warehouse.fact_btc_predictions WHERE symbol = %s;", (TARGET_SYMBOL,))
            total_count = cur.fetchone()[0]

            cur.execute("""
                SELECT 
                    prediction_id, feature_timestamp, current_price,
                    predicted_direction, probability_up, probability_not_up,
                    target_timestamp, actual_future_price, actual_direction, is_correct
                FROM warehouse.fact_btc_predictions
                WHERE symbol = %s
                ORDER BY feature_timestamp DESC
                LIMIT %s OFFSET %s;
            """, (TARGET_SYMBOL, page_size, offset))
            rows = cur.fetchall()

            now_utc = datetime.now(timezone.utc)
            history = []
            for r in rows:
                target_ts = r[6].astimezone(timezone.utc) if r[6] else None
                if target_ts and target_ts > now_utc:
                    eval_status = "PENDING_TARGET"
                elif r[8] is None:
                    eval_status = "MISSING_ACTUAL"
                else:
                    eval_status = "EVALUATED"

                history.append({
                    "prediction_id": r[0],
                    "feature_timestamp": format_iso_utc(r[1]),
                    "current_price": float(r[2]) if r[2] is not None else None,
                    "predicted_direction": r[3],
                    "direction_label": "UP" if r[3] == 1 else "NOT_UP",
                    "probability_up": round(float(r[4]), 4) if r[4] is not None else None,
                    "probability_not_up": round(float(r[5]), 4) if r[5] is not None else None,
                    "target_timestamp": format_iso_utc(r[6]),
                    "actual_future_price": float(r[7]) if r[7] is not None else None,
                    "actual_direction": r[8],
                    "actual_direction_label": "UP" if r[8] == 1 else ("NOT_UP" if r[8] == 0 else None),
                    "is_correct": r[9],
                    "eval_status": eval_status
                })

            total_pages = math.ceil(total_count / page_size) if total_count > 0 else 1

            return jsonify({
                "status": "success",
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "history": history
            })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi truy vấn lịch sử dự báo: {e}",
            "source": "postgres_dw"
        }), 500
    finally:
        conn.close()


@app.route("/api/ml/status")
def api_ml_status():
    """
    Trả về trạng thái hoạt động của worker tự động dự báo và đánh giá mục tiêu.
    """
    try:
        return jsonify({
            "status": "success",
            "worker": prediction_worker.get_status()
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/ml/trigger_prediction", methods=["POST"])
def api_ml_trigger():
    """
    Kích hoạt chạy chu kỳ suy luận dự báo mới và đánh giá từ PredictionWorker.
    """
    try:
        cycle_result = prediction_worker.perform_cycle()
        pred_res = cycle_result.get("prediction_result", {})
        if pred_res.get("status") == "success":
            p = pred_res.get("prediction", {})
            return jsonify({
                "status": "success",
                "message": "Dự báo thành công",
                "prediction": {
                    "symbol": p.get("symbol"),
                    "current_price": p.get("current_price"),
                    "predicted_direction": p.get("predicted_direction"),
                    "direction_label": p.get("direction_label"),
                    "probability_up": p.get("probability_up"),
                    "probability_not_up": p.get("probability_not_up"),
                    "feature_timestamp": format_iso_utc(p.get("feature_timestamp")),
                    "target_timestamp": format_iso_utc(p.get("target_timestamp")),
                    "model_version": p.get("model_version")
                },
                "cycle_result": cycle_result
            })
        else:
            return jsonify({
                "status": "rejected",
                "rejection_code": pred_res.get("rejection_code"),
                "message": pred_res.get("message", "Mô hình từ chối suy luận: Dữ liệu nến chưa đủ điều kiện."),
                "cycle_result": cycle_result
            }), 400
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi thực thi suy luận ML: {e}"
        }), 500


@app.route("/api/ml/metadata")
def api_ml_metadata():
    """
    Trả về thông tin metadata và kết quả đánh giá mô hình đọc trực tiếp từ artifact thật.
    Không hardcode 18 đặc trưng hay phiên bản.
    """
    meta_path = project_root / "ml" / "artifacts" / "model_metadata.json"
    eval_path = project_root / "ml" / "artifacts" / "evaluation_results.json"

    meta_data = {}
    if meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta_data = json.load(f)
        except Exception:
            pass

    eval_data = {}
    if eval_path.exists():
        try:
            with open(eval_path, "r", encoding="utf-8") as f:
                eval_data = json.load(f)
        except Exception:
            pass

    ds_meta = meta_data.get("dataset_metadata", {})
    return jsonify({
        "status": "success",
        "model_name": meta_data.get("model_name", "btc_xgboost_direction"),
        "model_version": meta_data.get("model_version", "2.0.0"),
        "model_type": meta_data.get("model_type", "XGBClassifier"),
        "prediction_horizon_minutes": meta_data.get("prediction_horizon_minutes", 5),
        "created_at": meta_data.get("created_at"),
        "n_features": ds_meta.get("n_features", len(ds_meta.get("feature_names", []))),
        "feature_names": ds_meta.get("feature_names", []),
        "metrics": eval_data.get("metrics", {}),
        "feature_importance": eval_data.get("feature_importance", [])
    })


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print("=" * 75)
    print("  [*] CRYPTO BIG DATA - MINIMAL CLEAN DASHBOARD")
    print(f"  URL       : http://127.0.0.1:{port}")
    print(f"  Database  : {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 75)
    app.run(host="0.0.0.0", port=port, debug=False)
