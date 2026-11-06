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
import urllib.request
from datetime import datetime, timezone
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

from processing.core.env import load_env_vars
from web.orderflow_realtime import get_orderflow_tracker
from ml.prediction_worker import get_prediction_worker
from web.services.overview_service import get_overview_data, get_candles_data
from web.services.trades_service import get_large_trades, generate_large_trades_csv
from web.services.prediction_service import (
    get_latest_prediction,
    get_prediction_history,
    get_model_metadata
)
from web.services.futures_plan_service import generate_futures_plan

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


def init_background_services():
    """
    Khởi động an toàn các dịch vụ nền khi tiến trình server web chính chạy.
    Tránh chạy 2 lần trong Flask reloader hoặc khi import module trong unit test.
    """
    # 1. Khởi động WebSocket orderflow tracker
    tracker = get_orderflow_tracker(symbol=TARGET_SYMBOL, window_seconds=60, autostart=False)
    tracker.start()

    # 2. Khởi động PredictionWorker theo cấu hình
    worker_mode = os.getenv("PREDICTION_WORKER_MODE", "embedded").lower()
    if worker_mode != "standalone":
        worker = get_prediction_worker(interval_seconds=15, autostart=False)
        started = worker.start()
        if not started:
            print("[*] Prediction worker mode: Running as external/standalone process or already running.")


# =============================================================================
# ROUTE: MAIN APPLICATION PAGE & HEALTH
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
        except Exception as e:
            print(f"[!] Warning checking table counts: {e}")
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
    data, code = get_overview_data(
        source_pref=source_pref,
        symbol=TARGET_SYMBOL,
        okx_rest_url=OKX_REST_URL,
        get_db_conn=get_db_connection,
        fetch_okx_json=fetch_okx_json
    )
    return jsonify(data), code


@app.route("/api/candles")
def api_candles():
    """
    Trả về danh sách nến OHLCV, VWAP và vi cấu trúc nến.
    Chỉ trả về các trường thật, không tự suy đoán hoặc chia đôi volume.
    Nguồn cố định: OKX REST (live). Hỗ trợ source=dw khi kiểm thử.
    """
    source_pref = request.args.get("source", "live").lower()
    timeframe = request.args.get("timeframe", "1m").lower()
    limit = min(int(request.args.get("limit", 150)), 300)

    data, code = get_candles_data(
        source_pref=source_pref,
        symbol=TARGET_SYMBOL,
        timeframe=timeframe,
        limit=limit,
        okx_rest_url=OKX_REST_URL,
        get_db_conn=get_db_connection,
        fetch_okx_json=fetch_okx_json
    )
    return jsonify(data), code


# =============================================================================
# REALTIME ORDER FLOW (60S SLIDING WINDOW)
# =============================================================================

@app.route("/api/orderflow/realtime")
def api_orderflow_realtime():
    """
    Trả về chỉ số cán cân mua/bán trong cửa sổ 60s thời gian thực (shared OKX WebSocket).
    """
    try:
        tracker = get_orderflow_tracker(symbol=TARGET_SYMBOL, window_seconds=60, autostart=False)
        metrics = tracker.get_orderflow_metrics()
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
        min_value = float(request.args.get("min_value", 100000.0))
    except ValueError:
        min_value = 100000.0

    page = max(1, int(request.args.get("page", 1)))
    page_size = min(max(10, int(request.args.get("page_size", 25))), 100)

    data, code = get_large_trades(
        symbol=TARGET_SYMBOL,
        time_range=time_range,
        min_value=min_value,
        page=page,
        page_size=page_size,
        get_db_conn=get_db_connection
    )
    return jsonify(data), code


@app.route("/api/trades/large/export")
def api_large_trades_export():
    """Xuất file CSV danh sách giao dịch lớn đúng theo bộ lọc hiện tại"""
    time_range = request.args.get("time_range", "all").lower()
    try:
        min_value = float(request.args.get("min_value", 100000.0))
    except ValueError:
        min_value = 100000.0

    csv_data, err = generate_large_trades_csv(
        symbol=TARGET_SYMBOL,
        time_range=time_range,
        min_value=min_value,
        get_db_conn=get_db_connection
    )
    if err:
        return Response(f"Lỗi xuất CSV: {err}", status=500, mimetype="text/plain")

    return Response(
        csv_data,
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f"attachment; filename=btc_large_trades_{time_range}_{int(min_value)}.csv"
        }
    )


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
    data, code = get_latest_prediction(
        symbol=TARGET_SYMBOL,
        get_db_conn=get_db_connection
    )
    return jsonify(data), code


@app.route("/api/futures/plan")
def api_futures_plan():
    """
    Tạo kế hoạch giao dịch phái sinh tham khảo (BTC-USDT-SWAP)
    kết hợp mô hình XGBoost Spot và dòng lệnh Order Flow 60s thời gian thực.
    """
    pred_res, _ = get_latest_prediction(symbol=TARGET_SYMBOL, get_db_conn=get_db_connection)
    tracker = get_orderflow_tracker(symbol=TARGET_SYMBOL, window_seconds=60, autostart=False)
    of_data = tracker.get_orderflow_metrics()

    plan = generate_futures_plan(
        prediction_data=pred_res,
        orderflow_data=of_data,
        okx_rest_url=OKX_REST_URL,
        fetch_okx_json=fetch_okx_json
    )
    return jsonify(plan), 200


@app.route("/api/ml/history")
def api_ml_history():
    """
    Trả về lịch sử dự báo dạng bảng có phân trang từ warehouse.fact_btc_predictions.
    """
    page = max(1, int(request.args.get("page", 1)))
    page_size = min(max(5, int(request.args.get("page_size", 10))), 50)

    data, code = get_prediction_history(
        symbol=TARGET_SYMBOL,
        page=page,
        page_size=page_size,
        get_db_conn=get_db_connection
    )
    return jsonify(data), code


@app.route("/api/ml/status")
def api_ml_status():
    """
    Trả về trạng thái hoạt động của worker tự động dự báo và đánh giá mục tiêu.
    """
    try:
        worker = get_prediction_worker(interval_seconds=15, autostart=False)
        return jsonify({
            "status": "success",
            "worker": worker.get_status()
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/ml/trigger_prediction", methods=["POST"])
def api_ml_trigger():
    """
    Kích hoạt chạy chu kỳ suy luận dự báo mới và đánh giá từ PredictionWorker.
    Trả về HTTP 429 nếu một chu kỳ khác đang chạy đồng thời (ALREADY_RUNNING).
    """
    try:
        worker = get_prediction_worker(interval_seconds=15, autostart=False)
        cycle_result = worker.perform_cycle(blocking=False)

        if cycle_result.get("status") == "busy":
            return jsonify({
                "status": "busy",
                "code": "ALREADY_RUNNING",
                "message": cycle_result.get("message", "Chu kỳ suy luận khác đang thực thi, vui lòng thử lại sau."),
                "cycle_result": cycle_result
            }), 429

        pred_res = cycle_result.get("prediction_result", {})
        if pred_res.get("success"):
            p = pred_res.get("prediction", {})
            return jsonify({
                "status": "success",
                "message": pred_res.get("message", "Dự báo thành công"),
                "prediction": {
                    "symbol": p.get("symbol"),
                    "current_price": p.get("current_price"),
                    "predicted_direction": p.get("predicted_direction"),
                    "direction_label": p.get("direction_label"),
                    "probability_up": p.get("probability_up"),
                    "probability_not_up": p.get("probability_not_up"),
                    "feature_timestamp": p.get("feature_timestamp"),
                    "target_timestamp": p.get("target_timestamp"),
                    "model_version": p.get("model_version")
                },
                "cycle_result": cycle_result
            })
        else:
            return jsonify({
                "status": "rejected",
                "rejection_code": pred_res.get("code"),
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
    data, code = get_model_metadata(project_root)
    return jsonify(data), code


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print("=" * 75)
    print("  [*] CRYPTO BIG DATA - MINIMAL CLEAN DASHBOARD")
    print(f"  URL       : http://127.0.0.1:{port}")
    print(f"  Database  : {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 75)
    init_background_services()
    app.run(host="0.0.0.0", port=port, debug=False)
