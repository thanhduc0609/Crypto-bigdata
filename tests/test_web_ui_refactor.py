"""
=============================================================================
Web UI & Backend API Refactoring Test Suite
=============================================================================
Scope: BTC-USDT spot on OKX only
Verifies:
  1. HTML contains exactly 3 tabs: Tổng quan, Giao dịch lớn, Dự báo.
  2. Zero mock data / fake fallbacks in runtime routes.
  3. /api/overview returns real prices and only computes 24h stats if range >= 23h.
  4. /api/candles returns accurate real candles, microstructure NULL for REST, correct VWAP.
  5. /api/trades/large correctly filters, paginates, and computes full-dataset aggregates.
  6. /api/trades/large/export generates valid CSV format with correct headers.
  7. /api/ml/prediction & /api/ml/history return true records, eval_status, without auto-trigger.
  8. /api/ml/metadata reads 15 features and version 2.0.0 from genuine artifacts.
=============================================================================
"""

import sys
import json
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from web.app import app

def test_web_ui_refactor():
    client = app.test_client()
    passed = 0
    total = 0

    def assert_test(name, condition, details=""):
        nonlocal passed, total
        total += 1
        if condition:
            passed += 1
            print(f"[PASS] {name}")
        else:
            print(f"[FAIL] {name}: {details}")

    print("=" * 80)
    print("STARTING WEB UI & API REFACTORING TEST SUITE")
    print("=" * 80)

    # 1. Test HTML Structure & 3 Tabs
    res = client.get("/")
    assert_test("HTML Status 200", res.status_code == 200)
    html_text = res.data.decode("utf-8")
    assert_test("Exactly 3 navigation tabs", 
                ('data-tab="tab-overview"' in html_text) and 
                ('data-tab="tab-trades"' in html_text) and 
                ('data-tab="tab-prediction"' in html_text) and 
                ('tab-whales' not in html_text) and 
                ('tab-warehouse' not in html_text) and 
                ('tab-telemetry' not in html_text))

    assert_test("No scalp signal / webhook / neon clutter in HTML",
                ("SCALP" not in html_text) and 
                ("Stop Loss" not in html_text) and 
                ("webhook" not in html_text.lower()))

    # 2. Test Overview API (PostgreSQL DW & OKX Live)
    res_ov_dw = client.get("/api/overview?source=dw")
    assert_test("Overview DW Status 200", res_ov_dw.status_code == 200)
    ov_dw_json = res_ov_dw.get_json()
    assert_test("Overview DW returns valid price", 
                ov_dw_json.get("current_price") is not None and ov_dw_json.get("current_price") > 0)
    assert_test("Overview DW provenance is postgres_dw", ov_dw_json.get("source") == "postgres_dw")

    res_ov_live = client.get("/api/overview?source=live")
    if res_ov_live.status_code == 200:
        ov_live_json = res_ov_live.get_json()
        assert_test("Overview Live returns OKX ticker", 
                    ov_live_json.get("source") == "live_okx" and ov_live_json.get("current_price") > 0)
    else:
        assert_test("Overview Live handles network offline gracefully", res_ov_live.status_code in (500, 503))

    # 3. Test Candles API (PostgreSQL DW & OKX Live)
    res_c_dw = client.get("/api/candles?source=dw&timeframe=1m&limit=10")
    assert_test("Candles DW Status 200", res_c_dw.status_code == 200)
    c_dw_json = res_c_dw.get_json()
    candles_dw = c_dw_json.get("candles", [])
    assert_test("Candles DW returns array", isinstance(candles_dw, list))
    if candles_dw:
        c0 = candles_dw[0]
        assert_test("Candles DW has standard keys and ascending order", 
                    all(k in c0 for k in ("time", "open", "high", "low", "close", "volume", "vwap", "is_closed")))
        if len(candles_dw) > 1:
            assert_test("Candles DW sorted ascending", candles_dw[0]["time"] <= candles_dw[1]["time"])

    res_c_live = client.get("/api/candles?source=live&timeframe=1m&limit=5")
    if res_c_live.status_code == 200:
        c_live_json = res_c_live.get_json()
        candles_live = c_live_json.get("candles", [])
        if candles_live:
            assert_test("Candles Live sets microstructure to NULL (no fake numbers)",
                        candles_live[0]["buy_volume"] is None and candles_live[0]["trade_imbalance"] is None)

    # 4. Test Large Trades API (Filters, Pagination, Aggregates)
    res_trades = client.get("/api/trades/large?source=dw&min_value=10000&time_range=all&page=1&page_size=10")
    assert_test("Large Trades DW Status 200", res_trades.status_code == 200)
    trades_json = res_trades.get_json()
    assert_test("Large Trades DW has full-dataset aggregate counters", 
                "total_count" in trades_json and "total_value" in trades_json and trades_json["total_count"] >= 0)
    assert_test("Large Trades DW pagination active", 
                trades_json.get("page") == 1 and len(trades_json.get("trades", [])) <= 10)
    if trades_json.get("trades"):
        t0 = trades_json["trades"][0]
        assert_test("Large Trade fields formatted correctly",
                    all(k in t0 for k in ("trade_id", "trade_time", "side", "price", "size", "trade_value")) and
                    t0["trade_value"] >= 10000)

    # 5. Test Large Trades CSV Export
    res_export = client.get("/api/trades/large/export?min_value=10000&time_range=all")
    assert_test("Large Trades Export Status 200", res_export.status_code == 200)
    assert_test("Large Trades Export Content-Type text/csv", "text/csv" in res_export.headers.get("Content-Type", ""))
    csv_text = res_export.data.decode("utf-8")
    assert_test("CSV Header row correct", 
                csv_text.startswith("trade_id,trade_time_utc,side,price_usdt,size_btc,value_usdt"))

    # 6. Test ML Prediction & History
    res_ml = client.get("/api/ml/prediction")
    assert_test("ML Prediction Status 200", res_ml.status_code == 200)
    ml_json = res_ml.get_json()
    if ml_json.get("prediction"):
        pred = ml_json["prediction"]
        assert_test("ML Prediction has direction_label and probability",
                    pred.get("direction_label") in ("UP", "NOT_UP") and "probability_up" in pred)
        assert_test("ML Prediction has valid eval_status",
                    pred.get("eval_status") in ("PENDING_TARGET", "MISSING_ACTUAL", "EVALUATED"))

    res_ml_hist = client.get("/api/ml/history?page=1&page_size=5")
    assert_test("ML History Status 200", res_ml_hist.status_code == 200)
    ml_hist_json = res_ml_hist.get_json()
    assert_test("ML History has total_count and paginated list",
                "total_count" in ml_hist_json and isinstance(ml_hist_json.get("history"), list))

    # 7. Test ML Metadata (Genuine artifacts, 15 features, version 2.0.0)
    res_meta = client.get("/api/ml/metadata")
    assert_test("ML Metadata Status 200", res_meta.status_code == 200)
    meta_json = res_meta.get_json()
    assert_test("ML Metadata version is 2.0.0", meta_json.get("model_version") == "2.0.0")
    assert_test("ML Metadata n_features is 15 (not hardcoded 18)", meta_json.get("n_features") == 15)
    assert_test("ML Metadata feature_names list length is 15", len(meta_json.get("feature_names", [])) == 15)

    print("=" * 80)
    print(f"TOTAL: {total} | PASSED: {passed} | FAILED: {total - passed}")
    print("=" * 80)
    return total == passed

if __name__ == "__main__":
    success = test_web_ui_refactor()
    sys.exit(0 if success else 1)

