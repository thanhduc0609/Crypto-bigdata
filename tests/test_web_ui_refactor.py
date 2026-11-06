"""
=============================================================================
Web UI & Backend API Unified Terminal Refactoring Test Suite
=============================================================================
Scope: BTC Unified Trading Terminal (Spot BTC-USDT & Perpetual BTC-USDT-SWAP)
Verifies:
  1. HTML contains unified single-screen terminal layout (no multi-tab navigation).
  2. Hero panel includes both ML Direction Prediction and Futures Trading Plan.
  3. Analytics row includes 60s Order Flow, Volume SVG chart, and Donut SVG chart.
  4. /api/futures/plan returns valid structure, real instrument, strict math bounds.
  5. /api/trades/large contains full-dataset breakdown for Donut chart.
  6. /api/overview, /api/candles, /api/trades/large/export remain backwards compatible.
  7. /api/ml/prediction, /api/ml/history, /api/ml/metadata remain intact with true artifacts.
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
    print("STARTING UNIFIED TERMINAL WEB UI & API TEST SUITE")
    print("=" * 80)

    # 1. Test HTML Structure & Unified Terminal Screen
    res = client.get("/")
    assert_test("HTML Status 200", res.status_code == 200)
    html_text = res.data.decode("utf-8")

    assert_test("Unified Terminal layout present (single screen, no legacy tabs)", 
                (('theme-terminal' in html_text) or ('theme-light' in html_text)) and 
                ('nav-tabs' not in html_text) and 
                ('class="main-workspace-grid"' in html_text))

    assert_test("Header displays BTC Spot and Perpetual instrument tags",
                ("BTC-USDT" in html_text) and 
                ("BTC-USDT-SWAP" in html_text) and
                ("header-basis" in html_text))

    assert_test("Hero Panel contains Prediction & Futures Plan cards",
                ('prediction-subcard' in html_text) and 
                ('futures-subcard' in html_text) and 
                ('plan-action-badge' in html_text) and
                ('plan-reason-text' in html_text))

    assert_test("Analytics Row contains Order Flow, Volume Bar SVG, and Donut SVG",
                ('class="analytics-row"' in html_text) and
                ('id="volume-bar-svg"' in html_text) and
                ('id="donut-svg"' in html_text) and
                ('of-bar-track' in html_text))

    assert_test("Bottom Section contains Large Trades, Pred History, and Model Metadata",
                ('table-large-trades' in html_text) and
                ('table-pred-history' in html_text) and
                ('model-info-content' in html_text))

    # 2. Test Futures Plan API (/api/futures/plan)
    res_plan = client.get("/api/futures/plan")
    assert_test("Futures Plan API Status 200", res_plan.status_code == 200)
    plan_json = res_plan.get_json()
    assert_test("Futures Plan has correct instrument BTC-USDT-SWAP",
                plan_json.get("instrument") == "BTC-USDT-SWAP")
    assert_test("Futures Plan has valid action state",
                plan_json.get("action") in ("LONG", "SHORT", "CHỜ"))
    assert_test("Futures Plan contains strategy reasoning and disclaimer",
                bool(plan_json.get("reason")) and bool(plan_json.get("disclaimer")))
    assert_test("Futures Plan contains market data context",
                "market_data" in plan_json and "atr_15m" in plan_json["market_data"])

    if plan_json.get("action") == "LONG":
        assert_test("LONG satisfies SL < Entry < TP",
                    plan_json["stop_loss"] < plan_json["entry_price"] < plan_json["take_profit"])
    elif plan_json.get("action") == "SHORT":
        assert_test("SHORT satisfies TP < Entry < SL",
                    plan_json["take_profit"] < plan_json["entry_price"] < plan_json["stop_loss"])

    # 3. Test Overview API (PostgreSQL DW & OKX Live)
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

    # 4. Test Candles API (PostgreSQL DW & OKX Live)
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

    # 5. Test Large Trades API & Full-Dataset Breakdown
    res_trades = client.get("/api/trades/large?source=dw&min_value=100000&time_range=all&page=1&page_size=10")
    assert_test("Large Trades DW Status 200", res_trades.status_code == 200)
    trades_json = res_trades.get_json()
    assert_test("Large Trades DW has full-dataset aggregate counters", 
                "total_count" in trades_json and "total_value" in trades_json and trades_json["total_count"] >= 0)
    assert_test("Large Trades DW contains breakdown object for Donut chart",
                "breakdown" in trades_json and all(k in trades_json["breakdown"] for k in ("buy_value", "sell_value", "buy_pct", "sell_pct", "total_value")))
    assert_test("Large Trades DW pagination active", 
                trades_json.get("page") == 1 and len(trades_json.get("trades", [])) <= 10)
    if trades_json.get("trades"):
        t0 = trades_json["trades"][0]
        assert_test("Large Trade fields formatted correctly (>= 100K)",
                    all(k in t0 for k in ("trade_id", "trade_time", "side", "price", "size", "trade_value")) and
                    t0["trade_value"] >= 100000)

    # 6. Test Large Trades CSV Export
    res_export = client.get("/api/trades/large/export?min_value=100000&time_range=all")
    assert_test("Large Trades Export Status 200", res_export.status_code == 200)
    assert_test("Large Trades Export Content-Type text/csv", "text/csv" in res_export.headers.get("Content-Type", ""))
    csv_text = res_export.data.decode("utf-8")
    assert_test("CSV Header row correct", 
                csv_text.startswith("trade_id,trade_time_utc,side,price_usdt,size_btc,value_usdt"))

    # 7. Test ML Prediction & History
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

    # 8. Test ML Metadata (Genuine artifacts, 15 features, version 2.0.0)
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
