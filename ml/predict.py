"""
=============================================================================
Crypto Big Data - Machine Learning: Real-Time BTC Direction Inference
=============================================================================
Executes near-real-time inference for BTC-USDT price direction (next 5m):
  1. Loads trained XGBoost model and verifies feature schema
  2. Fetches latest finalized 1m BTC candles (strictly confirmed/closed)
  3. Calculates rolling features without lookahead (15 common features)
  4. Predicts binary direction (UP / NOT_UP) and UP probability
  5. Stores prediction into PostgreSQL warehouse.fact_btc_predictions
  6. Evaluates actual outcomes once the target candle has closed (target + 60s)
  7. Provides structured rejection reasons: NO_DATA, INSUFFICIENT_HISTORY,
     STALE_DATA, MISSING_CANDLES, UNCLOSED_CANDLE, INVALID_OHLCV,
     FEATURE_SCHEMA_MISMATCH, MODEL_UNAVAILABLE, TARGET_EXPIRED, DATABASE_ERROR
=============================================================================
"""

import os
import sys
import json
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from xgboost import XGBClassifier
from processing.core.config import get_ml_config
from processing.features.feature_engineering import compute_features, FEATURE_COLUMNS
from warehouse.etl.db_config import get_db_connection
from warehouse.etl.streaming_sink import write_historical_candles_batch
from ingestion.historical.candle_parser import parse_okx_rest_candle

ml_cfg = get_ml_config()
artifacts_cfg = ml_cfg.get("artifacts", {})
ARTIFACTS_DIR = project_root / artifacts_cfg.get("directory", "ml/artifacts")
MODEL_FILE = ARTIFACTS_DIR / artifacts_cfg.get("model_filename", "btc_xgboost_model.json")
SCHEMA_FILE = ARTIFACTS_DIR / artifacts_cfg.get("feature_schema_filename", "feature_schema.json")
METADATA_FILE = ARTIFACTS_DIR / artifacts_cfg.get("model_metadata_filename", "model_metadata.json")

HORIZON_MINUTES = int(ml_cfg.get("prediction", {}).get("horizon_minutes", 5))
MODEL_NAME = str(ml_cfg.get("ml", {}).get("model_name", "btc_xgboost_direction"))
MAX_STALENESS_MINUTES = int(ml_cfg.get("prediction", {}).get("max_staleness_minutes", 10))

_cached_model = None
_cached_features = None
_last_rejection = {"code": None, "message": None}


class RejectionCode:
    NO_DATA = "NO_DATA"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    STALE_DATA = "STALE_DATA"
    MISSING_CANDLES = "MISSING_CANDLES"
    UNCLOSED_CANDLE = "UNCLOSED_CANDLE"
    INVALID_OHLCV = "INVALID_OHLCV"
    FEATURE_SCHEMA_MISMATCH = "FEATURE_SCHEMA_MISMATCH"
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"
    TARGET_EXPIRED = "TARGET_EXPIRED"
    DATABASE_ERROR = "DATABASE_ERROR"


def get_last_prediction_rejection() -> Dict[str, Any]:
    """Returns the most recent prediction rejection code and message."""
    return dict(_last_rejection)


def _set_rejection(code: str, message: str):
    global _last_rejection
    _last_rejection = {"code": code, "message": message}
    print(f"[PREDICT REJECTED: {code}] {message}")


def get_current_model_version() -> str:
    """Reads model version dynamically from metadata if available."""
    if METADATA_FILE.exists():
        try:
            with open(METADATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return str(data.get("model_version", "2.0.0"))
        except Exception:
            pass
    return str(ml_cfg.get("ml", {}).get("model_version", "2.0.0"))


def load_prediction_model():
    """Loads and validates the trained XGBoost model from artifacts."""
    global _cached_model, _cached_features
    if _cached_model is not None:
        return _cached_model, _cached_features

    if not MODEL_FILE.exists():
        raise FileNotFoundError(
            f"Trained model not found at {MODEL_FILE}. Please run ml/train.py first."
        )

    model = XGBClassifier()
    model.load_model(str(MODEL_FILE))

    if SCHEMA_FILE.exists():
        with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
            schema = json.load(f)
            features = schema.get("feature_names", FEATURE_COLUMNS)
    else:
        features = FEATURE_COLUMNS

    _cached_model = model
    _cached_features = features
    return model, features


def check_existing_prediction(symbol: str, feature_ts: datetime, model_version: str) -> Optional[Dict[str, Any]]:
    """Checks if a prediction has already been generated for this exact feature candle."""
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT prediction_id, predicted_direction, probability_up, probability_not_up,
                       current_price, feature_timestamp, target_timestamp, model_version
                FROM warehouse.fact_btc_predictions
                WHERE symbol = %s AND feature_timestamp = %s AND model_version = %s
                LIMIT 1;
            """, (symbol, feature_ts, model_version))
            r = cur.fetchone()
            if r:
                return {
                    "prediction_id": r[0],
                    "symbol": symbol,
                    "predicted_direction": r[1],
                    "direction_label": "UP" if r[1] == 1 else "NOT_UP",
                    "probability_up": float(r[2]),
                    "probability_not_up": float(r[3]),
                    "current_price": float(r[4]),
                    "feature_timestamp": r[5],
                    "target_timestamp": r[6],
                    "model_version": r[7],
                    "horizon_minutes": HORIZON_MINUTES
                }
    except Exception as e:
        print(f"[!] Warning reading existing prediction from DB: {e}")
    finally:
        conn.close()
    return None


def fetch_live_or_recent_candles(min_candles: int = 40) -> pd.DataFrame:
    """
    Fetches the most up-to-date finalized 1m BTC candles for live inference:
      1. First checks PostgreSQL DW for recent finalized candles (is_closed = TRUE, within staleness threshold).
      2. If DB candles are stale or insufficient, fetches recent 1m candles directly from OKX REST API,
         parses them using shared candle_parser (strictly confirm == '1', USDT turnover from row[7]),
         syncs them to DW, and uses only finalized candles.
      3. If no live finalized candles are available, returns empty DataFrame.
    """
    now_utc = datetime.now(timezone.utc)

    # 1. Try PostgreSQL DW for real-time finalized candles
    try:
        conn = get_db_connection()
        if conn:
            try:
                query = """
                    SELECT 
                        window_start, window_end, open, high, low, close, volume, 
                        buy_volume, sell_volume, vwap, trade_imbalance, total_trade_value, source, is_closed
                    FROM warehouse.fact_crypto_1m_candles
                    WHERE symbol = 'BTC-USDT'
                      AND source = 'historical_rest'
                      AND is_closed = TRUE
                      AND open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0
                    ORDER BY window_start DESC
                    LIMIT 120;
                """
                df_db = pd.read_sql(query, conn)
                if not df_db.empty and len(df_db) >= min_candles:
                    min_oc = np.minimum(df_db["open"], df_db["close"])
                    max_oc = np.maximum(df_db["open"], df_db["close"])
                    geom_valid = (df_db["low"] <= min_oc) & (max_oc <= df_db["high"])
                    df_db = df_db[geom_valid].copy()
                    df_db["window_start"] = pd.to_datetime(df_db["window_start"], utc=True)
                    df_db["window_end"] = pd.to_datetime(df_db["window_end"], utc=True)
                    latest_ts = df_db["window_start"].iloc[0]
                    # Nến 1 phút đã đóng gần nhất phải có mốc bắt đầu là phút vừa trôi qua
                    expected_latest_closed = now_utc.replace(second=0, microsecond=0) - timedelta(minutes=1)
                    if latest_ts >= expected_latest_closed:
                        # DB đã cập nhật đủ nến mới nhất vừa đóng cửa
                        df_sorted = df_db.sort_values("window_start").reset_index(drop=True)
                        return df_sorted
            finally:
                conn.close()
    except Exception as e:
        print(f"[PREDICT] Note checking DB candles: {e}")

    # 2. Fetch live recent candles directly from OKX REST API
    try:
        url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=100"
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8")).get("data", [])

        if data and len(data) >= 26:
            records = []
            for row in data:
                candle_dict = parse_okx_rest_candle(row, symbol="BTC-USDT", require_closed=True)
                if candle_dict is not None:
                    records.append(candle_dict)

            if records:
                try:
                    write_historical_candles_batch(records)
                except Exception as db_err:
                    print(f"[PREDICT] Database sync failed ({db_err}). Proceeding with in-memory inference from REST.")

                df_finalized = pd.DataFrame(records).sort_values("window_start").reset_index(drop=True)
                return df_finalized
    except Exception as e:
        print(f"[PREDICT] Note: Live OKX REST fetch error ({e}).")
        # Dự phòng: Nếu OKX REST lỗi mạng, dùng tạm nến DB nếu còn trong ngưỡng MAX_STALENESS_MINUTES
        if 'df_db' in locals() and not df_db.empty:
            latest_ts = df_db["window_start"].iloc[0]
            if (now_utc - latest_ts) <= timedelta(minutes=MAX_STALENESS_MINUTES):
                return df_db.sort_values("window_start").reset_index(drop=True)

    return pd.DataFrame()


def run_prediction_detailed(
    candles_df: Optional[pd.DataFrame] = None,
    check_duplicate: bool = True
) -> Dict[str, Any]:
    """
    Performs inference on the latest finalized BTC candle with structured reason codes:
      - Returns {"success": True, "already_predicted": True/False, "prediction": {...}} on success.
      - Returns {"success": False, "code": "<CODE>", "message": "<MSG>"} on failure.
    """
    # 1. Load model
    try:
        model, feature_names = load_prediction_model()
    except Exception as me:
        code = "MODEL_UNAVAILABLE"
        msg = f"Không tìm thấy hoặc không thể nạp mô hình XGBoost: {me}"
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    model_version = get_current_model_version()

    # 2. Fetch candles if not provided
    if candles_df is None:
        candles_df = fetch_live_or_recent_candles(min_candles=40)

    if candles_df is None or candles_df.empty:
        code = "NO_DATA"
        msg = "Không có nến nào khả dụng từ Data Warehouse hoặc OKX REST để suy luận."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    time_col = "window_start" if "window_start" in candles_df.columns else "timestamp"
    if time_col not in candles_df.columns:
        code = "NO_DATA"
        msg = f"Thiếu cột thời gian '{time_col}' trong tập nến đầu vào."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    candles_df = candles_df.copy()
    candles_df[time_col] = pd.to_datetime(candles_df[time_col], utc=True)
    if "window_end" not in candles_df.columns:
        candles_df["window_end"] = candles_df[time_col] + pd.Timedelta(minutes=1)
    candles_df["window_end"] = pd.to_datetime(candles_df["window_end"], utc=True)

    # Deduplicate & sort chronologically
    candles_df = candles_df.drop_duplicates(subset=[time_col]).sort_values(time_col).reset_index(drop=True)

    # 3. Validation: mandatory source and closure checks
    if "source" not in candles_df.columns:
        code = "UNCLOSED_CANDLE"
        msg = "Tập nến thiếu trường bắt buộc 'source'."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    invalid_sources = candles_df[candles_df["source"] != "historical_rest"]
    if not invalid_sources.empty:
        code = "UNCLOSED_CANDLE"
        msg = "Từ chối nến stream chưa qua đối soát canonical. Chỉ nến 'historical_rest' được chấp thuận."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    if "is_closed" not in candles_df.columns:
        code = "UNCLOSED_CANDLE"
        msg = "Tập nến thiếu trường bắt buộc 'is_closed'."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    if not candles_df["is_closed"].all():
        code = "UNCLOSED_CANDLE"
        msg = "Phát hiện nến chưa đóng (is_closed != True). Chỉ nến đã chốt phiên được đưa vào mô hình."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    # 4. OHLCV Geometry & Numeric Validation
    required_cols = ["open", "high", "low", "close", "volume"]
    for col in required_cols:
        if col not in candles_df.columns:
            code = "INVALID_OHLCV"
            msg = f"Thiếu cột bắt buộc '{col}' trong dữ liệu nến."
            _set_rejection(code, msg)
            return {"success": False, "code": code, "message": msg}
        if not np.isfinite(candles_df[col]).all():
            code = "INVALID_OHLCV"
            msg = f"Phát hiện giá trị không hợp lệ (NaN/Inf) trong cột '{col}'."
            _set_rejection(code, msg)
            return {"success": False, "code": code, "message": msg}

    if (candles_df["open"] <= 0).any() or (candles_df["high"] <= 0).any() or (candles_df["low"] <= 0).any() or (candles_df["close"] <= 0).any():
        code = "INVALID_OHLCV"
        msg = "Phát hiện giá trị giá <= 0 trong dữ liệu nến."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    if (candles_df["volume"] < 0).any():
        code = "INVALID_OHLCV"
        msg = "Phát hiện khối lượng giao dịch âm (< 0)."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    min_oc = np.minimum(candles_df["open"], candles_df["close"])
    max_oc = np.maximum(candles_df["open"], candles_df["close"])
    if (candles_df["low"] > min_oc).any() or (max_oc > candles_df["high"]).any():
        code = "INVALID_OHLCV"
        msg = "Vi phạm hình học nến Nhật Bản (Low > min(Open, Close) hoặc High < max(Open, Close))."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    # 5. Finalized mask (window_end <= now_utc)
    now_utc = datetime.now(timezone.utc)
    finalized_mask = (candles_df["window_end"] <= now_utc)
    if "is_closed" in candles_df.columns:
        finalized_mask = finalized_mask & (candles_df["is_closed"] == True)

    candles_df = candles_df[finalized_mask].copy().reset_index(drop=True)
    if candles_df.empty:
        code = "UNCLOSED_CANDLE"
        msg = "Chưa có nến nào kết thúc chu kỳ (đang chờ phút hiện tại đóng cửa)."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    # 6. Staleness check
    latest_candle_time = candles_df[time_col].iloc[-1]
    if (now_utc - latest_candle_time) > timedelta(minutes=MAX_STALENESS_MINUTES):
        code = "STALE_DATA"
        msg = f"Dữ liệu nến quá cũ: nến gần nhất là {latest_candle_time.strftime('%Y-%m-%d %H:%M:%S UTC')} (> {MAX_STALENESS_MINUTES} phút)."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    # 7. Warmup buffer & gap detection
    if len(candles_df) < 26:
        code = "INSUFFICIENT_HISTORY"
        msg = f"Không đủ dữ liệu warmup: hiện có {len(candles_df)} nến (cần tối thiểu 26 nến liên tục)."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    recent_eval_df = candles_df.iloc[-20:].copy()
    time_deltas = recent_eval_df[time_col].diff().dt.total_seconds().dropna() / 60.0
    if (time_deltas > 1.05).any():
        code = "MISSING_CANDLES"
        msg = "Phát hiện khoảng trống thời gian (gap) trong 20 nến gần nhất."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    # 8. Check duplicate prediction for this feature candle
    latest_row = candles_df.iloc[-1]
    feature_ts = pd.to_datetime(latest_row[time_col], utc=True)

    if check_duplicate:
        existing_pred = check_existing_prediction("BTC-USDT", feature_ts, model_version)
        if existing_pred:
            return {
                "success": True,
                "already_predicted": True,
                "prediction": existing_pred,
                "message": "Nến 1 phút gần nhất đã được dự báo. Đang chờ nến tiếp theo đóng cửa."
            }

    # 9. Compute rolling features
    feat_df = compute_features(candles_df, feature_mode="common")
    if feat_df.empty or len(feat_df) < 20:
        code = "INSUFFICIENT_HISTORY"
        msg = "Không đủ số hàng sau khi trích xuất đặc trưng để tạo vector quan sát."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    latest_feat_row = feat_df.iloc[-1]
    feature_ts = pd.to_datetime(latest_feat_row[time_col], utc=True)
    target_ts = feature_ts + timedelta(minutes=HORIZON_MINUTES)

    if target_ts <= now_utc:
        code = "TARGET_EXPIRED"
        msg = f"Mốc thời gian mục tiêu ({target_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}) đã trôi qua trong quá khứ."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    current_price = float(latest_feat_row["close"])
    X = latest_feat_row[feature_names].values.reshape(1, -1).astype(float)
    if not np.isfinite(X).all():
        code = "FEATURE_SCHEMA_MISMATCH"
        msg = "Xuất hiện giá trị không hữu hạn (NaN hoặc Inf) trong vector đặc trưng (15 common features)."
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    # 10. Predict
    pred_dir = int(model.predict(X)[0])
    probabilities = model.predict_proba(X)[0]
    prob_not_up = float(probabilities[0])
    prob_up = float(probabilities[1])

    result = {
        "symbol": "BTC-USDT",
        "feature_timestamp": feature_ts,
        "prediction_timestamp": now_utc,
        "target_timestamp": target_ts,
        "predicted_direction": pred_dir,
        "direction_label": "UP" if pred_dir == 1 else "NOT_UP",
        "probability_up": round(prob_up, 4),
        "probability_not_up": round(prob_not_up, 4),
        "model_version": model_version,
        "model_name": MODEL_NAME,
        "current_price": current_price,
        "horizon_minutes": HORIZON_MINUTES
    }

    # 11. Save to DB
    try:
        save_prediction_to_db(result)
    except Exception as db_err:
        code = "DATABASE_ERROR"
        msg = f"Lỗi lưu dự báo vào cơ sở dữ liệu: {db_err}"
        _set_rejection(code, msg)
        return {"success": False, "code": code, "message": msg}

    print(
        f"[PREDICTION] BTC-USDT | "
        f"Price: ${current_price:,.2f} | "
        f"Direction: {result['direction_label']} | "
        f"P(UP): {prob_up * 100:.1f}% | "
        f"Target Time: {target_ts.strftime('%H:%M:%S UTC')}"
    )

    return {
        "success": True,
        "already_predicted": False,
        "prediction": result,
        "message": "Dự báo mới được tạo thành công"
    }


def run_single_prediction(candles_df: Optional[pd.DataFrame] = None) -> Optional[Dict[str, Any]]:
    """
    Backward-compatible entry point returning prediction dict or None.
    """
    res = run_prediction_detailed(candles_df=candles_df, check_duplicate=False)
    if res.get("success"):
        return res["prediction"]
    return None


def save_prediction_to_db(pred: Dict[str, Any]):
    """Stores inference record in warehouse.fact_btc_predictions with safe ON CONFLICT DO NOTHING."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        sql = """
            INSERT INTO warehouse.fact_btc_predictions (
                symbol, feature_timestamp, prediction_timestamp, target_timestamp,
                predicted_direction, probability_up, probability_not_up,
                model_version, model_name, current_price, horizon_minutes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, feature_timestamp, model_version)
            DO NOTHING;
        """
        cur.execute(sql, (
            pred["symbol"],
            pred["feature_timestamp"],
            pred["prediction_timestamp"],
            pred["target_timestamp"],
            pred["predicted_direction"],
            pred["probability_up"],
            pred["probability_not_up"],
            pred["model_version"],
            pred["model_name"],
            pred["current_price"],
            pred["horizon_minutes"]
        ))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[!] Warning: Could not save prediction to DB: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


def evaluate_unresolved_predictions(candles_df: Optional[pd.DataFrame] = None) -> int:
    """
    Evaluates unresolved predictions once the target candle has closed:
      - Target candle opens at target_timestamp and closes at target_timestamp + 60 seconds.
      - Finds confirmed closed candle at target_timestamp (strictly window_start == target_timestamp).
      - Rejects legacy unaligned seconds, tolerance deviations, or unclosed candles.
      - Prioritizes recent pending predictions so unresolvable legacy records do not block evaluation.
      - Rate-limits OKX REST backfill requests to max 3 per cycle.
      - Updates actual_future_price, actual_direction, is_correct in fact_btc_predictions idempotently.
    """
    conn = get_db_connection()
    if not conn:
        return 0

    evaluated_count = 0
    rest_requests_count = 0
    MAX_REST_REQUESTS = 3

    try:
        cur = conn.cursor()
        # Prioritize recent records (target_timestamp DESC) so new predictions evaluate promptly,
        # plus up to 10 older unresolved records to drain backlog without blocking.
        cur.execute("""
            (
                SELECT prediction_id, feature_timestamp, current_price, target_timestamp, predicted_direction
                FROM warehouse.fact_btc_predictions
                WHERE symbol = 'BTC-USDT' 
                  AND actual_direction IS NULL
                  AND target_timestamp + INTERVAL '60 seconds' <= CURRENT_TIMESTAMP
                ORDER BY target_timestamp DESC
                LIMIT 20
            )
            UNION ALL
            (
                SELECT prediction_id, feature_timestamp, current_price, target_timestamp, predicted_direction
                FROM warehouse.fact_btc_predictions
                WHERE symbol = 'BTC-USDT' 
                  AND actual_direction IS NULL
                  AND target_timestamp + INTERVAL '60 seconds' <= CURRENT_TIMESTAMP
                ORDER BY target_timestamp ASC
                LIMIT 10
            );
        """)
        unresolved = cur.fetchall()
        if not unresolved:
            return 0

        # Deduplicate records by prediction_id in case UNION returned overlap
        seen_pred_ids = set()
        unique_unresolved = []
        for r in unresolved:
            if r[0] not in seen_pred_ids:
                seen_pred_ids.add(r[0])
                unique_unresolved.append(r)

        for r in unique_unresolved:
            pred_id, feat_ts, cur_px, tgt_ts, pred_dir = r[0], r[1], float(r[2]), r[3], r[4]
            tgt_ts_utc = tgt_ts.astimezone(timezone.utc) if tgt_ts.tzinfo else tgt_ts.replace(tzinfo=timezone.utc)

            # Legacy records with non-zero second timestamps are unaligned and ineligible for automated 1m matching
            if tgt_ts_utc.second != 0:
                continue

            future_px = None

            # 1. Exact match from in-memory candles if provided
            if candles_df is not None and not candles_df.empty:
                time_col = "window_start" if "window_start" in candles_df.columns else "timestamp"
                df_times = pd.to_datetime(candles_df[time_col], utc=True)
                exact_matches = candles_df.loc[df_times == tgt_ts_utc]
                if not exact_matches.empty:
                    matched_candle = exact_matches.iloc[0]
                    # Mandatory explicit closure check (do NOT default to True)
                    if "is_closed" in matched_candle and bool(matched_candle["is_closed"]) is True:
                        if "source" not in matched_candle or matched_candle["source"] in ("historical_rest", "okx_rest"):
                            future_px = float(matched_candle["close"])

            # 2. Exact match from PostgreSQL DW for the closed target candle
            if future_px is None:
                cur.execute("""
                    SELECT close
                    FROM warehouse.fact_crypto_1m_candles
                    WHERE symbol = 'BTC-USDT'
                      AND window_start = %s
                      AND is_closed = TRUE
                    LIMIT 1;
                """, (tgt_ts_utc,))
                candle_row = cur.fetchone()
                if candle_row:
                    future_px = float(candle_row[0])

            # 3. If still not in DW, fetch authoritative confirmed candle from OKX REST (rate limited)
            if future_px is None and rest_requests_count < MAX_REST_REQUESTS:
                rest_requests_count += 1
                try:
                    ts_ms = int(tgt_ts_utc.timestamp() * 1000)
                    url = f"https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=1m&after={ts_ms + 120000}&limit=5"
                    req = urllib.request.Request(
                        url,
                        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"}
                    )
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        res_data = json.loads(resp.read().decode("utf-8")).get("data", [])
                    for c_raw in res_data:
                        c_parsed = parse_okx_rest_candle(c_raw, symbol="BTC-USDT", require_closed=True)
                        if c_parsed and c_parsed["window_start"] == tgt_ts_utc:
                            future_px = float(c_parsed["close"])
                            write_historical_candles_batch([c_parsed], conn=conn, close_conn=False)
                            break
                except Exception:
                    pass

            # 4. If target price resolved, compute direction and update DB
            if future_px is not None:
                actual_dir = 1 if future_px > cur_px else 0
                is_correct = bool(actual_dir == pred_dir)
                cur.execute("""
                    UPDATE warehouse.fact_btc_predictions
                    SET actual_future_price = %s,
                        actual_direction = %s,
                        is_correct = %s
                    WHERE prediction_id = %s;
                """, (future_px, actual_dir, is_correct, pred_id))
                evaluated_count += 1
                conn.commit()

        return evaluated_count
    except Exception as e:
        print(f"[!] Warning during outcome evaluation: {e}")
        if conn:
            conn.rollback()
        return evaluated_count
    finally:
        if conn:
            conn.close()


def backfill_actual_outcomes(candles_df: pd.DataFrame):
    """Backward-compatible wrapper calling evaluate_unresolved_predictions."""
    return evaluate_unresolved_predictions(candles_df)


if __name__ == "__main__":
    run_single_prediction()
