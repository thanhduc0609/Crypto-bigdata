"""
=============================================================================
Crypto Big Data - Machine Learning: Real-Time BTC Direction Inference
=============================================================================
Executes near-real-time inference for BTC-USDT price direction (next 5m):
  1. Loads trained XGBoost model and verifies feature schema
  2. Fetches latest finalized 1m BTC candles
  3. Calculates rolling features without lookahead
  4. Predicts binary direction (UP / NOT_UP) and UP probability
  5. Stores prediction into PostgreSQL warehouse.fact_btc_predictions
  6. Backfills actual outcomes for past predictions once 5 minutes elapse
=============================================================================
"""

import os
import sys
import json
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from xgboost import XGBClassifier
from processing.core.config import get_ml_config
from processing.features.feature_engineering import compute_features, FEATURE_COLUMNS
from ml.dataset import fetch_historical_btc_candles
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

_cached_model = None
_cached_features = None


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


def fetch_live_or_recent_candles(min_candles: int = 40) -> pd.DataFrame:
    """
    Fetches the most up-to-date finalized 1m BTC candles for live inference:
      1. First checks PostgreSQL DW for recent finalized candles (is_closed = TRUE, within staleness threshold).
      2. If DB candles are stale or insufficient, fetches recent 1m candles directly from OKX REST API,
         parses them using shared candle_parser (strictly confirm == '1', USDT turnover from row[7]),
         syncs them to DW, and uses only finalized candles.
      3. If no live finalized candles are available, returns empty DataFrame to prevent emitting
         spurious real-time predictions on stale or unclosed data.
    """
    now_utc = datetime.now(timezone.utc)

    # 1. Try PostgreSQL DW for real-time finalized candles
    try:
        conn = get_db_connection()
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
                LIMIT 120
            """
            df_db = pd.read_sql(query, conn)
            if not df_db.empty and len(df_db) >= min_candles:
                # Validate candlestick geometry
                min_oc = np.minimum(df_db["open"], df_db["close"])
                max_oc = np.maximum(df_db["open"], df_db["close"])
                geom_valid = (df_db["low"] <= min_oc) & (max_oc <= df_db["high"])
                df_db = df_db[geom_valid].copy()
                df_db["window_start"] = pd.to_datetime(df_db["window_start"], utc=True)
                df_db["window_end"] = pd.to_datetime(df_db["window_end"], utc=True)
                latest_ts = df_db["window_start"].iloc[0]
                # Check freshness
                if (now_utc - latest_ts) <= timedelta(minutes=MAX_STALENESS_MINUTES):
                    df_sorted = df_db.sort_values("window_start").reset_index(drop=True)
                    return df_sorted
        finally:
            conn.close()
    except Exception as e:
        print(f"[PREDICT] Note checking DB candles: {e}")

    # 2. Fetch live recent candles directly from OKX REST API
    try:
        url = "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=100"
        req = urllib.request.Request(url, headers={"User-Agent": "CryptoDataBot/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8")).get("data", [])

        if data and len(data) >= 26:
            records = []
            for row in data:
                candle_dict = parse_okx_rest_candle(row, symbol="BTC-USDT", require_closed=True)
                if candle_dict is not None:
                    records.append(candle_dict)

            if records:
                # Sync authoritative REST candles to PostgreSQL DW using shared function
                try:
                    write_historical_candles_batch(records)
                    print(f"[PREDICT] Synchronized {len(records)} authoritative REST candles to PostgreSQL DW.")
                except Exception as db_err:
                    print(f"[PREDICT] Database sync failed ({db_err}). Proceeding with in-memory inference from REST.")

                df_finalized = pd.DataFrame(records).sort_values("window_start").reset_index(drop=True)
                print(f"[PREDICT] Loaded {len(df_finalized)} finalized/closed candles from OKX REST for inference.")
                return df_finalized
    except Exception as e:
        print(f"[PREDICT] Note: Live OKX REST fetch error ({e}).")

    # Do NOT fallback to stale historical training data for real-time inference
    return pd.DataFrame()


def run_single_prediction(candles_df: Optional[pd.DataFrame] = None) -> Optional[Dict[str, Any]]:
    """
    Performs inference on the latest finalized BTC candle and saves to PostgreSQL.
    """
    model, feature_names = load_prediction_model()
    model_version = get_current_model_version()

    if candles_df is None:
        candles_df = fetch_live_or_recent_candles(min_candles=40)

    # Check for empty data before accessing columns
    if candles_df is None or candles_df.empty:
        print("[PREDICT] No valid finalized candles available for inference. Skipping prediction.")
        return None

    time_col = "window_start" if "window_start" in candles_df.columns else "timestamp"
    if time_col not in candles_df.columns:
        print(f"[PREDICT] Missing timestamp column '{time_col}'. Skipping prediction.")
        return None

    candles_df = candles_df.copy()
    candles_df[time_col] = pd.to_datetime(candles_df[time_col], utc=True)

    if "window_end" not in candles_df.columns:
        candles_df["window_end"] = candles_df[time_col] + pd.Timedelta(minutes=1)
    candles_df["window_end"] = pd.to_datetime(candles_df["window_end"], utc=True)

    # Eliminate duplicate timestamps and sort chronologically
    candles_df = candles_df.drop_duplicates(subset=[time_col]).sort_values(time_col).reset_index(drop=True)

    # ML Quality Gate: strictly accept confirmed historical_rest candles
    if "source" in candles_df.columns:
        invalid_sources = candles_df[candles_df["source"] != "historical_rest"]
        if not invalid_sources.empty:
            print("[PREDICT] Rejected unapproved candle source(s) in ML input. Only confirmed 'historical_rest' candles are accepted.")
            return None

    if "is_closed" in candles_df.columns:
        if not candles_df["is_closed"].all():
            print("[PREDICT] Rejected unclosed candles in ML input. All input candles must have is_closed=True.")
            return None

    required_cols = ["open", "high", "low", "close", "volume"]
    for col in required_cols:
        if col not in candles_df.columns:
            print(f"[PREDICT] Missing required price column '{col}'. Skipping prediction.")
            return None
        if not np.isfinite(candles_df[col]).all():
            print(f"[PREDICT] Non-finite values detected in column '{col}'. Skipping prediction.")
            return None

    if (candles_df["open"] <= 0).any() or (candles_df["high"] <= 0).any() or (candles_df["low"] <= 0).any() or (candles_df["close"] <= 0).any():
        print("[PREDICT] Non-positive prices detected in ML input. Skipping prediction.")
        return None
    if (candles_df["volume"] < 0).any():
        print("[PREDICT] Negative volume detected in ML input. Skipping prediction.")
        return None

    min_oc = np.minimum(candles_df["open"], candles_df["close"])
    max_oc = np.maximum(candles_df["open"], candles_df["close"])
    if (candles_df["low"] > min_oc).any() or (max_oc > candles_df["high"]).any():
        print("[PREDICT] Candlestick geometry violation detected. Skipping prediction.")
        return None

    # ML Quality Gate: strictly accept confirmed historical_rest candles
    if "source" in candles_df.columns:
        invalid_sources = candles_df[candles_df["source"] != "historical_rest"]
        if not invalid_sources.empty:
            print("[PREDICT] Rejected unapproved candle source(s) in ML input. Only confirmed 'historical_rest' candles are accepted.")
            return None

    if "is_closed" in candles_df.columns:
        if not candles_df["is_closed"].all():
            print("[PREDICT] Rejected unclosed candles in ML input. All input candles must have is_closed=True.")
            return None

    required_cols = ["open", "high", "low", "close", "volume"]
    for col in required_cols:
        if col not in candles_df.columns:
            print(f"[PREDICT] Missing required price column '{col}'. Skipping prediction.")
            return None
        if not np.isfinite(candles_df[col]).all():
            print(f"[PREDICT] Non-finite values detected in column '{col}'. Skipping prediction.")
            return None

    if (candles_df["open"] <= 0).any() or (candles_df["high"] <= 0).any() or (candles_df["low"] <= 0).any() or (candles_df["close"] <= 0).any():
        print("[PREDICT] Non-positive prices detected in ML input. Skipping prediction.")
        return None
    if (candles_df["volume"] < 0).any():
        print("[PREDICT] Negative volume detected in ML input. Skipping prediction.")
        return None

    min_oc = np.minimum(candles_df["open"], candles_df["close"])
    max_oc = np.maximum(candles_df["open"], candles_df["close"])
    if (candles_df["low"] > min_oc).any() or (max_oc > candles_df["high"]).any():
        print("[PREDICT] Candlestick geometry violation detected. Skipping prediction.")
        return None

    # Filter strictly for finalized candles (candle must have fully ended: window_end <= now_utc)
    # NO future tolerance (+5s removed)
    now_utc = datetime.now(timezone.utc)
    finalized_mask = (candles_df["window_end"] <= now_utc)
    if "is_closed" in candles_df.columns:
        finalized_mask = finalized_mask & (candles_df["is_closed"] == True)

    candles_df = candles_df[finalized_mask].copy().reset_index(drop=True)
    if candles_df.empty:
        print("[PREDICT] No finalized closed candles available yet (waiting for current minute to close).")
        return None

    # Check staleness: latest closed candle must not be older than MAX_STALENESS_MINUTES
    latest_candle_time = candles_df[time_col].iloc[-1]
    if (now_utc - latest_candle_time) > timedelta(minutes=MAX_STALENESS_MINUTES):
        print(f"[PREDICT] Data is stale: latest candle is at {latest_candle_time} UTC (> {MAX_STALENESS_MINUTES}m old). Skipping prediction.")
        return None

    # Check minimum warmup buffer
    if len(candles_df) < 26:
        print(f"[PREDICT] Insufficient warmup buffer: {len(candles_df)} candles (need at least 26 consecutive minutes).")
        return None

    # Check for timestamp gaps in the recent evaluation sequence (last 20 candles)
    recent_eval_df = candles_df.iloc[-20:].copy()
    time_deltas = recent_eval_df[time_col].diff().dt.total_seconds().dropna() / 60.0
    if (time_deltas > 1.05).any():
        print("[PREDICT] Timestamp gap detected in recent candles. Consecutive 1m sequence required for indicators. Skipping prediction.")
        return None

    # Compute rolling common features
    feat_df = compute_features(candles_df, feature_mode="common")
    if feat_df.empty or len(feat_df) < 20:
        print("[PREDICT] Incomplete feature data: not enough historical candles to compute rolling features.")
        return None

    # Latest observation
    latest_row = feat_df.iloc[-1]
    feature_ts = pd.to_datetime(latest_row[time_col], utc=True)
    target_ts = feature_ts + timedelta(minutes=HORIZON_MINUTES)
    if target_ts <= now_utc:
        print(f"[PREDICT] Target timestamp ({target_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}) has already elapsed (now={now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}). Skipping realtime prediction.")
        return None
    current_price = float(latest_row["close"])

    X = latest_row[feature_names].values.reshape(1, -1).astype(float)
    if np.isnan(X).any():
        print("[PREDICT] Warning: NaN values detected in feature vector. Skipping prediction.")
        return None

    # Predict
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

    # Save to PostgreSQL
    save_prediction_to_db(result)
    backfill_actual_outcomes(candles_df)

    print(
        f"[PREDICTION] BTC-USDT | "
        f"Price: ${current_price:,.2f} | "
        f"Direction: {result['direction_label']} | "
        f"P(UP): {prob_up * 100:.1f}% | "
        f"Target Time: {target_ts.strftime('%H:%M:%S UTC')}"
    )

    return result


def save_prediction_to_db(pred: Dict[str, Any]):
    """Stores inference record in warehouse.fact_btc_predictions with upsert."""
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
            DO UPDATE SET
                predicted_direction = EXCLUDED.predicted_direction,
                probability_up = EXCLUDED.probability_up,
                probability_not_up = EXCLUDED.probability_not_up,
                prediction_timestamp = EXCLUDED.prediction_timestamp;
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
    finally:
        if conn:
            conn.close()


def backfill_actual_outcomes(candles_df: pd.DataFrame):
    """
    Backfills past predictions where 5 minutes have elapsed with the actual price and accuracy.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Find unresolved predictions older than 5 minutes
        cur.execute("""
            SELECT prediction_id, feature_timestamp, current_price, target_timestamp, predicted_direction
            FROM warehouse.fact_btc_predictions
            WHERE symbol = 'BTC-USDT' AND actual_direction IS NULL
              AND target_timestamp <= CURRENT_TIMESTAMP;
        """)
        rows = cur.fetchall()
        if not rows:
            return

        time_col = "window_start" if "window_start" in candles_df.columns else "timestamp"
        candles_df = candles_df.copy()
        candles_df[time_col] = pd.to_datetime(candles_df[time_col])

        update_sql = """
            UPDATE warehouse.fact_btc_predictions
            SET actual_future_price = %s,
                actual_direction = %s,
                is_correct = %s
            WHERE prediction_id = %s;
        """

        for r in rows:
            pred_id, feat_ts, cur_px, tgt_ts, pred_dir = r[0], r[1], r[2], r[3], r[4]
            # Match candle strictly equal to target_ts (within 30 seconds of target minute)
            time_diffs = (candles_df[time_col] - pd.to_datetime(tgt_ts)).dt.total_seconds().abs()
            valid_mask = time_diffs <= 30
            if valid_mask.any():
                closest_idx = time_diffs[valid_mask].idxmin()
                future_px = float(candles_df.loc[closest_idx, "close"])
                actual_dir = 1 if future_px > cur_px else 0
                is_correct = bool(actual_dir == pred_dir)
                cur.execute(update_sql, (future_px, actual_dir, is_correct, pred_id))

        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[!] Warning during outcome backfill: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    run_single_prediction()
