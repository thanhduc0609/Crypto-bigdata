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

ml_cfg = get_ml_config()
artifacts_cfg = ml_cfg.get("artifacts", {})
ARTIFACTS_DIR = project_root / artifacts_cfg.get("directory", "ml/artifacts")
MODEL_FILE = ARTIFACTS_DIR / artifacts_cfg.get("model_filename", "btc_xgboost_model.json")
SCHEMA_FILE = ARTIFACTS_DIR / artifacts_cfg.get("feature_schema_filename", "feature_schema.json")
METADATA_FILE = ARTIFACTS_DIR / artifacts_cfg.get("model_metadata_filename", "model_metadata.json")

HORIZON_MINUTES = int(ml_cfg.get("prediction", {}).get("horizon_minutes", 5))
MODEL_NAME = str(ml_cfg.get("ml", {}).get("model_name", "btc_xgboost_direction"))


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
    Fetches the most up-to-date 1m BTC candles for live inference:
      1. First checks PostgreSQL DW for recent streaming candles (<= 5 mins old).
      2. If DB candles are stale (> 5 mins old) or insufficient, fetches latest 100 
         1-minute candles directly from OKX REST API, syncs them to DW, and uses them.
      3. Falls back to historical Gold Lakehouse if API is unavailable.
    """
    now_utc = datetime.now(timezone.utc)

    # 1. Try PostgreSQL DW for real-time streaming candles
    try:
        conn = get_db_connection()
        try:
            query = """
                SELECT 
                    window_start, window_end, open, high, low, close, volume, 
                    buy_volume, sell_volume, vwap, trade_imbalance, total_trade_value, source
                FROM warehouse.fact_crypto_1m_candles
                WHERE symbol = 'BTC-USDT'
                ORDER BY window_start DESC
                LIMIT 120
            """
            df_db = pd.read_sql(query, conn)
            if not df_db.empty and len(df_db) >= min_candles:
                latest_ts = pd.to_datetime(df_db["window_start"].iloc[0])
                if latest_ts.tzinfo is None:
                    latest_ts = latest_ts.replace(tzinfo=timezone.utc)
                # Check freshness: candle must be within last 5 minutes
                if (now_utc - latest_ts) <= timedelta(minutes=5):
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
            conn = None
            try:
                conn = get_db_connection()
                cur = conn.cursor()
            except Exception:
                cur = None

            for row in data:
                # OKX candle schema: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                confirm = str(row[8]) if len(row) > 8 else "1"
                is_closed = bool(confirm != "0")

                ts_ms = int(row[0])
                dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                dt_end = dt + timedelta(minutes=1)
                o, h, l, c, v = float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])
                vol_ccy = float(row[6]) if len(row) > 6 else 0.0

                records.append({
                    "window_start": dt,
                    "window_end": dt_end,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                    "total_trade_value": vol_ccy,
                    "source": "okx_live_sync",
                    "is_closed": is_closed
                })

                if cur:
                    try:
                        cur.execute("""
                            INSERT INTO warehouse.fact_crypto_1m_candles (
                                symbol, window_start, window_end, open, high, low, close, volume, total_trade_value, source, is_closed
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (symbol, window_start) DO UPDATE SET
                                close = EXCLUDED.close,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                volume = EXCLUDED.volume,
                                total_trade_value = EXCLUDED.total_trade_value,
                                is_closed = EXCLUDED.is_closed;
                        """, ("BTC-USDT", dt, dt_end, o, h, l, c, v, vol_ccy, "okx_live_sync", is_closed))
                    except Exception:
                        pass

            if conn:
                try:
                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception:
                    pass

            df_live = pd.DataFrame(records).sort_values("window_start").reset_index(drop=True)
            # Filter strictly for closed finalized candles for feature calculation
            df_finalized = df_live[df_live["is_closed"] == True].copy().reset_index(drop=True)
            print(f"[PREDICT] Synced {len(df_live)} candles from OKX ({len(df_finalized)} finalized/closed).")
            return df_finalized if not df_finalized.empty else df_live
    except Exception as e:
        print(f"[PREDICT] Note: Live OKX REST fetch error ({e}).")

    # 3. Fallback to historical candles in Lakehouse / DB
    return fetch_historical_btc_candles(min_candles=min_candles)


def run_single_prediction(candles_df: Optional[pd.DataFrame] = None) -> Optional[Dict[str, Any]]:
    """
    Performs inference on the latest finalized BTC candle and saves to PostgreSQL.
    """
    model, feature_names = load_prediction_model()
    model_version = get_current_model_version()

    if candles_df is None:
        candles_df = fetch_live_or_recent_candles(min_candles=40)

    # Ensure window_end is present and filter for finalized candles only (discard active unclosed minute)
    now_utc = datetime.now(timezone.utc)
    time_col = "window_start" if "window_start" in candles_df.columns else "timestamp"
    candles_df = candles_df.copy()
    candles_df[time_col] = pd.to_datetime(candles_df[time_col])

    if "window_end" not in candles_df.columns:
        candles_df["window_end"] = candles_df[time_col] + pd.Timedelta(minutes=1)
    candles_df["window_end"] = pd.to_datetime(candles_df["window_end"])

    # Strict finalized filter: candle must have fully closed
    finalized_mask = candles_df["window_end"] <= (now_utc + timedelta(seconds=5))
    if finalized_mask.any():
        candles_df = candles_df[finalized_mask].copy()
    else:
        print("[PREDICT] No finalized candles available yet. Waiting for current minute to close.")
        return None

    # Sort chronologically
    candles_df = candles_df.sort_values(time_col).reset_index(drop=True)

    # Check minimum warmup rows
    if len(candles_df) < 26:
        print(f"[PREDICT] Insufficient warmup buffer: {len(candles_df)} candles (need at least 26).")
        return None

    # Compute rolling common features
    feat_df = compute_features(candles_df, feature_mode="common")
    if len(feat_df) < 20:
        print("[PREDICT] Incomplete data: not enough historical candles to compute features.")
        return None

    # Latest observation
    latest_row = feat_df.iloc[-1]
    feature_ts = pd.to_datetime(latest_row[time_col])
    if feature_ts.tzinfo is None:
        feature_ts = feature_ts.replace(tzinfo=timezone.utc)

    now_ts = datetime.now(timezone.utc)
    target_ts = feature_ts + timedelta(minutes=HORIZON_MINUTES)
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
        "prediction_timestamp": now_ts,
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
