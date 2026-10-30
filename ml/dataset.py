"""
=============================================================================
Crypto Big Data - Machine Learning: Dataset Preparation & Chronological Splits
=============================================================================
Reads historical BTC-USDT 1m Gold candles, performs feature engineering,
generates the 5-minute future price direction target, and splits into:
  - 70% Training set
  - 15% Validation set
  - 15% Test set
With a strict embargo buffer between partitions to eliminate boundary leakage.
=============================================================================
"""

import sys
from pathlib import Path
from typing import Tuple, Dict, Any, List, Optional
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.config import get_ml_config
from processing.features.feature_engineering import prepare_training_dataset, FEATURE_COLUMNS
from storage.s3 import get_gold_candles_path
from warehouse.etl.db_config import get_db_connection

ml_cfg = get_ml_config()
dataset_cfg = ml_cfg.get("dataset", {})
TRAIN_RATIO = float(dataset_cfg.get("train_split", 0.70))
VAL_RATIO = float(dataset_cfg.get("validation_split", 0.15))
TEST_RATIO = float(dataset_cfg.get("test_split", 0.15))
EMBARGO_BUFFER = int(dataset_cfg.get("embargo_buffer_minutes", 5))
HORIZON_MINUTES = int(ml_cfg.get("prediction", {}).get("horizon_minutes", 5))


def load_candles_from_postgres(limit: Optional[int] = None) -> pd.DataFrame:
    """Loads historical 1m BTC candles from PostgreSQL Data Warehouse."""
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                window_start, window_end, open, high, low, close, volume, 
                buy_volume, sell_volume, vwap, trade_imbalance, total_trade_value, source
            FROM warehouse.fact_crypto_1m_candles
            WHERE symbol = 'BTC-USDT'
              AND source = 'historical_rest'
              AND is_closed = TRUE
            ORDER BY window_start ASC
        """
        if limit:
            query += f" LIMIT {int(limit)}"
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


def load_candles_from_lake() -> pd.DataFrame:
    """Attempts to load 1m BTC candles from Gold Lakehouse (Parquet)."""
    # Check local canonical gold lake first
    local_gold = project_root / "data" / "lake" / "gold" / "candles_1m" / "symbol=BTC-USDT"
    if local_gold.exists():
        parquets = list(local_gold.glob("*.parquet"))
        if parquets:
            df = pd.read_parquet(local_gold)
            if not df.empty:
                if "source" in df.columns:
                    df = df[df["source"] == "historical_rest"]
                if "is_closed" in df.columns:
                    df = df[df["is_closed"] == True]
                return df

    # Check S3 gold path if local not populated
    gold_path = get_gold_candles_path("BTC-USDT")
    if not gold_path.startswith("s3a://"):
        local_p = Path(gold_path)
        if local_p.exists():
            parquets = list(local_p.rglob("*.parquet"))
            if parquets:
                return pd.read_parquet(local_p)
    return pd.DataFrame()


def fetch_historical_btc_candles(min_candles: int = 5000) -> pd.DataFrame:
    """
    Fetches real historical 1m BTC candles with cascading retrieval:
      1. Gold Lakehouse Parquet (fastest columnar store)
      2. PostgreSQL Data Warehouse
      3. OKX Historical Backfill Module (downloads full verifiable history)
    Never synthesizes fake buy/sell volume or fake VWAP.
    """
    # 1. Try Gold Lakehouse
    try:
        df = load_candles_from_lake()
        if len(df) >= min_candles:
            print(f"[DATASET] Loaded {len(df):,} valid 1m BTC candles from Gold Lakehouse.")
            return df
    except Exception as e:
        print(f"[DATASET] Note: Gold Lakehouse read ({e}).")

    # 2. Try PostgreSQL DW
    try:
        df = load_candles_from_postgres()
        if len(df) >= min_candles:
            print(f"[DATASET] Loaded {len(df):,} valid 1m BTC candles from PostgreSQL DW.")
            return df
    except Exception as e:
        print(f"[DATASET] Note: PostgreSQL DW read ({e}).")

    # 3. Trigger OKX Historical Backfill
    print(f"[DATASET] Insufficient historical data found (< {min_candles:,}). Triggering OKX Historical Backfill...")
    from ingestion.historical.okx_historical_backfill import OKXHistoricalBackfill
    backfill = OKXHistoricalBackfill(symbol="BTC-USDT", days=90)
    backfill.run_backfill()

    # Re-read from Gold Lakehouse
    df = load_candles_from_lake()
    if not df.empty:
        return df

    # Fallback to Postgres
    df = load_candles_from_postgres()
    if not df.empty:
        return df

    raise RuntimeError("Failed to load historical BTC-USDT candles from Lakehouse, Warehouse, and Backfill.")


def create_chronological_splits(
    candles_df: Optional[pd.DataFrame] = None
) -> Dict[str, Any]:
    """
    Prepares dataset, constructs features and labels, and splits chronologically into
    70% Train, 15% Validation, 15% Test with an embargo buffer.
    """
    if candles_df is None or len(candles_df) == 0:
        candles_df = fetch_historical_btc_candles()

    feat_mode = ml_cfg.get("features", {}).get("feature_set_mode", "common")
    dataset_df, feature_names = prepare_training_dataset(
        candles_df,
        horizon_minutes=HORIZON_MINUTES,
        feature_mode=feat_mode
    )

    n = len(dataset_df)
    if n < 60:
        raise ValueError(f"Insufficient samples for training ({n} rows, need >= 60).")

    # Chronological partition points
    train_end = int(n * TRAIN_RATIO)
    val_start = train_end + EMBARGO_BUFFER
    val_end = val_start + int(n * VAL_RATIO)
    test_start = val_end + EMBARGO_BUFFER

    train_df = dataset_df.iloc[:train_end].copy()
    val_df = dataset_df.iloc[val_start:val_end].copy()
    test_df = dataset_df.iloc[test_start:].copy()

    X_train = train_df[feature_names].values
    y_train = train_df["target_direction"].values.astype(int)

    X_val = val_df[feature_names].values
    y_val = val_df["target_direction"].values.astype(int)

    X_test = test_df[feature_names].values
    y_test = test_df["target_direction"].values.astype(int)

    time_col = "window_start" if "window_start" in dataset_df.columns else "timestamp"
    
    metadata = {
        "total_samples": n,
        "feature_names": feature_names,
        "n_features": len(feature_names),
        "train_samples": len(train_df),
        "val_samples": len(val_df),
        "test_samples": len(test_df),
        "train_period": (str(train_df[time_col].iloc[0]), str(train_df[time_col].iloc[-1])),
        "val_period": (str(val_df[time_col].iloc[0]), str(val_df[time_col].iloc[-1])) if len(val_df) > 0 else ("", ""),
        "test_period": (str(test_df[time_col].iloc[0]), str(test_df[time_col].iloc[-1])) if len(test_df) > 0 else ("", "")
    }

    return {
        "X_train": X_train,
        "y_train": y_train,
        "X_val": X_val,
        "y_val": y_val,
        "X_test": X_test,
        "y_test": y_test,
        "feature_names": feature_names,
        "metadata": metadata,
        "train_df": train_df,
        "val_df": val_df,
        "test_df": test_df
    }


if __name__ == "__main__":
    data = create_chronological_splits()
    print("Dataset Split Summary:")
    print(f"  Train : {data['metadata']['train_samples']} samples ({data['metadata']['train_period']})")
    print(f"  Val   : {data['metadata']['val_samples']} samples ({data['metadata']['val_period']})")
    print(f"  Test  : {data['metadata']['test_samples']} samples ({data['metadata']['test_period']})")
