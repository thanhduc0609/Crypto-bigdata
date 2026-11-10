"""
=============================================================================
Crypto Big Data - Feature Engineering & Direction Target Formulation
=============================================================================
Computes price-based, volume-based, and technical indicator features from
Gold 1-minute OHLCV candles without lookahead bias.
Generates 5-minute future price direction label (UP=1, NOT_UP=0) strictly
requiring consecutive minute timestamps.
=============================================================================
"""

import sys
from pathlib import Path
from typing import List, Tuple, Optional
import numpy as np
import pandas as pd

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.config import get_ml_config

ml_cfg = get_ml_config()
DEFAULT_HORIZON = ml_cfg.get("prediction", {}).get("horizon_minutes", 5)

# Canonical Common Feature Set: 15 features available identically from both historical OHLCV and live streaming candles
COMMON_FEATURE_COLUMNS = [
    "log_return",
    "price_momentum_5m",
    "high_low_range",
    "volume_change",
    "sma_5",
    "sma_15",
    "ema_12",
    "ema_26",
    "macd",
    "macd_signal",
    "bb_upper",
    "bb_lower",
    "bb_bandwidth",
    "rsi_14",
    "rolling_volatility_15m"
]

# Extended Microstructure Features: only available when true tick-level order flow is present
EXTENDED_FEATURE_COLUMNS = [
    "buy_sell_ratio",
    "trade_imbalance",
    "vwap_deviation"
]

# Default to common feature set for consistency across historical and live streaming inference
FEATURE_COLUMNS = COMMON_FEATURE_COLUMNS


def compute_features(df: pd.DataFrame, feature_mode: str = "common") -> pd.DataFrame:
    """
    Computes technical and price/volume features on 1m OHLCV data.
    Ensures all calculations are strictly backward-looking (no future data leakage).
    Does NOT synthesize fake order flow for historical data.
    """
    df = df.copy()

    # Ensure datetime index or sort by timestamp
    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"])
        df = df.sort_values("window_start").reset_index(drop=True)
    elif "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp").reset_index(drop=True)

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    volume = df["volume"].astype(float)

    # 1. Price-based features
    # Log Return (1m)
    df["log_return"] = np.log(close / close.shift(1).replace(0, np.nan)).fillna(0.0)

    # Price Momentum over 5 minutes (backward-looking)
    df["price_momentum_5m"] = ((close - close.shift(5)) / close.shift(5).replace(0, np.nan)).fillna(0.0)

    # Normalized High-Low Range
    df["high_low_range"] = ((high - low) / close.replace(0, np.nan)).fillna(0.0)

    # 2. Volume-based features
    # Volume Change rate
    prev_vol = volume.shift(1)
    df["volume_change"] = ((volume - prev_vol) / (prev_vol + 1e-8)).fillna(0.0)

    # 3. Technical Indicators
    # Moving Averages: SMA 5 & SMA 15
    df["sma_5"] = close.rolling(window=5, min_periods=1).mean()
    df["sma_15"] = close.rolling(window=15, min_periods=1).mean()

    # Exponential Moving Averages: EMA 12 & EMA 26
    df["ema_12"] = close.ewm(span=12, adjust=False).mean()
    df["ema_26"] = close.ewm(span=26, adjust=False).mean()

    # MACD & Signal line (9-period EMA of MACD)
    df["macd"] = df["ema_12"] - df["ema_26"]
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()

    # Bollinger Bands (20-period lookback, 2 standard deviations)
    bb_mid = close.rolling(window=20, min_periods=1).mean()
    bb_std = close.rolling(window=20, min_periods=1).std().fillna(0.0)
    df["bb_upper"] = bb_mid + (2.0 * bb_std)
    df["bb_lower"] = bb_mid - (2.0 * bb_std)
    df["bb_bandwidth"] = ((df["bb_upper"] - df["bb_lower"]) / bb_mid.replace(0, np.nan)).fillna(0.0)

    # Relative Strength Index (RSI 14)
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(com=13, adjust=False).mean()
    avg_loss = loss.ewm(com=13, adjust=False).mean()
    rs = avg_gain / (avg_loss + 1e-8)
    df["rsi_14"] = (100.0 - (100.0 / (1.0 + rs))).fillna(50.0).clip(0.0, 100.0)

    # Rolling Volatility (15-period rolling std of log returns)
    df["rolling_volatility_15m"] = (df["log_return"].rolling(window=15, min_periods=1).std() * np.sqrt(15)).fillna(0.0)

    # 4. Optional Extended Microstructure Features (only if requested and real trade-level data exists)
    if feature_mode == "extended":
        has_real_orderflow = (
            "buy_volume" in df.columns and 
            "sell_volume" in df.columns and 
            df["buy_volume"].notna().any() and
            (df["buy_volume"] != df["sell_volume"]).any()
        )
        if has_real_orderflow:
            buy_vol = df["buy_volume"].astype(float)
            sell_vol = df["sell_volume"].astype(float)
            df["buy_sell_ratio"] = (buy_vol / (sell_vol + 1e-8)).clip(0.0, 50.0).fillna(1.0)
            if "trade_imbalance" in df.columns and df["trade_imbalance"].notna().any():
                df["trade_imbalance"] = df["trade_imbalance"].fillna(0.0).clip(-1.0, 1.0)
            else:
                df["trade_imbalance"] = ((buy_vol - sell_vol) / (buy_vol + sell_vol + 1e-8)).clip(-1.0, 1.0)
        else:
            # Do NOT fake values - mark as NaN if requested in extended mode without real orderflow
            df["buy_sell_ratio"] = np.nan
            df["trade_imbalance"] = np.nan

        if "vwap" in df.columns and df["vwap"].notna().any():
            vwap = df["vwap"].astype(float)
            df["vwap_deviation"] = ((close - vwap) / vwap.replace(0, np.nan)).fillna(0.0)
        else:
            df["vwap_deviation"] = np.nan

    return df


def add_future_direction_target(
    df: pd.DataFrame,
    horizon_minutes: int = DEFAULT_HORIZON
) -> pd.DataFrame:
    """
    Constructs the binary target variable for future price direction strictly matching
    the exact timestamp t + horizon_minutes (NOT defaulting to 5 rows):
      future_return = (close[t + horizon] - close[t]) / close[t]
      target_direction = 1 if future_return > 0 else 0
    If no candle exists at exact target time (e.g., due to missing minutes / network gap),
    the target remains NaN to prevent lookahead or incorrect horizon labeling.
    """
    df = df.copy()
    time_col = "window_start" if "window_start" in df.columns else "timestamp"
    df[time_col] = pd.to_datetime(df[time_col], utc=True)
    df = df.sort_values(time_col).reset_index(drop=True)

    # Fast and strictly timestamp-matched alignment:
    # Build series indexed by exact UTC timestamp
    close_series = pd.Series(df["close"].astype(float).values, index=df[time_col])

    # Target timestamp for each row is exact t + horizon_minutes
    target_timestamps = df[time_col] + pd.Timedelta(minutes=horizon_minutes)

    # Reindex matches EXACT timestamp (NaN if candle is missing)
    future_closes = close_series.reindex(target_timestamps).values
    current_closes = df["close"].astype(float).values

    valid_mask = (~np.isnan(future_closes)) & (current_closes > 0)
    future_returns = np.full(len(df), np.nan)
    target_directions = np.full(len(df), np.nan)

    future_returns[valid_mask] = (future_closes[valid_mask] - current_closes[valid_mask]) / current_closes[valid_mask]
    target_directions[valid_mask] = np.where(future_returns[valid_mask] > 0, 1.0, 0.0)

    df["future_return"] = future_returns
    df["target_direction"] = target_directions

    return df


def prepare_training_dataset(
    df: pd.DataFrame,
    horizon_minutes: int = DEFAULT_HORIZON,
    feature_mode: str = "common"
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Full pipeline: feature engineering + target creation + warm-up removal.
    Returns (cleaned_df, feature_names).
    """
    feat_df = compute_features(df, feature_mode=feature_mode)
    labeled_df = add_future_direction_target(feat_df, horizon_minutes=horizon_minutes)

    features = COMMON_FEATURE_COLUMNS if feature_mode == "common" else (COMMON_FEATURE_COLUMNS + EXTENDED_FEATURE_COLUMNS)

    # Drop unobservable rows (warmup period and rows where future horizon or required features are unavailable)
    valid_mask = labeled_df["target_direction"].notna()
    for col_name in features:
        if col_name in labeled_df.columns:
            valid_mask = valid_mask & labeled_df[col_name].notna()

    # Skip first 20 rows to let technical indicators stabilize
    valid_dataset = labeled_df[valid_mask].iloc[20:].copy().reset_index(drop=True)

    return valid_dataset, features
