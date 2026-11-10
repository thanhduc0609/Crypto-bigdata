"""
=============================================================================
Crypto Big Data - Storage Layer (S3 / Local Lakehouse Path Abstraction)
=============================================================================
Provides consistent URI and path resolution for:
  - Bronze Layer : Raw incoming Kafka trades
  - Silver Layer : Validated, deduplicated trades
  - Gold Layer   : 1m Candlestick aggregations and feature stores
Supports both AWS S3 (s3a://) and local filesystem storage for flexibility.
=============================================================================
"""

import os
import sys
from pathlib import Path
from typing import Optional

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.config import get_pipeline_config

pipeline_cfg = get_pipeline_config()
storage_cfg = pipeline_cfg.get("storage", {})


def get_storage_mode() -> str:
    """Returns 's3' or 'local' based on config and environment."""
    dest = os.getenv("STORAGE_DESTINATION", storage_cfg.get("destination", "s3")).lower()
    return "s3" if dest in ("s3", "all") else "local"


def get_s3_bucket() -> str:
    """Resolves S3 bucket name from environment or config."""
    return os.getenv("S3_BUCKET_NAME", "crypto-bigdata-lake")


def get_bronze_trades_path(symbol: str = "BTC-USDT") -> str:
    """Returns the storage path for Bronze (raw) trade data."""
    if get_storage_mode() == "s3":
        bucket = get_s3_bucket()
        rel = storage_cfg.get("s3", {}).get("bronze_path", "crypto/bronze/trades")
        return f"s3a://{bucket}/{rel}"
    else:
        local_dir = project_root / "data" / "lake" / "bronze" / "trades"
        local_dir.mkdir(parents=True, exist_ok=True)
        return str(local_dir).replace("\\", "/")


def get_silver_trades_path(symbol: str = "BTC-USDT") -> str:
    """Returns the storage path for Silver (cleaned) trade data."""
    if get_storage_mode() == "s3":
        bucket = get_s3_bucket()
        rel = storage_cfg.get("s3", {}).get("silver_path", "crypto/silver/trades")
        return f"s3a://{bucket}/{rel}"
    else:
        local_dir = project_root / "data" / "lake" / "silver" / "trades"
        local_dir.mkdir(parents=True, exist_ok=True)
        return str(local_dir).replace("\\", "/")


def get_gold_candles_path(symbol: str = "BTC-USDT") -> str:
    """Returns the storage path for Gold (1m OHLCV Candlesticks) data."""
    from storage.canonical_snapshot import resolve_gold
    snapshot=resolve_gold(get_storage_mode(),get_s3_bucket())
    if snapshot is not None:
        if symbol!="BTC-USDT":
            raise ValueError("Activated canonical snapshot only supports BTC-USDT")
        return snapshot
    if get_storage_mode() == "s3":
        bucket = get_s3_bucket()
        rel = storage_cfg.get("s3", {}).get("gold_candles_path", "crypto/gold/candles_1m")
        return f"s3a://{bucket}/{rel}"
    else:
        local_dir = project_root / "data" / "lake" / "gold" / "candles_1m"
        local_dir.mkdir(parents=True, exist_ok=True)
        return str(local_dir).replace("\\", "/")


def get_speed_candles_path(symbol: str = "BTC-USDT") -> str:
    """Returns the storage path for auxiliary speed layer streaming candles (non-canonical)."""
    if get_storage_mode() == "s3":
        bucket = get_s3_bucket()
        rel = storage_cfg.get("s3", {}).get("speed_candles_path", "crypto/streaming/candles_1m")
        return f"s3a://{bucket}/{rel}"
    else:
        local_dir = project_root / "data" / "lake" / "speed" / "candles_1m"
        local_dir.mkdir(parents=True, exist_ok=True)
        return str(local_dir).replace("\\", "/")


def get_gold_features_path(symbol: str = "BTC-USDT") -> str:
    """Returns the storage path for Gold Feature Store datasets."""
    if get_storage_mode() == "s3":
        bucket = get_s3_bucket()
        rel = storage_cfg.get("s3", {}).get("gold_features_path", "crypto/gold/features")
        return f"s3a://{bucket}/{rel}"
    else:
        local_dir = project_root / "data" / "lake" / "gold" / "features"
        local_dir.mkdir(parents=True, exist_ok=True)
        return str(local_dir).replace("\\", "/")


def get_raw_historical_candles_path(symbol: str = "BTC-USDT") -> str:
    """Returns the storage path for raw historical REST candles."""
    if get_storage_mode() == "s3":
        bucket = get_s3_bucket()
        rel = storage_cfg.get("s3", {}).get("raw_historical_candles_path", "crypto/raw/historical_candles_1m")
        return f"s3a://{bucket}/{rel}"
    else:
        local_dir = project_root / "data" / "lake" / "raw" / "historical_candles_1m"
        local_dir.mkdir(parents=True, exist_ok=True)
        return str(local_dir).replace("\\", "/")


def get_checkpoint_path(query_name: str) -> str:
    """Resolves checkpoint location (defaults to local for zero latency on Windows)."""
    use_s3 = os.getenv("USE_S3_CHECKPOINTS", "false").lower() in ("true", "1")
    if use_s3:
        bucket = get_s3_bucket()
        return f"s3a://{bucket}/checkpoints/{query_name}"
    else:
        chk_dir = project_root / "data" / "checkpoints" / query_name
        chk_dir.mkdir(parents=True, exist_ok=True)
        return str(chk_dir).replace("\\", "/")
