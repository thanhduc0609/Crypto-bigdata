"""
=============================================================================
Crypto Big Data - Centralized Configuration Loader
=============================================================================
Loads configurations from YAML files (configs/pipeline.yaml, configs/ml.yaml)
and merges them with environment variables (.env).
=============================================================================
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any

# Ensure project root is in sys.path
project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.env import load_env_vars

load_env_vars()

CONFIGS_DIR = project_root / "configs"


def _read_yaml(file_path: Path) -> Dict[str, Any]:
    """Reads a YAML file with PyYAML, or falls back to basic parsing if not yet loaded."""
    if not file_path.exists():
        return {}
    try:
        import yaml
        with open(file_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        # Fallback rudimentary parser for key-value if pyyaml not yet imported
        result = {}
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and ":" in line:
                    k, v = line.split(":", 1)
                    result[k.strip()] = v.strip().strip("'\"")
        return result


def get_pipeline_config() -> Dict[str, Any]:
    """Retrieves pipeline configuration with environment variable overrides."""
    config = _read_yaml(CONFIGS_DIR / "pipeline.yaml")
    
    # Overrides from environment
    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_server and "kafka" in config:
        config["kafka"]["bootstrap_servers"] = kafka_server

    kafka_topic = os.getenv("KAFKA_TOPIC_TRADES", os.getenv("KAFKA_TOPIC"))
    if kafka_topic and "kafka" in config:
        config["kafka"]["topic_trades"] = kafka_topic

    s3_bucket = os.getenv("S3_BUCKET_NAME")
    if s3_bucket and "storage" in config and "s3" in config["storage"]:
        config["storage"]["s3"]["bucket"] = s3_bucket

    storage_dest = os.getenv("STORAGE_DESTINATION")
    if storage_dest and "storage" in config:
        config["storage"]["destination"] = storage_dest

    return config


def get_ml_config() -> Dict[str, Any]:
    """Retrieves machine learning configuration."""
    return _read_yaml(CONFIGS_DIR / "ml.yaml")


# Easy direct exports
PIPELINE_CONFIG = get_pipeline_config()
ML_CONFIG = get_ml_config()
TARGET_SYMBOL = PIPELINE_CONFIG.get("pipeline", {}).get("target_symbol", "BTC-USDT")
PREDICTION_HORIZON = ML_CONFIG.get("prediction", {}).get("horizon_minutes", 5)
