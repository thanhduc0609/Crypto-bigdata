"""
=============================================================================
Crypto Big Data - Core Lightweight Environment Variable Loader
=============================================================================
Đọc và nạp các biến môi trường từ file .env một cách độc lập:
  - KHÔNG phụ thuộc vào PySpark hoặc SparkSession.
  - Hỗ trợ giải quyết các biến lồng nhau (${VAR_NAME}).
  - An toàn khi gọi nhiều lần (idempotent).
=============================================================================
"""

import os
import sys
from pathlib import Path
from typing import Optional

project_root = Path(__file__).resolve().parent.parent.parent


def load_env_vars(env_path: Optional[Path] = None):
    """
    Đọc file .env nếu chưa có trong biến môi trường và mở rộng các biến lồng nhau.
    Không phụ thuộc vào PySpark hay thư viện nặng.
    """
    if env_path is None:
        env_path = project_root / ".env"

    if not env_path.exists():
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip("'\"")
            if k not in os.environ or os.environ[k].startswith("${"):
                os.environ[k] = v

    # Mở rộng các biến có dạng ${VAR_NAME}
    bucket_name = os.getenv("S3_BUCKET_NAME", "")
    if bucket_name:
        for key in ["S3_RAW_PATH", "S3_PROCESSED_PATH", "S3_CHECKPOINT_RAW", "S3_CHECKPOINT_PROCESSED"]:
            val = os.getenv(key, "")
            if val and "${S3_BUCKET_NAME}" in val:
                os.environ[key] = val.replace("${S3_BUCKET_NAME}", bucket_name)

