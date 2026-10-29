"""
=============================================================================
Crypto Big Data - Machine Learning: S3 Model Registry & Integrity Manager
=============================================================================
Provides safe, versioned synchronization of XGBoost model artifacts with AWS S3:
  1. Computes cryptographic SHA-256 hashes of all model artifacts.
  2. Uploads artifacts to versioned S3 path:
     s3://<bucket>/crypto/models/v<version>/
  3. Downloads and verifies artifact integrity via SHA-256 before staging.
  4. Never automatically overwrites or activates models without explicit instruction.
=============================================================================
"""

import os
import sys
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
import boto3

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from processing.core.spark_session import load_env_vars
from storage.s3 import get_s3_bucket

load_env_vars()

ARTIFACT_FILENAMES = [
    "btc_xgboost_model.json",
    "feature_schema.json",
    "model_metadata.json",
    "evaluation_results.json"
]


def calculate_sha256(filepath: Path) -> str:
    """Computes SHA-256 checksum of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def get_s3_client():
    """Initializes AWS S3 client using environment credentials."""
    ak = os.getenv("AWS_ACCESS_KEY_ID")
    sk = os.getenv("AWS_SECRET_ACCESS_KEY")
    reg = os.getenv("AWS_REGION", "ap-southeast-1")
    return boto3.client("s3", region_name=reg, aws_access_key_id=ak, aws_secret_access_key=sk)


def upload_model_artifacts(
    version: str = "2.0.0",
    artifacts_dir: Optional[Path] = None,
    s3_prefix: str = "crypto/models"
) -> Dict[str, Any]:
    """
    Safely uploads local model artifacts with SHA-256 checksums to versioned S3 directory.
    Does NOT overwrite existing active models without explicit pathing.
    """
    if artifacts_dir is None:
        artifacts_dir = project_root / "ml" / "artifacts"

    bucket = get_s3_bucket()
    s3 = get_s3_client()

    print("=" * 80)
    print(f"S3 MODEL REGISTRY - UPLOAD VERSION v{version}")
    print(f"Artifacts Dir : {artifacts_dir}")
    print(f"Destination   : s3://{bucket}/{s3_prefix}/v{version}/")
    print("=" * 80)

    checksums = {}
    uploaded_files = []

    for fname in ARTIFACT_FILENAMES:
        fpath = artifacts_dir / fname
        if not fpath.exists():
            print(f"[!] Warning: Artifact {fname} does not exist. Skipping.")
            continue

        sha256 = calculate_sha256(fpath)
        checksums[fname] = sha256
        s3_key = f"{s3_prefix}/v{version}/{fname}"

        print(f"[*] Uploading {fname:<28} | SHA-256: {sha256[:12]}... -> s3://{bucket}/{s3_key}")
        s3.upload_file(str(fpath), bucket, s3_key)
        uploaded_files.append(s3_key)

    # Save and upload checksum manifest
    manifest_file = artifacts_dir / f"checksums_v{version}.json"
    manifest_data = {
        "version": version,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "checksums": checksums
    }
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

    manifest_s3_key = f"{s3_prefix}/v{version}/checksums.json"
    s3.upload_file(str(manifest_file), bucket, manifest_s3_key)
    uploaded_files.append(manifest_s3_key)

    print(f"[SUCCESS] Successfully registered {len(uploaded_files)} files to S3 version v{version} with checksums.")
    return {
        "version": version,
        "bucket": bucket,
        "s3_prefix": f"{s3_prefix}/v{version}",
        "uploaded_files": uploaded_files,
        "checksums": checksums
    }


def download_model_artifacts(
    version: str = "2.0.0",
    dest_dir: Optional[Path] = None,
    s3_prefix: str = "crypto/models",
    activate: bool = False
) -> Dict[str, Any]:
    """
    Downloads model artifacts from S3, validates SHA-256 checksums, and stages them.
    Safety rule: only overwrites active local artifacts if activate=True.
    """
    bucket = get_s3_bucket()
    s3 = get_s3_client()

    if dest_dir is None:
        if activate:
            dest_dir = project_root / "ml" / "artifacts"
        else:
            dest_dir = project_root / "ml" / "artifacts" / "staged" / f"v{version}"

    dest_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print(f"S3 MODEL REGISTRY - DOWNLOAD & VERIFY VERSION v{version}")
    print(f"Source        : s3://{bucket}/{s3_prefix}/v{version}/")
    print(f"Destination   : {dest_dir}")
    print(f"Activate Model: {activate} (Protected from accidental overwrite)")
    print("=" * 80)

    # 1. Download checksum manifest
    manifest_s3_key = f"{s3_prefix}/v{version}/checksums.json"
    local_manifest = dest_dir / "checksums.json"
    try:
        s3.download_file(bucket, manifest_s3_key, str(local_manifest))
    except Exception as e:
        raise FileNotFoundError(f"Could not download manifest from s3://{bucket}/{manifest_s3_key}: {e}")

    with open(local_manifest, "r", encoding="utf-8") as f:
        manifest_data = json.load(f)

    expected_checksums = manifest_data.get("checksums", {})

    # 2. Download and verify each artifact
    verified_files = []
    for fname, exp_hash in expected_checksums.items():
        s3_key = f"{s3_prefix}/v{version}/{fname}"
        local_target = dest_dir / fname

        print(f"[*] Downloading s3://{bucket}/{s3_key} -> {local_target.name}...")
        s3.download_file(bucket, s3_key, str(local_target))

        actual_hash = calculate_sha256(local_target)
        if actual_hash != exp_hash:
            raise ValueError(
                f"INTEGRITY FAILURE on {fname}! Expected SHA-256: {exp_hash}, Got: {actual_hash}"
            )
        print(f" [PASS] Verified SHA-256 integrity: {actual_hash[:12]}...")
        verified_files.append(str(local_target))

    print(f"[SUCCESS] Downloaded and verified all {len(verified_files)} artifacts with 100% cryptographic integrity.")
    return {
        "version": version,
        "verified_files": verified_files,
        "activated": activate,
        "dest_dir": str(dest_dir)
    }


def list_s3_model_versions(s3_prefix: str = "crypto/models") -> List[str]:
    """Lists all available model versions stored on S3."""
    bucket = get_s3_bucket()
    s3 = get_s3_client()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{s3_prefix}/", Delimiter="/")
    prefixes = resp.get("CommonPrefixes", [])
    versions = []
    for p in prefixes:
        v_name = p.get("Prefix", "").rstrip("/").split("/")[-1]
        if v_name.startswith("v"):
            versions.append(v_name[1:])
    return sorted(versions)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="S3 Model Registry Manager")
    parser.add_argument("--action", choices=["upload", "download", "list"], default="list")
    parser.add_argument("--version", type=str, default="2.0.0")
    parser.add_argument("--activate", action="store_true", help="Explicitly activate model into ml/artifacts")
    args = parser.parse_args()

    if args.action == "upload":
        upload_model_artifacts(version=args.version)
    elif args.action == "download":
        download_model_artifacts(version=args.version, activate=args.activate)
    elif args.action == "list":
        vers = list_s3_model_versions()
        print("Available S3 Model Versions:", vers)
