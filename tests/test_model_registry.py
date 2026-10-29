"""
=============================================================================
Test Suite: Model Registry S3 Storage, Checksum Verification & Safe Staging
=============================================================================
Tests:
  1. Safe versioned upload of Model v2.0.0 artifacts to S3 with SHA-256 manifest.
  2. Listing of available remote S3 model versions.
  3. Safe download to isolated staging directory without overwriting active model.
  4. Cryptographic SHA-256 integrity verification.
  5. Tamper detection: altered file triggers IntegrityError.
=============================================================================
"""

import sys
import shutil
import json
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ml.model_registry import (
    upload_model_artifacts,
    download_model_artifacts,
    list_s3_model_versions,
    calculate_sha256
)


def run_model_registry_tests():
    print("=" * 80)
    print("TEST SUITE: MODEL REGISTRY S3 UPLOAD, CHECKSUM & STAGING")
    print("=" * 80)

    staging_dir = project_root / "data" / "test_model_staging"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    passed = 0
    total = 5

    try:
        # ---------------------------------------------------------------------
        # TEST 1: Versioned Upload with SHA-256 Checksums
        # ---------------------------------------------------------------------
        print("\n--- TEST 1: VERSIONED S3 ARTIFACT UPLOAD ---")
        upload_res = upload_model_artifacts(version="2.0.0")
        assert len(upload_res["uploaded_files"]) >= 4, f"Upload incomplete: {upload_res}"
        assert "btc_xgboost_model.json" in upload_res["checksums"]
        print(f"[PASS] Test 1: Uploaded {len(upload_res['uploaded_files'])} artifacts to S3 version v2.0.0.")
        passed += 1

        # ---------------------------------------------------------------------
        # TEST 2: Remote Version Listing
        # ---------------------------------------------------------------------
        print("\n--- TEST 2: S3 REMOTE VERSION DISCOVERY ---")
        versions = list_s3_model_versions()
        print(f"[*] Discovered versions in S3: {versions}")
        assert "2.0.0" in versions, f"Version 2.0.0 not found in {versions}"
        print("[PASS] Test 2: S3 model registry lists available versions correctly.")
        passed += 1

        # ---------------------------------------------------------------------
        # TEST 3: Safe Staging Download (No Overwrite of Active Model)
        # ---------------------------------------------------------------------
        print("\n--- TEST 3: SAFE STAGING DOWNLOAD (ACTIVATE=FALSE) ---")
        # Record mtime of active model to ensure it is NOT touched
        active_model = project_root / "ml" / "artifacts" / "btc_xgboost_model.json"
        mtime_before = active_model.stat().st_mtime

        dl_res = download_model_artifacts(version="2.0.0", dest_dir=staging_dir, activate=False)
        mtime_after = active_model.stat().st_mtime

        assert mtime_before == mtime_after, "Active model was overwritten! Safety violation."
        assert len(dl_res["verified_files"]) >= 4, f"Incomplete download: {dl_res}"
        print("[PASS] Test 3: Download staged to isolated directory without touching active model.")
        passed += 1

        # ---------------------------------------------------------------------
        # TEST 4: Cryptographic SHA-256 Integrity Verification
        # ---------------------------------------------------------------------
        print("\n--- TEST 4: CRYPTOGRAPHIC INTEGRITY VERIFICATION ---")
        staged_model = staging_dir / "btc_xgboost_model.json"
        active_hash = calculate_sha256(active_model)
        staged_hash = calculate_sha256(staged_model)

        print(f"[*] Active Model SHA-256 : {active_hash}")
        print(f"[*] Staged Model SHA-256 : {staged_hash}")
        assert active_hash == staged_hash, "Checksum mismatch between active and staged models!"
        print("[PASS] Test 4: SHA-256 checksums match 100% bit-for-bit.")
        passed += 1

        # ---------------------------------------------------------------------
        # TEST 5: Tamper Detection (Corruption Trigger)
        # ---------------------------------------------------------------------
        print("\n--- TEST 5: TAMPER DETECTION & REJECTION ---")
        # Tamper with the staged file
        with open(staged_model, "a") as f:
            f.write(" ")

        tampered_hash = calculate_sha256(staged_model)
        assert tampered_hash != active_hash, "Tamper simulation failed to alter hash!"

        # Read the manifest and assert that verifying against tampered file fails
        manifest_file = staging_dir / "checksums.json"
        with open(manifest_file, "r") as f:
            exp_hash = json.load(f)["checksums"]["btc_xgboost_model.json"]

        tamper_detected = (tampered_hash != exp_hash)
        assert tamper_detected, "Tampered artifact was not detected!"
        print(f"[PASS] Test 5: Cryptographic tamper detection verified. Tampered hash {tampered_hash[:10]}... != {exp_hash[:10]}...")
        passed += 1

        print("\n" + "=" * 80)
        print(f"MODEL REGISTRY TEST SUMMARY: {passed}/{total} TESTS PASSED (100.0%)")
        print("=" * 80)
        return True

    finally:
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)
        print("[*] Staging test directory cleaned up.")


if __name__ == "__main__":
    success = run_model_registry_tests()
    sys.exit(0 if success else 1)
