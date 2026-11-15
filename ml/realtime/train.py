"""
=============================================================================
Crypto Big Data - Machine Learning: XGBoost Training Pipeline
=============================================================================
Trains a single XGBoost Classifier on chronological BTC-USDT 1m feature data:
  - Strict 70% Train / 15% Validation / 15% Test chronological splits
  - Early stopping and validation against overfitting
  - Unseen test split evaluation & comparison with majority-class baseline
  - Persists artifacts: btc_xgboost_model.json, feature_schema.json,
    model_metadata.json, evaluation_results.json
=============================================================================
"""

import os
import sys
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from xgboost import XGBClassifier
from processing.core.config import get_ml_config
from ml.realtime.dataset import create_chronological_splits
from ml.realtime.evaluate import evaluate_model

ml_cfg = get_ml_config()
hyperparams = ml_cfg.get("hyperparameters", {})
artifacts_cfg = ml_cfg.get("artifacts", {})

ARTIFACTS_DIR = project_root / artifacts_cfg.get("directory", "ml/artifacts")
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)


def train_btc_xgboost(candles_df=None) -> Dict[str, Any]:
    """
    Executes the complete chronological training and evaluation pipeline for BTC price direction.
    """
    print("=" * 80)
    print("XGBOOST MODEL TRAINING - BTC-USDT PRICE DIRECTION (NEXT 5 MINUTES)")
    print("=" * 80)

    # 1. Prepare chronological dataset
    print("[1/4] Preparing chronological datasets with 5-minute embargo buffers...")
    data = create_chronological_splits(candles_df)
    X_train, y_train = data["X_train"], data["y_train"]
    X_val, y_val = data["X_val"], data["y_val"]
    X_test, y_test = data["X_test"], data["y_test"]
    feature_names = data["feature_names"]
    metadata = data["metadata"]

    print(f"  - Features Count : {len(feature_names)}")
    print(f"  - Train Set Size : {len(X_train)} samples")
    print(f"  - Val Set Size   : {len(X_val)} samples")
    print(f"  - Test Set Size  : {len(X_test)} samples")

    # 2. Configure and Fit XGBoost
    print("\n[2/4] Training XGBClassifier with early stopping...")
    model = XGBClassifier(
        n_estimators=int(hyperparams.get("n_estimators", 200)),
        max_depth=int(hyperparams.get("max_depth", 4)),
        learning_rate=float(hyperparams.get("learning_rate", 0.05)),
        subsample=float(hyperparams.get("subsample", 0.8)),
        colsample_bytree=float(hyperparams.get("colsample_bytree", 0.8)),
        objective=str(hyperparams.get("objective", "binary:logistic")),
        eval_metric=str(hyperparams.get("eval_metric", "logloss")),
        random_state=int(hyperparams.get("random_state", 42)),
        early_stopping_rounds=int(hyperparams.get("early_stopping_rounds", 20))
    )

    eval_set = [(X_train, y_train)]
    if len(X_val) > 0 and len(y_val) > 0 and len(set(y_val)) > 1:
        eval_set.append((X_val, y_val))

    model.fit(
        X_train,
        y_train,
        eval_set=eval_set,
        verbose=False
    )
    print("  [OK] Model training converged.")

    # 3. Evaluate on unseen chronological test data
    print("\n[3/4] Evaluating model on unseen test set...")
    eval_results = evaluate_model(
        model=model,
        X_test=X_test,
        y_test=y_test,
        feature_names=feature_names,
        y_train_for_baseline=y_train
    )

    m = eval_results["metrics"]
    base = eval_results["baseline_comparison"]
    print(f"  - Accuracy          : {m['accuracy'] * 100:.2f}% (Baseline: {base['baseline']['accuracy'] * 100:.2f}%)")
    print(f"  - Precision         : {m['precision'] * 100:.2f}%")
    print(f"  - Recall            : {m['recall'] * 100:.2f}%")
    print(f"  - F1-Score          : {m['f1_score'] * 100:.2f}%")
    print(f"  - Balanced Accuracy : {m['balanced_accuracy'] * 100:.2f}%")
    print(f"  - ROC-AUC           : {m['roc_auc'] if m['roc_auc'] is not None else 'N/A'}")
    print(f"  - Baseline Lift     : {base['accuracy_lift_pct']:+.2f}% ({'OUTPERFORMS' if base['outperforms_baseline'] else 'UNDERPERFORMS'})")

    # 4. Save artifacts
    print("\n[4/4] Persisting model artifacts & preserving previous model...")
    model_filename = artifacts_cfg.get("model_filename", "btc_xgboost_model.json")
    model_path = ARTIFACTS_DIR / model_filename

    # Preserve previous model artifacts for comparison if they exist
    backup_dir = ARTIFACTS_DIR / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    if model_path.exists():
        import shutil
        prev_model_backup = backup_dir / "btc_xgboost_model_v1.0.0_prev.json"
        if not prev_model_backup.exists():
            shutil.copy2(model_path, prev_model_backup)
            print(f"  [ARCHIVE] Preserved previous model at: {prev_model_backup}")
        eval_path_prev = ARTIFACTS_DIR / "evaluation_results.json"
        if eval_path_prev.exists():
            shutil.copy2(eval_path_prev, backup_dir / "evaluation_results_v1.0.0_prev.json")
        meta_path_prev = ARTIFACTS_DIR / "model_metadata.json"
        if meta_path_prev.exists():
            shutil.copy2(meta_path_prev, backup_dir / "model_metadata_v1.0.0_prev.json")

    model.save_model(str(model_path))

    schema_filename = artifacts_cfg.get("feature_schema_filename", "feature_schema.json")
    schema_path = ARTIFACTS_DIR / schema_filename
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump({
            "feature_names": feature_names,
            "feature_count": len(feature_names),
            "target": "target_direction",
            "prediction_horizon_minutes": ml_cfg.get("prediction", {}).get("horizon_minutes", 5)
        }, f, indent=2)

    eval_filename = artifacts_cfg.get("evaluation_results_filename", "evaluation_results.json")
    eval_path = ARTIFACTS_DIR / eval_filename
    with open(eval_path, "w", encoding="utf-8") as f:
        json.dump(eval_results, f, indent=2)

    metadata_filename = artifacts_cfg.get("model_metadata_filename", "model_metadata.json")
    metadata_path = ARTIFACTS_DIR / metadata_filename
    full_metadata = {
        "model_name": ml_cfg.get("ml", {}).get("model_name", "btc_xgboost_direction"),
        "model_version": "2.0.0",
        "model_type": "XGBClassifier",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "target_symbol": "BTC-USDT",
        "prediction_horizon_minutes": ml_cfg.get("prediction", {}).get("horizon_minutes", 5),
        "hyperparameters": hyperparams,
        "dataset_metadata": metadata,
        "test_metrics": eval_results["metrics"],
        "baseline_comparison": eval_results["baseline_comparison"]
    }
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(full_metadata, f, indent=2)

    print(f"  [OK] Saved model to            : {model_path}")
    print(f"  [OK] Saved feature schema to   : {schema_path}")
    print(f"  [OK] Saved evaluation to       : {eval_path}")
    print(f"  [OK] Saved model metadata to   : {metadata_path}")

    # Persist evaluation to PostgreSQL Data Warehouse
    try:
        from warehouse.etl.db_config import get_db_connection
        conn = get_db_connection()
        cur = conn.cursor()
        sql = """
            INSERT INTO warehouse.fact_btc_model_evaluation (
                model_name, model_version, target_symbol, test_samples,
                accuracy, precision, recall, f1_score, balanced_accuracy,
                roc_auc, baseline_accuracy, accuracy_lift_pct,
                confusion_matrix, feature_importance
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cur.execute(sql, (
            full_metadata["model_name"],
            full_metadata["model_version"],
            full_metadata["target_symbol"],
            int(eval_results["sample_counts"]["test_samples"]),
            float(m["accuracy"]),
            float(m["precision"]),
            float(m["recall"]),
            float(m["f1_score"]),
            float(m["balanced_accuracy"]),
            float(m["roc_auc"]) if m["roc_auc"] is not None else None,
            float(base["baseline"]["accuracy"]),
            float(base["accuracy_lift_pct"]),
            json.dumps(eval_results["confusion_matrix"]),
            json.dumps(eval_results["feature_importance"])
        ))
        conn.commit()
        cur.close()
        conn.close()
        print("  [OK] Saved evaluation record to PostgreSQL warehouse.fact_btc_model_evaluation")
    except Exception as db_err:
        print(f"  [!] Note: PostgreSQL evaluation record skipped ({db_err})")

    print("=" * 80)

    return {
        "model": model,
        "eval_results": eval_results,
        "metadata": full_metadata
    }


if __name__ == "__main__":
    train_btc_xgboost()
