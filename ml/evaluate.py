"""
=============================================================================
Crypto Big Data - Machine Learning: Model Evaluation & Baseline Comparison
=============================================================================
Evaluates the trained XGBoost Classifier on unseen chronological test data:
  - Accuracy, Precision, Recall, F1-Score, Balanced Accuracy, ROC-AUC
  - Confusion Matrix
  - Feature Importance (Weight & Gain)
  - Comparison against Majority-Class Baseline
=============================================================================
"""

import sys
from pathlib import Path
from typing import Dict, Any, List
import numpy as np
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    balanced_accuracy_score,
    roc_auc_score,
    confusion_matrix
)

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


def calculate_majority_baseline(y_train: np.ndarray, y_test: np.ndarray) -> Dict[str, Any]:
    """
    Calculates reference baseline metrics using the majority class from training data.
    Does NOT train any competing ML model; serves as a statistical benchmark.
    """
    classes, counts = np.unique(y_train, return_counts=True)
    majority_class = int(classes[np.argmax(counts)])
    y_pred_baseline = np.full_like(y_test, fill_value=majority_class)

    baseline_acc = float(accuracy_score(y_test, y_pred_baseline))
    baseline_prec = float(precision_score(y_test, y_pred_baseline, zero_division=0))
    baseline_rec = float(recall_score(y_test, y_pred_baseline, zero_division=0))
    baseline_f1 = float(f1_score(y_test, y_pred_baseline, zero_division=0))

    return {
        "strategy": "Majority Class Predictor",
        "majority_class": majority_class,
        "class_label": "UP" if majority_class == 1 else "NOT_UP",
        "accuracy": round(baseline_acc, 4),
        "precision": round(baseline_prec, 4),
        "recall": round(baseline_rec, 4),
        "f1": round(baseline_f1, 4)
    }


def evaluate_model(
    model,
    X_test: np.ndarray,
    y_test: np.ndarray,
    feature_names: List[str],
    y_train_for_baseline: np.ndarray
) -> Dict[str, Any]:
    """
    Evaluates XGBoost model on the unseen test split and compares against majority baseline.
    """
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else None

    acc = float(accuracy_score(y_test, y_pred))
    prec = float(precision_score(y_test, y_pred, zero_division=0))
    rec = float(recall_score(y_test, y_pred, zero_division=0))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))
    bal_acc = float(balanced_accuracy_score(y_test, y_pred))

    roc_auc = None
    if y_prob is not None and len(np.unique(y_test)) > 1:
        try:
            roc_auc = round(float(roc_auc_score(y_test, y_prob)), 4)
        except Exception:
            roc_auc = None

    cm = confusion_matrix(y_test, y_pred).tolist()

    # Feature Importance
    importances = model.feature_importances_
    feat_imp = []
    for name, imp in zip(feature_names, importances):
        feat_imp.append({
            "feature": name,
            "importance": round(float(imp), 4)
        })
    feat_imp.sort(key=lambda x: x["importance"], reverse=True)

    # Baseline Comparison
    baseline = calculate_majority_baseline(y_train_for_baseline, y_test)
    outperforms_baseline = acc > baseline["accuracy"]

    results = {
        "metrics": {
            "accuracy": round(acc, 4),
            "precision": round(prec, 4),
            "recall": round(rec, 4),
            "f1_score": round(f1, 4),
            "balanced_accuracy": round(bal_acc, 4),
            "roc_auc": roc_auc
        },
        "confusion_matrix": cm,
        "feature_importance": feat_imp,
        "baseline_comparison": {
            "baseline": baseline,
            "outperforms_baseline": outperforms_baseline,
            "accuracy_lift_pct": round(((acc - baseline["accuracy"]) / max(baseline["accuracy"], 1e-4)) * 100, 2)
        },
        "sample_counts": {
            "test_samples": len(y_test),
            "actual_up": int(np.sum(y_test == 1)),
            "actual_not_up": int(np.sum(y_test == 0)),
            "predicted_up": int(np.sum(y_pred == 1)),
            "predicted_not_up": int(np.sum(y_pred == 0))
        }
    }

    return results
