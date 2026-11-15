"""
Crypto Big Data - Machine Learning: Real-time & Short-term Prediction (+5 Minutes)
==================================================================================
Chuyên trách bài toán dự đoán hướng giá tức thời (+5 phút) cho BTC-USDT:
- Feature Engineering: 15 vi cấu trúc & kỹ thuật từ nến 1m đã đóng.
- Model: XGBoost Direction Classifier v2.0.0.
- Inference: Dự đoán thời gian thực & PredictionWorker chạy ngầm mỗi phút.
- Evaluation: Đối chiếu nhãn ground-truth tự động khi nến mục tiêu đóng phiên.
"""

from ml.realtime.predict import (
    run_single_prediction,
    run_prediction_detailed,
    evaluate_unresolved_predictions,
    load_prediction_model,
    get_current_model_version,
    get_last_prediction_rejection,
    RejectionCode,
)
from ml.realtime.prediction_worker import PredictionWorker, get_prediction_worker

__all__ = [
    "run_single_prediction",
    "run_prediction_detailed",
    "evaluate_unresolved_predictions",
    "load_prediction_model",
    "get_current_model_version",
    "get_last_prediction_rejection",
    "RejectionCode",
    "PredictionWorker",
    "get_prediction_worker",
]
