"""
Crypto Big Data - Machine Learning: Long-term 3-Year Forecasting (36 Months)
============================================================================
Chuyên trách bài toán dự báo giá đóng cửa BTC-USDT từng tháng cho 3 năm tiếp theo (11/2026 - 10/2029):
- Dữ liệu: 105 tháng nến sạch từ OKX (02/2018 - 10/2026).
- Phương pháp: Multi-step recursive/direct forecast, rolling walk-forward validation.
- So sánh: Baseline (Mean/Last value), Ridge regression.
- Độ bất định: Dải tin cậy 95% (uncertainty bands) dựa trên phân phối sai số lịch sử.
"""

from ml.long_term_3y.long_term import (
    build_forecast,
    forecast_log,
    uncertainty_band,
    metrics,
    MODELS,
    LAGS,
    LABELS,
)

__all__ = [
    "build_forecast",
    "forecast_log",
    "uncertainty_band",
    "metrics",
    "MODELS",
    "LAGS",
    "LABELS",
]

