# ML Realtime — Dự đoán tức thời (+5 Phút)

Thư mục này phụ trách **toàn bộ tác vụ Machine Learning thời gian thực** (Real-time & Short-term Directional Inference) cho cặp BTC-USDT spot.

---

## 1. Mục tiêu bài toán
- **Bài toán:** Phân loại nhị phân hướng biến động giá đóng cửa sau 5 phút:
  $$\text{Direction}_{t+5m} = \begin{cases} 1 \ (\text{UP}) & \text{nếu } \text{Close}_{t+5m} > \text{Close}_t \\ 0 \ (\text{DOWN}) & \text{nếu } \text{Close}_{t+5m} \le \text{Close}_t \end{cases}$$
- **Dữ liệu đầu vào:** Nến 1 phút canonical đã chốt phiên (`is_closed == True`) và dòng vi cấu trúc giao dịch khớp lệnh.
- **Mô hình chính:** XGBoost Binary Classifier v2.0.0 (15 common features).
- **Hiệu năng thực tế:** Accuracy ~51.32% – 51.42% (phù hợp với Giả thuyết Thị trường Hiệu quả EMH ở khung 5 phút).

---

## 2. Cấu trúc các module trong thư mục

| Tên file | Vai trò & Chức năng |
|---|---|
| [`predict.py`](predict.py) | Module suy luận thời gian thực, nạp model, trích xuất đặc trưng và lưu kết quả vào Data Warehouse (`fact_btc_predictions`). |
| [`prediction_worker.py`](prediction_worker.py) | Tiến trình nền (Daemon Worker) tự động thức dậy mỗi 15s/1m để suy luận khi có nến đóng và đối chiếu nhãn thực tế sau 5 phút. |
| [`train.py`](train.py) | Script huấn luyện mô hình XGBoost v2.0.0 từ tập nến lịch sử. |
| [`evaluate.py`](evaluate.py) | Đánh giá hiệu năng mô hình (Accuracy, Precision, Recall, F1, ROC-AUC, Log Loss). |
| [`dataset.py`](dataset.py) | Xây dựng dataset nến 1m, tính nhãn target +5m, chia tập theo thời gian (Time-series Split). |
| [`model_registry.py`](model_registry.py) | Quản lý registry phiên bản mô hình, kiểm tra tính toàn vẹn và metadata. |
| [`short_term_features.py`](short_term_features.py) | Định nghĩa 15 đặc trưng kỹ thuật & vi cấu trúc (returns, volatility, imbalance, vwap_spread,...). |
| [`short_term_metrics.py`](short_term_metrics.py) | Thống kê chuyên sâu: Balanced Accuracy, Paired-day Bootstrap CI, Confusion matrix. |
| [`short_term_experiment.py`](short_term_experiment.py) | Pipeline thí nghiệm khoa học có khóa phiên (4-fold chronological validation, holdout test). |
| [`locked_dataset.py`](locked_dataset.py) | Trình nạp dữ liệu nến đã khóa phục vụ nghiên cứu độc lập. |

---

## 3. Cách chạy kiểm tra & suy luận

```powershell
# Chạy một lần suy luận thủ công:
python scripts/run_prediction.py --once

# Chạy worker ngầm tự động liên tục:
python scripts/run_prediction.py

# Huấn luyện lại mô hình (khi cần nghiên cứu):
python scripts/train_model.py
```

