# ML Long-term 3Y — Dự đoán trong 3 năm (36 Tháng)

Thư mục này phụ trách **bài toán nghiên cứu dự báo giá đóng cửa hàng tháng cho 3 năm tiếp theo** (Monthly Forecasting 11/2026 – 10/2029) cho cặp BTC-USDT.

---

## 1. Mục tiêu bài toán
- **Bài toán:** Dự báo chuỗi thời gian giá đóng cửa theo từng tháng cho chân trời 36 tháng tới:
  $$\hat{Y}_{T+h} = f(Y_1, Y_2, \dots, Y_T), \quad h \in \{1, 2, \dots, 36\}$$
- **Tập dữ liệu:** 105 nến tháng lịch sử hoàn chỉnh từ sàn OKX (02/2018 đến 10/2026), chia thành 69 tháng train/validation và 36 tháng holdout test.
- **Phương pháp tiếp cận:**
  - Mô hình đệ quy đa bước (Multi-step recursive forecasting) trên không gian $\log(\text{Close})$.
  - Đối chiếu với các mô hình cơ sở: **Baseline Historical Mean** và **Baseline Last Value**.
  - Mô hình hồi quy **Ridge** có điều chuẩn $L_2$ trên không gian lag.
- **Dải bất định (Uncertainty Bounds):**
  - Xây dựng dải tin cậy 95% (empirical error quantiles) phản ánh tính biến động và độ phân kỳ sai số theo thời gian.

---

## 2. Cấu trúc các module trong thư mục

| Tên file | Vai trò & Chức năng |
|---|---|
| [`long_term.py`](long_term.py) | Module dự báo chính 36 tháng, tính toán sai số (MAE, RMSE, MAPE), xây dựng dải bất định 95% và xuất kết quả sang định dạng JSON/CSV. |
| [`long_term_research.py`](long_term_research.py) | Pipeline nghiên cứu hồi cứu mở rộng: kiểm định đa mốc (3 retrospective cutoffs) và so sánh baseline vs Ridge. |

---

## 3. Cách chạy kiểm tra & dự báo

```powershell
# Chạy pipeline dự báo 36 tháng và xuất kết quả ra data/long_term/:
python scripts/run_long_term.py

# Chạy kiểm nghiệm nghiên cứu bổ sung:
python scripts/run_long_term_research.py

# Chạy unit test kiểm tra tính đúng đắn:
python -m unittest tests/test_long_term_forecast.py tests/test_long_term_research.py -v
```

