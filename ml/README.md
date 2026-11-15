# Kiến trúc Machine Learning — Crypto Big Data

Thư mục `ml/` được phân chia thành **2 thư mục chuyên biệt** theo đúng phạm vi và bản chất của 2 bài toán dự đoán:

```text
ml/
├── realtime/               <-- [THƯ MỤC 1] DỰ ĐOÁN TỨC THỜI (+5 PHÚT)
│   ├── predict.py          # Suy luận thời gian thực & lưu DW
│   ├── prediction_worker.py# Worker chạy ngầm tự động mỗi 15s/1m
│   ├── dataset.py          # Bộ nạp & chia tập nến 1m
│   ├── train.py            # Huấn luyện XGBoost Classifier v2.0.0
│   ├── evaluate.py         # Đánh giá độ chính xác mô hình
│   ├── model_registry.py   # Quản lý phiên bản model
│   ├── short_term_*.py     # Đặc trưng, metric & thí nghiệm offline
│   └── README.md           # Hướng dẫn chi tiết bài toán tức thời
│
├── long_term_3y/           <-- [THƯ MỤC 2] DỰ ĐOÁN TRONG 3 NĂM (36 THÁNG)
│   ├── long_term.py        # Pipeline dự báo 36 tháng, rolling validation & dải 95%
│   ├── long_term_research.py # Nghiên cứu hồi cứu mở rộng (Baseline vs Ridge)
│   └── README.md           # Hướng dẫn chi tiết bài toán 3 năm
│
└── artifacts/              # File nhị phân model, metadata và checksums v2.0.0
```

---

## So sánh 2 bài toán dự đoán

| Tiêu chí so sánh | [1] Thư mục `realtime/` (Tức thời) | [2] Thư mục `long_term_3y/` (3 năm) |
|---|---|---|
| **Mục tiêu** | Phân loại nhị phân hướng giá sau 5 phút (UP / DOWN) | Dự báo chuỗi thời gian giá đóng cửa từng tháng cho 36 tháng tới |
| **Chân trời dự báo** | **5 phút** tiếp theo ($t + 5\text{m}$) | **3 năm** (11/2026 – 10/2029, 36 bước tháng) |
| **Độ phân giải nến** | Nến 1 phút (`1m`) kết hợp Order Flow Imbalance | Nến 1 tháng (`1M`) tổng hợp từ 3.223 nến ngày sạch |
| **Mô hình cốt lõi** | **XGBoost Binary Classifier** (15 common features) | **Multi-step Recursive Log-price** + **Ridge** + **Baseline** |
| **Độ bất định** | Xác suất $P(\text{UP}) \in [0, 1]$ | Dải bất định 95% (Uncertainty Bounds) mở rộng theo thời gian |
| **Tần suất chạy** | Tự động liên tục mỗi phút khi nến đóng | Chạy định kỳ hoặc theo yêu cầu cập nhật báo cáo vĩ mô |
| **Trang hiển thị** | Trang **Phân tích thị trường** (`/thi-truong`) | Trang **Dự báo 3 năm** (`/` hoặc `/du-bao-3-nam`) |

