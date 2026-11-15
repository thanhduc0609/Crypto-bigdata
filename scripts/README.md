# Danh Mục Scripts Dự Án Crypto-BigData

Thư mục `scripts/` được chuẩn hóa thành 4 phân hệ chuyên trách, giúp dễ dàng định vị, bảo trì và mở rộng:

---

## 1. `scripts/demo/` — Khởi chạy & Vận hành Demo Cục bộ
Chịu trách nhiệm khởi động, dừng, giám sát tiến trình và kiểm tra tính chịu lỗi trên một máy:
- **`setup_env.ps1 -StartDemo` / `-StopDemo`**: Bộ điều phối trung tâm khởi chạy và dừng an toàn trọn gói Web dashboard, OKX producer, Spark Streaming, Batch Medallion và Monitor.
  - `.\scripts\setup_env.ps1 -StartDemo -Streaming -Batch`
  - `.\scripts\setup_env.ps1 -StopDemo`
- **`docker_environment.py`**: Quản lý vòng đời Docker containers (PostgreSQL 5433, Kafka 9092).
  - `python scripts/demo/docker_environment.py up` / `stop` / `status`
- **`local_service.py`**: Supervisor tự động khởi động lại (auto-restart with backoff) cho `web`, `producer`, `spark`.
- **`monitor_demo.py`**: Giám sát offsets Kafka, checkpoint Spark, và nạp DW mỗi 30s.
  - `python scripts/demo/monitor_demo.py --watch`
- **`check_demo_recovery.py`**: Kiểm thử kịch bản tự phục hồi sau sự cố dừng tiến trình Spark / Producer.
- **`demo_load_check.py`**: Đo độ trễ và kiểm tra tải các API endpoints.
- **`audit_demo_pipeline.py`**: Kiểm toán dữ liệu đầu cuối từ Kafka -> S3 -> DW -> REST API.
- **`export_demo_handover.py`**: Tạo gói bàn giao báo cáo demo.

---

## 2. `scripts/pipeline/` — Luồng Dữ liệu & Tầng Medallion (Data Pipeline)
Chịu trách nhiệm cho toàn bộ chu trình xử lý dữ liệu:
- **`run_ingestion.py`**: Kết nối WebSocket OKX và gửi trades BTC-USDT vào Kafka.
- **`run_streaming.py`**: Khởi chạy Spark Structured Streaming xử lý Bronze/Speed layer.
- **`run_backfill.py`**: Tải dữ liệu lịch sử nến 1m từ OKX REST API và nạp vào Data Warehouse.
- **`run_batch_medallion.py`**: Hợp nhất xử lý Silver & Gold Medallion và đồng bộ sang DW.
  - Chạy 1 lần: `python scripts/pipeline/run_batch_medallion.py`
  - Chạy chu kỳ: `python scripts/pipeline/run_batch_medallion.py --watch --interval 300`
- **`build_features.py`**: Tính toán đặc trưng và chuẩn bị tập dữ liệu train/val/test.
- **`organize_s3.py`**: Chuẩn hóa cấu trúc thư mục và prefix trên AWS S3.

---

## 3. `scripts/ml/` — Máy học & Dự báo (Machine Learning)
Chịu trách nhiệm huấn luyện, suy luận và các thí nghiệm dự báo:
- **`train_model.py`**: Huấn luyện mô hình XGBoost phân loại xu hướng giá 5 phút tiếp theo.
- **`run_prediction.py`**: Chạy dịch vụ suy luận thời gian thực cho nến 1m.
  - Chạy 1 lần: `python scripts/ml/run_prediction.py --once`
  - Vòng lặp: `python scripts/ml/run_prediction.py --interval 15`
- **`run_short_term_experiment.py`**: Thực hiện quy trình chọn mô hình và đánh giá holdout.
- **`verify_short_term_experiment.py`**: Xác thực bằng chứng, mã băm niêm phong và chỉ số mô hình.
- **`export_short_term_experiment.py`**: Xuất tài liệu Markdown và gói kiểm chứng thí nghiệm.
- **`run_long_term.py`**: Chạy dự báo giá đóng cửa hàng tháng cho 3 năm tiếp theo (36 tháng).
- **`run_long_term_research.py`**: Thực hiện nghiên cứu lồng rolling holdout cho chuỗi thời gian 3 năm.

---

## 4. `scripts/quality/` — Kiểm toán Chất lượng & Kiểm thử CI (Quality Assurance)
Chịu trách nhiệm đối soát dữ liệu, sửa lỗi và kiểm định toàn hệ thống:
- **`check_project.py`**: Bộ kiểm định CI offline toàn diện (284 unit tests, node test, syntax check, ML verify).
  - `python scripts/quality/check_project.py`
- **`research_artifacts.py`**: Khôi phục và đối chiếu 41 file artifact nghiên cứu đã niêm phong.
  - `python scripts/quality/research_artifacts.py restore`
  - `python scripts/quality/research_artifacts.py verify`
- **`run_data_quality_audit.py`**: Kiểm toán chất lượng dữ liệu kho (S3, Parquet, DW, Kafka).
- **`render_data_quality_report.py`**: Kết xuất báo cáo kiểm toán ra định dạng Markdown và CSV.
- **`run_canonical_repair.py`**: Xây dựng phiên bản phục hồi dữ liệu chuẩn (Canonical Gold).
- **`run_legacy_regressions.py`**: Chạy kiểm thử hồi quy với các fixture cũ.

---

## 5. Entrypoints Trung tâm tại Thư mục gốc `scripts/`
- **`setup_env.ps1`**: CLI quản trị trung tâm của đồ án (PowerShell). Tự động điều phối trọn gói Docker, Pipeline, ML và Dashboard.
- **`setup_env.sh`**: Kịch bản tương thích cho Linux/macOS.
- **`__init__.py`**: Cung cấp package re-export cho các module chuyên biệt (`scripts.demo`, `scripts.pipeline`, `scripts.ml`, `scripts.quality`).

