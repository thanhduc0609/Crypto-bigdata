"""
Crypto Big Data - Data Pipeline & Medallion Architecture
========================================================
Quản lý các quy trình ETL, streaming, backfill và batch medallion:
- run_ingestion.py: OKX WebSocket -> Kafka streaming producer
- run_streaming.py: Spark Structured Streaming processor
- run_backfill.py: Historical 1m candle backfill từ OKX REST
- run_batch_medallion.py: Xử lý tầng Bronze -> Silver -> Gold & nạp DW (hỗ trợ single-run & watch loop)
- build_features.py: Trích xuất đặc trưng & phân chia dataset
- organize_s3.py: Di chuyển & chuẩn hóa layout lưu trữ S3
"""

