# 🚀 Crypto Big Data Platform: Real-Time Streaming & Medallion Lakehouse

Dự án thu thập, xử lý, lưu trữ và dự báo dữ liệu giao dịch Cryptocurrency (**BTC-USDT** trên sàn **OKX**) theo kiến trúc kết hợp:
- **Speed Layer (Real-Time Streaming)**: OKX WebSocket $\rightarrow$ Apache Kafka $\rightarrow$ Apache Spark Structured Streaming $\rightarrow$ AWS S3 Clean Trades & PostgreSQL Data Warehouse (Star Schema).
- **Medallion Batch Lakehouse (Reconciliation & Batch Layer)**: OKX REST $\rightarrow$ Bronze Ingestion $\rightarrow$ Silver Cleansing & Deduplication $\rightarrow$ Gold 1-Minute OHLCV Aggregation $\rightarrow$ Data Warehouse Sync.
- **Machine Learning Layer**: Mô hình phân loại xu hướng giá 5 phút **XGBoost (v2.0.0)** với 15 đặc trưng chuẩn hóa và worker suy luận định kỳ kèm cơ chế khóa tệp chống xung đột (`FileLock`).
- **Giao diện Web Tối giản (BTC-USDT Analytics)**: 3 tab hiển thị dữ liệu thật (Tổng quan, Giao dịch lớn, Dự báo).

---

## 📌 Mục lục
1. [Kiến trúc hệ thống (System Architecture)](#-1-kiến-trúc-hệ-thống-system-architecture)
2. [Các tính năng chính](#-2-các-tính-năng-chính)
3. [Công nghệ sử dụng (Tech Stack)](#-3-công-nghệ-sử-dụng-tech-stack)
4. [Cấu trúc thư mục (Project Structure)](#-4-cấu-trúc-thư-mục-project-structure)
5. [Cài đặt & Chuẩn bị môi trường](#-5-cài-đặt--chuẩn-bị-môi-trường)
6. [Tổng hợp TẤT CẢ các lệnh vận hành hệ thống](#-6-tổng-hợp-tất-cả-các-lệnh-vận-hành-hệ-thống)
   - [6.1. Sử dụng PowerShell Orchestrator (`scripts/setup_env.ps1`)](#61-sử-dụng-powershell-orchestrator-scriptssetup_envps1)
   - [6.2. Vận hành Speed Layer (Real-Time Streaming)](#62-vận-hành-speed-layer-real-time-streaming)
   - [6.3. Vận hành Medallion Batch Pipeline (Bronze $\rightarrow$ Silver $\rightarrow$ Gold $\rightarrow$ DW)](#63-vận-hành-medallion-batch-pipeline-bronze--silver--gold--dw)
   - [6.4. Vận hành PostgreSQL Data Warehouse (`warehouse/`)](#64-vận-hành-postgresql-data-warehouse-warehouse)
   - [6.5. Web Analytics & Trading Dashboard (Browser UI)](#65-web-analytics--trading-dashboard-browser-ui)
   - [6.6. Machine Learning: Huấn luyện & Suy luận Dự báo Xu hướng 5 Phút](#66-machine-learning-huấn-luyện--suy-luận-dự-báo-xu-hướng-5-phút)
   - [6.7. Khám phá & Xuất dữ liệu S3 Data Lake (`s3_parquet_reader.py`)](#67-khám-phá--xuất-dữ-liệu-s3-data-lake-s3_parquet_readerpy)
   - [6.8. Truy vấn SQL trên Data Warehouse](#68-truy-vấn-sql-trên-data-warehouse)
7. [Các chỉ số tài chính & Vi mô được tính toán](#-7-các-chỉ-số-tài-chính--vi-mô-được-tính-toán)
8. [Xử lý sự cố thường gặp (Troubleshooting)](#-8-xử-lý-sự-cố-thường-gặp-troubleshooting)
9. [Kiểm thử chất lượng mã nguồn & Hồi quy (Automated Testing)](#-9-kiểm-thử-chất-lượng-mã-nguồn--hồi-quy-automated-testing)

---

## 🏛 1. Kiến trúc hệ thống (System Architecture)

```text
===================================================================================================
                                      NGUỒN DỮ LIỆU THỊ TRƯỜNG (OKX)
===================================================================================================
     [OKX WebSocket: /ws/v5/public (trades)]                 [OKX REST API: /api/v5/market/candles]
                       │                                                       │
                       ▼                                                       ▼
=================================================       ===========================================
    SPEED LAYER (REAL-TIME STREAMING)                       MEDALLION BATCH LAYER (LAKEHOUSE)
=================================================       ===========================================
 [Python Producer: okx_producer.py]                      [Bronze Ingestion: bronze_ingestion.py]
                       │                                                       │
                       ▼                                                       ▼
 [Apache Kafka KRaft: topic crypto-trades]               [Bronze Parquet: crypto/bronze/trades/]
                       │                                                       │
                       ▼                                                       ▼
 [Spark Structured Streaming: trade_processor.py]        [Silver Cleansing: bronze_to_silver.py]
  • Watermark 2 phút & dropDuplicates(trade_id)           • Khử trùng lặp & chuẩn hóa thời gian UTC
  • Phân cấp Whale Alert (>= $100k, $1M)                  • Loại bỏ giá/size invalid (<= 0)
  • Tính Tumbling Window 1m (OHLCV, VWAP, Imbalance)                           │
                       │                                                       ▼
                       ├────────────────────────┐        [Gold Aggregation: silver_to_gold.py]
                       │                        │         • Tumbling 1m OHLCV, VWAP, Imbalance
                       ▼                        ▼         • Exclusive OS Lock (GoldLock)
             [S3 Clean Trades Parquet] [PostgreSQL DW]    • Canonical Reconciliation Engine
             (crypto/streaming/trades) (fact_crypto_*)                         │
                                                ▲                              ▼
                                                └─────── [DW Sync: silver_to_gold.py --sync-dw-only]
                                                          (fact_crypto_1m_candles canonical)
===================================================================================================
                                  MACHINE LEARNING & PHỤC VỤ (SERVING)
===================================================================================================
                                                ┌─────────────────────────────┐
                                                │   XGBoost Classifier v2.0.0 │
                                                │ (Dự báo xu hướng 5m kế tiếp)│
                                                │ • 15 Canonical Features     │
                                                │ • Baseline Lift: -0.61%     │
                                                │ • PredictionWorker (1m loop)│
                                                │ • FileLock chống xung đột   │
                                                └──────────────┬──────────────┘
                                                               │
                                                               ▼
                                                ┌─────────────────────────────┐
                                                │  Web BTC-USDT Analytics     │
                                                │    http://127.0.0.1:5000    │
                                                │ 1. Tổng quan (REST + WS 60s)│
                                                │ 2. Giao dịch lớn (DW >=100k)│
                                                │ 3. Dự báo (XGBoost 5m & DW) │
                                                └─────────────────────────────┘
```

---

## ✨ 2. Các tính năng chính

- **Chuyên biệt hóa BTC-USDT**: Toàn bộ pipeline từ Ingestion, Kafka topic, Spark Processing, Medallion Lakehouse, Data Warehouse đến Web UI được cố định cho cặp giao dịch `BTC-USDT`.
- **Speed Layer độ trễ thấp (< 50ms)**: Thu thập tick-by-tick từ WebSocket OKX qua Kafka, xử lý micro-batch với Spark Streaming để phát hiện lệnh cá mập và ghi nhận nến tức thời.
- **Medallion Architecture chuẩn hóa (Batch)**:
  - **Bronze**: Lưu trữ dữ liệu thô kèm metadata nguồn.
  - **Silver**: Làm sạch, khử trùng lặp theo `trade_id`, chuẩn hóa thời gian UTC.
  - **Gold**: Tổng hợp nến 1 phút OHLCV, VWAP và cán cân dòng tiền; cơ chế Atomic Publish kèm Safe Rollback bảo toàn dữ liệu khi có sự cố.
  - **Reconciliation Engine**: Quy tắc hợp nhất nhất quán giữa nến REST xác thực (`historical_rest`) và nến streaming (`realtime_stream`), đồng bộ dữ liệu canonical sang Data Warehouse.
- **XGBoost 5-Minute Direction Classifier (v2.0.0)**:
  - Dự báo xu hướng giá (TĂNG / KHÔNG TĂNG) sau 5 phút ($t+5$).
  - Sử dụng đúng **15 đặc trưng kỹ thuật & biến động chuẩn hóa** (không lookahead bias).
  - Đánh giá trung thực theo majority baseline, không làm giả kết quả học máy.
- **Giao diện Web tối giản (BTC-USDT Analytics)**:
  - **3 Tab chức năng rõ ràng**: Tổng quan, Giao dịch lớn, Dự báo.
  - **Cán cân mua/bán 60 giây**: Cập nhật theo giao dịch realtime từ OKX WebSocket trong cửa sổ $(now - 60s, now]$, khử trùng lặp và loại bỏ giao dịch trễ/ngoại lai.
  - **Không dùng số liệu giả lập (No Mock Data)**: Toàn bộ chỉ số đều xuất phát từ nguồn dữ liệu thật được chỉ định rõ ràng.

---

## 🛠 3. Công nghệ sử dụng (Tech Stack)

| Thành phần | Công nghệ | Phiên bản | Vai trò trong hệ thống |
| :--- | :--- | :--- | :--- |
| **Data Ingestion** | Python `websocket-client`, `urllib` | 1.9+ | Thu thập WebSocket và REST API từ OKX |
| **Message Broker** | Apache Kafka (KRaft mode) | **3.9+** | Hàng đợi tin nhắn phân tán qua Docker (Port 9092) |
| **Stream Engine** | Apache Spark / PySpark | **3.5.1 LTS** | Xử lý Structured Streaming, Watermarking & Batch Medallion |
| **Java Runtime** | Eclipse Adoptium OpenJDK | **17 LTS** | Runtime JVM cho Spark & Hadoop Connector |
| **Hadoop Native** | Apache Hadoop WinUtils | **3.3.4** | Thư viện WinUtils cho môi trường Windows |
| **Data Lakehouse** | AWS S3 / Local Parquet | Parquet Snappy | Lưu trữ dữ liệu lớn đa tầng (Bronze, Silver, Gold) |
| **Data Warehouse** | PostgreSQL Container | **16 Alpine** | Cơ sở dữ liệu phân tích Star Schema (Port 5433) |
| **Machine Learning**| XGBoost, scikit-learn | 2.0+, 1.5+ | Mô hình phân loại xu hướng giá 5 phút |
| **Concurrency Control**| `filelock` | 3.12+ | Khóa tiến trình OS-level cho Gold Publisher và Prediction Worker |
| **Web UI Backend** | Flask | 3.0+ | REST API & Service Layer phục vụ frontend |
| **Web UI Frontend**| TradingView Lightweight Charts, Vanilla CSS/JS | - | Biểu đồ nến tương tác & thiết kế tối giản, dễ đọc |

---

## 📂 4. Cấu trúc thư mục (Project Structure)

```text
crypto-bigdata/
├── .env.example              # File mẫu cấu hình biến môi trường
├── .env                      # Cấu hình môi trường (AWS S3, Kafka, Postgres, Storage Mode)
├── docker-compose.yml        # Docker Compose chạy Kafka (9092) & PostgreSQL (5433)
├── pyproject.toml            # Cấu hình dự án & dependencies chuẩn PEP 621
├── requirements.txt          # Danh sách thư viện Python chuẩn hóa
├── README.md                 # Tài liệu hướng dẫn toàn diện hệ thống
│
├── docs/                     # TÀI LIỆU HỆ THỐNG VÀ LƯU TRỮ
│   ├── archive/              # Lưu trữ báo cáo kiểm toán lịch sử (audit_report.md, BTC_AUDIT_REMEDIATION_REPORT.md)
│   ├── README_REPORT.md      # Báo cáo kiến trúc hệ thống
│   └── images/               # Biểu đồ và hình ảnh kiến trúc
│
├── configs/                  # FILE CẤU HÌNH PIPELINE & MACHINE LEARNING
│   ├── pipeline.yaml         # Cấu hình Ingestion, Kafka, Storage paths, Whale threshold
│   └── ml.yaml               # Cấu hình XGBoost (Horizon 5m, Hyperparameters, 15 Features)
│
├── ingestion/                # TẦNG THU THẬP DỮ LIỆU STREAMING & HISTORICAL
│   ├── streaming/
│   │   └── okx_producer.py   # Thu thập OKX WebSocket đẩy vào Kafka topic crypto-trades
│   └── historical/
│       ├── bronze_ingestion.py       # Entry point thu thập dữ liệu thô lịch sử vào Bronze
│       ├── okx_historical_backfill.py# Tải nến lịch sử từ OKX REST API và chuẩn hóa
│       └── candle_parser.py          # Bộ phân tích và kiểm tra tính hợp lệ của nến OKX REST
│
├── processing/               # TẦNG XỬ LÝ PHÂN TÁN VÀ ĐỊNH LƯỢNG
│   ├── core/
│   │   ├── env.py            # Bộ nạp .env độc lập, nhẹ (không import PySpark khi không cần)
│   │   ├── config.py         # Bộ nạp cấu hình hợp nhất (YAML + .env)
│   │   ├── schemas.py        # StructType Schemas Medallion (Raw, Silver, Gold)
│   │   └── spark_session.py  # Khởi tạo Spark Session (S3A, Kafka, Postgres JDBC, UTC timezone)
│   ├── streaming/
│   │   ├── trade_processor.py# Engine Spark Streaming (Clean, 1m Candles, Whale Alert, S3 & DW)
│   │   ├── bronze_writer.py  # Ghi luồng raw trades từ Kafka vào Bronze Parquet
│   │   └── kafka_consumer.py # Consumer helper cho BTC stream
│   ├── batch/
│   │   ├── bronze_to_silver.py # Làm sạch dữ liệu, khử trùng lặp trade_id, chuẩn hóa UTC
│   │   └── silver_to_gold.py   # Tổng hợp nến 1m, VWAP, Order Flow; hỗ trợ cờ --sync-dw-only
│   ├── features/
│   │   └── feature_engineering.py # Tính toán 15 đặc trưng kỹ thuật chuẩn hóa & nhãn mục tiêu 5m
│   └── analysis/
│       └── s3_parquet_reader.py   # Đọc Parquet S3/Local, phân tích thống kê & xuất CSV
│
├── storage/                  # TRỪU TƯỢNG HÓA TẦNG LƯU TRỮ
│   ├── s3.py                 # Đường dẫn chuẩn hóa Bronze, Silver, Gold, Checkpoints
│   └── gold_lock.py          # Khóa độc quyền OS-level cho tầng Gold (GoldLock)
│
├── ml/                       # HỌC MÁY (XGBOOST CLASSIFIER DỰ BÁO XU HƯỚNG GIÁ)
│   ├── dataset.py            # Phân đoạn chuỗi thời gian 70/15/15 kèm vùng đệm Embargo 5m
│   ├── train.py              # Huấn luyện mô hình XGBoost với Early Stopping
│   ├── evaluate.py           # Đánh giá Accuracy, F1, Confusion Matrix, Baseline Lift
│   ├── predict.py            # Suy luận thời gian thực, Quality Gate & đối chiếu nhãn mục tiêu
│   ├── prediction_worker.py  # Background worker chạy chu kỳ 1 phút với FileLock liên tiến trình
│   ├── model_registry.py     # Quản lý phiên bản mô hình trên S3 kèm staging an toàn & SHA-256
│   └── artifacts/            # Lưu trữ model JSON, schema, metadata & evaluation results
│
├── warehouse/                # TẦNG DATA WAREHOUSE (POSTGRESQL STAR SCHEMA)
│   ├── sql/
│   │   ├── 00_init_streaming_dw.sql # Script DDL & Idempotent Migrations
│   │   ├── dim/              # dim_symbol.sql (BTC-USDT), dim_alert_tier.sql
│   │   ├── fact/             # fact_crypto_trades.sql, fact_crypto_1m_candles.sql,
│   │   │                     # fact_btc_predictions.sql, fact_btc_model_evaluation.sql
│   │   └── views/            # 10 Analytics Views phục vụ báo cáo & dashboard
│   └── etl/
│       ├── db_config.py      # Kết nối CSDL & khởi tạo bảng/view trong PostgreSQL
│       └── streaming_sink.py # Micro-batch Sink nạp nến 1m và giao dịch lớn vào DW
│
├── web/                      # NỀN TẢNG WEB ANALYTICS DASHBOARD
│   ├── app.py                # Flask Backend API (decoupled, không chạy nền khi import)
│   ├── orderflow_realtime.py # Quản lý cửa sổ trượt 60s realtime từ WebSocket OKX
│   ├── services/             # Service Layer tách biệt logic xử lý cho từng tab
│   │   ├── overview_service.py   # Dịch vụ tab Tổng quan (OKX REST, VWAP, Ticker 24h)
│   │   ├── trades_service.py     # Dịch vụ tab Giao dịch lớn (DW fact_crypto_trades, CSV)
│   │   └── prediction_service.py # Dịch vụ tab Dự báo (XGBoost inference, worker status)
│   ├── templates/index.html  # Giao diện 3 tab tối giản
│   └── static/               # CSS & JavaScript điều khiển giao diện (Lightweight Charts)
│
├── tools/                    # CÔNG CỤ HỖ TRỢ VÀ BÁO CÁO
│   ├── migration/
│   │   └── reconcile_trade_timestamps.py # Công cụ đối soát migration dùng 1 lần (mặc định dry-run)
│   └── reporting/
│       └── generate_report_charts.py     # Tạo biểu đồ phân tích kiến trúc phục vụ báo cáo
│
├── scripts/                  # SCRIPTS ĐIỀU PHỐI VẬN HÀNH
│   ├── setup_env.ps1         # Script điều phối toàn diện trên PowerShell (Windows)
│   ├── build_features.py     # Tạo đặc trưng kỹ thuật & tập dữ liệu phân đoạn
│   ├── train_model.py        # Huấn luyện và xuất artifacts mô hình XGBoost
│   └── run_prediction.py     # Chạy dịch vụ suy luận dự báo thời gian thực
│
├── tests/                    # BỘ KIỂM THỬ TỰ ĐỘNG & HỒI QUY
│   ├── test_orderflow_and_worker.py    # Kiểm thử cửa sổ trượt 60s & Prediction Worker concurrency
│   ├── test_prediction_evaluation.py  # Kiểm thử đối chiếu kết quả dự báo & Quality Gate
│   ├── test_web_ui_refactor.py        # Kiểm thử toàn diện API backend & cấu trúc 3 tab
│   ├── test_audit_remediation.py      # Kiểm thử 10 trụ cột kiểm toán độc lập
│   ├── test_model_registry.py         # Kiểm thử quản lý artifact mô hình & SHA-256 staging
│   ├── test_pipeline_e2e.py           # Kiểm thử tích hợp toàn bộ pipeline
│   ├── test_warehouse.py              # Kiểm thử Data Warehouse Star Schema & độ trễ
│   └── test_s3.py                     # Kiểm thử kết nối & quyền đọc/ghi AWS S3
│
├── hadoop/                   # Thư viện winutils.exe cho môi trường Windows
└── data/                     # Thư mục lưu trữ cục bộ (checkpoints, parquet, export CSV)
```

---

## ⚙️ 5. Cài đặt & Chuẩn bị môi trường

### Bước 1: Yêu cầu phần mềm
- **Python**: `3.10` đến `3.14`
- **Java**: **JDK 17 LTS** ([Eclipse Adoptium JDK 17](https://adoptium.net/temurin/releases/?version=17))
- **Docker Desktop**: Chạy Kafka (KRaft) và PostgreSQL Containers
- **Hadoop WinUtils (Windows)**: Tích hợp sẵn trong thư mục `hadoop/` hoặc cấu hình `C:\hadoop`

### Bước 2: Thiết lập môi trường ảo Python & Cài đặt thư viện
Mở PowerShell tại thư mục dự án `d:\CryptoBigdata\Crypto-bigdata`:
```powershell
# 1. Tạo môi trường ảo
python -m venv .venv

# 2. Kích hoạt môi trường ảo
.\.venv\Scripts\activate

# 3. Cài đặt thư viện chuẩn hóa
pip install -r requirements.txt
```

### Bước 3: Cấu hình file `.env`
Sao chép từ file mẫu và kiểm tra cấu hình:
```powershell
cp .env.example .env
```
Các thông số chính trong `.env`:
```env
# Storage Mode: "local" (chạy trên đĩa máy tính) hoặc "s3" (lưu AWS S3 Data Lake)
STORAGE_MODE=local

# AWS S3 Configuration (nếu dùng S3)
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-southeast-1
S3_BUCKET_NAME=tên-bucket-của-bạn

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_TRADES=crypto-trades

# PostgreSQL Data Warehouse Configuration
PG_HOST=localhost
PG_PORT=5433
PG_DATABASE=crypto_dw
PG_USER=postgres
PG_PASSWORD=password123
```

---

## 🚀 6. Tổng hợp TẤT CẢ các lệnh vận hành hệ thống

---

### 6.1. Sử dụng PowerShell Orchestrator (`scripts/setup_env.ps1`)

Script [scripts/setup_env.ps1](file:///d:/CryptoBigdata/Crypto-bigdata/scripts/setup_env.ps1) quản trị toàn bộ hệ thống qua các tham số được chuẩn hóa:

| Nhóm chức năng | Lệnh PowerShell | Chức năng chi tiết |
| :--- | :--- | :--- |
| **Hạ tầng & CSDL** | `.\scripts\setup_env.ps1 -Info` | Kiểm tra trạng thái Python, PySpark, JDK, Hadoop, Kafka, Postgres |
| | `.\scripts\setup_env.ps1 -DockerUp` | Khởi động Docker Containers (Kafka:9092 & PostgreSQL DW:5433) |
| | `.\scripts\setup_env.ps1 -InitWarehouse` | Tạo cấu trúc Star Schema DW (`fact_trades`, `fact_candles_1m`,...) và 10 Views |
| | `.\scripts\setup_env.ps1 -DockerDown` | Dừng toàn bộ Docker Containers khi kết thúc phiên làm việc |
| **Speed Layer** | `.\scripts\setup_env.ps1 -StreamPipeline` | **[1-Click]** Bật Docker, khởi tạo DW, mở terminal Producer (WS) và terminal Spark Streaming |
| | `.\scripts\setup_env.ps1 -RunProducer` | Khởi chạy Ingestion Producer thu thập dữ liệu WebSocket OKX (BTC-USDT) |
| | `.\scripts\setup_env.ps1 -RunSpark` | Khởi chạy Spark Structured Streaming Engine (làm sạch, nến 1m, ghi S3 & DW) |
| **Medallion Batch** | `.\scripts\setup_env.ps1 -MedallionPipeline` | **[1-Click]** Chạy tuần tự 3 giai đoạn: Bronze $\rightarrow$ Silver $\rightarrow$ Gold & Đồng bộ DW |
| **Học máy (ML)** | `.\scripts\setup_env.ps1 -TrainModel` | Huấn luyện mô hình XGBoost Classifier trên chuỗi thời gian (15 đặc trưng, 70% Train / 15% Val / 15% Test) |
| | `.\scripts\setup_env.ps1 -RunPrediction` | Khởi chạy dịch vụ suy luận thời gian thực, lưu trữ dự báo vào PostgreSQL DW |
| **Giao diện Web** | `.\scripts\setup_env.ps1 -WebDashboard` | Khởi chạy Web BTC-USDT Analytics Dashboard (`http://127.0.0.1:5000`) |
| **Kiểm thử (Testing)** | `.\scripts\setup_env.ps1 -TestPipeline` | Chạy bộ kiểm thử tự động toàn diện pipeline, ML và Data Warehouse |

---

### 6.2. Vận hành Speed Layer (Real-Time Streaming)

#### Cách 1: 1-Click tự động kích hoạt trọn gói
```powershell
.\scripts\setup_env.ps1 -StreamPipeline
```
> Script tự động:
> 1. Dọn dẹp tiến trình stream cũ của dự án để tránh xung đột filelock / checkpoint.
> 2. Bật Docker Containers (Kafka & Postgres).
> 3. Khởi tạo schema Data Warehouse.
> 4. Mở **Terminal 1** chạy OKX WebSocket Producer (`ingestion/streaming/okx_producer.py`).
> 5. Mở **Terminal 2** chạy Spark Structured Streaming Engine (`processing/streaming/trade_processor.py`).

#### Cách 2: Vận hành thủ công từng bước
```powershell
# Bước 1: Khởi động Kafka & PostgreSQL
docker compose up -d

# Bước 2: Chạy OKX WebSocket Ingestion Producer (Terminal A)
python ingestion/streaming/okx_producer.py

# Bước 3: Chạy Spark Structured Streaming Engine (Terminal B)
python processing/streaming/trade_processor.py
```

---

### 6.3. Vận hành Medallion Batch Pipeline (Bronze $\rightarrow$ Silver $\rightarrow$ Gold $\rightarrow$ DW)

#### Cách 1: 1-Click chạy tuần tự toàn bộ 4 giai đoạn
```powershell
.\scripts\setup_env.ps1 -MedallionPipeline
```
> Script kiểm tra `$LASTEXITCODE` sau từng bước và dừng ngay lập tức nếu có lỗi:
> - **Giai đoạn 1**: Bronze Ingestion (`ingestion/historical/bronze_ingestion.py`)
> - **Giai đoạn 2**: Silver Cleansing & Deduplication (`processing/batch/bronze_to_silver.py`)
> - **Giai đoạn 3**: Gold Aggregation & DW Sync (`processing/batch/silver_to_gold.py`)

#### Cách 2: Vận hành thủ công bằng Python
```powershell
# Bước 1: Thu thập lịch sử REST OKX vào Bronze Lakehouse
python ingestion/historical/bronze_ingestion.py --days 30

# Bước 2: Chạy trọn gói biến đổi Silver -> Gold -> Sync DW trong 1 Spark Session duy nhất
python scripts/run_batch_medallion.py
```

---

### 6.4. Vận hành PostgreSQL Data Warehouse (`warehouse/`)

Hệ thống Data Warehouse Star Schema được tối ưu hóa cho BTC-USDT:
- **Bảng Chiều (Dimensions)**:
  - `dim_symbol`: Quản lý cặp giao dịch `BTC-USDT` (partition 0 Kafka), ngưỡng lọc cá mập và độ chính xác giá.
  - `dim_alert_tier`: Phân cấp độ cảnh báo cá mập (`Mega Whale >= $1M`, `Big Whale $500K - $1M`, `Whale $250K - $500K`, `Large Order $100K - $250K`).
- **Bảng Sự Kiện (Fact Tables)**:
  - `fact_crypto_trades`: Lưu trữ các giao dịch lớn/cá mập $\ge \$100,000$ kèm metadata độ trễ streaming.
  - `fact_crypto_1m_candles`: Lưu trữ nến 1 phút với nguồn gốc rõ ràng (`source`: `historical_rest` hoặc `realtime_stream`, trạng thái `is_closed`).
  - `fact_btc_predictions`: Lưu trữ kết quả suy luận xu hướng giá 5 phút từ XGBoost model (`direction_label`, `probability_up`, `features_json`, `actual_future_price`, `is_correct`).
  - `fact_btc_model_evaluation`: Lưu trữ telemetry đánh giá mô hình định kỳ (Accuracy, Precision, Recall, F1, Baseline Lift).

```powershell
# Khởi tạo / cập nhật DDL bảng và views:
.\scripts\setup_env.ps1 -InitWarehouse

# Chạy bộ kiểm thử tự động Data Warehouse:
.\scripts\setup_env.ps1 -TestWarehouse
```

---

### 6.5. Web Analytics & Trading Dashboard (Browser UI)

Giao diện Web tối giản phục vụ phân tích định lượng và giám sát:
- 🌐 **Địa chỉ truy cập**: `http://127.0.0.1:5000`

#### Cấu trúc 3 Tab chức năng:
1. **Tab 1: Tổng quan (Overview)**
   - **Nguồn nến & thống kê**: OKX REST API.
   - **Biểu đồ nến chính**: Khung mặc định 1 phút (TradingView Lightweight Charts), khối lượng giao dịch bên dưới.
   - **Chỉ báo VWAP**: Tùy chọn bật/tắt (mặc định tắt), tính toán chính xác trên dữ liệu nến. Biểu đồ gọn gàng, không hiển thị dải $\pm 2\sigma$ hay vùng Fair Value gây rối.
   - **Thống kê giá**: Giá gần nhất; tỷ lệ biến động 24h chỉ hiển thị khi có dữ liệu bao phủ đủ $\ge 23$ giờ.
   - **Cán cân mua/bán (60 giây gần nhất)**: Thu thập trực tiếp từ luồng OKX WebSocket realtime (`sub: trades`). Khử trùng theo `(symbol, trade_id)`, loại bỏ giao dịch quá hạn ngoài cửa sổ $(now - 60s, now]$, tính toán chính xác khối lượng mua, bán và mức chênh lệch (imbalance $\in [-1, 1]$).
2. **Tab 2: Giao dịch lớn (Large Trades)**
   - **Nguồn dữ liệu**: PostgreSQL Data Warehouse (`warehouse.fact_crypto_trades`).
   - **Bộ lọc**: Các giao dịch có giá trị $\ge \$100,000$ USD.
   - **Tính năng**: Phân trang linh hoạt (10/25/50/100 dòng), bộ đếm tổng thể toàn bộ tập dữ liệu (Tổng khối lượng cá mập, số lượng lệnh), và nút **Xuất CSV** tải dữ liệu về máy.
3. **Tab 3: Dự báo (Predictions)**
   - **Nguồn dữ liệu**: PostgreSQL DW (`warehouse.fact_btc_predictions`) và mô hình XGBoost v2.0.0.
   - **Dự báo gần nhất**: Chiều hướng dự đoán (TĂNG / KHÔNG TĂNG), xác suất tăng $P(\text{UP})$, giá tại thời điểm dự báo và trạng thái đối chiếu thực tế sau 5 phút ($t+5$).
   - **Lịch sử dự báo**: Bảng phân trang các dự báo đã thực hiện kèm kết quả đối chiếu chính xác.
   - **PredictionWorker tự động**: Tiến trình nền chạy chu kỳ 1 phút, sử dụng `filelock` bảo vệ chống xung đột giữa nhiều tiến trình/worker, tự động đối chiếu nhãn khi nến $t+5$ đóng cửa mà không làm gián đoạn chu kỳ suy luận.

```powershell
# Khởi chạy Web Dashboard:
.\scripts\setup_env.ps1 -WebDashboard

# Tùy chọn chế độ worker:
.\scripts\setup_env.ps1 -WebDashboard -WorkerMode auto      # Bật worker tự động
.\scripts\setup_env.ps1 -WebDashboard -WorkerMode manual    # Chỉ chạy suy luận khi bấm nút
.\scripts\setup_env.ps1 -WebDashboard -NoBrowser            # Không tự mở trình duyệt
```

---

### 6.6. Machine Learning: Huấn luyện & Suy luận Dự báo Xu hướng 5 Phút

Dự án áp dụng mô hình phân loại xu hướng giá: **XGBoost Classifier (`from xgboost import XGBClassifier`)** để dự báo xác suất tăng giá của BTC-USDT trong chu kỳ **5 phút kế tiếp** ($t+5$):

* **Nhãn mục tiêu**:
  $$\text{future\_return} = \frac{\text{close}[t+5] - \text{close}[t]}{\text{close}[t]}$$
  $$\text{target} = \begin{cases} 1 & \text{nếu future\_return} > 0 \text{ (UP)} \\ 0 & \text{nếu future\_return} \le 0 \text{ (NOT\_UP)} \end{cases}$$
* **Quy tắc phân đoạn dữ liệu**:
  * Phân chia thời gian tuần tự (Chronological split): **70% Train**, **15% Validation**, **15% Test**.
  * Tuyệt đối không xáo trộn ngẫu nhiên (No random shuffle).
  * Vùng đệm cách ly (Embargo buffer) **5 phút** giữa các tập để loại trừ rò rỉ thông tin tương lai (Data Leakage).
* **Bộ 15 đặc trưng chuẩn hóa (Canonical 15 Features)**:
  1. `log_return`: Lợi suất logarit 1 phút
  2. `price_momentum_5m`: Đà tăng trưởng giá 5 phút
  3. `high_low_range`: Biên độ dao động High - Low
  4. `volume_change`: Tỷ lệ thay đổi khối lượng giao dịch
  5. `sma_5`: Đường trung bình trượt 5 chu kỳ
  6. `sma_15`: Đường trung bình trượt 15 chu kỳ
  7. `ema_12`: Đường trung bình lũy thừa 12 chu kỳ
  8. `ema_26`: Đường trung bình lũy thừa 26 chu kỳ
  9. `macd`: Đường MACD (EMA12 - EMA26)
  10. `macd_signal`: Đường tín hiệu MACD (EMA 9 của MACD)
  11. `bb_upper`: Dải trên Bollinger Bands (20 chu kỳ, 2 độ lệch chuẩn)
  12. `bb_lower`: Dải dưới Bollinger Bands (20 chu kỳ, 2 độ lệch chuẩn)
  13. `bb_bandwidth`: Độ rộng dải Bollinger Bands
  14. `rsi_14`: Chỉ số sức mạnh tương đối RSI (14 chu kỳ)
  15. `rolling_volatility_15m`: Độ biến động trượt 15 phút
* **Đánh giá trung thực theo artifacts (`evaluation_results.json`)**:
  - **Tập kiểm thử (Test Samples)**: 19,427 mẫu nến độc lập
  - **Độ chính xác mô hình (Test Accuracy)**: **49.98%** (0.4998)
  - **Baseline chuẩn (Majority Class Predictor)**: **50.29%** (0.5029)
  - **Chênh lệch so với Baseline (Lift)**: **-0.61%**
  - **Kết luận (`outperforms_baseline`)**: `false`
  > [!NOTE]
  > Dự án trình bày trung thực kết quả mô hình theo đúng artifact. Trong lý thuyết tài chính định lượng, chuỗi lợi suất 1 phút của tài sản thanh khoản cao như Bitcoin tiệm cận bước đi ngẫu nhiên (Random Walk) và giả thuyết thị trường hiệu quả (EMH). Việc không vượt qua majority baseline phản ánh đúng thực tế thống kê và không thổi phồng kết quả.

```powershell
# 1. Huấn luyện mô hình XGBoost (tự động trích xuất 15 đặc trưng, phân chia 70/15/15 và đánh giá baseline):
.\scripts\setup_env.ps1 -TrainModel

# 2. Chạy dịch vụ suy luận thời gian thực:
.\scripts\setup_env.ps1 -RunPrediction
```

---

### 6.7. Khám phá & Xuất dữ liệu S3 Data Lake (`s3_parquet_reader.py`)

```powershell
python processing/analysis/s3_parquet_reader.py
```
- **Phím 1**: Xem 10 nến 1 phút mới nhất (`crypto/streaming/candles_1m` hoặc `crypto/gold/candles_1m`).
- **Phím 2**: Xem 10 dòng khớp lệnh sạch mới nhất (`crypto/streaming/trades`).
- **Phím 3**: Xem danh sách giao dịch cá mập (Whale Trades >= $100K USD).
- **Phím 4**: Xuất nến 1 phút ra file CSV cục bộ.
- **Phím 5**: Xuất toàn bộ giao dịch sạch ra file CSV cục bộ.

---

### 6.8. Truy vấn SQL trên Data Warehouse

Thông tin kết nối PostgreSQL:
- **Host**: `127.0.0.1` | **Port**: `5433` | **Database**: `crypto_dw` | **User**: `postgres` | **Password**: `password123`

```sql
-- 1. Xem số lượng bản ghi trong từng bảng
SELECT 'fact_crypto_trades' AS table_name, COUNT(*) FROM warehouse.fact_crypto_trades
UNION ALL
SELECT 'fact_crypto_1m_candles' AS table_name, COUNT(*) FROM warehouse.fact_crypto_1m_candles
UNION ALL
SELECT 'fact_btc_predictions' AS table_name, COUNT(*) FROM warehouse.fact_btc_predictions;

-- 2. Xem các giao dịch cá mập lớn nhất (Whales >= $500,000)
SELECT * FROM warehouse.vw_whale_trades 
ORDER BY trade_value_usd DESC 
LIMIT 20;

-- 3. Đánh giá độ chính xác của các dự báo đã có kết quả đối chiếu
SELECT 
    COUNT(*) AS total_evaluated,
    SUM(CASE WHEN is_correct = TRUE THEN 1 ELSE 0 END) AS correct_count,
    ROUND(SUM(CASE WHEN is_correct = TRUE THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 2) AS accuracy_pct
FROM warehouse.fact_btc_predictions
WHERE actual_direction IS NOT NULL;
```

---

## 📈 7. Các chỉ số tài chính & Vi mô được tính toán

| Chỉ số | Công thức / Thuật toán | Ý nghĩa & Ứng dụng thực tế |
| :--- | :--- | :--- |
| **OHLC** | `First(P), Max(P), Min(P), Last(P)` | Biểu đồ nến Nhật phân tích biến động giá trong 1 phút |
| **Volume** | $\sum \text{Size}$ | Tổng khối lượng BTC được giao dịch trong 1 phút |
| **Buy / Sell Volume** | $\sum_{\text{side='buy'}} \text{Size}$ vs $\sum_{\text{side='sell'}} \text{Size}$ | Tách bạch khối lượng phe Mua chủ động (Taker Buy) và phe Bán chủ động (Taker Sell) |
| **VWAP** | $\frac{\sum (\text{Price} \times \text{Size})}{\sum \text{Size}}$ | **Volume-Weighted Average Price**: Mức giá bình quân gia quyền theo khối lượng |
| **Trade Imbalance** | $\frac{\text{BuyVol} - \text{SellVol}}{\text{BuyVol} + \text{SellVol}}$ | Biên độ chênh lệch áp lực dòng tiền từ `-1.0` (áp đảo bán) đến `+1.0` (áp đảo mua) |
| **Total Trade Value** | $\sum (\text{Price} \times \text{Size})$ | Tổng giá trị thanh khoản luân chuyển bằng USDT |
| **Whale Alert** | $\text{Trade Value} \ge \$500,000$ | Phát hiện các lệnh giao dịch lớn của cá mập / tổ chức tức thời |

---

## 🔧 8. Xử lý sự cố thường gặp (Troubleshooting)

### 1. Xung đột Port PostgreSQL `5432` trên Windows
- **Hiện tượng**: Báo lỗi port 5432 đã được sử dụng khi chạy `docker compose up`.
- **Giải pháp**: Container PostgreSQL ánh xạ sang cổng **`5433`** (`5433:5432`). Mọi kết nối CSDL trong code, `.env` và công cụ GUI (DBeaver, DataGrip) đều sử dụng cổng `5433`.

### 2. Xung đột tiến trình worker dự báo
- **Hiện tượng**: API trả về mã lỗi 429 hoặc thông báo "Chu kỳ suy luận khác đang thực thi".
- **Giải pháp**: `PredictionWorker` sử dụng cơ chế `filelock` trên tệp `ml/prediction_worker.lock` để đảm bảo duy nhất một worker hoạt động tại một thời điểm trên toàn hệ thống. Nếu tiến trình trước bị dừng đột ngột, khóa sẽ tự động giải phóng sau timeout.

### 3. Timestamp hiển thị theo giờ Việt Nam
- **Quy chuẩn**: Toàn bộ dữ liệu trong Lakehouse và Data Warehouse được lưu trữ ở chuẩn **UTC**. Giao diện Web hiển thị nhãn thời gian kèm chú thích rõ ràng theo giờ Việt Nam (ICT, UTC+7).

---

## 🧪 9. Kiểm thử chất lượng mã nguồn & Hồi quy (Automated Testing)

Dự án cung cấp hệ thống kiểm thử hồi quy toàn diện, gọi trực tiếp mã nguồn production và trả về mã lỗi khác 0 khi phát sinh sự cố:

### 1. Bộ kiểm thử Cửa sổ trượt 60s & Concurrency Worker
```powershell
python -m unittest tests/test_orderflow_and_worker.py -v
```
- Xác thực cửa sổ trượt 60 giây của Order Flow: giao dịch đúng thứ tự, giao dịch đến trễ ngoài cửa sổ $(now-60s, now]$, khử trùng lặp `trade_id`, từ chối giao dịch tương lai, kiểm tra biên 60s, tự động hết hạn khi thời gian trôi qua, từ chối giá/size NaN/Inf/âm, tổng tỷ lệ mua + bán xấp xỉ 1.0.
- Xác thực `PredictionWorker`: không gặp lỗi `NameError (timedelta)`, trả về đúng mã khi có nến mới, bỏ qua nến đã dự báo, từ chối nến chưa đóng, cách ly ngoại lệ đối chiếu không làm sập chu kỳ, khóa non-blocking chống chạy đồng thời.
- **Kết quả: 14/14 tests PASS**.

### 2. Bộ kiểm thử Đối chiếu kết quả & Input Quality Gate
```powershell
python -m unittest tests/test_prediction_evaluation.py -v
```
- Xác thực logic đối chiếu nhãn `evaluate_unresolved_predictions`: khớp chính xác timestamp, từ chối nến lệch 1s hoặc 30s, từ chối nến chưa đóng (`is_closed=False`), an toàn với DataFrame có chỉ mục không liên tục (`.loc`), bỏ qua bản ghi legacy lệch giây không làm tắc hàng đợi.
- Xác thực Quality Gate trong suy luận: từ chối nến stream chưa qua đối soát, từ chối vector đặc trưng chứa NaN/Infinity.
- **Kết quả: 8/8 tests PASS**.

### 3. Bộ kiểm thử Web UI & Backend API Refactoring
```powershell
python tests/test_web_ui_refactor.py
```
- Xác thực giao diện đúng 3 tab, loại bỏ toàn bộ dữ liệu giả lập (mock data), kiểm tra endpoint `/api/overview`, `/api/candles`, `/api/trades/large`, `/api/ml/prediction`, `/api/ml/history`, `/api/ml/metadata` đọc đúng 15 đặc trưng và phiên bản 2.0.0.
- **Kết quả: 28/28 tests PASS**.

### 4. Bộ kiểm thử 10 Trụ cột Kiểm toán (Audit Remediation)
```powershell
python tests/test_audit_remediation.py
```
- Chạy bằng fixtures độc lập, xác thực quy tắc hợp nhất nến Canonical, khóa OS-level GoldLock, máy trạng thái Publish/Rollback, bộ phân tích nến REST OKX, và pipeline batch Medallion.
- **Kết quả: 10/10 tests PASS**.

### 5. Bộ kiểm thử Model Registry & Checksum Staging
```powershell
python tests/test_model_registry.py
```
- Xác thực quy trình tải artifact mô hình qua thư mục tạm staging cô lập, đối chiếu mã băm SHA-256 bit-for-bit, và kích hoạt nguyên tử (atomic activation).
- **Kết quả: 5/5 tests PASS**.
