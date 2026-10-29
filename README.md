# 🚀 Crypto Big Data Pure Real-Time Streaming Pipeline & Lakehouse

Pipeline thu thập, xử lý và phân tích dữ liệu giao dịch Cryptocurrency **thời gian thực (Pure Real-Time Streaming)** từ sàn **OKX**, truyền tải qua **Apache Kafka**, xử lý phân tán tức thời với **Spark Structured Streaming**, lưu trữ dài hạn trên **AWS S3 Data Lake (Parquet)** và nạp trực tiếp vào **PostgreSQL Data Warehouse (Star Schema)** phục vụ phân tích định lượng & giao dịch chuyên sâu.

---

## 📌 Mục lục
1. [Kiến trúc hệ thống (Streaming Architecture)](#-1-kiến-trúc-hệ-thống-streaming-architecture)
2. [Các tính năng chính](#-2-các-tính-năng-chính)
3. [Công nghệ sử dụng (Tech Stack)](#-3-công-nghệ-sử-dụng-tech-stack)
4. [Cấu trúc thư mục (Project Structure)](#-4-cấu-trúc-thư-mục-project-structure)
5. [Cài đặt & Chuẩn bị môi trường](#-5-cài-đặt--chuẩn-bị-môi-trường)
6. [Tổng hợp TẤT CẢ các lệnh vận hành hệ thống](#-6-tổng-hợp-tất-cả-các-lệnh-vận-hành-hệ-thống)
   - [6.1. Sử dụng PowerShell Orchestrator (`scripts/setup_env.ps1`)](#61-sử-dụng-powershell-orchestrator-scriptssetup_envps1)
   - [6.2. Vận hành luồng Streaming (1-Click hoặc 3 Terminals)](#62-vận-hành-luồng-streaming-1-click-hoặc-3-terminals)
   - [6.3. Khởi tạo & Kiểm tra AWS S3 Data Lake (`tests/test_s3.py`)](#63-khởi-tạo--kiểm-tra-aws-s3-data-lake-teststest_s3py)
   - [6.4. Khám phá & Xuất dữ liệu S3 Data Lake (`s3_parquet_reader.py`)](#64-khám-phá--xuất-dữ-liệu-s3-data-lake-s3_parquet_readerpy)
   - [6.5. Vận hành PostgreSQL Data Warehouse (`warehouse/`)](#65-vận-hành-postgresql-data-warehouse-warehouse)
   - [6.6. Web Trading & Quant Analytics Dashboard (Browser UI)](#66-web-trading--quant-analytics-dashboard-browser-ui)
   - [6.7. Huấn luyện Mô hình XGBoost & Suy luận Dự báo Xu hướng BTC (5 Phút)](#67-huấn-luyện-mô-hình-xgboost--suy-luận-dự-báo-xu-hướng-btc-5-phút)
   - [6.8. Truy vấn SQL trên Data Warehouse](#68-truy-vấn-sql-trên-data-warehouse)
   - [6.9. Truy vấn Serverless SQL với Amazon Athena](#69-truy-vấn-serverless-sql-với-amazon-athena)
7. [Các chỉ số tài chính & Dòng tiền được tính toán](#-7-các-chỉ-số-tài-chính--dòng-tiền-được-tính-toán)
8. [Xử lý sự cố thường gặp (Troubleshooting)](#-8-xử-lý-sự-cố-thường-gặp-troubleshooting)

---

## 🏛 1. Kiến trúc hệ thống (Streaming Architecture)

```text
       ┌─────────────────────────────────────────────────────────┐
       │                OKX WebSocket Market Data                │
       │                   (BTC-USDT Dedicated)                  │
       └────────────────────────────┬────────────────────────────┘
                                    │ Tick-by-tick Trades Stream (< 50ms)
                                    ▼
       ┌─────────────────────────────────────────────────────────┐
       │             Python Multi-Stream Producer                │
       │         (ingestion/streaming/okx_producer.py)           │
       └────────────────────────────┬────────────────────────────┘
                                    │ Partition 0 (BTC-USDT)
                                    ▼
       ┌─────────────────────────────────────────────────────────┐
       │             Apache Kafka (crypto-trades)                │
       │                   Partition 0 (BTC-USDT)                │
       └────────────────────────────┬────────────────────────────┘
                                    │ Spark Structured Streaming
                                    ▼
       ┌─────────────────────────────────────────────────────────┐
       │             AWS S3 Data Lakehouse (Medallion)           │
       │  ┌───────────────────────────────────────────────────┐  │
       │  │ • BRONZE: Raw Kafka Trades & Telemetry Metadata   │  │
       │  │ • SILVER: Validated, Deduplicated, Clean Trades   │  │
       │  │ • GOLD  : 1m OHLCV, VWAP, Imbalance & Features    │  │
       │  └───────────────────────────────────────────────────┘  │
       └──────────────┬───────────────────────────┬──────────────┘
                      │                           │
          Gold Features Dataset        Gold 1m Candles & Whales
                      │                           │
                      ▼                           ▼
       ┌─────────────────────────────┐  ┌─────────────────────────────┐
       │    XGBoost Classifier ML    │  │  PostgreSQL Data Warehouse  │
       │ (Next 5m Direction Predict) │  │       (Star Schema)         │
       │                             │  │                             │
       │ • 70/15/15 Chrono Splits    │  │ • fact_crypto_trades        │
       │ • 18 Technical Features     │  │ • fact_crypto_1m_candles    │
       │ • Outperforms Baseline Ref  │  │ • fact_btc_predictions      │
       │ • Real-time Prediction Sink ├──┤ • fact_btc_model_evaluation │
       └─────────────────────────────┘  └──────────────┬──────────────┘
                                                       │
                                                       ▼
                                        ┌─────────────────────────────┐
                                        │ Web Quant Trading Dashboard │
                                        │  (TradingView Charts & ML)  │
                                        │    http://127.0.0.1:5000    │
                                        └─────────────────────────────┘
```
       └──────────────┬──────────────┘  │ • Business Analytics Views  │
                      │                 └──────────────┬──────────────┘
                      │                                │
                      ▼                                ▼
       ┌─────────────────────────────┐  ┌─────────────────────────────┐
       │     S3 Parquet Explorer     │  │    Web Trading Dashboard    │
       │   (s3_parquet_reader.py)    │  │   (http://127.0.0.1:5000)   │
       │ • Thống kê Parquet Data Lake│  │ • Biểu đồ TradingView & VWAP│
       │ • Xuất CSV mở Excel / PowerBI│ │ • Radar Cá mập & Tín hiệu   │
       │ • Interactive Console Menu  │  │ • Data Warehouse Explorer   │
       └─────────────────────────────┘  └─────────────────────────────┘
```

---

## ✨ 2. Các tính năng chính

- **Real-Time Streaming Ingestion**: Thu thập dữ liệu khớp lệnh từng tick (tick-by-tick) từ sàn OKX qua giao thức WebSocket chuẩn công nghiệp với độ trễ thấp (< 50ms).
- **Phân luồng chuyên biệt trên Kafka**: Định tuyến dữ liệu `BTC-USDT` vào partition 0 chuyên biệt của topic `crypto-trades`, đảm bảo thứ tự khớp lệnh tuyệt đối và hỗ trợ mở rộng quy mô (scalability).
- **Làm sạch & Chuẩn hóa dữ liệu phân tán (Spark Structured Streaming)**:
  - Lọc bỏ bản ghi lỗi, giá `<= 0`, khối lượng `<= 0`.
  - Chuẩn hóa phân loại `side` (`buy` / `sell`).
  - Xử lý Event-time với cơ chế **Watermarking (2 phút)** và khử trùng lặp (**`dropDuplicates`**).
- **Tính toán Định lượng & Nến 1 phút (Tumbling Window Aggregations)**:
  - Sinh nến giá 1 phút chuẩn: **OHLC** (Open, High, Low, Close).
  - Tách bạch khối lượng **Buy Volume** (Taker Buy) vs **Sell Volume** (Taker Sell).
  - Tính toán giá trung bình gia quyền theo khối lượng (**VWAP** - Volume-Weighted Average Price).
  - Đo lường mức độ mất cân bằng áp lực cung cầu (**Trade Imbalance** $\in [-1, 1]$).
- **Radar Phát hiện Cá mập tức thời (Real-Time Whale Alert)**: Tự động phát hiện các lệnh khớp lớn $\ge \$500,000$ và phân cấp (Whale, Big Whale, Mega Whale) hiển thị trực tiếp lên console, Web Dashboard và lưu vào Database.
- **Kiến trúc Lưu trữ Đa tầng (Dual-Sink Storage)**:
  - **AWS S3 Data Lake (Parquet)**: Lưu trữ lâu dài toàn bộ raw trades và nến 1m, phân vùng theo `symbol` và `trade_date`.
  - **PostgreSQL Data Warehouse (Star Schema)**: Nạp liên tục qua streaming sink để phục vụ báo cáo quản trị, BI và truy vấn phân tích.
- **Giao diện Trực quan hóa Cao cấp**:
  - **Web Dashboard**: Biểu đồ TradingView tương tác, VWAP indicator, Radar cá mập, Scalping signals, và Data Warehouse Explorer kèm tính năng xuất file CSV.

---

## 🛠 3. Công nghệ sử dụng (Tech Stack)

| Thành phần | Công nghệ | Phiên bản | Vai trò trong hệ thống |
| :--- | :--- | :--- | :--- |
| **Data Ingestion** | Python `websocket-client` | 1.9+ | Kết nối WebSocket đa luồng tới sàn OKX |
| **Message Broker** | Apache Kafka (KRaft mode) | **3.9+** | Hàng đợi tin nhắn phân tán qua Docker (Port 9092) |
| **Stream Engine** | Apache Spark / PySpark | **3.5.1 LTS** | Xử lý Structured Streaming, Watermarking & Aggregations |
| **Java Runtime** | Eclipse Adoptium OpenJDK | **17 LTS** | Runtime tối ưu cho Spark & Hadoop Connector |
| **Hadoop Native** | Apache Hadoop WinUtils | **3.3.4** | Thư viện WinUtils & S3A FileSystem connector |
| **Data Lake** | AWS S3 / MinIO | Parquet Snappy | Kho lưu trữ dữ liệu lớn dạng cột (Columnar format) |
| **Data Warehouse** | PostgreSQL Container | **16 Alpine** | Cơ sở dữ liệu phân tích Star Schema (Port 5433) |
| **Web UI Backend** | Flask | 3.0+ | REST API & Data Mart Serving |
| **Web UI Frontend** | TradingView Lightweight Charts, Vanilla CSS | - | Biểu đồ nến tương tác & Dark Theme FinTech |
| **Serverless SQL** | Amazon Athena / DBeaver | - | Truy vấn SQL trực tiếp trên S3 Parquet Data Lake |

---

## 📂 4. Cấu trúc thư mục (Project Structure)

```text
crypto-bigdata/
├── .env.example              # File mẫu cấu hình môi trường
├── .env                      # File cấu hình biến môi trường (AWS Keys, S3, Kafka, Postgres)
├── .gitignore                # Danh sách file và thư mục loại trừ khỏi Git
├── docker-compose.yml        # Docker Compose chạy Kafka (9092) & PostgreSQL (5433)
├── requirements.txt          # Danh sách thư viện Python chuẩn hóa
├── README.md                 # Tài liệu hướng dẫn toàn diện hệ thống
│
├── configs/                  # FILE CẤU HÌNH PIPELINE & MACHINE LEARNING
│   ├── pipeline.yaml         # Cấu hình Ingestion, Kafka, Storage (Bronze/Silver/Gold)
│   └── ml.yaml               # Cấu hình XGBoost (Horizon 5m, Hyperparameters, Features)
│
├── ingestion/                # TẦNG THU THẬP DỮ LIỆU STREAMING
│   └── streaming/
│       └── okx_producer.py   # Thu thập OKX WebSocket (BTC-USDT) đẩy vào Kafka topic
│
├── processing/               # TẦNG XỬ LÝ PHÂN TÁN VÀ ĐỊNH LƯỢNG
│   ├── core/
│   │   ├── config.py         # Bộ nạp cấu hình hợp nhất (YAML + .env)
│   │   ├── schemas.py        # Định nghĩa StructType Schemas Medallion
│   │   └── spark_session.py  # Spark Session Builder (cấu hình S3A, Kafka, Postgres JDBC)
│   ├── streaming/
│   │   ├── bronze_writer.py  # Ghi luồng raw trades từ Kafka vào Bronze Layer (Parquet)
│   │   ├── kafka_consumer.py # Consumer helper cho BTC stream
│   │   └── trade_processor.py# Engine Spark Streaming (Clean, 1m Candles, Whale Alert)
│   ├── batch/
│   │   ├── bronze_to_silver.py # Làm sạch dữ liệu, chuẩn hóa thời gian, khử trùng lặp
│   │   └── silver_to_gold.py   # Tổng hợp nến 1m OHLCV, VWAP, Imbalance vào Gold
│   ├── features/
│   │   └── feature_engineering.py # Tính 18 chỉ số kỹ thuật & nhãn mục tiêu xu hướng 5m
│   └── analysis/
│       └── s3_parquet_reader.py   # Đọc S3 Parquet, phân tích thống kê & xuất CSV
│
├── storage/                  # TRỪU TƯỢNG HÓA TẦNG LƯU TRỮ
│   └── s3.py                 # Đường dẫn chuẩn hóa cho Bronze, Silver, Gold (S3 / Local)
│
├── ml/                       # HỌC MÁY (XGBOOST CLASSIFIER DỰ BÁO XU HƯỚNG GIÁ)
│   ├── dataset.py            # Chia tập dữ liệu thời gian 70/15/15 kèm vùng đệm Embargo
│   ├── train.py              # Huấn luyện mô hình XGBoost với Early Stopping
│   ├── evaluate.py           # Đánh giá Accuracy, Precision, Recall, F1, Baseline Lift
│   ├── predict.py            # Suy luận thời gian thực & lưu vào PostgreSQL
│   └── artifacts/            # Lưu trữ model JSON, schemas, metadata & kết quả test
│
├── warehouse/                # TẦNG DATA WAREHOUSE (POSTGRESQL STAR SCHEMA)
│   ├── sql/
│   │   ├── dim/              # dim_symbol.sql, dim_alert_tier.sql
│   │   ├── fact/             # fact_crypto_trades.sql, fact_crypto_1m_candles.sql,
│   │   │                     # fact_btc_predictions.sql, fact_btc_model_evaluation.sql
│   │   └── views/            # 10 Analytics Materialized/Streaming Views
│   └── etl/
│       ├── db_config.py      # Kết nối CSDL & khởi tạo bảng/view trong PostgreSQL
│       └── streaming_sink.py # Micro-batch Sink nạp nến 1m và giao dịch lớn vào DW
│
├── web/                      # NỀN TẢNG WEB ANALYTICS & TRADING DASHBOARD
│   ├── app.py                # Flask Backend API cung cấp nến, dự báo ML & DW Marts
│   ├── templates/index.html  # Giao diện FinTech Dark Theme với Tab Dự Báo ML
│   └── static/               # CSS & JavaScript điều khiển TradingView
│
├── scripts/                  # SCRIPTS ĐIỀU PHỐI VẬN HÀNH
│   ├── setup_env.ps1         # Script điều khiển toàn diện trên PowerShell (Windows)
│   ├── setup_env.sh          # Script cấu hình Bash (Linux/Mac/WSL)
│   ├── run_ingestion.py      # Chạy Ingestion WebSocket OKX -> Kafka
│   ├── run_streaming.py      # Chạy PySpark Structured Streaming Engine
│   ├── build_features.py     # Tạo đặc trưng kỹ thuật & tập dữ liệu phân đoạn
│   ├── train_model.py        # Huấn luyện và xuất artifacts mô hình XGBoost
│   └── run_prediction.py     # Chạy vòng lặp suy luận dự báo thời gian thực
│
├── tests/                    # KIỂM THỬ TỰ ĐỘNG & BENCHMARK
│   ├── test_pipeline_e2e.py  # Bộ kiểm thử tích hợp toàn diện 7/7 bài test
│   ├── test_warehouse.py     # Kiểm tra PostgreSQL Star Schema & độ trễ
│   └── test_s3.py            # Kiểm tra kết nối AWS S3 Data Lake
│
├── hadoop/                   # Thư viện winutils.exe & hadoop.dll cho môi trường Windows
├── pyproject.toml            # Cấu hình dự án & dependencies chuẩn PEP 621
├── requirements.txt          # Danh sách thư viện Python chuẩn hóa
└── data/                     # Checkpoints cục bộ, Lakehouse data và file xuất CSV
```

---

## ⚙️ 5. Cài đặt & Chuẩn bị môi trường

### Bước 1: Yêu cầu phần mềm
- **Python**: `3.10` đến `3.14`
- **Java**: **JDK 17 LTS** ([Eclipse Adoptium JDK 17](https://adoptium.net/temurin/releases/?version=17))
- **Docker Desktop**: Chạy Kafka và PostgreSQL Containers
- **Hadoop WinUtils (Windows)**: Đã tích hợp sẵn trong thư mục `hadoop/` hoặc cấu hình `C:\hadoop`

### Bước 2: Thiết lập môi trường ảo Python & Cài đặt thư viện
Mở PowerShell tại thư mục dự án `d:\crypto-bigdata`:
```powershell
# 1. Tạo môi trường ảo
python -m venv .venv

# 2. Kích hoạt môi trường ảo
.\.venv\Scripts\activate

# 3. Cài đặt toàn bộ thư viện cần thiết
pip install -r requirements.txt
```

### Bước 3: Cấu hình file `.env`
Sao chép từ file mẫu và điền thông tin AWS S3 của bạn:
```powershell
cp .env.example .env
```
Mở file `.env` và kiểm tra/điền các thông số:
```env
# AWS S3 Configuration
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=ap-southeast-1
S3_BUCKET_NAME=tên-bucket-của-bạn

# S3 Streaming Paths
S3_RAW_PATH=s3a://${S3_BUCKET_NAME}/crypto/streaming/trades
S3_PROCESSED_PATH=s3a://${S3_BUCKET_NAME}/crypto/streaming/candles_1m
```

---

## 🚀 6. Tổng hợp TẤT CẢ các lệnh vận hành hệ thống

---

### 6.1. Sử dụng PowerShell Orchestrator (`scripts/setup_env.ps1`)

Script [scripts/setup_env.ps1](file:///d:/crypto-bigdata/scripts/setup_env.ps1) cung cấp các tham số giúp quản trị toàn bộ hệ thống bằng một dòng lệnh duy nhất:

| Lệnh PowerShell | Chức năng chi tiết |
| :--- | :--- |
| `.\scripts\setup_env.ps1 -Info` | Kiểm tra trạng thái toàn bộ môi trường (Python, PySpark, JDK, Hadoop, Kafka, Postgres) |
| `.\scripts\setup_env.ps1 -DockerUp` | Khởi động Docker Containers (Kafka 9092 & PostgreSQL DW 5433) |
| `.\scripts\setup_env.ps1 -DockerDown` | Dừng toàn bộ Docker Containers |
| `.\scripts\setup_env.ps1 -InitWarehouse` | Tạo cấu trúc Star Schema DW (`dim_symbol`, `dim_alert_tier`, `fact_trades`, `fact_candles_1m`, `fact_btc_predictions`, `fact_btc_model_evaluation`) và 10 Views |
| `.\scripts\setup_env.ps1 -TestWarehouse` | Chạy bộ kiểm thử tự động Data Warehouse (đo độ trễ truy vấn < 100ms) |
| `.\scripts\setup_env.ps1 -InitS3` | Kiểm tra kết nối và khởi tạo cây thư mục Parquet trên AWS S3 Data Lake |
| `.\scripts\setup_env.ps1 -StreamPipeline` | **[1-Click]** Tự động bật Docker, mở Terminal Producer và Terminal Spark Streaming |
| `.\scripts\setup_env.ps1 -RunProducer` | Khởi chạy Ingestion Producer thu thập dữ liệu WebSocket OKX (BTC-USDT) |
| `.\scripts\setup_env.ps1 -RunSpark` | Khởi chạy Spark Structured Streaming Engine (làm sạch, nến 1m, ghi S3 & DW) |
| `.\scripts\setup_env.ps1 -BuildFeatures` | Trích xuất 18 đặc trưng vi mô/kỹ thuật và tạo nhãn xu hướng tương lai 5m |
| `.\scripts\setup_env.ps1 -TrainModel` | Huấn luyện mô hình XGBoost Classifier trên chuỗi thời gian (70% Train / 15% Val / 15% Test) |
| `.\scripts\setup_env.ps1 -RunPrediction` | Khởi chạy dịch vụ suy luận thời gian thực, lưu trữ dự báo vào PostgreSQL DW |
| `.\scripts\setup_env.ps1 -TestPipeline` | Chạy bộ kiểm thử tự động toàn diện (7/7 bài test passed 100%) |
| `.\scripts\setup_env.ps1 -WebDashboard` | Khởi chạy Web Trading & ML Analytics Dashboard (`http://127.0.0.1:5000`) |
| `.\scripts\setup_env.ps1 -Dashboard` | (Alias) Khởi chạy Web Trading & ML Analytics Dashboard |
| `.\scripts\setup_env.ps1 -ViewS3` | Mở Menu tương tác khám phá Parquet trên S3 Data Lake và xuất file CSV |

---

### 6.2. Vận hành luồng Streaming (1-Click hoặc 3 Terminals)

#### Cách A: 1-Click tự động hóa hoàn toàn
```powershell
.\scripts\setup_env.ps1 -StreamPipeline
```
> Script sẽ tự động bật Docker (Kafka & Postgres), khởi tạo Schema DW, và mở **2 cửa sổ PowerShell riêng biệt**: một cửa sổ chạy **OKX Producer** và một cửa sổ chạy **Spark Streaming Engine**.

---

#### Cách B: Khởi chạy thủ công trên 3 Terminals (Dễ theo dõi log từng bước)

##### 🟢 Terminal 1: Khởi động Kafka & PostgreSQL
```powershell
docker compose up -d
```

##### 🟢 Terminal 2: Chạy OKX Ingestion Producer
```powershell
.\.venv\Scripts\activate
python ingestion/streaming/okx_producer.py
```
> Producer sẽ kết nối WebSocket OKX chuyên biệt cho cặp coin `BTC-USDT` và đẩy message vào Kafka (partition 0) theo thời gian thực.

##### 🟢 Terminal 3: Chạy Spark Structured Streaming Engine
```powershell
.\.venv\Scripts\activate
python processing/streaming/trade_processor.py
```
> Spark sẽ đọc stream từ Kafka, làm sạch, khử trùng lặp qua Watermark, phát hiện cá mập tức thời, tính toán nến 1m (OHLCV, VWAP, Order Flow Imbalance) và đồng thời ghi vào **AWS S3 Parquet Data Lake** và **PostgreSQL Data Warehouse**.

---

### 6.3. Khởi tạo & Kiểm tra AWS S3 Data Lake (`tests/test_s3.py`)

Nếu bạn vừa tạo S3 bucket mới hoặc kiểm tra quyền đọc/ghi trên S3:

```powershell
# Chạy qua lệnh tiện ích:
.\scripts\setup_env.ps1 -InitS3

# Hoặc chạy trực tiếp script Python:
python tests/test_s3.py
```

**Cơ chế:** Script sẽ kết nối tới bucket S3 khai báo trong `.env`, ghi dữ liệu mẫu chuẩn hóa cho cặp `BTC-USDT` vào:
- `s3a://<bucket>/crypto/streaming/trades/symbol=BTC-USDT/`
- `s3a://<bucket>/crypto/streaming/candles_1m/symbol=BTC-USDT/`
Đồng thời đọc ngược lại từ S3 để xác nhận quyền Đọc/Ghi thành công 100%.

> [!NOTE]
> Hệ thống Lakehouse chuẩn hóa hiện tại vận hành theo mô hình **Medallion Architecture**:
> - **Bronze**: `crypto/bronze/trades/symbol=BTC-USDT/trade_date=YYYY-MM-DD/` (Dữ liệu thô từ Kafka)
> - **Silver**: `crypto/silver/trades/symbol=BTC-USDT/trade_date=YYYY-MM-DD/` (Dữ liệu sạch & khử trùng lặp)
> - **Gold**: `crypto/gold/candles_1m/symbol=BTC-USDT/` (Nến chuẩn 90 ngày `canonical_btc_1m.parquet` + nến stream vi mô)
> - **Models**: `crypto/models/v2.0.0/` (XGBoost Classifier + Checksums SHA-256)

---

### 6.4. Khám phá & Xuất dữ liệu S3 Data Lake (`s3_parquet_reader.py`)

Công cụ tương tác chuyên biệt giúp bạn kiểm tra các file Parquet đã ghi lên S3:

```powershell
.\scripts\setup_env.ps1 -ViewS3
# Hoặc: python processing/analysis/s3_parquet_reader.py
```

**Các tính năng tương tác:**
- **Phím 1**: Xem 10 nến 1 phút mới nhất (`crypto/streaming/candles_1m` hoặc `crypto/gold/candles_1m`).
- **Phím 2**: Xem 10 dòng khớp lệnh sạch mới nhất (`crypto/streaming/trades`).
- **Phím 3**: Xem danh sách giao dịch cá mập (Whale Trades >= $10K USD).
- **Phím 4**: Xuất nến 1 phút ra file CSV cục bộ.
- **Phím 5**: Xuất toàn bộ giao dịch sạch ra file CSV cục bộ.

---

### 6.5. Vận hành PostgreSQL Data Warehouse (`warehouse/`)

Hệ thống Data Warehouse được tinh giản và tối ưu hóa chuyên biệt cho **Pure Real-Time Streaming** theo mô hình **Star Schema**:
- **Bảng Chiều (Dimensions)**:
  - `dim_symbol`: Quản lý danh mục coin, topic Kafka, partition mapping (`BTC` -> 0, `ETH` -> 1, `SOL` -> 2), ngưỡng cảnh báo cá mập và độ chính xác giá.
  - `dim_alert_tier`: Danh mục phân cấp độ cảnh báo cá mập (`Mega Whale >= $500K`, `Big Whale $100K - $500K`, `Whale $50K - $100K`, `Large Order $10K - $50K`).
- **Bảng Sự Kiện (Fact Tables)**:
  - `fact_crypto_trades`: Lưu trữ các giao dịch lớn/cá mập streaming $\ge \$10,000$ kèm metadata độ trễ (`kafka_arrival_time`, `streaming_latency_sec`).
  - `fact_crypto_1m_candles`: Lưu trữ nến 1 phút sinh ra liên tục từ micro-batch của Spark Streaming (OHLC, VWAP, Trade Imbalance, Buy/Sell Volumes).
- **10 Views Phân Tích Streaming Tốc Độ Cao (Speed Layer Views)**:
  1. `vw_whale_trades`: Radar giao dịch cá mập theo phân cấp và độ trễ streaming.
  2. `vw_rolling_5m_metrics`: Cửa sổ trượt 5 phút phát hiện đà bứt phá (Momentum) & tín hiệu Scalping.
  3. `vw_order_flow_pressure`: Áp lực Mua/Bán, Cumulative Volume Delta (CVD) và Sentiment thị trường.
  4. `vw_streaming_volatility`: Đo lường biến động giá theo mô hình Parkinson High-Low.
  5. `vw_streaming_pipeline_telemetry`: Giám sát sức khỏe, độ trễ thời gian thực và trạng thái luồng streaming.
  6. `vw_hourly_market_summary`: Tổng hợp thị trường theo Giờ từ nến 1m.
  7. `vw_daily_market_summary`: Tổng hợp thị trường theo Ngày từ nến 1m.
  8. `vw_weekly_market_summary`: Tổng hợp thị trường theo Tuần.
  9. `vw_monthly_market_summary`: Tổng hợp thị trường theo Tháng.
  10. `vw_yearly_market_summary`: Tổng hợp thị trường theo Năm.

#### Khởi tạo CSDL và Kiểm thử tự động:
```powershell
# 1. Khởi tạo / Cập nhật DDL bảng và views:
.\scripts\setup_env.ps1 -InitWarehouse
# Hoặc chạy trực tiếp: python warehouse/etl/db_config.py

# 2. Chạy bộ kiểm thử tự động xác thực Data Warehouse:
.\scripts\setup_env.ps1 -TestWarehouse
# Hoặc chạy trực tiếp: python tests/test_warehouse.py
```

---

### 6.6. Web Trading & Quant Analytics Dashboard (Browser UI)

Nền tảng Web Dashboard chuyên nghiệp phát triển trên nền **Flask Backend** kết hợp thư viện đồ thị tài chính cao cấp **TradingView Lightweight Charts**:
- 🌐 **Địa chỉ truy cập**: `http://127.0.0.1:5000`
- 📊 **Tab 1 - Terminal Giao Dịch & Định Lượng**:
  - Biểu đồ nến TradingView thời gian thực mượt mà với đường giá trung bình gia quyền (**VWAP**) và Volume.
  - Thước đo cân bằng áp lực Mua / Bán (**Order Flow Imbalance Gauge**).
  - Tín hiệu lướt sóng Scalping & Cảnh báo Cá mập thời gian thực cho BTC, ETH, SOL.
- 🏛 **Tab 2 - Kho Dữ Liệu DW (Data Warehouse Explorer)**:
  - Xem dữ liệu bảng và view trong PostgreSQL (`fact_crypto_trades`, `fact_crypto_1m_candles`, `vw_whale_trades`,...).
  - Bộ lọc theo Symbol và phân trang linh hoạt.
  - **Nút "Xuất File CSV"** trực tiếp từ trình duyệt để mở ngay trong Excel hoặc Power BI.
- 📈 **Tab 3 - Báo Cáo Định Lượng & Cơ cấu Dòng tiền**:
  - Biểu đồ cơ cấu giá trị giao dịch Cá mập (Whale Turnover Breakdown).
  - Biểu đồ tỷ lệ Mua vs Bán chủ động của từng tài sản.
  - Bảng tổng hợp các chỉ số tài chính định lượng toàn diện.
- ⚙️ **Tab 4 - Giám Sát Kiến Trúc Pipeline**:
  - Sơ đồ trực quan luồng dữ liệu End-to-End từ OKX -> Kafka -> Spark -> S3 & Postgres DW.
  - Trạng thái kiểm tra sức khỏe hệ thống và phím tắt thao tác nhanh.

```powershell
# Khởi chạy Web Dashboard:
.\scripts\setup_env.ps1 -WebDashboard
# Hoặc chạy trực tiếp:
python web/app.py
```

---

### 6.7. Huấn luyện Mô hình XGBoost & Suy luận Dự báo Xu hướng BTC (5 Phút)

Hệ thống tích hợp một mô hình học máy duy nhất: **XGBoost Classifier (`from xgboost import XGBClassifier`)** để dự báo xác suất tăng giá của BTC-USDT trong chu kỳ **5 phút kế tiếp** ($t+5$):

* **Nhãn mục tiêu**:
  $$\text{future\_return} = \frac{\text{close}[t+5] - \text{close}[t]}{\text{close}[t]}$$
  $$\text{target} = \begin{cases} 1 & \text{nếu future\_return} > 0 \text{ (UP)} \\ 0 & \text{nếu future\_return} \le 0 \text{ (NOT\_UP)} \end{cases}$$
* **Quy tắc phân đoạn dữ liệu**:
  * Phân chia thời gian tuần tự (Chronological split): **70% Train**, **15% Validation**, **15% Test**.
  * Tuyệt đối không xáo trộn ngẫu nhiên (No random shuffle).
  * Vùng đệm cách ly (Embargo buffer) **5 phút** giữa các tập để loại trừ rò rỉ thông tin tương lai (Data Leakage).
* **Bộ 18 đặc trưng vi mô & chỉ báo kỹ thuật**:
  * Giá: `log_return`, `price_momentum_5m`, `high_low_range`
  * Khối lượng & Dòng tiền: `volume_change`, `buy_sell_ratio`, `trade_imbalance`, `vwap_deviation`
  * Kỹ thuật: `sma_5`, `sma_15`, `ema_12`, `ema_26`, `macd`, `macd_signal`, `bb_upper`, `bb_lower`, `bb_bandwidth`, `rsi_14`, `rolling_volatility_15m`

#### Các lệnh vận hành tầng Machine Learning:

```powershell
# 1. Trích xuất đặc trưng và kiểm tra phân đoạn dữ liệu:
.\scripts\setup_env.ps1 -BuildFeatures
# Hoặc: python scripts/build_features.py

# 2. Huấn luyện mô hình XGBoost và lưu artifacts:
.\scripts\setup_env.ps1 -TrainModel
# Hoặc: python scripts/train_model.py

# 3. Chạy dịch vụ suy luận thời gian thực (lưu vào PostgreSQL DW):
.\scripts\setup_env.ps1 -RunPrediction
# Chạy 1 lần duy nhất:
python scripts/run_prediction.py --once
```

#### Các tệp Artifacts sinh ra trong `ml/artifacts/`:
* `btc_xgboost_model.json`: Trọng số mô hình XGBoost chuẩn hóa.
* `feature_schema.json`: Danh sách và thứ tự 18 đặc trưng đầu vào.
* `model_metadata.json`: Phiên bản, tham số huấn luyện, khoảng thời gian dữ liệu.
* `evaluation_results.json`: Độ chính xác, ma trận nhầm lẫn (Confusion Matrix) và so sánh với Majority Baseline.

---

### 6.8. Truy vấn SQL trên Data Warehouse

Thông tin kết nối PostgreSQL Data Warehouse:
- **Host**: `127.0.0.1` (hoặc `localhost`) | **Port**: `5433`
- **Database**: `crypto_dw` | **Schema**: `warehouse`
- **User**: `postgres` | **Password**: `password123`

#### Các truy vấn SQL phân tích mẫu (chạy trên DBeaver / pgAdmin / DataGrip):

```sql
-- 1. Xem số lượng bản ghi trong từng bảng
SELECT 'fact_crypto_trades' AS table_name, COUNT(*) FROM warehouse.fact_crypto_trades
UNION ALL
SELECT 'fact_crypto_1m_candles' AS table_name, COUNT(*) FROM warehouse.fact_crypto_1m_candles
UNION ALL
SELECT 'dim_symbol' AS table_name, COUNT(*) FROM warehouse.dim_symbol;

-- 2. Truy vấn danh sách giao dịch cá mập lớn nhất (Whales >= $500,000)
SELECT * FROM warehouse.vw_whale_trades 
ORDER BY trade_value_usd DESC 
LIMIT 20;

-- 3. Phân tích thị trường theo khung giờ (Hourly Summary)
SELECT symbol, hour_bucket, open_price, high_price, low_price, close_price, total_volume, hourly_vwap, avg_trade_imbalance
FROM warehouse.vw_hourly_market_summary
ORDER BY hour_bucket DESC;

-- 4. Phân tích áp lực Mua vs Bán chủ động (Order Flow Pressure)
SELECT symbol, window_start, buy_volume, sell_volume, order_flow_status, trade_imbalance
FROM warehouse.vw_order_flow_pressure
ORDER BY window_start DESC
LIMIT 20;
```

---

### 6.9. Truy vấn Serverless SQL với Amazon Athena

Bạn có thể tạo bảng ngoại vi (External Tables) trên [Amazon Athena](https://ap-southeast-1.console.aws.amazon.com/athena/) để truy vấn trực tiếp trên S3 Parquet Data Lake mà không tốn tài nguyên máy chủ:

```sql
-- 1. Bảng External Table cho Nến 1 phút
CREATE EXTERNAL TABLE IF NOT EXISTS crypto_streaming_candles_1m (
  window_start timestamp,
  window_end timestamp,
  open double,
  high double,
  low double,
  close double,
  volume double,
  trade_count bigint,
  buy_volume double,
  sell_volume double,
  vwap double,
  trade_imbalance double,
  total_trade_value double
)
PARTITIONED BY (symbol string)
STORED AS PARQUET
LOCATION 's3://your-bucket-name/crypto/streaming/candles_1m/';

-- 2. Bảng External Table cho Giao dịch sạch (Cleaned Trades)
CREATE EXTERNAL TABLE IF NOT EXISTS crypto_streaming_trades (
  trade_id string,
  price double,
  size double,
  side string,
  trade_value double,
  trade_time timestamp,
  raw_timestamp bigint,
  kafka_partition int,
  kafka_offset bigint,
  kafka_arrival_time timestamp
)
PARTITIONED BY (symbol string, trade_date string)
STORED AS PARQUET
LOCATION 's3://your-bucket-name/crypto/streaming/trades/';

-- 3. Quét nhận diện Partitions
MSCK REPAIR TABLE crypto_streaming_candles_1m;
MSCK REPAIR TABLE crypto_streaming_trades;

-- 4. Truy vấn nến mới nhất
SELECT symbol, window_start, open, high, low, close, volume, vwap, trade_imbalance
FROM crypto_streaming_candles_1m
ORDER BY window_start DESC
LIMIT 25;
```

---

## 📈 7. Các chỉ số tài chính & Dòng tiền được tính toán

Engine Spark Structured Streaming liên tục tổng hợp các chỉ số theo cửa sổ trượt 1 phút (`1-Minute Tumbling Window`):

| Chỉ số | Công thức / Thuật toán | Ý nghĩa & Ứng dụng thực tế |
| :--- | :--- | :--- |
| **OHLC** | `First(P), Max(P), Min(P), Last(P)` | Biểu đồ nến Nhật phân tích biến động giá trong 1 phút |
| **Volume** | $\sum \text{Size}$ | Tổng khối lượng coin được giao dịch trong 1 phút |
| **Buy / Sell Volume** | $\sum_{\text{side='buy'}} \text{Size}$ vs $\sum_{\text{side='sell'}} \text{Size}$ | Tách bạch khối lượng phe Mua chủ động (Taker Buy) và phe Bán chủ động (Taker Sell) |
| **VWAP** | $\frac{\sum (\text{Price} \times \text{Size})}{\sum \text{Size}}$ | **Volume-Weighted Average Price**: Mức giá bình quân gia quyền theo khối lượng - chỉ báo cốt lõi của các quỹ đầu tư định lượng |
| **Trade Imbalance** | $\frac{\text{BuyVol} - \text{SellVol}}{\text{BuyVol} + \text{SellVol}}$ | Biên độ chênh lệch dòng tiền từ `-1.0` (áp đảo bán xả) đến `+1.0` (áp đảo mua gom) |
| **Total Trade Value** | $\sum (\text{Price} \times \text{Size})$ | Tổng giá trị thanh khoản luân chuyển bằng USDT |
| **Whale Alert** | $\text{Trade Value} \ge \$500,000$ | Phát hiện các lệnh giao dịch lớn của cá mập / tổ chức tức thời |

---

## 🔧 8. Xử lý sự cố thường gặp (Troubleshooting)

### 1. Xung đột Port PostgreSQL `5432` trên Windows
- **Hiện tượng**: Báo lỗi port 5432 đã được sử dụng khi chạy `docker compose up`.
- **Giải pháp**: Dự án đã cấu hình container PostgreSQL ánh xạ sang cổng **`5433`** (`5433:5432`). Mọi kết nối đến Database trong code và DBeaver đều trỏ tới cổng `5433`.

### 2. Lỗi tương thích Hadoop AWS với phiên bản Spark
- **Hiện tượng**: Báo lỗi `NumberFormatException: For input string: "60s"` khi Spark kết nối tới S3.
- **Giải pháp**: Dự án đã được chuẩn hóa về `pyspark==3.5.1` và `hadoop-aws:3.3.4` với `aws-java-sdk-bundle:1.12.262`. Không nâng cấp lên PySpark 4.x.

### 3. Lỗi Unicode `charmap codec can't encode character` trên PowerShell
- **Hiện tượng**: PowerShell hiển thị lỗi font hoặc không in được ký tự UTF-8.
- **Giải pháp**: Script `setup_env.ps1` và các file Python đều tích hợp `[Console]::OutputEncoding = [System.Text.Encoding]::UTF8` và `PYTHONIOENCODING=utf-8`.

### 4. S3 Bucket vừa tạo bị trống hoặc báo lỗi Path NotFound
- **Hiện tượng**: S3 chưa có thư mục nào sau khi tạo bucket rỗng trên AWS.
- **Giải pháp**: Chạy lệnh `.\scripts\setup_env.ps1 -InitS3` hoặc `python tests/test_s3.py`. Script sẽ tự động tạo toàn bộ cấu trúc thư mục phân vùng và ghi dữ liệu mẫu chuẩn lên S3.