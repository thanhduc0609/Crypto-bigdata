# 🚀 Crypto Big Data Streaming Pipeline & Data Warehouse

Pipeline thu thập, xử lý và phân tích dữ liệu giao dịch Cryptocurrency thời gian thực (Real-time Streaming) từ sàn **OKX**, truyền tải qua **Apache Kafka**, xử lý & biến đổi dữ liệu với **Spark Structured Streaming**, lưu trữ vào **AWS S3 Data Lake** dưới định dạng chuẩn **Parquet**, và nạp vào **PostgreSQL Data Warehouse (Star Schema)** phục vụ phân tích chuyên sâu.

> 📄 **Xem báo cáo đồ án chi tiết (theo 7 yêu cầu chuẩn):** [README_REPORT.md](file:///d:/crypto-bigdata/README_REPORT.md)

---

## 📌 Mục lục
1. [Kiến trúc hệ thống (Architecture)](#-1-kiến-trúc-hệ-thống-architecture)
2. [Các tính năng chính](#-2-các-tính-năng-chính)
3. [Công nghệ sử dụng (Tech Stack)](#-3-công-nghệ-sử-dụng-tech-stack)
4. [Cấu trúc thư mục (Project Structure)](#-4-cấu-trúc-thư-mục-project-structure)
5. [Cài đặt & Chuẩn bị môi trường](#-5-cài-đặt--chuẩn-bị-môi-trường)
6. [Tổng hợp TẤT CẢ các lệnh chạy trong hệ thống](#-6-tổng-hợp-tất-cả-các-lệnh-chạy-trong-hệ-thống)
   - [6.1. Sử dụng PowerShell Helper Script (`scripts/setup_env.ps1`)](#61-sử-dụng-powershell-helper-script-scriptssetup_envps1)
   - [6.2. Chạy Pipeline Streaming thời gian thực (3 Terminals)](#62-chạy-pipeline-streaming-thời-gian-thực-3-terminals)
   - [6.3. Đọc, tra cứu & Xuất dữ liệu từ S3 Data Lake (`spark/s3_parquet_reader.py`)](#63-đọc-tra-cứu--xuất-dữ-liệu-từ-s3-data-lake-sparks3_parquet_readerpy)
   - [6.4. Vận hành Data Warehouse & ETL (`warehouse/`)](#64-vận-hành-data-warehouse--etl-warehouse)
   - [6.5. Trực quan hóa & Phân tích chuyên sâu trên Web Dashboard (TradingView & DW Explorer)](#65-trực-quan-hóa--phân-tích-chuyên-sâu-trên-web-dashboard-tradingview--dw-explorer)
   - [6.6. Trực quan hóa dữ liệu trực tiếp trên Terminal Dashboard (TUI)](#66-trực-quan-hóa-dữ-liệu-trực-tiếp-trên-terminal-dashboard-tui)
   - [6.7. Truy vấn & Khai thác dữ liệu trên PostgreSQL Data Warehouse](#67-truy-vấn--khai-thác-dữ-liệu-trên-postgresql-data-warehouse)
   - [6.8. Truy vấn Serverless SQL với Amazon Athena](#68-truy-vấn-serverless-sql-với-amazon-athena)
7. [Các chỉ số tài chính được tính toán (Market Metrics)](#-7-các-chỉ-số-tài-chính-được-tính-toán)
8. [Xử lý sự cố thường gặp (Troubleshooting)](#-8-xử-lý-sự-cố-thường-gặp-troubleshooting)

---

## 🏛 1. Kiến trúc hệ thống (Architecture)

```text
       ┌─────────────────────────────────────────────────────────┐
       │                OKX WebSocket Market Data                │
       │           (BTC-USDT, ETH-USDT, SOL-USDT)                │
       └────────────────────────────┬────────────────────────────┘
                                    │ Real-time Stream
                                    ▼
       ┌─────────────────────────────────────────────────────────┐
       │             Python Multi-Stream Producer                │
       │                (producer/okx_producer.py)               │
       └────────────────────────────┬────────────────────────────┘
                                    │ Partition Key = Symbol
                                    ▼
       ┌─────────────────────────────────────────────────────────┐
       │             Apache Kafka (crypto-trades)                │
       │       Partition 0         Partition 1     Partition 2   │
       │       (BTC-USDT)          (ETH-USDT)      (SOL-USDT)    │
       └────────────────────────────┬────────────────────────────┘
                                    │ Structured Streaming Source
                                    ▼
       ┌─────────────────────────────────────────────────────────┐
       │         Apache Spark Structured Streaming (3.5.1)       │
       │                (spark/trade_processor.py)               │
       │  ┌───────────────────────────────────────────────────┐  │
       │  │ • Schema Validation & Cleaning (Lọc Null, Outliers)│  │
       │  │ • Event-time Watermarking & Deduplication         │  │
       │  │ • 1-Minute Window Aggregations (OHLC, VWAP,...)   │  │
       │  └───────────────────────────────────────────────────┘  │
       └──────────────┬───────────────────────────┬──────────────┘
                      │                           │
         Raw Cleaned Trades Parquet      1-Minute Aggregated Parquet
                      │                           │
                      ▼                           ▼
       ┌─────────────────────────────────────────────────────────┐
       │               AWS S3 / MinIO Data Lake                  │
       │  s3a://<BUCKET>/crypto/raw/trades/                      │
       │  s3a://<BUCKET>/crypto/processed/trades_1m/             │
       │  s3a://<BUCKET>/checkpoints/                            │
       └──────────────┬───────────────────────────┬──────────────┘
                      │                           │
           PySpark Batch Reader             PySpark ETL Pipeline
                      │                           │
                      ▼                           ▼
       ┌────────────────────────┐  ┌─────────────────────────────┐
       │  S3 Parquet CLI Tool   │  │  PostgreSQL Data Warehouse  │
       │(spark/s3_parquet_reader│  │        (Star Schema)        │
       │  • Interactive Menu    │  │  • dim_symbol               │
       │  • Filter / Stats      │  │  • fact_crypto_trades       │
       │  • Export Local CSV    │  │  • fact_crypto_1m_candles   │
       └────────────────────────┘  │  • Analytics Views (Whales, │
                                   │    Hourly Summary, Imbalance│
                                   └──────────────┬──────────────┘
                                                  │
                                                  ▼
                                   ┌─────────────────────────────┐
                                   │    BI & Visualization       │
                                   │  Power BI / Grafana / Metabase
                                   └─────────────────────────────┘
```

---

## ✨ 2. Các tính năng chính

- **Real-time Ingestion**: Thu thập luồng giao dịch trực tiếp từ WebSocket của OKX với độ trễ tính bằng mili-giây.
- **Kafka Partitioning**: Phân luồng song song theo từng cặp giao dịch (`BTC-USDT`, `ETH-USDT`, `SOL-USDT`) vào các Partition riêng biệt.
- **Data Cleaning & Quality**:
  - Loại bỏ các bản ghi null, giá `<= 0`, khối lượng `<= 0`.
  - Chuẩn hóa phân loại `side` (`buy` / `sell`).
  - Event-time processing với cơ chế **Watermarking** và **Khử trùng lặp (`dropDuplicates`)**.
- **Financial Feature Engineering**:
  - Tính toán nến giá 1 phút (**OHLC**: Open, High, Low, Close).
  - Khối lượng giao dịch phân tách (**Buy Volume**, **Sell Volume**, **Total Volume**, **Trade Count**).
  - Khối lượng giá trung bình gia quyền (**VWAP** - Volume-Weighted Average Price).
  - Mức độ mất cân bằng lệnh thị trường (**Trade Imbalance**).
- **Enterprise Data Lake (S3)**: Lưu trữ định dạng nén tối ưu **Parquet** (Partition theo `symbol` và `trade_date`).
- **PostgreSQL Data Warehouse (Star Schema)**: Đồng bộ ETL tự động từ S3 Data Lake vào CSDL phân tích quan hệ với các View nghiệp vụ nâng cao (Whale tracking, Order flow pressure).

---

## 🛠 3. Công nghệ sử dụng (Tech Stack)

| Thành phần | Công nghệ | Phiên bản | Ghi chú |
| :--- | :--- | :--- | :--- |
| **Data Ingestion** | Python `websocket-client`, `kafka-python` | 3.10+ | Kết nối WebSocket sàn OKX |
| **Message Broker** | Apache Kafka (KRaft mode) | **3.9+** | Triển khai qua Docker (Port 9092) |
| **Stream Engine** | Apache Spark / PySpark | **3.5.1 LTS** | Structured Streaming & Batch ETL |
| **Java Runtime** | Eclipse Adoptium OpenJDK | **17 LTS** | Runtime cho Spark & Hadoop |
| **Hadoop Native** | Apache Hadoop WinUtils | **3.3.4** | Tương thích Windows & S3A Connector |
| **Data Lake** | AWS S3 / MinIO | Snappy Parquet | Phân vùng theo `symbol` và `trade_date` |
| **Data Warehouse** | PostgreSQL Container | **16 Alpine** | Chạy trên Port **5433** (Star Schema) |
| **BI & Analytics** | Power BI / DBeaver / Athena | - | Khai thác trực quan & SQL Analytics |

---

## 📂 4. Cấu trúc thư mục (Project Structure)

```text
crypto-bigdata/
├── .env.example              # Mẫu cấu hình môi trường
├── .env                      # File cấu hình bí mật (Kafka, AWS Keys, S3, PostgreSQL)
├── .gitignore                # Danh sách file loại trừ khỏi Git
├── docker-compose.yml        # Docker Compose (Apache Kafka:9092 & PostgreSQL DW:5433)
├── requirements.txt          # Danh sách thư viện Python chuẩn hóa
├── README.md                 # Tài liệu hướng dẫn toàn diện
│
├── ingestion/                # TẦNG THU THẬP DỮ LIỆU (INGESTION LAYER)
│   ├── streaming/
│   │   └── okx_producer.py   # Thu thập OKX WebSocket & đẩy vào Kafka topic
│   └── batch/
│       └── okx_derivatives_fetcher.py # Cào Funding Rate, OI, Long/Short qua REST API
│
├── processing/               # TẦNG XỬ LÝ DỮ LIỆU (SPARK PROCESSING)
│   ├── core/
│   │   ├── schemas.py        # Schema StructType dùng chung
│   │   └── spark_session.py  # Khởi tạo Spark Session chuẩn (S3, JDBC, Hadoop)
│   ├── streaming/
│   │   └── trade_processor.py # Real-time stream (Clean, 1m Candlestick, Whale Alert, S3)
│   └── batch/
│       ├── s3_parquet_reader.py       # Đọc Parquet S3, phân tích thống kê & xuất CSV
│       ├── batch_analytics_engine.py  # Tổng hợp nến Đa khung thời gian (Ngày/Tuần/Tháng/Năm) & Xuất CSV
│       └── market_predictor.py        # Mô hình Lượng hóa Dự báo Chu kỳ kế tiếp (Quant Predictive)
│
├── warehouse/                # TẦNG DATA WAREHOUSE & ANALYTICS MART
│   ├── sql/
│   │   ├── dim/
│   │   │   └── dim_symbol.sql
│   │   ├── fact/
│   │   │   ├── fact_crypto_trades.sql
│   │   │   ├── fact_crypto_1m_candles.sql
│   │   │   ├── fact_crypto_funding_rates.sql
│   │   │   ├── fact_crypto_open_interest.sql
│   │   │   ├── fact_crypto_long_short_ratio.sql
│   │   │   └── fact_crypto_liquidations.sql
│   │   └── views/
│   │       └── 02_analytics_views.sql # Views phân tích (Whales, Hourly, Order Flow)
│   └── etl/
│       ├── db_config.py      # Quản lý kết nối & tự động tạo DDL trong PostgreSQL
│       └── s3_to_warehouse.py # PySpark ETL đọc S3 Parquet nạp vào PostgreSQL qua JDBC
│
├── tests/                    # KIỂM THỬ TỰ ĐỘNG
│   ├── test_kafka_consumer.py
│   ├── test_spark_streaming.py
│   └── test_s3.py
│
├── data/
│   └── exports/              # Thư mục chứa các file CSV xuất ra để mở Excel / Power BI
├── scripts/
│   ├── setup_env.ps1         # Script điều khiển toàn diện trên PowerShell (Windows)
│   └── setup_env.sh          # Script cấu hình Bash (Linux/Mac/WSL)
└── hadoop/                   # Thư mục winutils / hadoop.dll
```

---

## ⚙️ 5. Cài đặt & Chuẩn bị môi trường

### Bước 1: Yêu cầu hệ thống
- **Python**: `3.10` - `3.12`
- **Java**: **JDK 17 LTS** ([Eclipse Adoptium JDK 17](https://adoptium.net/temurin/releases/?version=17))
- **Docker Desktop**: Chạy Kafka và PostgreSQL
- **Hadoop Winutils (cho Windows)**: Thư mục `C:\hadoop` (chứa `bin/winutils.exe` và `bin/hadoop.dll`)

### Bước 2: Tạo môi trường ảo Python & Cài đặt thư viện
Mở PowerShell tại thư mục dự án:
```powershell
# 1. Tạo môi trường ảo
python -m venv .venv

# 2. Kích hoạt môi trường ảo
.\.venv\Scripts\activate

# 3. Cài đặt toàn bộ dependencies
pip install -r requirements.txt
```

### Bước 3: Cấu hình file `.env`
Sao chép từ file mẫu và điền thông tin AWS của bạn:
```powershell
cp .env.example .env
```

---

## 🚀 6. Tổng hợp TẤT CẢ các lệnh chạy trong hệ thống

---

### 6.1. Sử dụng PowerShell Helper Script (`scripts/setup_env.ps1`)

Script [scripts/setup_env.ps1](file:///d:/crypto-bigdata/scripts/setup_env.ps1) cung cấp các phím tắt nhanh để quản trị toàn bộ hệ thống chỉ với một dòng lệnh:

| Lệnh PowerShell | Chức năng |
| :--- | :--- |
| `.\scripts\setup_env.ps1 -Info` | Kiểm tra toàn bộ phiên bản (Python, PySpark, JDK, Hadoop, Kafka, Docker) |
| `.\scripts\setup_env.ps1 -DockerUp` | Khởi động Docker containers (Kafka Broker + PostgreSQL Data Warehouse) |
| `.\scripts\setup_env.ps1 -DockerDown` | Dừng toàn bộ Docker containers |
| `.\scripts\setup_env.ps1 -RunProducer` | Khởi chạy Ingestion Producer thu thập dữ liệu WebSocket sàn OKX |
| `.\scripts\setup_env.ps1 -RunSpark` | Khởi chạy Spark Structured Streaming Engine (kèm Whale Alert & S3 Sink) |
| `.\scripts\setup_env.ps1 -WebDashboard` | Khởi chạy Web Analytics & Trading Dashboard trên trình duyệt (`http://127.0.0.1:5000`) |
| `.\scripts\setup_env.ps1 -Dashboard` | Mở Terminal Analytics TUI Dashboard trực tiếp trên PowerShell / CMD |
| `.\scripts\setup_env.ps1 -ViewS3` | Mở Menu tương tác trực quan đọc và phân tích dữ liệu trên S3 Data Lake |
| `.\scripts\setup_env.ps1 -InitWarehouse` | Khởi tạo Schema DDL (dim, fact) và các View phân tích trong PostgreSQL |
| `.\scripts\setup_env.ps1 -RunETL` | Chạy pipeline ETL PySpark đồng bộ dữ liệu từ S3 Parquet vào PostgreSQL |

---

### 6.2. Chạy Pipeline Streaming thời gian thực (3 Terminals)

Để chạy luồng dữ liệu thời gian thực từ sàn OKX lên AWS S3 Data Lake, hãy mở **3 cửa sổ PowerShell**:

#### 🟢 Terminal 1: Khởi động Kafka & PostgreSQL
```powershell
docker compose up -d
```

---

#### 🟢 Terminal 2: Chạy OKX Ingestion Producer
```powershell
.\.venv\Scripts\activate
python ingestion/streaming/okx_producer.py
```
> Producer sẽ kết nối WebSocket OKX cho 3 cặp `BTC-USDT`, `ETH-USDT`, `SOL-USDT` và đẩy message vào Kafka theo thời gian thực.

---

#### 🟢 Terminal 3: Chạy Spark Structured Streaming Engine
```powershell
.\.venv\Scripts\activate
python processing/streaming/trade_processor.py
```
> Spark sẽ đọc stream từ Kafka, làm sạch, khử trùng lặp, phát hiện cá mập tức thời (Real-time Whale Alert), tính toán nến OHLCV 1m + VWAP + Imbalance và ghi trực tiếp vào AWS S3 Parquet Data Lake.

---

#### 🔍 Kiểm tra dữ liệu trong Kafka (Tùy chọn):
Nếu muốn xem trực tiếp các bản tin JSON thô vừa đẩy vào Kafka:
```powershell
python tests/test_kafka_consumer.py
```

---

---

### 6.3. Luồng Batch Analytics & Xuất File CSV Đa Khung Thời Gian (`processing/batch/`)

Hệ thống cung cấp module **Batch Analytics Engine** ([batch_analytics_engine.py](file:///d:/crypto-bigdata/processing/batch/batch_analytics_engine.py)) chuyên biệt để phân tích dữ liệu lịch sử và tự động xuất file CSV:

#### A. Chạy Batch Analytics & Tự động xuất CSV (Ngày, Tuần, Tháng):
```powershell
# 1 Click: Chạy trọn gói Backfill -> Cào phái sinh -> Batch Analytics & Xuất CSV:
.\scripts\setup_env.ps1 -BatchPipeline

# Hoặc chạy riêng Batch Analytics Engine (tự động xuất CSV vào data/exports/):
.\scripts\setup_env.ps1 -BatchAnalytics

# Hoặc chỉ xuất trực tiếp CSV Ngày / Tuần / Tháng từ S3:
.\scripts\setup_env.ps1 -ExportCSV
```

#### B. Sử dụng lệnh Python với các tùy chọn nâng cao:
```powershell
# Phân tích toàn bộ Ngày, Tuần, Tháng:
python processing/batch/batch_analytics_engine.py --timeframe all

# Phân tích riêng theo Ngày cho một đồng coin cụ thể (BTC-USDT):
python processing/batch/batch_analytics_engine.py --timeframe day --symbol BTC-USDT

# Phân tích đồng thời upload file CSV trực tiếp lên S3 (s3a://<bucket>/crypto/exports/csv/):
python processing/batch/batch_analytics_engine.py --timeframe all --upload-s3
```

#### C. Menu tương tác khám phá S3 Data Lake (`s3_parquet_reader.py`):
```powershell
.\scripts\setup_env.ps1 -ViewS3
```
- Phím **7**: Xuất nến 1 phút ra CSV (`candles_1m_all.csv`)
- Phím **8**: Xuất Raw Trades ra CSV (`cleaned_raw_trades_all.csv`)
- Phím **9**: Xuất nến Ngày / Tuần / Tháng ra CSV (`summary_daily_all.csv`, `summary_weekly_all.csv`, `summary_monthly_all.csv`)


#### E. Chạy truy vấn Spark SQL tùy ý trực tiếp trên S3 Parquet:
```powershell
# Truy vấn giá trung bình và tổng khối lượng:
python spark/s3_parquet_reader.py --sql "SELECT symbol, round(avg(close), 2) AS avg_close, round(sum(volume), 4) AS total_vol FROM trades_1m GROUP BY symbol"

# Tìm top 10 giao dịch có giá trị lớn nhất (Whales):
python spark/s3_parquet_reader.py --sql "SELECT trade_id, symbol, side, price, size, trade_value FROM raw_trades ORDER BY trade_value DESC LIMIT 10"
```

---

### 6.4. Vận hành Data Warehouse & ETL (`warehouse/`)

Hệ thống Data Warehouse được xây dựng theo mô hình **Star Schema** trên PostgreSQL.

#### A. Khởi tạo Schema DDL và Views trong PostgreSQL:
```powershell
# Cách 1: Sử dụng setup script
.\scripts\setup_env.ps1 -InitWarehouse

# Cách 2: Sử dụng lệnh Python trực tiếp
.\.venv\Scripts\python.exe -m warehouse.etl.s3_to_warehouse --init-db
```

#### B. Chạy ETL nạp dữ liệu từ S3 Parquet vào PostgreSQL:
```powershell
# Cách 1: Nạp toàn bộ dữ liệu (Raw Trades + 1m Candles) qua setup script
.\scripts\setup_env.ps1 -RunETL

# Cách 2: Nạp toàn bộ dữ liệu bằng lệnh Python
.\.venv\Scripts\python.exe -m warehouse.etl.s3_to_warehouse --type all

# Cách 3: Nạp riêng bảng giao dịch thô (Raw Trades) kèm giới hạn số dòng:
.\.venv\Scripts\python.exe -m warehouse.etl.s3_to_warehouse --type raw --limit 500

# Cách 4: Nạp riêng bảng nến 1 phút:
.\.venv\Scripts\python.exe -m warehouse.etl.s3_to_warehouse --type 1m
```

---

### 6.5. Trực quan hóa & Phân tích chuyên sâu trên Web Dashboard (TradingView & DW Explorer)

Nền tảng Web Dashboard định lượng hiện đại phát triển bằng **Flask Backend**, giao diện Dark Theme FinTech với **TradingView Lightweight Charts** và **Chart.js**:
- 🌐 **Địa chỉ truy cập**: `http://127.0.0.1:5000`
- 📊 **Tab 1 - Terminal Giao Dịch & Định Lượng**:
  - Biểu đồ nến TradingView tương tác mượt mà với đường giá trung bình gia quyền (**VWAP**) và biểu đồ cột Volume.
  - Hỗ trợ chuyển đổi linh hoạt đa khung thời gian (`1m`, `5m`, `15m`, `1H`, `1D`).
  - Nút chuyển đổi nguồn dữ liệu: **⚡ Live Stream (OKX Feed)** hoặc **🏛 PostgreSQL Data Warehouse**.
  - Thước đo cán cân áp lực Mua / Bán (**Order Flow Imbalance Gauge**) và phân loại tâm lý thị trường.
  - Chỉ số thị trường Phái sinh: Funding Rate (8h), APY dự phóng năm, Khối lượng mở Open Interest ($ và số hợp đồng).
  - Radar Cá mập thời gian thực với phân loại cấp độ (Mega Whale > $50K, Whale $10K-$50K, Large Order $5K-$10K).
- 🏛 **Tab 2 - Kho Dữ Liệu DW (Data Warehouse & Marts Explorer)**:
  - Thống kê toàn diện dung lượng CSDL và số bản ghi trong từng bảng / view.
  - Trình duyệt dữ liệu tương tác cho 8 bảng / view: `vw_whale_trades`, `vw_hourly_market_summary`, `vw_daily_market_summary`, `vw_weekly_market_summary`, `vw_order_flow_pressure`, `fact_crypto_trades`, `fact_crypto_1m_candles`, `dim_symbol`.
  - Bộ lọc theo Symbol (BTC, ETH, SOL), phân trang linh hoạt (25, 50, 100 dòng).
  - **Nút "Xuất File CSV"** trực tiếp từ trình duyệt để mở ngay trong Excel hoặc Power BI.
- 📈 **Tab 3 - Báo Cáo Định Lượng (Quant Analytics Visualizations)**:
  - Biểu đồ tròn cơ cấu giá trị giao dịch Cá mập (Whale Turnover Breakdown).
  - Biểu đồ cột so sánh tổng giá trị giao dịch (Turnover USD) giữa BTC, ETH, SOL.
  - Biểu đồ thanh tỷ lệ Mua vs Bán chủ động của từng tài sản.
  - Biểu đồ so sánh Độ biến động (Volatility %) & Tỷ suất sinh lời (Return %).
  - Bảng tổng hợp các chỉ số tài chính định lượng toàn diện.
- ⚙️ **Tab 4 - Giám Sát Kiến Trúc Pipeline (Architecture & Telemetry)**:
  - Sơ đồ trực quan luồng dữ liệu End-to-End từ OKX WebSocket -> Kafka -> Spark Streaming -> S3 Parquet -> PostgreSQL DW -> Web Terminal.
  - Trạng thái kiểm tra sức khỏe và thông số kỹ thuật chi tiết của từng thành phần trong hệ thống.
  - Bảng tổng hợp các lệnh PowerShell phím tắt tiện lợi.

```powershell
# Khởi chạy Web Dashboard:
.\scripts\setup_env.ps1 -WebDashboard
# Hoặc chạy trực tiếp:
python web/app.py
```

---

### 6.6. Trực quan hóa dữ liệu trực tiếp trên Terminal Dashboard (TUI)

Màn hình trực quan hóa định lượng dạng **Terminal User Interface (TUI)** theo phong cách **Bloomberg Terminal**, hiển thị trực quan tức thì ngay trên PowerShell / CMD mà không cần trình duyệt:
- **Thước đo Áp lực Mua/Bán (Order Flow Gauge Bar)**: Trực quan hóa tỷ lệ Mua/Bán và cán cân lệnh `trade_imbalance` $\in [-1, +1]$.
- **Ma trận thị trường & VWAP**: Giá mới nhất, biểu đồ nến mini Sparkline (` ▂▃▄▅▆▇█`), tỷ suất lợi nhuận Return %, độ biến động Volatility %, đường VWAP và đánh giá vị thế so với VWAP.
- **Radar Cá mập (Live Whale Alerts)**: Bảng xếp hạng các lệnh lớn nhất phân cấp theo tier (🚨 Mega Whale, ⚡ Big Whale, 🐋 Whale).
- **Tâm lý Phái sinh (Derivatives & Sentiment Feed)**: Tỷ lệ Funding Rate (8h), APY dự phóng năm, Khối lượng vị thế mở Open Interest (OI).

```powershell
# Cách 1: Mở màn hình Live Dashboard (tự động làm mới mỗi 3 giây)
.\scripts\setup_env.ps1 -Dashboard
# Hoặc:
python processing/analysis/terminal_dashboard.py

# Cách 2: Trải nghiệm ngay ở chế độ dữ liệu mô phỏng (Demo Mode)
.\scripts\setup_env.ps1 -Dashboard -Demo
# Hoặc:
python processing/analysis/terminal_dashboard.py --demo

# Cách 3: Xem ảnh chụp nhanh phân tích 1 lần rồi thoát (Snapshot Mode)
python processing/analysis/terminal_dashboard.py --once
```

---

### 6.7. Truy vấn & Khai thác dữ liệu trên PostgreSQL Data Warehouse

Thông tin kết nối Data Warehouse:
- **Host**: `127.0.0.1` (hoặc `localhost`)
- **Port**: `5433`
- **Database**: `crypto_dw`
- **User**: `postgres`
- **Password**: `password123`
- **Schema**: `warehouse`

#### Các truy vấn SQL phân tích mẫu (chạy trên DBeaver / pgAdmin / psql):

```sql
-- 1. Xem số lượng bản ghi trong từng bảng
SELECT 'fact_crypto_trades' AS table_name, COUNT(*) FROM warehouse.fact_crypto_trades
UNION ALL
SELECT 'fact_crypto_1m_candles' AS table_name, COUNT(*) FROM warehouse.fact_crypto_1m_candles
UNION ALL
SELECT 'dim_symbol' AS table_name, COUNT(*) FROM warehouse.dim_symbol;

-- 2. Khai thác View: Top các giao dịch cá mập (> $50,000)
SELECT * FROM warehouse.vw_whale_trades 
ORDER BY trade_value DESC 
LIMIT 20;

-- 3. Khai thác View: Thống kê thị trường theo khung giờ (Hourly Summary)
SELECT symbol, hour_bucket, total_volume, vwap, highest_price, lowest_price, total_turnover_usd
FROM warehouse.vw_hourly_market_summary
ORDER BY hour_bucket DESC;

-- 4. Khai thác View: Phân tích áp lực Mua vs Bán chủ động (Order Flow Pressure)
SELECT symbol, window_start, buy_volume, sell_volume, order_flow_status, trade_imbalance
FROM warehouse.vw_order_flow_pressure
ORDER BY window_start DESC
LIMIT 20;

-- 5. Kết nối bảng Fact với Dimension để tính phí giao dịch ước tính
SELECT 
    f.symbol,
    d.base_currency,
    COUNT(f.trade_id) AS total_trades,
    SUM(f.trade_value) AS total_volume_usd,
    SUM(f.trade_value * d.taker_fee_pct) AS estimated_taker_fees_usd
FROM warehouse.fact_crypto_trades f
JOIN warehouse.dim_symbol d ON f.symbol = d.symbol
GROUP BY f.symbol, d.base_currency;
```

---

### 6.8. Truy vấn Serverless SQL với Amazon Athena

Truy cập [Amazon Athena Query Editor](https://ap-southeast-1.console.aws.amazon.com/athena/) và thực thi:

```sql
-- 1. Tạo bảng External Table cho Nến 1 phút
CREATE EXTERNAL TABLE IF NOT EXISTS crypto_trades_1m (
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
LOCATION 's3://your-bucket-name/crypto/processed/trades_1m/';

-- 2. Tạo bảng External Table cho Cleaned Raw Trades
CREATE EXTERNAL TABLE IF NOT EXISTS crypto_raw_trades (
  trade_id string,
  price double,
  size double,
  side string,
  raw_timestamp bigint,
  kafka_partition int,
  kafka_offset bigint,
  kafka_arrival_time timestamp,
  trade_time timestamp,
  trade_value double
)
PARTITIONED BY (symbol string, trade_date string)
STORED AS PARQUET
LOCATION 's3://your-bucket-name/crypto/raw/trades/';

-- 3. Cập nhật nhận diện Partitions
MSCK REPAIR TABLE crypto_trades_1m;
MSCK REPAIR TABLE crypto_raw_trades;

-- 4. Truy vấn nến 1m mới nhất
SELECT symbol, window_start, open, high, low, close, volume, vwap, trade_imbalance
FROM crypto_trades_1m
ORDER BY window_start DESC
LIMIT 20;
```

---

## 📈 7. Các chỉ số tài chính được tính toán

Spark tự động tổng hợp các chỉ số theo cửa sổ trượt 1 phút (`1-Minute Tumbling Window`):

| Chỉ số | Công thức / Ý nghĩa | Mục đích phân tích |
| :--- | :--- | :--- |
| **OHLC** | `First(P), Max(P), Min(P), Last(P)` | Biểu đồ nến phân tích kỹ thuật |
| **Volume** | $\sum \text{size}$ | Khối lượng coin giao dịch trong 1 phút |
| **Buy / Sell Volume** | $\sum_{\text{side=buy}} \text{size}$ / $\sum_{\text{side=sell}} \text{size}$ | Đo lường áp lực mua vs bán chủ động (Taker Flow) |
| **VWAP** | $\frac{\sum (\text{Price} \times \text{Size})}{\sum \text{Size}}$ | Giá bình quân gia quyền theo khối lượng giao dịch |
| **Trade Imbalance** | $\frac{\text{BuyVol} - \text{SellVol}}{\text{BuyVol} + \text{SellVol}}$ | Biên độ lệch lệnh (từ `-1.0` cực bán đến `+1.0` cực mua) |
| **Total Trade Value** | $\sum (\text{Price} \times \text{Size})$ | Tổng giá trị thanh khoản quy đổi USD |

---

## 🔧 8. Xử lý sự cố thường gặp (Troubleshooting)

### 1. Lỗi `NumberFormatException: For input string: "60s"`
- **Nguyên nhân**: Spark 4.x runtime ngầm định timeout dạng chuỗi `"60s"`, không tương thích với Hadoop AWS 3.3.4.
- **Giải pháp**: Chuẩn hóa dự án về `pyspark==3.5.1` và `pandas<3.0.0` trong [requirements.txt](file:///d:/crypto-bigdata/requirements.txt).

### 2. Xung đột Port PostgreSQL `5432` trên Windows
- **Nguyên nhân**: Máy Windows đã cài đặt sẵn dịch vụ PostgreSQL cục bộ chạy trên port 5432.
- **Giải pháp**: Docker PostgreSQL Data Warehouse đã được ánh xạ sang port **`5433`** (`5433:5432`). Mọi kết nối tới Data Warehouse sử dụng port `5433`.

### 3. Lỗi Unicode `charmap codec can't encode character` trên Windows PowerShell
- **Nguyên nhân**: Terminal Windows mặc định dùng bảng mã `cp1252`.
- **Giải pháp**: Tất cả script Python và PowerShell trong dự án đã được tích hợp tự động cấu hình `utf-8` và xuất log dạng ASCII an toàn.

### 4. S3 Select báo lỗi `The specified method is not permitted`
- **Nguyên nhân**: AWS đã ngừng hỗ trợ tính năng S3 Select trên Web Console.
- **Giải pháp**: Dùng công cụ [spark/s3_parquet_reader.py](file:///d:/crypto-bigdata/spark/s3_parquet_reader.py) hoặc **Amazon Athena**.