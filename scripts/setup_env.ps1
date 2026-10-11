# =============================================================================
# Crypto Big Data - Dynamic Environment Setup & Pipeline Orchestration
# =============================================================================
# QUY TRÌNH VẬN HÀNH DỰ ÁN THEO 4 BƯỚC CHUẨN:
#
#   BƯỚC 1: KHỞI TẠO HẠ TẦNG & DATABASE
#     .\scripts\setup_env.ps1 -DockerUp
#       -> Công dụng: Khởi động container Kafka Broker (Port 9092) & PostgreSQL DW (Port 5433).
#     .\scripts\setup_env.ps1 -InitWarehouse
#       -> Công dụng: Quét và nạp toàn bộ Schema SQL (dim -> fact -> views) vào PostgreSQL.
#
#   BƯỚC 2: TẢI BÙ DỮ LIỆU LỊCH SỬ (HISTORICAL BACKFILL)
#     .\scripts\setup_env.ps1 -Backfill
#       -> Công dụng: Tải nến 1m lịch sử 7 ngày qua từ OKX REST API nạp thẳng vào AWS S3.
#     .\scripts\setup_env.ps1 -Backfill -ExtraArgs "--days 30"
#       -> Công dụng: Tùy chỉnh tải 30 ngày lịch sử của BTC, ETH, SOL vào S3.
#
#   BƯỚC 3: VẬN HÀNH LUỒNG DỮ LIỆU THỜI GIAN THỰC (REAL-TIME STREAMING)
#     .\scripts\setup_env.ps1 -StartPipeline
#       -> Công dụng: [Khuyên dùng - 1 Click] Tự động bật Docker + mở 2 Terminal chạy
#                     song song OKX WebSocket Producer và Spark Streaming Engine.
#     Hoặc chạy thủ công từng luồng riêng lẻ:
#       .\scripts\setup_env.ps1 -RunProducer  (Chạy luồng WebSocket OKX -> Kafka)
#       .\scripts\setup_env.ps1 -RunSpark     (Chạy Spark Streaming, Whale Alert, S3)
#
#   BƯỚC 4: PHÂN TÍCH ĐA KHUNG THỜI GIAN & KHAI THÁC BI (ANALYTICS & SERVING)
#     .\scripts\setup_env.ps1 -TimeframeAgg
#       -> Công dụng: Chạy PySpark Batch tổng hợp nến Day/Week/Month từ S3 Data Lake.
#     .\scripts\setup_env.ps1 -FetchBatch
#       -> Công dụng: Cào các chỉ số phái sinh (Funding Rates, Open Interest, Long/Short).
#     .\scripts\setup_env.ps1 -ViewS3
#       -> Công dụng: Mở Menu tương tác khám phá file Parquet trên S3 & xuất CSV.
#     .\scripts\setup_env.ps1 -RunETL
#       -> Công dụng: Nạp dữ liệu từ S3 Parquet vào các bảng Fact trong PostgreSQL.
#
#   CÁC LỆNH TIỆN ÍCH KHÁC:
#     .\scripts\setup_env.ps1 -Info        (Kiểm tra trạng thái & phiên bản các thành phần)
#     .\scripts\setup_env.ps1 -DockerDown  (Dừng toàn bộ Docker containers khi nghỉ)
# =============================================================================

[CmdletBinding()]
param (
    [switch]$Info,
    [switch]$StartPipeline,
    [switch]$StreamPipeline,
    [switch]$BatchPipeline,
    [switch]$BatchAnalytics,
    [switch]$ExportCSV,
    [switch]$Backfill,
    [switch]$TimeframeAgg,
    [switch]$RunBatchAgg,
    [switch]$FetchBatch,
    [switch]$RunProducer,
    [switch]$RunSpark,
    [switch]$ViewS3,
    [switch]$InitWarehouse,
    [switch]$RunETL,
    [switch]$Dashboard,
    [switch]$WebDashboard,
    [switch]$Demo,
    [switch]$Predict,
    [switch]$DockerUp,
    [switch]$DockerDown,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$PROJECT_ROOT = (Resolve-Path "$PSScriptRoot\..").Path
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

# -----------------------------------------------------------------------------
# 1. DETECT & CONFIGURE JAVA_HOME (JDK 17 / JDK 11)
# -----------------------------------------------------------------------------
$javaCandidates = @(
    "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot",
    "C:\Program Files\Eclipse Adoptium\jdk-17*",
    "C:\Program Files\Java\jdk-17*",
    "C:\Program Files\Java\jdk-11*",
    "C:\Program Files\Eclipse Adoptium\*"
)

$foundJava = $null
if ($env:JAVA_HOME -and (Test-Path "$env:JAVA_HOME\bin\java.exe")) {
    $foundJava = $env:JAVA_HOME
} else {
    foreach ($cand in $javaCandidates) {
        $resolved = Resolve-Path $cand -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($resolved -and (Test-Path "$($resolved.Path)\bin\java.exe")) {
            $foundJava = $resolved.Path
            break
        }
    }
}

if ($foundJava) {
    $env:JAVA_HOME = $foundJava
} else {
    Write-Warning "[!] JAVA_HOME not found. Please install JDK 17."
}

# -----------------------------------------------------------------------------
# 2. DETECT & CONFIGURE HADOOP_HOME (WinUtils for Windows)
# -----------------------------------------------------------------------------
$hadoopCandidates = @(
    "C:\hadoop",
    "$PROJECT_ROOT\hadoop"
)

$foundHadoop = $null
foreach ($cand in $hadoopCandidates) {
    if (Test-Path "$cand\bin\winutils.exe") {
        $foundHadoop = $cand
        break
    }
}

if ($foundHadoop) {
    $env:HADOOP_HOME = $foundHadoop
} else {
    $env:HADOOP_HOME = "C:\hadoop"
}

# -----------------------------------------------------------------------------
# 3. DETECT & CONFIGURE PYTHON VIRTUAL ENVIRONMENT (.venv)
# -----------------------------------------------------------------------------
$venvPath = "$PROJECT_ROOT\.venv"
if (Test-Path "$venvPath\Scripts\python.exe") {
    $env:PYSPARK_PYTHON = "$venvPath\Scripts\python.exe"
    $env:PYSPARK_DRIVER_PYTHON = "$venvPath\Scripts\python.exe"
    $env:VIRTUAL_ENV = $venvPath
    
    # Prepend virtual environment Scripts to PATH so any python or pip command uses .venv
    if ($env:PATH -notlike "*$venvPath\Scripts*") {
        $env:PATH = "$venvPath\Scripts;$env:PATH"
    }

    # Auto activate virtual environment inside current PowerShell session safely
    $venvActivate = "$venvPath\Scripts\Activate.ps1"
    if (Test-Path $venvActivate) {
        try {
            & $venvActivate
        } catch {}
    }
} else {
    $env:PYSPARK_PYTHON = "python"
    $env:PYSPARK_DRIVER_PYTHON = "python"
}

# -----------------------------------------------------------------------------
# 4. CONFIGURE SPARK_HOME & UPDATE PATH
# -----------------------------------------------------------------------------
if (Test-Path "$venvPath\Lib\site-packages\pyspark") {
    $env:SPARK_HOME = "$venvPath\Lib\site-packages\pyspark"
}

# Prepend to PATH
$newPaths = @(
    "$env:JAVA_HOME\bin",
    "$env:HADOOP_HOME\bin",
    "$env:SPARK_HOME\bin"
) | Where-Object { $_ -and (Test-Path $_) }

foreach ($p in $newPaths) {
    if ($env:PATH -notlike "*$p*") {
        $env:PATH = "$p;$env:PATH"
    }
}

# -----------------------------------------------------------------------------
# 5. RETRIEVE COMPONENT VERSIONS
# -----------------------------------------------------------------------------
function Get-ComponentVersions {
    $pyVer = "N/A"
    $sparkVer = "N/A"
    $javaVer = "N/A"
    $hadoopVer = "N/A"
    $kafkaStatus = "Stopped"
    $pgStatus = "Stopped"

    # Python Version
    if (Test-Path $env:PYSPARK_PYTHON) {
        $pyVer = (& $env:PYSPARK_PYTHON --version 2>&1).ToString().Trim()
        
        # PySpark Version
        $sparkCheck = & $env:PYSPARK_PYTHON -c "import pyspark; print(pyspark.__version__)" 2>&1
        if ($LASTEXITCODE -eq 0) {
            $sparkVer = "PySpark " + $sparkCheck.ToString().Trim()
        }
    }

    # Java Version
    if ($env:JAVA_HOME -and (Test-Path "$env:JAVA_HOME\bin\java.exe")) {
        $jOut = & "$env:JAVA_HOME\bin\java.exe" -version 2>&1
        if ($jOut.Count -gt 0) {
            $javaVer = $jOut[0].ToString().Trim()
        }
    }

    # Hadoop Winutils Version
    if ($env:HADOOP_HOME -and (Test-Path "$env:HADOOP_HOME\bin\winutils.exe")) {
        $hadoopVer = "WinUtils 3.3.4+ ($env:HADOOP_HOME)"
    }

    # Docker Containers Status
    $dockerCheck = Get-Command docker -ErrorAction SilentlyContinue
    if ($dockerCheck) {
        $kContainer = docker ps --filter "name=crypto-kafka" --format "{{.Status}}" 2>&1
        if ($kContainer -and $kContainer -notmatch "error") {
            $kafkaStatus = "Running ($kContainer)"
        }
        $pContainer = docker ps --filter "name=crypto-postgres" --format "{{.Status}}" 2>&1
        if ($pContainer -and $pContainer -notmatch "error") {
            $pgStatus = "Running ($pContainer)"
        }
    }

    return @{
        Python = $pyVer
        PySpark = $sparkVer
        Java = $javaVer
        Hadoop = $hadoopVer
        Kafka = $kafkaStatus
        Postgres = $pgStatus
    }
}

$v = Get-ComponentVersions

# -----------------------------------------------------------------------------
# 6. DISPLAY DASHBOARD
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host "  Crypto Big Data Streaming Pipeline - Environment Ready" -ForegroundColor Cyan
Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host " [OK] Project Root     : $PROJECT_ROOT" -ForegroundColor Green
Write-Host " [OK] Python Version   : $($v.Python)" -ForegroundColor Green
Write-Host " [OK] PySpark Version  : $($v.PySpark)" -ForegroundColor Green
Write-Host " [OK] Java Runtime     : $($v.Java)" -ForegroundColor Green
Write-Host " [OK] JAVA_HOME        : $env:JAVA_HOME" -ForegroundColor DarkGray
Write-Host " [OK] HADOOP_HOME      : $env:HADOOP_HOME" -ForegroundColor DarkGray
Write-Host " [OK] Kafka Broker     : $($v.Kafka)" -ForegroundColor $(if ($v.Kafka -like "Running*") { "Green" } else { "Yellow" })
Write-Host " [OK] PostgreSQL DWH   : $($v.Postgres)" -ForegroundColor $(if ($v.Postgres -like "Running*") { "Green" } else { "Yellow" })
Write-Host "==========================================================================" -ForegroundColor Cyan

# -----------------------------------------------------------------------------
# 7. EXECUTE COMMANDS BASED ON PARAMETERS
# -----------------------------------------------------------------------------
if ($StartPipeline -or $StreamPipeline) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  LUỒNG 1: STREAMING PIPELINE (SPEED LAYER / LOW LATENCY)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    # 1. Bật Docker Containers
    Write-Host "`n[1/3] Starting Kafka & PostgreSQL Containers..." -ForegroundColor Yellow
    docker compose up -d
    Start-Sleep -Seconds 3

    # 2. Khởi tạo Schema Database
    Write-Host "`n[2/3] Checking Warehouse Schema..." -ForegroundColor Yellow
    try {
        & $env:PYSPARK_PYTHON "$PROJECT_ROOT\warehouse\etl\db_config.py"
    } catch {
        Write-Warning "Database init skipped or already initialized."
    }

    # 3. Mở cửa sổ riêng cho Producer
    Write-Host "`n[3/3] Spawning Producer and Spark Streaming in separate terminals..." -ForegroundColor Green
    
    $producerCmd = "& '$PROJECT_ROOT\scripts\setup_env.ps1' -RunProducer"
    Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", "$producerCmd" -WorkingDirectory $PROJECT_ROOT

    Start-Sleep -Seconds 2

    # 4. Mở cửa sổ riêng cho Spark Streaming Engine
    $sparkCmd = "& '$PROJECT_ROOT\scripts\setup_env.ps1' -RunSpark"
    Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", "$sparkCmd" -WorkingDirectory $PROJECT_ROOT

    Write-Host "`n[SUCCESS] Streaming Pipeline launched! 2 separate PowerShell terminals opened." -ForegroundColor Cyan
    Write-Host "  - Window 1: OKX WebSocket -> Kafka Producer (crypto-trades)" -ForegroundColor White
    Write-Host "  - Window 2: Spark Structured Streaming & Whale Detector Engine" -ForegroundColor White
}
elseif ($BatchPipeline) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  LUỒNG 2: BATCH PIPELINE (BATCH LAYER / MULTI-TIMEFRAME QUANT ANALYTICS)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan
    
    Write-Host "`n[1/3] Thu thap nen lich su tu OKX REST API nap vao S3 Data Lake..." -ForegroundColor Yellow
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\ingestion\batch\historical_backfill.py" --days 7

    Write-Host "`n[2/3] Cao du lieu phai sinh (Funding Rates, Open Interest, Long/Short)..." -ForegroundColor Yellow
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\ingestion\batch\okx_derivatives_fetcher.py"

    Write-Host "`n[3/4] Chay Batch Analytics Engine (Ngay/Tuan/Thang/Nam) & Tu dong xuat CSV..." -ForegroundColor Green
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\processing\batch\batch_analytics_engine.py" $ExtraArgs

    Write-Host "`n[4/4] Chay Quant Predictive Engine du doan Ngay/Tuan/Thang/Nam ke tiep..." -ForegroundColor Green
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\processing\batch\market_predictor.py"
}
elseif ($Predict) {
    Write-Host "`n[>] Running Quant Predictive & Forecasting Engine (Next Period)..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\processing\batch\market_predictor.py" $ExtraArgs
}
elseif ($BatchAnalytics -or $TimeframeAgg -or $ExportCSV) {
    Write-Host "`n[>] Running Batch Analytics Engine (Multi-Timeframe Day/Week/Month/Year & CSV Exporter)..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\processing\batch\batch_analytics_engine.py" $ExtraArgs
}
elseif ($DockerUp) {
    Write-Host "`n[+] Starting Kafka & PostgreSQL Containers..." -ForegroundColor Yellow
    docker compose up -d
}
elseif ($DockerDown) {
    Write-Host "`n[-] Stopping Docker Containers..." -ForegroundColor Yellow
    docker compose down
}
elseif ($Backfill) {
    Write-Host "`n[>] Running OKX Historical Candlestick Backfill -> S3 Data Lake..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\ingestion\batch\historical_backfill.py" $ExtraArgs
}
elseif ($FetchBatch) {
    Write-Host "`n[>] Fetching Derivatives Batch Data (Funding Rates, OI, Long/Short)..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\ingestion\batch\okx_derivatives_fetcher.py"
}
elseif ($RunBatchAgg) {
    Write-Host "`n[>] Running Daily Market Batch Aggregator from S3 Data Lake..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\processing\batch\batch_analytics_engine.py" --timeframe day
}
elseif ($RunProducer) {
    Write-Host "`n[>] Launching OKX Market Data Producer..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON -u "$PROJECT_ROOT\ingestion\streaming\okx_producer.py"
}
elseif ($RunSpark) {
    Write-Host "`n[>] Launching Spark Structured Streaming Processor..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON -u "$PROJECT_ROOT\processing\streaming\trade_processor.py"
}
elseif ($ViewS3) {
    Write-Host "`n[>] Launching S3 Parquet Data Lake Reader..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\processing\batch\s3_parquet_reader.py" $ExtraArgs
}
elseif ($InitWarehouse) {
    Write-Host "`n[+] Initializing PostgreSQL Data Warehouse Schema & Views..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\warehouse\etl\db_config.py"
}
elseif ($RunETL) {
    Write-Host "`n[>] Running S3 Data Lake -> PostgreSQL Data Warehouse ETL Pipeline..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\warehouse\etl\s3_to_warehouse.py" $ExtraArgs
}
elseif ($Dashboard) {
    Write-Host "`n[>] Launching Terminal Analytics & Quant Trading Dashboard (TUI)..." -ForegroundColor Cyan
    $dashArgs = @()
    if ($Demo) { $dashArgs += "--demo" }
    if ($ExtraArgs) { $dashArgs += $ExtraArgs }
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\processing\analysis\terminal_dashboard.py" $dashArgs
}
elseif ($WebDashboard) {
    Write-Host "`n[>] Launching Web Analytics & Quant Trading Dashboard (Browser UI)..." -ForegroundColor Cyan
    Write-Host "    Open browser at: http://127.0.0.1:5000" -ForegroundColor Green
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\web\app.py"
}
elseif (-not $Info) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  DANH SÁCH LỆNH VẬN HÀNH & CÔNG DỤNG TỪNG BƯỚC (SETUP_ENV.PS1)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    Write-Host "`n[BƯỚC 1] KHỞI TẠO HẠ TẦNG & CƠ SỞ DỮ LIỆU:" -ForegroundColor Yellow
    Write-Host "  .\scripts\setup_env.ps1 -DockerUp" -ForegroundColor White
    Write-Host "    -> Công dụng: Bật Docker containers cho Kafka (9092) và PostgreSQL (5433)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -InitWarehouse" -ForegroundColor White
    Write-Host "    -> Công dụng: Nạp toàn bộ DDL bảng (dim, fact) và Views (Ngày/Tuần/Tháng) vào PostgreSQL DW." -ForegroundColor DarkGray

    Write-Host "`n==========================================================================" -ForegroundColor Green
    Write-Host "  LUỒNG 1: REAL-TIME STREAMING (SPEED LAYER / LOW LATENCY)" -ForegroundColor Green
    Write-Host "==========================================================================" -ForegroundColor Green
    Write-Host "  .\scripts\setup_env.ps1 -StreamPipeline   ★ (1 CLICK BẬT TRỌN GÓI STREAMING)" -ForegroundColor Green
    Write-Host "    -> Công dụng: Tự bật Docker, mở Producer (WebSocket) & Spark Streaming Engine." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunProducer" -ForegroundColor White
    Write-Host "    -> Công dụng: Thu thập lệnh khớp thời gian thực OKX đẩy vào Kafka topic." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunSpark" -ForegroundColor White
    Write-Host "    -> Công dụng: Spark Streaming Engine (Lọc nhiễu, Whale Alert, Order Flow, ghi S3 & DW)." -ForegroundColor DarkGray

    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  LUỒNG 2: BATCH ANALYTICS & EXPORT (BATCH LAYER / TIME-SERIES QUANT)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan
    Write-Host "  .\scripts\setup_env.ps1 -BatchPipeline    ★ (1 CLICK: BACKFILL -> PHÁI SINH -> BATCH ANALYTICS -> DỰ BÁO)" -ForegroundColor Cyan
    Write-Host "    -> Công dụng: Backfill nến lịch sử -> Cào phái sinh -> Batch Analytics -> Dự báo chu kỳ kế tiếp -> Xuất toàn bộ CSV." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -Predict          ★ (DỰ BÁO ĐỊNH LƯỢNG CHU KỲ KẾ TIẾP)" -ForegroundColor Green
    Write-Host "    -> Công dụng: Chạy mô hình lượng hóa dự đoán nến Ngày, Tuần, Tháng, Năm kế tiếp (Target Price, Signals)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -BatchAnalytics" -ForegroundColor White
    Write-Host "    -> Công dụng: Tổng hợp nến Ngày/Tuần/Tháng & Tự động xuất CSV vào data/exports/." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -ExportCSV" -ForegroundColor White
    Write-Host "    -> Công dụng: Xuất trực tiếp file CSV Ngày, Tuần, Tháng từ S3 Data Lake." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -Backfill -ExtraArgs '--days 30'" -ForegroundColor White
    Write-Host "    -> Công dụng: Nạp 30 ngày nến 1m lịch sử từ OKX REST API vào S3 Data Lake." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -FetchBatch" -ForegroundColor White
    Write-Host "    -> Công dụng: Cào các chỉ số phái sinh (Funding Rates, Open Interest, Long/Short)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -ViewS3" -ForegroundColor White
    Write-Host "    -> Công dụng: Menu tương tác khám phá Parquet trên S3 & xuất CSV theo lựa chọn." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunETL" -ForegroundColor White
    Write-Host "    -> Công dụng: Nạp dữ liệu từ S3 Data Lake vào PostgreSQL Data Warehouse." -ForegroundColor DarkGray

    Write-Host "`n[DASHBOARDS & GIAO DIỆN PHÂN TÍCH]:" -ForegroundColor Magenta
    Write-Host "  .\scripts\setup_env.ps1 -WebDashboard     ★ (WEB QUANT DASHBOARD TRÊN TRÌNH DUYỆT)" -ForegroundColor Green
    Write-Host "    -> Công dụng: Khởi chạy Web Dashboard tại http://127.0.0.1:5000 (TradingView, Whales, DW Explorer)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -Dashboard        ★ (TRỰC QUAN HÓA TERMINAL TUI)" -ForegroundColor Yellow
    Write-Host "    -> Công dụng: Màn hình trực quan hóa Terminal (Cá mập, VWAP, Order Flow Gauge)." -ForegroundColor DarkGray

    Write-Host "`n[TIỆN ÍCH]:" -ForegroundColor DarkYellow
    Write-Host "  .\scripts\setup_env.ps1 -Info      -> Kiểm tra phiên bản Python, Spark, Java, Docker." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -DockerDown-> Tắt sạch Docker containers khi kết thúc." -ForegroundColor DarkGray
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host ""
}