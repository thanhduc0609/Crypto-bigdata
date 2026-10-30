# =============================================================================
# Crypto Big Data - Real-time Streaming Pipeline Orchestration (PowerShell)
# =============================================================================
# KIẾN TRÚC HỆ THỐNG: PURE REAL-TIME STREAMING & DATA LAKE
#
#   1. THU THẬP & TRUYỀN TẢI:
#      OKX WebSocket (BTC-USDT Dedicated) -> Python Producer -> Apache Kafka (KRaft)
#
#   2. XỬ LÝ PHÂN TÁN (SPARK STRUCTURED STREAMING):
#      - Lọc sạch & chuẩn hóa dữ liệu giao dịch
#      - Watermarking (2 phút) & Khử trùng lặp (dropDuplicates)
#      - Tính nến 1 phút Tumbling Window (OHLCV, VWAP, Order Flow Imbalance)
#      - Phát hiện giao dịch Cá mập (Whale Alert >= $500,000) tức thời
#
#   3. LƯU TRỮ ĐA TẦNG:
#      - AWS S3 Data Lake (Parquet Columnar): Lưu trữ giao dịch sạch & nến 1m
#      - PostgreSQL Data Warehouse (Star Schema): Nạp trực tiếp qua Streaming Sink
#
#   4. PHÂN TÍCH & TRỰC QUAN HÓA:
#      - Web Analytics Dashboard (TradingView Lightweight Charts, Port 5000)
# =============================================================================

[CmdletBinding()]
param (
    [switch]$Info,
    [switch]$StartPipeline,
    [switch]$StreamPipeline,
    [switch]$RunProducer,
    [switch]$RunSpark,
    [switch]$ViewS3,
    [switch]$InitS3,
    [switch]$TestS3,
    [switch]$InitWarehouse,
    [switch]$TestWarehouse,
    [switch]$BuildFeatures,
    [switch]$TrainModel,
    [switch]$RunPrediction,
    [switch]$TestPipeline,
    [switch]$Dashboard,
    [switch]$WebDashboard,
    [switch]$Demo,
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
    "C:\Program Files\BellSoft\LibericaJDK-17*",
    "C:\Program Files\Eclipse Adoptium\jdk-11*",
    "C:\Program Files\Java\jdk-11*"
)

$foundJava = $null
foreach ($pattern in $javaCandidates) {
    $matches = Get-Item $pattern -ErrorAction SilentlyContinue | Sort-Object Name -Descending
    if ($matches) {
        $foundJava = $matches[0].FullName
        break
    }
}

if ($foundJava) {
    $env:JAVA_HOME = $foundJava
    $env:PATH = "$foundJava\bin;$env:PATH"
} elseif (-not $env:JAVA_HOME) {
    $javaCmd = Get-Command java -ErrorAction SilentlyContinue
    if ($javaCmd) {
        $javaBin = Split-Path -Parent $javaCmd.Source
        $env:JAVA_HOME = Split-Path -Parent $javaBin
    }
}

# -----------------------------------------------------------------------------
# 2. DETECT & CONFIGURE HADOOP_HOME (WINUTILS CHO WINDOWS)
# -----------------------------------------------------------------------------
$hadoopCandidates = @(
    "$PROJECT_ROOT\hadoop",
    "C:\hadoop",
    "D:\hadoop"
)

foreach ($hPath in $hadoopCandidates) {
    if (Test-Path "$hPath\bin\winutils.exe") {
        $env:HADOOP_HOME = (Resolve-Path $hPath).Path
        $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
        break
    }
}

# -----------------------------------------------------------------------------
# 3. DETECT & CONFIGURE PYTHON ENVIRONMENT (.venv)
# -----------------------------------------------------------------------------
$venvPython = "$PROJECT_ROOT\.venv\Scripts\python.exe"
if (Test-Path $venvPython) {
    $env:PYSPARK_PYTHON = $venvPython
    $env:PYSPARK_DRIVER_PYTHON = $venvPython
} else {
    $env:PYSPARK_PYTHON = "python"
    $env:PYSPARK_DRIVER_PYTHON = "python"
}

# -----------------------------------------------------------------------------
# 4. LOAD BIẾN MÔI TRƯỜNG TỪ FILE .env
# -----------------------------------------------------------------------------
$envFile = "$PROJECT_ROOT\.env"
if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
            $parts = $line.Split("=", 2)
            $k = $parts[0].Trim()
            $v = $parts[1].Trim().Trim("'`"")
            if (-not [string]::IsNullOrEmpty($k)) {
                [Environment]::SetEnvironmentVariable($k, $v, "Process")
            }
        }
    }
    $bucketName = [Environment]::GetEnvironmentVariable("S3_BUCKET_NAME", "Process")
    if ($bucketName) {
        @("S3_RAW_PATH", "S3_PROCESSED_PATH", "S3_CHECKPOINT_RAW", "S3_CHECKPOINT_PROCESSED") | ForEach-Object {
            $curVal = [Environment]::GetEnvironmentVariable($_, "Process")
            if ($curVal -and $curVal.Contains('${S3_BUCKET_NAME}')) {
                $newVal = $curVal.Replace('${S3_BUCKET_NAME}', $bucketName)
                [Environment]::SetEnvironmentVariable($_, $newVal, "Process")
            }
        }
    }
}

# -----------------------------------------------------------------------------
# 5. HÀM KIỂM TRA PHIÊN BẢN CÁC THÀNH PHẦN HẠ TẦNG
# -----------------------------------------------------------------------------
function Get-ComponentVersions {
    $pyVer = "Not Found"
    $sparkVer = "Not Found"
    $javaVer = "Not Found"
    $hadoopVer = "Not Configured"
    $kafkaStatus = "Stopped"
    $pgStatus = "Stopped"

    # Python Version
    try {
        $pyOut = & $env:PYSPARK_PYTHON --version 2>&1
        if ($pyOut -match "Python (\d+\.\d+\.\d+)") {
            $pyVer = $Matches[1]
        }
    } catch {}

    # PySpark Version
    try {
        $spOut = & $env:PYSPARK_PYTHON -c "import pyspark; print(pyspark.__version__)" 2>&1
        if ($LASTEXITCODE -eq 0 -and $spOut -notmatch "Error") {
            $sparkVer = $spOut.Trim()
        }
    } catch {}

    # Java Version
    try {
        $jOut = & java -version 2>&1
        if ($jOut -match 'version "([^"]+)"') {
            $javaVer = $Matches[1]
        }
    } catch {}

    # Hadoop WinUtils
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
# 6. HIỂN THỊ TRẠNG THÁI HỆ THỐNG
# -----------------------------------------------------------------------------
Write-Host ""
Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host "  Crypto Big Data Pure Streaming Pipeline - Environment Ready" -ForegroundColor Cyan
Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host " [OK] Project Root     : $PROJECT_ROOT" -ForegroundColor Green
Write-Host " [OK] Python Version   : $($v.Python)" -ForegroundColor Green
Write-Host " [OK] PySpark Version  : $($v.PySpark)" -ForegroundColor Green
Write-Host " [OK] Java Runtime     : $($v.Java)" -ForegroundColor Green
Write-Host " [OK] JAVA_HOME        : $env:JAVA_HOME" -ForegroundColor DarkGray
Write-Host " [OK] HADOOP_HOME      : $env:HADOOP_HOME" -ForegroundColor DarkGray
Write-Host " [OK] Kafka Broker     : $($v.Kafka)" -ForegroundColor $(if ($v.Kafka -like "Running*") { "Green" } else { "Yellow" })
Write-Host " [OK] PostgreSQL DW    : $($v.Postgres)" -ForegroundColor $(if ($v.Postgres -like "Running*") { "Green" } else { "Yellow" })
Write-Host "==========================================================================" -ForegroundColor Cyan

# -----------------------------------------------------------------------------
# 7. ĐIỀU PHỐI VẬN HÀNH DỰA TRÊN THAM SỐ
# -----------------------------------------------------------------------------
if ($StartPipeline -or $StreamPipeline) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  KICH HOAT TRON GOI PIPELINE STREAMING THOI GIAN THUC (SPEED LAYER)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    # 1. Bật Docker Containers
    Write-Host "`n[1/3] Khoi dong Docker Containers (Kafka:9092 va PostgreSQL:5433)..." -ForegroundColor Yellow
    docker compose up -d
    Start-Sleep -Seconds 3

    # 2. Khởi tạo Schema Database
    Write-Host "`n[2/3] Kiem tra va khoi tao Schema Data Warehouse..." -ForegroundColor Yellow
    try {
        & $env:PYSPARK_PYTHON "$PROJECT_ROOT\warehouse\etl\db_config.py"
    } catch {
        Write-Warning "Database init skipped or already initialized."
    }

    # 3. Mở cửa sổ riêng cho Producer
    Write-Host "`n[3/3] Mo cua so PowerShell rieng cho Producer va Spark Streaming..." -ForegroundColor Green
    
    $producerCmd = "powershell -NoExit -ExecutionPolicy Bypass -Command `"& '$PROJECT_ROOT\scripts\setup_env.ps1' -RunProducer`""
    Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", "& '$PROJECT_ROOT\scripts\setup_env.ps1' -RunProducer" -WorkingDirectory $PROJECT_ROOT

    Start-Sleep -Seconds 2

    # 4. Mở cửa sổ riêng cho Spark Streaming Engine
    Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", "& '$PROJECT_ROOT\scripts\setup_env.ps1' -RunSpark" -WorkingDirectory $PROJECT_ROOT

    Write-Host "`n[SUCCESS] Pipeline Streaming da duoc kich hoat thanh cong trong 2 terminal rieng!" -ForegroundColor Cyan
    Write-Host "  - Terminal 1: OKX WebSocket -> Kafka Producer (crypto-trades)" -ForegroundColor White
    Write-Host "  - Terminal 2: Spark Structured Streaming [Clean, 1m Candles, Whale Alert, S3 and DW]" -ForegroundColor White
}
elseif ($DockerUp) {
    Write-Host "`n[+] Starting Kafka and PostgreSQL Containers..." -ForegroundColor Yellow
    docker compose up -d
}
elseif ($DockerDown) {
    Write-Host "`n[-] Stopping Docker Containers..." -ForegroundColor Yellow
    docker compose down
}
elseif ($RunProducer) {
    Write-Host "`n[>] Launching OKX Market Data WebSocket Producer..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON -u "$PROJECT_ROOT\ingestion\streaming\okx_producer.py"
}
elseif ($RunSpark) {
    Write-Host "`n[>] Launching Spark Structured Streaming Processor (Whale Alerts and S3 Sink)..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON -u "$PROJECT_ROOT\processing\streaming\trade_processor.py"
}
elseif ($ViewS3) {
    Write-Host "`n[>] Launching S3 Parquet Streaming Data Lake Explorer..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\processing\analysis\s3_parquet_reader.py" $ExtraArgs
}
elseif ($InitS3 -or $TestS3) {
    Write-Host "`n[>] Testing S3 Connection & Initializing Streaming Folders on AWS S3..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\tests\test_s3.py"
}
elseif ($InitWarehouse) {
    Write-Host "`n[+] Initializing PostgreSQL Pure Streaming Data Warehouse Schema and Views..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\warehouse\etl\db_config.py"
}
elseif ($TestWarehouse) {
    Write-Host "`n[>] Running Automated Streaming Data Warehouse Test Suite..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\tests\test_warehouse.py"
}
elseif ($BuildFeatures) {
    Write-Host "`n[>] Executing Feature Engineering & Dataset Creation..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\scripts\build_features.py"
}
elseif ($TrainModel) {
    Write-Host "`n[>] Training XGBoost BTC Direction Classifier (5-Minute Horizon)..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\scripts\train_model.py"
}
elseif ($RunPrediction) {
    Write-Host "`n[>] Running BTC 5-Minute Direction Inference Service..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\scripts\run_prediction.py" $ExtraArgs
}
elseif ($TestPipeline) {
    Write-Host "`n[>] Running Full Pipeline & XGBoost Automated Test Suite..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\tests\test_pipeline_e2e.py"
}
elseif ($WebDashboard -or $Dashboard) {
    Write-Host "`n[>] Launching Web Analytics and Quant Trading Dashboard (Browser UI)..." -ForegroundColor Cyan
    Write-Host "    Open browser at: http://127.0.0.1:5000" -ForegroundColor Green
    Start-Process "http://127.0.0.1:5000"
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\web\app.py"
}
elseif (-not $Info) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  DANH SACH LENH VAN HANH HE THONG REAL-TIME STREAMING" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    Write-Host "`n[BUOC 1] KHOI TAO HA TANG VA CO SO DU LIEU:" -ForegroundColor Yellow
    Write-Host "  .\scripts\setup_env.ps1 -DockerUp" -ForegroundColor White
    Write-Host "    -> Bat Docker containers cho Kafka (9092) va PostgreSQL (5433)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -InitWarehouse" -ForegroundColor White
    Write-Host "    -> Nap DDL bang Pure Streaming DW (dim_symbol, dim_alert_tier, fact_trades, fact_candles_1m) va 10 Views." -ForegroundColor DarkGray
  Write-Host "  .\scripts\setup_env.ps1 -TestWarehouse" -ForegroundColor White
    Write-Host "    -> Chay bo kiem thu tu dong Data Warehouse, xac thuc do tre thap (< 100ms) cua Speed Layer." -ForegroundColor DarkGray

    Write-Host "`n[BUOC 2] VAN HANH PIPELINE STREAMING THOI GIAN THUC:" -ForegroundColor Green
    Write-Host "  .\scripts\setup_env.ps1 -StreamPipeline   [1-CLICK BAT TRON GOI PIPELINE]" -ForegroundColor Green
    Write-Host "    -> Tu bat Docker, mo Producer (WebSocket) va Spark Streaming Engine." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunProducer" -ForegroundColor White
    Write-Host "    -> Thu thap lenh khop thoi gian thuc tu san OKX day vao Kafka topic." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunSpark" -ForegroundColor White
    Write-Host "    -> Spark Structured Streaming Engine (Clean, Whale Alert, 1m Candles, ghi S3 va DW)." -ForegroundColor DarkGray

    Write-Host "`n[BUOC 3] HOC MAY & DU BAO (XGBOOST CLASSIFIER):" -ForegroundColor Blue
    Write-Host "  .\scripts\setup_env.ps1 -BuildFeatures    -> Tinh toan 18 dac trung ky thuat va tao nhan du bao 5 phut." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -TrainModel       -> Huan luyen mo hinh XGBoost tren chuoi thoi gian (70/15/15)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunPrediction    -> Chay dich vu suy luan real-time, luu vao PostgreSQL DW." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -TestPipeline     -> Chay bo kiem thu tu dong toan dien (7/7 tests passed)." -ForegroundColor DarkGray

    Write-Host "`n[BUOC 4] KHAI THAC VA TRUC QUAN HOA:" -ForegroundColor Magenta
    Write-Host "  .\scripts\setup_env.ps1 -WebDashboard     [WEB TRADING & ML DASHBOARD]" -ForegroundColor Green
    Write-Host "    -> Khoi chay Web Dashboard tai http://127.0.0.1:5000 (TradingView, ML Predictions, DW Explorer)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -ViewS3" -ForegroundColor White
    Write-Host "    -> Menu tuong tac kham pha Parquet Streaming tren S3 Data Lake va xuat CSV." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -InitS3" -ForegroundColor Yellow
    Write-Host "    -> Khoi tao nhanh cay thu muc Parquet tren AWS S3 (trades va candles_1m)." -ForegroundColor DarkGray

    Write-Host "`n[TIEN ICH]:" -ForegroundColor DarkYellow
    Write-Host "  .\scripts\setup_env.ps1 -Info      -> Kiem tra phien ban Python, Spark, Java, Docker." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -DockerDown-> Dung toan bo Docker containers khi ket thuc." -ForegroundColor DarkGray
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host ""
}