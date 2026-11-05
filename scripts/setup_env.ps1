# =============================================================================
# Crypto Big Data - Real-time Streaming & Medallion Pipeline Orchestration (PowerShell)
# =============================================================================
# KIẾN TRÚC HỆ THỐNG: SPEED LAYER + MEDALLION BATCH DATA LAKE
#
#   1. SPEED LAYER (REAL-TIME STREAMING):
#      OKX WebSocket (BTC-USDT) -> Kafka (crypto-trades) -> Spark Structured Streaming
#      -> S3 Clean Trades + PostgreSQL DW (fact_trades, fact_crypto_1m_candles, whale alerts)
#
#   2. MEDALLION BATCH LAYER (RECONCILIATION & REPRODUCIBILITY):
#      - Bronze: Ingestion dữ liệu thô (OKX REST / Raw Kafka archives)
#      - Silver: Làm sạch, khử trùng lặp theo trade_id, chuẩn hóa schema
#      - Gold: Tổng hợp nến 1m OHLCV, VWAP, Order Flow Imbalance, đồng thuận Canonical
#      - Data Warehouse Sync: Đồng bộ kết quả canonical sang PostgreSQL
#
#   3. MACHINE LEARNING & PREDICTION:
#      - 15 Canonical Technical & Microstructure Features
#      - XGBoost 5-Minute Direction Classifier
#      - PredictionWorker: Chu kỳ 1 phút, FileLock chống xung đột, đối chiếu nhãn chuẩn xác
#
#   4. WEB ANALYTICS & DASHBOARD:
#      - Port 5000: TradingView Lightweight Charts (REST), Cán cân 60s (WS), Whale Trades & Dự báo (DW)
# =============================================================================

[CmdletBinding()]
param (
    # [1] Thông tin môi trường & Kiểm tra hệ thống
    [switch]$Info,

    # [2] Quản lý Hạ tầng Docker (Kafka & PostgreSQL DW)
    [switch]$DockerUp,
    [switch]$DockerDown,

    # [3] Khởi tạo Schema Data Warehouse
    [switch]$InitWarehouse,

    # [4] Speed Layer (Real-time Streaming End-to-End)
    [Alias("StartPipeline")]
    [switch]$StreamPipeline,
    [switch]$RunProducer,
    [switch]$RunSpark,

    # [5] Medallion Batch Pipeline (Data Lakehouse End-to-End)
    [switch]$MedallionPipeline,

    # [6] Machine Learning (Huấn luyện & Suy luận End-to-End)
    [switch]$TrainModel,
    [switch]$RunPrediction,

    # [7] Giao diện Web Analytics & Trading Dashboard
    [Alias("Dashboard")]
    [switch]$WebDashboard,
    [string]$WorkerMode = "",
    [switch]$NoBrowser,

    # [8] Kiểm thử tự động toàn diện End-to-End
    [switch]$TestPipeline,

    # [9] Tham số mở rộng (Pass-through arguments)
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs
)

$PROJECT_ROOT = (Resolve-Path "$PSScriptRoot\..").Path
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch {}

# Helper function to invoke python scripts with strict exit code checking
function Invoke-PythonStep {
    param (
        [string]$Title,
        [string]$ScriptPath,
        [string[]]$ArgsList = @()
    )
    Write-Host "`n[>] $Title..." -ForegroundColor Cyan
    & $env:PYSPARK_PYTHON -u $ScriptPath @ArgsList
    $code = $LASTEXITCODE
    if ($null -ne $code -and $code -ne 0) {
        Write-Host "`n[ERROR] Step failed with exit code ${code}: $ScriptPath" -ForegroundColor Red
        exit $code
    }
}

# -----------------------------------------------------------------------------
# 1. DETECT & CONFIGURE JAVA_HOME (JDK 17 / JDK 11)
# -----------------------------------------------------------------------------
$javaCandidates = @(
    "C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot",
    "C:\Program Files\Eclipse Adoptium\jdk-17*",
    "C:\Program Files\Java\jdk-17*",
    "C:\Program Files\BellSoft\LibericaJDK-17*",
    "C:\Program Files\Eclipse Adoptium\jdk-11*",
    "C:\Program Files\Java\jdk-11*",
    "C:\Program Files\Java\jdk*",
    "C:\Program Files\Java\jre*"
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
    if ($javaCmd -and $javaCmd.Source -notmatch "Common Files" -and $javaCmd.Source -notmatch "x86") {
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
# 3. DETECT & CONFIGURE PYTHON ENVIRONMENT (.venv / local Python / system)
# -----------------------------------------------------------------------------
$foundPython = $null
$pythonCandidates = @(
    "$PROJECT_ROOT\.venv\Scripts\python.exe",
    "$PROJECT_ROOT\..\.venv\Scripts\python.exe",
    "C:\Users\ADMIN\AppData\Local\Python\bin\python.exe",
    "$env:LOCALAPPDATA\Python\bin\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python314\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python310\python.exe",
    "C:\Program Files\Python314\python.exe",
    "C:\Program Files\Python313\python.exe",
    "C:\Program Files\Python312\python.exe",
    "C:\Program Files\Python311\python.exe",
    "C:\Program Files\Python310\python.exe",
    "C:\Python314\python.exe",
    "C:\Python313\python.exe",
    "C:\Python312\python.exe"
)

foreach ($pCandidate in $pythonCandidates) {
    if ($pCandidate -and (Test-Path $pCandidate -ErrorAction SilentlyContinue)) {
        $foundPython = (Resolve-Path $pCandidate -ErrorAction SilentlyContinue).Path
        break
    }
}

if (-not $foundPython) {
    $pyCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pyCmd -and $pyCmd.Source) {
        $foundPython = $pyCmd.Source
    } else {
        $pyCmd = Get-Command py -ErrorAction SilentlyContinue
        if ($pyCmd -and $pyCmd.Source) {
            $foundPython = $pyCmd.Source
        }
    }
}

if ($foundPython) {
    $env:PYSPARK_PYTHON = $foundPython
    $env:PYSPARK_DRIVER_PYTHON = $foundPython
    $pyDir = Split-Path -Parent $foundPython
    if ($env:PATH -notlike "*$pyDir*") {
        $env:PATH = "$pyDir;$env:PATH"
    }
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
        $pyOut = & $env:PYSPARK_PYTHON --version 2>&1 | Out-String
        if ($pyOut -match "Python (\d+\.\d+\.\d+)") {
            $pyVer = $Matches[1]
        }
    } catch {}

    # PySpark Version
    try {
        $spOut = & $env:PYSPARK_PYTHON -c "import pyspark; print(pyspark.__version__)" 2>&1 | Out-String
        if ($LASTEXITCODE -eq 0 -and $spOut -notmatch "Error") {
            $sparkVer = $spOut.Trim()
        }
    } catch {}

    # Java Version
    try {
        $jOut = (& java -version 2>&1) | Out-String
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
Write-Host "  Crypto Big Data Speed & Medallion Pipeline - Environment Ready" -ForegroundColor Cyan
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
# 7. ĐIỀU PHỐI VẬN HÀNH DỰA TRÊN THAM SỐ (END-TO-END PIPELINE & CORE COMMANDS)
# -----------------------------------------------------------------------------
if ($StreamPipeline) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  KICH HOAT TRON GOI PIPELINE STREAMING THOI GIAN THUC (SPEED LAYER)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    # 0. Dọn dẹp tiến trình stream cũ để tránh xung đột Checkpoint Spark Concurrent Update
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue | Where-Object { 
        ($_.CommandLine -like "*$PROJECT_ROOT*" -or $_.CommandLine -like "*Crypto-bigdata*") -and
        ($_.CommandLine -like "*trade_processor.py*" -or $_.CommandLine -like "*okx_producer.py*")
    } | ForEach-Object { 
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue 
    }

    # 1. Bật Docker Containers
    Write-Host "`n[1/3] Khoi dong Docker Containers (Kafka:9092 va PostgreSQL:5433)..." -ForegroundColor Yellow
    docker compose up -d
    if ($null -ne $LASTEXITCODE -and $LASTEXITCODE -ne 0) {
        Write-Host "`n[ERROR] Failed to start Docker containers." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Start-Sleep -Seconds 3

    # 2. Khởi tạo Schema Database
    Write-Host "`n[2/3] Kiem tra va khoi tao Schema Data Warehouse..." -ForegroundColor Yellow
    try {
        & $env:PYSPARK_PYTHON "$PROJECT_ROOT\warehouse\etl\db_config.py"
        if ($null -ne $LASTEXITCODE -and $LASTEXITCODE -ne 0) {
            Write-Host "`n[ERROR] Database initialization failed with exit code $LASTEXITCODE." -ForegroundColor Red
            exit $LASTEXITCODE
        }
    } catch {
        Write-Warning "Database init encountered an issue: $_"
    }

    # 3. Mở cửa sổ riêng cho Producer và Spark Streaming Engine
    Write-Host "`n[3/3] Mo cua so PowerShell rieng cho Producer va Spark Streaming..." -ForegroundColor Green
    
    $producerProc = Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-File", "$PROJECT_ROOT\scripts\setup_env.ps1", "-RunProducer" -WorkingDirectory $PROJECT_ROOT -PassThru
    Start-Sleep -Seconds 2

    $sparkProc = Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-File", "$PROJECT_ROOT\scripts\setup_env.ps1", "-RunSpark" -WorkingDirectory $PROJECT_ROOT -PassThru

    Write-Host "`n[SUCCESS] Pipeline Streaming da duoc kich hoat thanh cong trong 2 terminal rieng!" -ForegroundColor Cyan
    Write-Host "  - Terminal 1 (PID $($producerProc.Id)): OKX WebSocket -> Kafka Producer (crypto-trades)" -ForegroundColor White
    Write-Host "  - Terminal 2 (PID $($sparkProc.Id)): Spark Structured Streaming [Clean, 1m Candles, Whale Alert, S3 and DW]" -ForegroundColor White
    Write-Host "`n[TIP] De chay truc tiep trong terminal hien tai thay vi mo cua so moi, su dung:" -ForegroundColor DarkYellow
    Write-Host "  Terminal 1: .\scripts\setup_env.ps1 -RunProducer" -ForegroundColor DarkGray
    Write-Host "  Terminal 2: .\scripts\setup_env.ps1 -RunSpark" -ForegroundColor DarkGray
}
elseif ($MedallionPipeline) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  MEDALLION BATCH PIPELINE (BRONZE -> SILVER -> GOLD & DW SYNC)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    Invoke-PythonStep "Stage 1/3: Medallion Bronze Historical Ingestion" "$PROJECT_ROOT\ingestion\historical\bronze_ingestion.py" $ExtraArgs
    Invoke-PythonStep "Stage 2/3: Medallion Silver Cleansing & Deduplication" "$PROJECT_ROOT\processing\batch\bronze_to_silver.py" $ExtraArgs
    Invoke-PythonStep "Stage 3/3: Medallion Gold 1m OHLCV Aggregation & DW Sync" "$PROJECT_ROOT\processing\batch\silver_to_gold.py" $ExtraArgs

    Write-Host "`n[SUCCESS] Medallion Pipeline completed all 3 stages successfully!" -ForegroundColor Green
}
elseif ($DockerUp) {
    Write-Host "`n[+] Starting Kafka and PostgreSQL Containers..." -ForegroundColor Yellow
    docker compose up -d
}
elseif ($DockerDown) {
    Write-Host "`n[-] Stopping Docker Containers..." -ForegroundColor Yellow
    docker compose down
}
elseif ($InitWarehouse) {
    Invoke-PythonStep "Initializing PostgreSQL DW Schema and Views" "$PROJECT_ROOT\warehouse\etl\db_config.py"
}
elseif ($RunProducer) {
    Invoke-PythonStep "Launching OKX Market Data WebSocket Producer" "$PROJECT_ROOT\ingestion\streaming\okx_producer.py"
}
elseif ($RunSpark) {
    Invoke-PythonStep "Launching Spark Structured Streaming Processor (Whale Alerts and S3 Sink)" "$PROJECT_ROOT\processing\streaming\trade_processor.py"
}
elseif ($TrainModel) {
    Invoke-PythonStep "Training XGBoost BTC Direction Classifier (5-Minute Horizon)" "$PROJECT_ROOT\scripts\train_model.py"
}
elseif ($RunPrediction) {
    Invoke-PythonStep "Running BTC 5-Minute Direction Inference Service" "$PROJECT_ROOT\scripts\run_prediction.py" $ExtraArgs
}
elseif ($WebDashboard) {
    Write-Host "`n[>] Launching Web Analytics and Quant Trading Dashboard (Browser UI)..." -ForegroundColor Cyan
    if ($WorkerMode) {
        $env:PREDICTION_WORKER_MODE = $WorkerMode
        Write-Host "    Prediction Worker Mode set to: $WorkerMode" -ForegroundColor DarkGray
    }
    Write-Host "    Open browser at: http://127.0.0.1:5000" -ForegroundColor Green
    if (-not $NoBrowser) {
        Start-Process "http://127.0.0.1:5000"
    }
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT\web\app.py"
}
elseif ($TestPipeline) {
    Invoke-PythonStep "Running Full Pipeline & XGBoost Automated Test Suite" "$PROJECT_ROOT\tests\test_pipeline_e2e.py"
}
elseif (-not $Info) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  DANH SACH LENH VAN HANH HE THONG (SPEED LAYER & MEDALLION BATCH)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    Write-Host "`n[1] HA TANG VA CO SO DU LIEU:" -ForegroundColor Yellow
    Write-Host "  .\scripts\setup_env.ps1 -DockerUp" -ForegroundColor White
    Write-Host "    -> Bat Docker containers cho Kafka (9092) va PostgreSQL (5433)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -InitWarehouse" -ForegroundColor White
    Write-Host "    -> Nap DDL khoi tao Star Schema Data Warehouse va 10 Views phan tich." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -DockerDown" -ForegroundColor White
    Write-Host "    -> Dung toan bo Docker containers khi ket thuc phien lam viec." -ForegroundColor DarkGray

    Write-Host "`n[2] SPEED LAYER (REAL-TIME STREAMING END-TO-END):" -ForegroundColor Green
    Write-Host "  .\scripts\setup_env.ps1 -StreamPipeline   [1-CLICK BAT TRON GOI SPEED LAYER]" -ForegroundColor Green
    Write-Host "    -> Tu bat Docker, khoi tao DB, mo Producer (WS) va Spark Streaming trong terminal rieng." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunProducer" -ForegroundColor White
    Write-Host "    -> Chay rieng bo thu thap OKX WebSocket day vao Kafka topic (crypto-trades)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunSpark" -ForegroundColor White
    Write-Host "    -> Chay rieng Spark Structured Streaming Engine (Whale Alerts, 1m Candles, S3 & DW)." -ForegroundColor DarkGray

    Write-Host "`n[3] MEDALLION BATCH LAYER (LAKEHOUSE PIPELINE END-TO-END):" -ForegroundColor Cyan
    Write-Host "  .\scripts\setup_env.ps1 -MedallionPipeline [1-CLICK CHAY TRON GOI MEDALLION BATCH]" -ForegroundColor Cyan
    Write-Host "    -> Chay tuan tu 3 giai doan: Bronze Ingestion -> Silver Cleansing -> Gold Aggregation & DW Sync." -ForegroundColor DarkGray

    Write-Host "`n[4] HOC MAY & DU BAO (XGBOOST ML END-TO-END):" -ForegroundColor Blue
    Write-Host "  .\scripts\setup_env.ps1 -TrainModel" -ForegroundColor White
    Write-Host "    -> Tinh 15 dac trung, phan chia 70/15/15 kem embargo 5m, huan luyen va danh gia baseline." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunPrediction" -ForegroundColor White
    Write-Host "    -> Chay dich vu suy luan real-time (ho tro --once hoac continuous loop), luu vao DW." -ForegroundColor DarkGray

    Write-Host "`n[5] GIAO DIEN TRUC QUAN HOA (WEB DASHBOARD):" -ForegroundColor Magenta
    Write-Host "  .\scripts\setup_env.ps1 -WebDashboard     [WEB TRADING & ML DASHBOARD]" -ForegroundColor Green
    Write-Host "    -> Khoi chay Web Dashboard tai http://127.0.0.1:5000 (Tuy chon: -WorkerMode auto/manual/off, -NoBrowser)." -ForegroundColor DarkGray

    Write-Host "`n[6] KIEM THU TOAN DIEN & TIEN ICH:" -ForegroundColor DarkYellow
    Write-Host "  .\scripts\setup_env.ps1 -TestPipeline" -ForegroundColor White
    Write-Host "    -> Chay bo kiem thu tu dong toan dien End-to-End Pipeline & ML." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -Info" -ForegroundColor White
    Write-Host "    -> Kiem tra phien ban va trang thai moi truong (Python, Spark, Java, Docker, Kafka, DW)." -ForegroundColor DarkGray
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host ""
}
