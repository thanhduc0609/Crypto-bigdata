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
    [switch]$Info,
    [switch]$StartPipeline,
    [switch]$StreamPipeline,
    [switch]$RunProducer,
    [switch]$RunSpark,
    [switch]$RunBronze,
    [switch]$RunSilver,
    [switch]$RunGold,
    [switch]$SyncGoldToDW,
    [switch]$MedallionPipeline,
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
    [string]$WorkerMode = "",
    [switch]$NoBrowser,
    [switch]$Demo,
    [switch]$DockerUp,
    [switch]$DockerDown,
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
# 7. ĐIỀU PHỐI VẬN HÀNH DỰA TRÊN THAM SỐ
# -----------------------------------------------------------------------------
if ($StartPipeline -or $StreamPipeline) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  KICH HOAT TRON GOI PIPELINE STREAMING THOI GIAN THUC (SPEED LAYER)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    # 0. Don dep tien trinh cu cua du an nay (tranh xung dot Checkpoint Spark Concurrent Update)
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

    # 3. Mở cửa sổ riêng cho Producer
    Write-Host "`n[3/3] Mo cua so PowerShell rieng cho Producer va Spark Streaming..." -ForegroundColor Green
    
    $producerProc = Start-Process powershell -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-File", "$PROJECT_ROOT\scripts\setup_env.ps1", "-RunProducer" -WorkingDirectory $PROJECT_ROOT -PassThru
    Start-Sleep -Seconds 2

    # 4. Mở cửa sổ riêng cho Spark Streaming Engine
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
    Write-Host "  MEDALLION BATCH PIPELINE (BRONZE -> SILVER -> GOLD -> DW SYNC)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    Invoke-PythonStep "Stage 1/4: Medallion Bronze Historical Ingestion" "$PROJECT_ROOT\ingestion\historical\bronze_ingestion.py" $ExtraArgs
    Invoke-PythonStep "Stage 2/4: Medallion Silver Cleansing & Deduplication" "$PROJECT_ROOT\processing\batch\bronze_to_silver.py" $ExtraArgs
    Invoke-PythonStep "Stage 3/4: Medallion Gold 1m OHLCV Aggregation" "$PROJECT_ROOT\processing\batch\silver_to_gold.py" $ExtraArgs
    Invoke-PythonStep "Stage 4/4: Synchronize Canonical Gold Candles to DW" "$PROJECT_ROOT\processing\batch\silver_to_gold.py" @("--sync-dw-only")

    Write-Host "`n[SUCCESS] Medallion Pipeline completed all 4 stages successfully!" -ForegroundColor Green
}
elseif ($RunBronze) {
    Invoke-PythonStep "Launching Medallion Bronze Historical Ingestion" "$PROJECT_ROOT\ingestion\historical\bronze_ingestion.py" $ExtraArgs
}
elseif ($RunSilver) {
    Invoke-PythonStep "Launching Medallion Silver Batch Transformation" "$PROJECT_ROOT\processing\batch\bronze_to_silver.py" $ExtraArgs
}
elseif ($RunGold) {
    Invoke-PythonStep "Launching Medallion Gold Batch Aggregation" "$PROJECT_ROOT\processing\batch\silver_to_gold.py" $ExtraArgs
}
elseif ($SyncGoldToDW) {
    Invoke-PythonStep "Synchronizing Canonical Gold Candles to PostgreSQL DW" "$PROJECT_ROOT\processing\batch\silver_to_gold.py" @("--sync-dw-only")
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
    Invoke-PythonStep "Launching OKX Market Data WebSocket Producer" "$PROJECT_ROOT\ingestion\streaming\okx_producer.py"
}
elseif ($RunSpark) {
    Invoke-PythonStep "Launching Spark Structured Streaming Processor (Whale Alerts and S3 Sink)" "$PROJECT_ROOT\processing\streaming\trade_processor.py"
}
elseif ($ViewS3) {
    Invoke-PythonStep "Launching S3 Parquet Streaming Data Lake Explorer" "$PROJECT_ROOT\processing\analysis\s3_parquet_reader.py" $ExtraArgs
}
elseif ($InitS3 -or $TestS3) {
    Invoke-PythonStep "Testing S3 Connection & Initializing Streaming Folders on AWS S3" "$PROJECT_ROOT\tests\test_s3.py"
}
elseif ($InitWarehouse) {
    Invoke-PythonStep "Initializing PostgreSQL DW Schema and Views" "$PROJECT_ROOT\warehouse\etl\db_config.py"
}
elseif ($TestWarehouse) {
    Invoke-PythonStep "Running Automated Streaming Data Warehouse Test Suite" "$PROJECT_ROOT\tests\test_warehouse.py"
}
elseif ($BuildFeatures) {
    Invoke-PythonStep "Executing Feature Engineering & Dataset Creation" "$PROJECT_ROOT\scripts\build_features.py"
}
elseif ($TrainModel) {
    Invoke-PythonStep "Training XGBoost BTC Direction Classifier (5-Minute Horizon)" "$PROJECT_ROOT\scripts\train_model.py"
}
elseif ($RunPrediction) {
    Invoke-PythonStep "Running BTC 5-Minute Direction Inference Service" "$PROJECT_ROOT\scripts\run_prediction.py" $ExtraArgs
}
elseif ($TestPipeline) {
    Invoke-PythonStep "Running Full Pipeline & XGBoost Automated Test Suite" "$PROJECT_ROOT\tests\test_pipeline_e2e.py"
}
elseif ($WebDashboard -or $Dashboard) {
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
elseif (-not $Info) {
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host "  DANH SACH LENH VAN HANH HE THONG (SPEED LAYER & MEDALLION BATCH)" -ForegroundColor Cyan
    Write-Host "==========================================================================" -ForegroundColor Cyan

    Write-Host "`n[1] KHOI TAO HA TANG VA CO SO DU LIEU:" -ForegroundColor Yellow
    Write-Host "  .\scripts\setup_env.ps1 -DockerUp" -ForegroundColor White
    Write-Host "    -> Bat Docker containers cho Kafka (9092) va PostgreSQL (5433)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -InitWarehouse" -ForegroundColor White
    Write-Host "    -> Nap DDL bang DW (dim_symbol, dim_alert_tier, fact_trades, fact_crypto_1m_candles) va 10 Views." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -TestWarehouse" -ForegroundColor White
    Write-Host "    -> Chay bo kiem thu tu dong Data Warehouse, xac thuc do tre thap cua Speed Layer." -ForegroundColor DarkGray

    Write-Host "`n[2] SPEED LAYER (REAL-TIME STREAMING):" -ForegroundColor Green
    Write-Host "  .\scripts\setup_env.ps1 -StreamPipeline   [1-CLICK BAT TRON GOI SPEED LAYER]" -ForegroundColor Green
    Write-Host "    -> Tu bat Docker, mo Producer (WebSocket) va Spark Streaming Engine trong terminal rieng." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunProducer" -ForegroundColor White
    Write-Host "    -> Thu thap lenh khop thoi gian thuc tu OKX day vao Kafka topic (crypto-trades)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunSpark" -ForegroundColor White
    Write-Host "    -> Spark Structured Streaming Engine (Whale Alert, 1m Candles, ghi S3 va DW)." -ForegroundColor DarkGray

    Write-Host "`n[3] MEDALLION BATCH LAYER (LAKEHOUSE PIPELINE):" -ForegroundColor Cyan
    Write-Host "  .\scripts\setup_env.ps1 -MedallionPipeline [1-CLICK CHAY TUAN TU 4 GIAI DOAN]" -ForegroundColor Cyan
    Write-Host "    -> Chay Bronze -> Silver -> Gold -> Sync DW voi kiem tra exit code tung buoc." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunBronze" -ForegroundColor White
    Write-Host "    -> Thu thap lich su REST OKX vao phan vung Bronze (ingestion/historical/bronze_ingestion.py)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunSilver" -ForegroundColor White
    Write-Host "    -> Lam sach, khu trung lap va chuan hoa vao Silver Parquet (processing/batch/bronze_to_silver.py)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunGold" -ForegroundColor White
    Write-Host "    -> Tong hop nen 1m, VWAP, Order Flow va luu Gold Parquet (processing/batch/silver_to_gold.py)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -SyncGoldToDW" -ForegroundColor White
    Write-Host "    -> Dong bo ket qua Gold Parquet canonical truc tiep vao PostgreSQL fact_crypto_1m_candles." -ForegroundColor DarkGray

    Write-Host "`n[4] HOC MAY & DU BAO (XGBOOST CLASSIFIER):" -ForegroundColor Blue
    Write-Host "  .\scripts\setup_env.ps1 -BuildFeatures    -> Tinh toan 15 dac trung ky thuat va tao tap du lieu huan luyen." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -TrainModel       -> Huan luyen mo hinh XGBoost 5 phut tren chuoi thoi gian." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -RunPrediction    -> Chay dich vu suy luan real-time, luu vao PostgreSQL DW." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -TestPipeline     -> Chay bo kiem thu tu dong toan dien pipeline & ML." -ForegroundColor DarkGray

    Write-Host "`n[5] KHAI THAC VA TRUC QUAN HOA:" -ForegroundColor Magenta
    Write-Host "  .\scripts\setup_env.ps1 -WebDashboard     [WEB TRADING & ML DASHBOARD]" -ForegroundColor Green
    Write-Host "    -> Khoi chay Web Dashboard tai http://127.0.0.1:5000 (Tuy chon: -WorkerMode auto/manual/off, -NoBrowser)." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -ViewS3" -ForegroundColor White
    Write-Host "    -> Menu tuong tac kham pha Parquet Streaming tren S3 Data Lake." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -InitS3" -ForegroundColor Yellow
    Write-Host "    -> Khoi tao nhanh cay thu muc Parquet tren AWS S3." -ForegroundColor DarkGray

    Write-Host "`n[TIEN ICH]:" -ForegroundColor DarkYellow
    Write-Host "  .\scripts\setup_env.ps1 -Info      -> Kiem tra phien ban Python, Spark, Java, Docker." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -DockerDown-> Dung toan bo Docker containers khi ket thuc." -ForegroundColor DarkGray
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host ""
}
