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

    # [8] Trọn gói Local Demo (Khởi chạy & Dừng an toàn)
    [Alias("Demo")]
    [switch]$StartDemo,
    [switch]$StopDemo,
    [switch]$Streaming,
    [switch]$Batch,
    [switch]$Admin,

    # [9] Kiểm thử tự động toàn diện End-to-End
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
        $kContainer = docker compose --project-name crypto-bigdata --env-file "$PROJECT_ROOT/.env" -f "$PROJECT_ROOT/docker-compose.yml" ps --status running --format "{{.Status}}" kafka 2>&1
        if ($kContainer -and $kContainer -notmatch "error") {
            $kafkaStatus = "Running ($kContainer)"
        }
        $pContainer = docker compose --project-name crypto-bigdata --env-file "$PROJECT_ROOT/.env" -f "$PROJECT_ROOT/docker-compose.yml" ps --status running --format "{{.Status}}" postgres 2>&1
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
# 7. HÀM QUẢN LÝ VÒNG ĐỜI DEMO CỤC BỘ (SELF-CONTAINED LOCAL DEMO ORCHESTRATION)
# -----------------------------------------------------------------------------
function Start-ProjectLocalDemo {
    param(
        [switch]$Streaming,
        [switch]$Batch,
        [switch]$Admin
    )
    $demoRepo = $PROJECT_ROOT
    $demoPython = Join-Path $demoRepo '.venv\Scripts\python.exe'
    $demoState = Join-Path $demoRepo 'data\runtime'
    $demoLogs = Join-Path $demoState 'logs'
    if (-not (Test-Path -LiteralPath $demoPython)) { throw 'Python virtual environment (.venv) not found. Create .venv and install requirements.txt first.' }
    if (-not (Test-Path -LiteralPath (Join-Path $demoRepo '.env'))) { throw 'Copy .env.example to .env and fill the settings.' }
    New-Item -ItemType Directory -Path $demoLogs -Force | Out-Null
    $demoDockerArguments = @((Join-Path $PSScriptRoot 'demo\docker_environment.py'), 'up')
    if ($Admin) { $demoDockerArguments += '--admin' }
    & $demoPython @demoDockerArguments
    if ($LASTEXITCODE -ne 0) { throw 'Docker failed. Inspect the message above; no data volumes were removed.' }
    $demoReady = $false
    for ($demoAttempt = 0; $demoAttempt -lt 15; $demoAttempt++) {
        docker compose --project-name crypto-bigdata --env-file (Join-Path $demoRepo '.env') -f (Join-Path $demoRepo 'docker-compose.yml') exec -T postgres sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { $demoReady = $true; break }
        Start-Sleep -Seconds 2
    }
    if (-not $demoReady) { throw 'PostgreSQL is not ready; inspect Docker logs.' }
    $demoServices = @(
        @{Name = 'web'; Script = 'scripts\demo\local_service.py'; Args = @('web') },
        @{Name = 'monitor'; Script = 'scripts\demo\monitor_demo.py'; Args = @('--watch') }
    )
    if ($Streaming) {
        $demoServices += @{Name = 'producer'; Script = 'scripts\demo\local_service.py'; Args = @('producer') }
        $demoServices += @{Name = 'spark'; Script = 'scripts\demo\local_service.py'; Args = @('spark') }
    }
    if ($Batch -or $Streaming) {
        $demoServices += @{Name = 'batch'; Script = 'scripts\pipeline\run_batch_medallion.py'; Args = @('--watch', '--interval', '300') }
    }
    foreach ($demoService in $demoServices) {
        $demoScript = Join-Path $demoRepo $demoService.Script
        $demoPidFile = Join-Path $demoState ($demoService.Name + '.pid')
        if (Test-Path -LiteralPath $demoPidFile) {
            $demoExistingId = [int](Get-Content -LiteralPath $demoPidFile -Raw)
            $demoExisting = Get-CimInstance Win32_Process -Filter "ProcessId = $demoExistingId" -ErrorAction SilentlyContinue
            if ($demoExisting -and $demoExisting.CommandLine.Contains($demoScript) -and $demoExisting.CommandLine.Contains($demoService.Args[0])) {
                Write-Host ($demoService.Name + ' already running')
                continue
            }
        }
        $demoArguments = @('-u', ('"' + $demoScript + '"')) + $demoService.Args
        $demoOldZone = $env:TZ
        try {
            $env:TZ = 'UTC'
            $demoProcess = Start-Process -FilePath $demoPython -ArgumentList $demoArguments -WorkingDirectory $demoRepo -WindowStyle Hidden -RedirectStandardOutput (Join-Path $demoLogs ($demoService.Name + '.out.log')) -RedirectStandardError (Join-Path $demoLogs ($demoService.Name + '.err.log')) -PassThru
        }
        finally { $env:TZ = $demoOldZone }
        Set-Content -LiteralPath $demoPidFile -Value $demoProcess.Id
        Write-Host ($demoService.Name + ' started: ' + $demoProcess.Id)
    }
    $demoWebReady = $false
    for ($demoAttempt = 0; $demoAttempt -lt 20; $demoAttempt++) {
        try {
            $demoHealth = Invoke-RestMethod -Uri 'http://127.0.0.1:5001/api/health' -TimeoutSec 2
            if ($demoHealth.database_connected -and $demoHealth.status -eq 'online') { $demoWebReady = $true; break }
        }
        catch {}
        Start-Sleep -Milliseconds 500
    }
    if (-not $demoWebReady) { throw 'Web or database did not become ready; inspect data/runtime/logs.' }
    Write-Host 'Dashboard ready: http://127.0.0.1:5001 | Process logs: data/runtime/logs'
    Write-Host 'Readiness: /api/system/status reports measured stages; startup alone is not an end-to-end pass.'
}

function Stop-ProjectLocalDemo {
    param(
        [switch]$IncludeLegacy,
        [string[]]$Service = @('batch', 'monitor', 'producer', 'spark', 'web')
    )
    $demoRepo = $PROJECT_ROOT
    $demoState = Join-Path $demoRepo 'data\runtime'

    function Stop-OwnedService($demoPidFile, $demoExpectedScript, $demoExpectedArgument) {
        if (-not (Test-Path -LiteralPath $demoPidFile)) { return }
        $demoProcessId = [int](Get-Content -LiteralPath $demoPidFile -Raw)
        $demoProcess = Get-CimInstance Win32_Process -Filter "ProcessId = $demoProcessId" -ErrorAction SilentlyContinue
        if (-not $demoProcess) { return }
        if (-not $demoProcess.CommandLine.Contains($demoExpectedScript) -or ($demoExpectedArgument -and -not $demoProcess.CommandLine.Contains($demoExpectedArgument))) {
            throw ('Refusing to stop an unowned PID ' + $demoProcessId)
        }
        $demoAll = @(Get-CimInstance Win32_Process)
        function Stop-ChildTree([int]$demoParent) {
            foreach ($demoChild in $demoAll | Where-Object { $_.ParentProcessId -eq $demoParent }) {
                Stop-ChildTree $demoChild.ProcessId
                Stop-Process -Id $demoChild.ProcessId -Force -ErrorAction SilentlyContinue
            }
        }
        Stop-ChildTree $demoProcessId
        Stop-Process -Id $demoProcessId -Force -ErrorAction SilentlyContinue
        Write-Host ('Stopped owned service ' + $demoProcessId)
    }

    foreach ($demoName in $Service) {
        $demoScript = if ($demoName -eq 'batch') { 'run_batch_medallion.py' } elseif ($demoName -eq 'monitor') { 'monitor_demo.py' } else { 'local_service.py' }
        $demoArgument = if ($demoName -in @('producer', 'spark', 'web')) { $demoName } else { '' }
        Stop-OwnedService (Join-Path $demoState ($demoName + '.pid')) $demoScript $demoArgument
    }
    if ($IncludeLegacy) {
        $demoLegacy = Join-Path (Split-Path -Parent $demoRepo) 'local-run'
        foreach ($demoName in ($Service | Where-Object { $_ -in @('producer', 'spark', 'web') })) {
            $demoFile = if ($demoName -eq 'spark') { 'run_streaming.py' } else { 'run_' + $demoName + '.py' }
            Stop-OwnedService (Join-Path $demoLegacy ($demoName + '.pid')) $demoFile ''
        }
    }
    Write-Host 'Docker services, persisted data and checkpoints are retained.'
}

# -----------------------------------------------------------------------------
# 8. ĐIỀU PHỐI VẬN HÀNH DỰA TRÊN THAM SỐ (END-TO-END PIPELINE & CORE COMMANDS)
# -----------------------------------------------------------------------------
if ($StreamPipeline) {
    Start-ProjectLocalDemo -Streaming -Batch
}
elseif ($StartDemo) {
    Start-ProjectLocalDemo -Streaming:$Streaming -Batch:$Batch -Admin:$Admin
    if (-not $NoBrowser) { Start-Process "http://127.0.0.1:5001" }
}
elseif ($StopDemo) {
    Stop-ProjectLocalDemo -IncludeLegacy
}
elseif ($MedallionPipeline) {
    Invoke-PythonStep "Versioned project Silver/Gold and DW sync" "$PROJECT_ROOT/scripts/pipeline/run_batch_medallion.py" $ExtraArgs
}
elseif ($DockerUp) {
    Write-Host "`n[+] Starting Kafka and PostgreSQL Containers..." -ForegroundColor Yellow
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT/scripts/demo/docker_environment.py" up
}
elseif ($DockerDown) {
    Write-Host "`n[-] Stopping Docker Containers..." -ForegroundColor Yellow
    & $env:PYSPARK_PYTHON "$PROJECT_ROOT/scripts/demo/docker_environment.py" stop
}
elseif ($InitWarehouse) {
    Invoke-PythonStep "Initializing PostgreSQL DW Schema and Views" "$PROJECT_ROOT\warehouse\etl\db_config.py"
}
elseif ($RunProducer) {
    Invoke-PythonStep "Launching OKX Market Data WebSocket Producer" "$PROJECT_ROOT/scripts/demo/local_service.py" @("producer")
}
elseif ($RunSpark) {
    Invoke-PythonStep "Launching Spark Structured Streaming Processor (Whale Alerts and S3 Sink)" "$PROJECT_ROOT/scripts/demo/local_service.py" @("spark")
}
elseif ($TrainModel) {
    Invoke-PythonStep "Training XGBoost BTC Direction Classifier (5-Minute Horizon)" "$PROJECT_ROOT\scripts\ml\train_model.py"
}
elseif ($RunPrediction) {
    Invoke-PythonStep "Running BTC 5-Minute Direction Inference Service" "$PROJECT_ROOT\scripts\ml\run_prediction.py" $ExtraArgs
}
elseif ($WebDashboard) {
    if ($WorkerMode) { $env:PREDICTION_WORKER_MODE = $WorkerMode }
    Start-ProjectLocalDemo
    if (-not $NoBrowser) { Start-Process "http://127.0.0.1:5001" }
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
    Write-Host "    -> Dung launcher chung, quan ly PID/log va khong tao tien trinh trung." -ForegroundColor DarkGray
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

    Write-Host "`n[5] GIAO DIEN TRUC QUAN HOA VA TRON GOI LOCAL DEMO:" -ForegroundColor Magenta
    Write-Host "  .\scripts\setup_env.ps1 -StartDemo        [1-CLICK KHOI CHAY LOCAL DEMO]" -ForegroundColor Green
    Write-Host "    -> Bat Docker, Web Dashboard tai http://127.0.0.1:5001 va tu dong giam sat." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -StartDemo -Streaming -Batch" -ForegroundColor White
    Write-Host "    -> Bat tron goi ca Speed Layer (OKX Producer, Spark Streaming) va Medallion Batch." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -StopDemo         [1-CLICK DUNG AN TOAN LOCAL DEMO]" -ForegroundColor Red
    Write-Host "    -> Dung sach se toan bo cac tien trinh web, spark, producer, batch va monitor." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -WebDashboard     [WEB TRADING & ML DASHBOARD]" -ForegroundColor White
    Write-Host "    -> Khoi chay rieng Web Dashboard tai http://127.0.0.1:5001 (Tuy chon: -WorkerMode, -NoBrowser)." -ForegroundColor DarkGray

    Write-Host "`n[6] KIEM THU TOAN DIEN & TIEN ICH:" -ForegroundColor DarkYellow
    Write-Host "  .\scripts\setup_env.ps1 -TestPipeline" -ForegroundColor White
    Write-Host "    -> Chay bo kiem thu tu dong toan dien End-to-End Pipeline & ML." -ForegroundColor DarkGray
    Write-Host "  .\scripts\setup_env.ps1 -Info" -ForegroundColor White
    Write-Host "    -> Kiem tra phien ban va trang thai moi truong (Python, Spark, Java, Docker, Kafka, DW)." -ForegroundColor DarkGray
    Write-Host "`n==========================================================================" -ForegroundColor Cyan
    Write-Host ""
}
