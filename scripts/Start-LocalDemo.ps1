param([switch]$Streaming,[switch]$Batch,[switch]$Admin)
$ErrorActionPreference='Stop'
$demoRepo=(Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$demoPython=Join-Path $demoRepo '.venv\Scripts\python.exe'
$demoState=Join-Path $demoRepo 'data\runtime'
$demoLogs=Join-Path $demoState 'logs'
if (-not (Test-Path -LiteralPath $demoPython)) { throw 'Create .venv and install requirements.local.lock.txt first.' }
if (-not (Test-Path -LiteralPath (Join-Path $demoRepo '.env'))) { throw 'Copy .env.example to .env and fill the settings.' }
New-Item -ItemType Directory -Path $demoLogs -Force | Out-Null
$demoDockerArguments=@((Join-Path $PSScriptRoot 'docker_environment.py'),'up')
if ($Admin) {$demoDockerArguments+='--admin'}
& $demoPython @demoDockerArguments
if ($LASTEXITCODE -ne 0) { throw 'Docker failed. Inspect the message above; no data volumes were removed.' }
$demoReady=$false
for ($demoAttempt=0;$demoAttempt -lt 15;$demoAttempt++) {
    docker compose --project-name crypto-bigdata --env-file (Join-Path $demoRepo '.env') -f (Join-Path $demoRepo 'docker-compose.yml') exec -T postgres sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {$demoReady=$true;break}
    Start-Sleep -Seconds 2
}
if (-not $demoReady) {throw 'PostgreSQL is not ready; inspect Docker logs.'}
$demoServices=@(@{Name='web';Script='local_service.py';Args=@('web')},@{Name='monitor';Script='monitor_demo.py';Args=@('--watch')})
if ($Streaming) {
    $demoServices+=@{Name='producer';Script='local_service.py';Args=@('producer')}
    $demoServices+=@{Name='spark';Script='local_service.py';Args=@('spark')}
}
if ($Batch -or $Streaming) {$demoServices+=@{Name='batch';Script='run_operational_batch.py';Args=@('--watch','--interval','300')}}
foreach ($demoService in $demoServices) {
    $demoScript=Join-Path $PSScriptRoot $demoService.Script
    $demoPidFile=Join-Path $demoState ($demoService.Name+'.pid')
    if (Test-Path -LiteralPath $demoPidFile) {
        $demoExistingId=[int](Get-Content -LiteralPath $demoPidFile -Raw)
        $demoExisting=Get-CimInstance Win32_Process -Filter "ProcessId = $demoExistingId" -ErrorAction SilentlyContinue
        if ($demoExisting -and $demoExisting.CommandLine.Contains($demoScript) -and $demoExisting.CommandLine.Contains($demoService.Args[0])) {
            Write-Host ($demoService.Name+' already running');continue
        }
    }
    $demoArguments=@('-u',('"'+$demoScript+'"'))+$demoService.Args
    $demoOldZone=$env:TZ
    try {
        $env:TZ='UTC'
        $demoProcess=Start-Process -FilePath $demoPython -ArgumentList $demoArguments -WorkingDirectory $demoRepo -WindowStyle Hidden -RedirectStandardOutput (Join-Path $demoLogs ($demoService.Name+'.out.log')) -RedirectStandardError (Join-Path $demoLogs ($demoService.Name+'.err.log')) -PassThru
    } finally {$env:TZ=$demoOldZone}
    Set-Content -LiteralPath $demoPidFile -Value $demoProcess.Id
    Write-Host ($demoService.Name+' started: '+$demoProcess.Id)
}
$demoWebReady=$false
for ($demoAttempt=0;$demoAttempt -lt 20;$demoAttempt++) {
    try {
        $demoHealth=Invoke-RestMethod -Uri 'http://127.0.0.1:5001/api/health' -TimeoutSec 2
        if ($demoHealth.database_connected -and $demoHealth.status -eq 'online') {$demoWebReady=$true;break}
    } catch {}
    Start-Sleep -Milliseconds 500
}
if (-not $demoWebReady) {throw 'Web or database did not become ready; inspect data/runtime/logs.'}
Write-Host 'Dashboard ready: http://127.0.0.1:5001 | Process logs: data/runtime/logs'
Write-Host 'Readiness: /api/system/status reports measured stages; startup alone is not an end-to-end pass.'
