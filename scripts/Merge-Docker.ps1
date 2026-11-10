param([ValidateSet('codex-crypto-local','crypto-bigdata')][string]$SourceProject)
$ErrorActionPreference='Stop'
$demoRepo=(Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$demoPython=Join-Path $demoRepo '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $demoPython)) {throw 'Create .venv and install requirements.local.lock.txt first.'}
& (Join-Path $PSScriptRoot 'Stop-LocalDemo.ps1') -IncludeLegacy
$demoArguments=@((Join-Path $PSScriptRoot 'docker_environment.py'),'migrate')
if ($SourceProject) {$demoArguments+=@('--source-project',$SourceProject)}
& $demoPython @demoArguments
if ($LASTEXITCODE -ne 0) {throw 'Migration stopped. Source data and backups are retained. See docs/DOCKER_SETUP.md.'}
Write-Host 'Start-LocalDemo.ps1 can now run the unified stack. See docs/DOCKER_SETUP.md for rollback and retirement.'
