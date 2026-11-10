param([switch]$IncludeLegacy,[ValidateSet('batch','monitor','producer','spark','web')][string[]]$Service=@('batch','monitor','producer','spark','web'))
$ErrorActionPreference='Stop'
$demoRepo=(Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$demoState=Join-Path $demoRepo 'data\runtime'
function Stop-OwnedService($demoPidFile,$demoExpectedScript,$demoExpectedArgument) {
    if (-not (Test-Path -LiteralPath $demoPidFile)) {return}
    $demoProcessId=[int](Get-Content -LiteralPath $demoPidFile -Raw)
    $demoProcess=Get-CimInstance Win32_Process -Filter "ProcessId = $demoProcessId" -ErrorAction SilentlyContinue
    if (-not $demoProcess) {return}
    if (-not $demoProcess.CommandLine.Contains($demoExpectedScript) -or ($demoExpectedArgument -and -not $demoProcess.CommandLine.Contains($demoExpectedArgument))) {
        throw ('Refusing to stop an unowned PID '+$demoProcessId)
    }
    $demoAll=@(Get-CimInstance Win32_Process)
    function Stop-ChildTree([int]$demoParent) {
        foreach ($demoChild in $demoAll | Where-Object {$_.ParentProcessId -eq $demoParent}) {
            Stop-ChildTree $demoChild.ProcessId
            Stop-Process -Id $demoChild.ProcessId -Force -ErrorAction SilentlyContinue
        }
    }
    Stop-ChildTree $demoProcessId
    Stop-Process -Id $demoProcessId -Force -ErrorAction SilentlyContinue
    Write-Host ('Stopped owned service '+$demoProcessId)
}
foreach ($demoName in $Service) {
    $demoScript=if ($demoName -eq 'batch') {'run_operational_batch.py'} elseif ($demoName -eq 'monitor') {'monitor_demo.py'} else {'local_service.py'}
    $demoArgument=if ($demoName -in @('producer','spark','web')) {$demoName} else {''}
    Stop-OwnedService (Join-Path $demoState ($demoName+'.pid')) (Join-Path $PSScriptRoot $demoScript) $demoArgument
}
if ($IncludeLegacy) {
    $demoLegacy=Join-Path (Split-Path -Parent $demoRepo) 'local-run'
    foreach ($demoName in ($Service | Where-Object {$_ -in @('producer','spark','web')})) {
        $demoFile=if ($demoName -eq 'spark') {'run_streaming.py'} else {'run_'+$demoName+'.py'}
        Stop-OwnedService (Join-Path $demoLegacy ($demoName+'.pid')) (Join-Path $demoLegacy $demoFile) ''
    }
}
Write-Host 'Docker services, persisted data and checkpoints are retained.'
