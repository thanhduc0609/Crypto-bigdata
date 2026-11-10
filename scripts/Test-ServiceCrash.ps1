param([Parameter(Mandatory=$true)][ValidateSet('spark','producer')][string]$Service)
$ErrorActionPreference='Stop'
$demoRepo=(Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$demoScript=Join-Path $PSScriptRoot 'local_service.py'
$demoRootId=[int](Get-Content -LiteralPath (Join-Path $demoRepo ('data\runtime\'+$Service+'.pid')) -Raw)
$demoAll=@(Get-CimInstance Win32_Process)
$demoRoot=$demoAll | Where-Object {$_.ProcessId -eq $demoRootId}
if (-not $demoRoot -or -not $demoRoot.CommandLine.Contains($demoScript)) {throw 'No verified owned service root'}
$demoOwned=@($demoRoot)
for ($demoDepth=0;$demoDepth -lt 5;$demoDepth++) {
    $demoIds=@($demoOwned | ForEach-Object {$_.ProcessId})
    $demoNew=@($demoAll | Where-Object {$_.ParentProcessId -in $demoIds -and $_.ProcessId -notin $demoIds})
    if ($demoNew.Count -eq 0) {break}
    $demoOwned+=$demoNew
}
$demoChildren=@($demoOwned | Where-Object {$_.Name -eq 'python.exe' -and $_.CommandLine.Contains($demoScript) -and $_.CommandLine.Contains('--child') -and $_.CommandLine -match ('\s'+$Service+'\s')})
$demoChildIds=@($demoChildren | ForEach-Object {$_.ProcessId})
$demoTargets=@($demoChildren | Where-Object {$_.ParentProcessId -notin $demoChildIds})
if ($demoTargets.Count -ne 1) {throw 'Expected exactly one supervised child; refusing ambiguous crash test'}
function Stop-TestChildTree([int]$demoParent) {
    foreach ($demoChild in $demoOwned | Where-Object {$_.ParentProcessId -eq $demoParent}) {
        Stop-TestChildTree $demoChild.ProcessId
        Stop-Process -Id $demoChild.ProcessId -Force -ErrorAction SilentlyContinue
    }
}
$demoChildRoot=$demoTargets[0].ProcessId
Stop-TestChildTree $demoChildRoot
Stop-Process -Id $demoChildRoot -Force -ErrorAction SilentlyContinue
[pscustomobject]@{service=$Service;supervised_root_retained=$demoRootId;stopped_child_root=$demoChildRoot;at=[DateTime]::UtcNow.ToString('o')} | ConvertTo-Json -Compress
