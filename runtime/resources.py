"""Read-only Windows resource sampling for the owned demo process tree."""
import json
import os
from pathlib import Path
import subprocess
from runtime.environment import state_directory


def sample():
    if os.name!="nt":return dict(status="unavailable",reason="Windows sampler only")
    roots=[]
    for file in state_directory().glob("*.pid"):
        try:roots.append(int(file.read_text().strip()))
        except (ValueError,OSError):pass
    if not roots:return dict(status="unavailable",reason="No managed process ids")
    script=r'''
$demoRoots=@(ROOT_IDS)
$demoAll=@(Get-CimInstance Win32_Process)
$demoOwned=@($demoAll | Where-Object {$_.ProcessId -in $demoRoots})
for ($demoDepth=0;$demoDepth -lt 5;$demoDepth++) {
  $demoIds=@($demoOwned | ForEach-Object {$_.ProcessId})
  $demoNew=@($demoAll | Where-Object {$_.ParentProcessId -in $demoIds -and $_.ProcessId -notin $demoIds})
  if ($demoNew.Count -eq 0) {break}
  $demoOwned+=$demoNew
}
$demoRows=@($demoOwned | ForEach-Object {[pscustomobject]@{
  pid=$_.ProcessId;name=$_.Name;rss_bytes=[double]$_.WorkingSetSize;
  cpu_seconds=([double]$_.KernelModeTime+[double]$_.UserModeTime)/10000000
}})
$demoOS=Get-CimInstance Win32_OperatingSystem
[pscustomobject]@{status='measured';at=[DateTime]::UtcNow.ToString('o');
  processes=$demoRows;logical_processors=[Environment]::ProcessorCount;
  machine_total_memory_bytes=[double]$demoOS.TotalVisibleMemorySize*1024;
  machine_free_memory_bytes=[double]$demoOS.FreePhysicalMemory*1024} | ConvertTo-Json -Depth 5 -Compress
'''.replace("ROOT_IDS",",".join(str(p) for p in roots))
    try:
        result=subprocess.run(["powershell.exe","-NoProfile","-NonInteractive","-Command",script],capture_output=True,text=True,timeout=15)
        if result.returncode:raise ValueError("Resource query failed")
        return json.loads(result.stdout)
    except (ValueError,subprocess.SubprocessError,OSError) as exc:return dict(status="unavailable",reason=type(exc).__name__)
