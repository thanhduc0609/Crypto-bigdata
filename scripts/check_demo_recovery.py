"""Controlled owned-child crash; supervisor, Kafka replay and DW keys verified."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from runtime.telemetry import collect
from runtime.medallion import atomic_json
from runtime.environment import state_directory


def head(report):
    return sum(report["stages"]["kafka"].get("end_offsets",{}).values())


def run(service):
    folder=ROOT/"data/demo_audits"/(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")+"-recovery-"+service)
    folder.mkdir(parents=True)
    pointer=ROOT/"configs/canonical_snapshot.json";digest=hashlib.sha256(pointer.read_bytes()).hexdigest()
    before=collect()
    if before["stages"]["kafka"]["status"]!="verified":raise ValueError("Kafka not ready before crash test")
    log=state_directory()/"logs"/(service+".out.log")
    log_position=log.stat().st_size
    start=time.monotonic()
    crash=subprocess.run(["powershell.exe","-NoProfile","-NonInteractive","-File",str(ROOT/"scripts/Test-ServiceCrash.ps1"),"-Service",service],
        capture_output=True,text=True,timeout=20,check=True)
    action=json.loads(crash.stdout)
    time.sleep(10)
    buffered=collect();target=head(buffered)
    samples=[buffered];deadline=start+180
    while time.monotonic()<deadline:
        current=collect();samples.append(current)
        stages=current["stages"]
        k=stages["kafka"]
        topic=k.get("topic","")
        def processed(name):return sum(stages[name].get("offsets",{}).get(topic,{}).values())>=target
        if service=="spark":recovered=processed("bronze") and processed("speed_dw")
        else:
            with log.open("rb") as handle:handle.seek(log_position);newlog=handle.read().decode("utf-8",errors="replace")
            recovered=head(current)>head(before) and "Active Subscription" in newlog
        if recovered:break
        time.sleep(5)
    else:recovered=False
    elapsed=time.monotonic()-start
    after=samples[-1]
    checks=dict(service_automatically_recovered=bool(recovered),kafka_offset_did_not_rewind=head(after)>=head(before),
        dw_no_duplicate_candle_keys=after["stages"]["warehouse"].get("duplicate_candle_keys")==0,
        frozen_canonical_unchanged=hashlib.sha256(pointer.read_bytes()).hexdigest()==digest)
    if service=="spark":checks["producer_kept_buffering_during_restart"]=head(buffered)>head(before)
    report=dict(service=service,crash_action=action,recovery_seconds=elapsed,target_kafka_offset_sum=target,
        checks=checks,passed=all(checks.values()),before=before,after=after,samples=samples,
        scope="Owned process crash only. Demonstrates captured Kafka replay/reconnect, not completeness of exchange trades while source is unavailable.")
    atomic_json(folder/"recovery.json",report)
    print(json.dumps(dict(service=service,recovery_seconds=elapsed,checks=checks,report=str(folder/"recovery.json")),indent=2),flush=True)
    if not report["passed"]:raise SystemExit(1)


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("service",choices=["spark","producer"])
    run(p.parse_args().service)
