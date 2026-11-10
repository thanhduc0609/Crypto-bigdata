"""Bounded local HTTP soak: measured latency/error rate, not an Internet-scale claim."""
import argparse
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import sys
import threading
import time
import requests
import numpy as np
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from runtime.medallion import atomic_json
from runtime.environment import state_directory
from runtime.resources import sample as sample_resources

ROUTES=["/api/health","/api/candles?timeframe=1m&limit=120","/api/system/status", "/api/ml/review",
        "/api/forecast/long-term","/api/forecast/long-term/research","/api/ml/prediction","/api/trades/large?limit=10"]
local=threading.local()


def request(route):
    if not hasattr(local,"session"):local.session=requests.Session()
    start=time.perf_counter()
    try:
        response=local.session.get("http://127.0.0.1:5001"+route,timeout=10)
        result=dict(route=route,status=response.status_code,ok=response.status_code==200,bytes=len(response.content))
        try:result["application_status"]=response.json().get("status")
        except (ValueError,AttributeError):pass
    except requests.RequestException as exc:result=dict(route=route,status=None,ok=False,error_type=type(exc).__name__)
    result.update(elapsed_ms=(time.perf_counter()-start)*1000,at=datetime.now(timezone.utc).isoformat())
    return result


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--seconds",type=int,default=300);p.add_argument("--output",type=Path,required=True)
    args=p.parse_args()
    if not 30<=args.seconds<=1800:p.error("Bounded duration is 30..1800 seconds")
    args.output.mkdir(parents=True,exist_ok=True)
    if (args.output/"load-result.json").exists():p.error("Use a new output directory; preserve prior attempts")
    readiness_started=time.monotonic();readiness=[]
    while True:
        try:
            r=requests.get("http://127.0.0.1:5001/api/health",timeout=2)
            ready=r.status_code==200 and r.json().get("database_connected") is True
            readiness.append(dict(status=r.status_code,ready=ready))
            if ready:break
        except (requests.RequestException,ValueError):readiness.append(dict(status=None,ready=False))
        if time.monotonic()-readiness_started>30:raise RuntimeError("Web/DB did not become ready within 30 seconds")
        time.sleep(.5)
    readiness_seconds=time.monotonic()-readiness_started
    resource_records=[];resource_stop=threading.Event()
    def measure_resources():
        while not resource_stop.is_set():
            resource_records.append(sample_resources())
            resource_stop.wait(10)
    resource_thread=threading.Thread(target=measure_resources,daemon=True);resource_thread.start()
    before=json.loads((state_directory()/"telemetry.json").read_text())
    start=time.monotonic();submitted=[]
    with ThreadPoolExecutor(max_workers=4) as pool:
        for i in range(args.seconds*2):
            submitted.append(pool.submit(request,ROUTES[i%len(ROUTES)]))
            delay=start+(i+1)*.5-time.monotonic()
            if delay>0:time.sleep(delay)
        records=[future.result() for future in submitted]
    after=json.loads((state_directory()/"telemetry.json").read_text())
    latency=np.array([r["elapsed_ms"] for r in records])
    resource_stop.set();resource_thread.join(timeout=16)
    measured=[r for r in resource_records if r.get("status")=="measured"]
    rss=[sum(p["rss_bytes"] for p in r["processes"]) for r in measured]
    cpu=[]
    for a,b in zip(measured,measured[1:]):
        elapsed=(datetime.fromisoformat(b["at"].replace('Z','+00:00'))-datetime.fromisoformat(a["at"].replace('Z','+00:00'))).total_seconds()
        previous={p["pid"]:p["cpu_seconds"] for p in a["processes"]}
        delta=sum(max(0,p["cpu_seconds"]-previous[p["pid"]]) for p in b["processes"] if p["pid"] in previous)
        if elapsed>0:cpu.append(100*delta/elapsed/b["logical_processors"])
    failures=[r for r in records if not r["ok"]]
    result=dict(started_at=records[0]["at"],finished_at=datetime.now(timezone.utc).isoformat(),requests=len(records),
        target_requests_per_second=2,max_concurrency=4,failures=len(failures),error_rate=len(failures)/len(records),
        p50_ms=float(np.quantile(latency,.5)),p95_ms=float(np.quantile(latency,.95)),max_ms=float(latency.max()),
        per_route={route:dict(requests=sum(r["route"]==route for r in records),failures=sum(r["route"]==route and not r["ok"] for r in records),
            p95_ms=float(np.quantile([r["elapsed_ms"] for r in records if r["route"]==route],.95))) for route in ROUTES},
        readiness_wait_seconds=readiness_seconds,readiness_attempts=readiness,
        resource_summary=dict(samples=len(measured),peak_demo_rss_mib=max(rss)/1024**2 if rss else None,
            peak_cpu_pct_total_machine=max(cpu) if cpu else None,
            cpu_scope="Interval deltas for surviving owned demo PIDs; short-lived processes may be missed"),
        telemetry_before=before,telemetry_after=after,passed=not failures,
        scope="Five-minute demo soak at 2 requests/sec, max 4 in flight; not a capacity benchmark or long-duration SLA")
    atomic_json(args.output/"load-result.json",result)
    atomic_json(args.output/"http-records.json",records)
    atomic_json(args.output/"resource-records.json",resource_records)
    print(json.dumps({k:v for k,v in result.items() if k not in ["telemetry_before","telemetry_after","per_route"]}),flush=True)
    raise SystemExit(0 if result["passed"] else 1)
