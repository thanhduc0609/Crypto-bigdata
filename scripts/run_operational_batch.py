"""Versioned Silver/Gold batch, once or at a non-overlapping local interval."""
import argparse
import json
import os
from pathlib import Path
import sys
import threading
import time
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT));os.chdir(ROOT)
from runtime.medallion import run_batch,atomic_json,now
from runtime.environment import state_directory


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--watch",action="store_true");p.add_argument("--interval",type=int,default=300)
    p.add_argument("--no-db",action="store_true")
    args=p.parse_args()
    if args.interval<60:p.error("Interval must be at least 60 seconds")
    while True:
        start=time.monotonic()
        if args.watch:atomic_json(state_directory()/"scheduler.json",dict(mode="watch",status="running",checked_at=now(),interval_seconds=args.interval))
        try:print(json.dumps(run_batch(not args.no_db),ensure_ascii=False),flush=True)
        except Exception as exc:
            print("BATCH_FAILED "+type(exc).__name__,flush=True)
            if not args.watch:raise
        if not args.watch:return
        deadline=start+args.interval
        while time.monotonic()<deadline:
            atomic_json(state_directory()/"scheduler.json",dict(mode="watch",status="waiting",checked_at=now(),interval_seconds=args.interval))
            time.sleep(min(10,max(.1,deadline-time.monotonic())))


if __name__=="__main__":main()
