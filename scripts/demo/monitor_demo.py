"""Bounded read-only stage measurements every 30 seconds."""
import argparse
import json
import os
from pathlib import Path
import sys
import time
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT));os.chdir(ROOT)
from runtime.telemetry import collect

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--watch",action="store_true");args=p.parse_args()
    while True:
        started=time.monotonic()
        try:print(json.dumps(collect(),ensure_ascii=False),flush=True)
        except Exception as exc:print("MONITOR_FAILED "+type(exc).__name__,flush=True)
        if not args.watch:break
        time.sleep(max(1,30-(time.monotonic()-started)))
