"""Repository-owned entry points for local web, producer and Spark services."""
import argparse
import os
import sys
import subprocess
import time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT));os.chdir(ROOT)
from runtime.environment import configure,stream_checkpoints


def restart_delay(uptime,failures):
    return 5 if uptime>=120 else min(60,5*2**min(failures,4))


def supervise(service):
    from runtime.medallion import atomic_json,now
    from runtime.environment import state_directory
    failures=0
    while True:
        started=time.monotonic()
        atomic_json(state_directory()/(service+"-supervisor.json"),dict(status="starting",at=now(),restart_count=failures))
        child=subprocess.Popen([sys.executable,"-u",str(Path(__file__).resolve()),service,"--child"],cwd=ROOT)
        try:code=child.wait()
        except KeyboardInterrupt:
            child.terminate();child.wait(timeout=15);return
        uptime=time.monotonic()-started
        delay=restart_delay(uptime,failures)
        failures=0 if uptime>=120 else failures+1
        atomic_json(state_directory()/(service+"-supervisor.json"),dict(status="restarting",at=now(),exit_code=code,
            delay_seconds=delay,restart_count=failures))
        print(f"SERVICE_RESTART {service} exit={code} delay={delay}s",flush=True)
        time.sleep(delay)


def run(service):
    configure(spark=service=="spark")
    from ingestion.streaming.heartbeat import install_okx_heartbeat
    install_okx_heartbeat()
    if service=="producer":
        from ingestion.streaming.okx_producer import run_producer
        run_producer()
    elif service=="web":
        from web.app import app,init_background_services
        init_background_services()
        app.run(host="127.0.0.1",port=int(os.getenv("PORT","5001")),debug=False,use_reloader=False,threaded=True)
    elif service=="spark":
        from storage.s3 import get_storage_mode
        from processing.core.spark_session import create_spark_session
        from processing.streaming.bronze_writer import start_bronze_writer
        import processing.streaming.trade_processor as processor
        checkpoint=stream_checkpoints();checkpoint.mkdir(parents=True,exist_ok=True)
        for setting,name in [("CHECKPOINT_PROCESSED","speed_candles_1m"),("CHECKPOINT_WHALE","whale_alerts"),
                             ("CHECKPOINT_DW_WHALES","dw_whales"),("CHECKPOINT_DW_CANDLES","dw_candles")]:
            setattr(processor,setting,(checkpoint/name).as_posix())
        spark=create_spark_session(app_name="CryptoLocalDemo",master="local[2]",include_kafka=True,
                                  include_s3=get_storage_mode()=="s3",include_postgres=False)
        try:
            start_bronze_writer(spark,checkpoint_location=(checkpoint/"bronze").as_posix(),
                                starting_offsets=os.getenv('BRONZE_STARTING_OFFSETS','latest'))
            processor.process_crypto_streams(spark)
        finally:spark.stop()


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("service",choices=["web","producer","spark"])
    p.add_argument("--child",action="store_true",help=argparse.SUPPRESS)
    args=p.parse_args()
    if args.child:run(args.service)
    else:supervise(args.service)
