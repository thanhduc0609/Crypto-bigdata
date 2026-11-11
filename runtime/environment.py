"""Portable local settings; credentials stay in the ignored environment file."""
import os
from pathlib import Path
import shutil
import sys
from processing.core.env import load_env_vars

ROOT=Path(__file__).resolve().parents[1]


def run_prefix():
    from storage.project_layout import project_prefix
    return project_prefix()


def state_directory():
    return ROOT/"data/runtime"


def configure(spark=False):
    load_env_vars()
    os.environ.setdefault("TZ","UTC")
    os.environ.setdefault("PYTHONUTF8","1")
    os.environ.setdefault("SPARK_LOCAL_IP","127.0.0.1")
    state=state_directory();state.mkdir(parents=True,exist_ok=True)
    if spark:
        if not Path(os.getenv("JAVA_HOME","__missing__")).joinpath("bin/java.exe" if os.name=="nt" else "bin/java").exists():
            candidates=[]
            java=shutil.which("java")
            if java:candidates.append(Path(java).resolve().parent.parent)
            for directory in [Path("C:/Program Files/Java"),Path("C:/Program Files/Eclipse Adoptium")]:
                if directory.exists():candidates+=sorted(directory.iterdir(),reverse=True)
            home=next((p for p in candidates if (p/("bin/java.exe" if os.name=="nt" else "bin/java")).exists()),None)
            if home is None:raise RuntimeError("Java 8/11/17 required; configure JAVA_HOME")
            os.environ["JAVA_HOME"]=str(home)
        os.environ["PYSPARK_PYTHON"]=sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"]=sys.executable
        legacy=ROOT.parent/"runtimes/ivy"
        ivy=Path(os.getenv("SPARK_IVY_DIR",str(legacy if legacy.exists() else ROOT/"data/ivy")))
        ivy.mkdir(parents=True,exist_ok=True)
        os.environ["PYSPARK_SUBMIT_ARGS"]='--conf spark.jars.ivy="'+ivy.as_posix()+'" pyspark-shell'
    # Keep the configured run scoped consistently across producer, stream and batch.
    from storage.s3 import get_storage_mode,storage_cfg
    if get_storage_mode()=="s3":
        from storage.project_layout import writer_id,starting_offsets
        prefix=run_prefix()
        writer=writer_id()
        storage_cfg.setdefault("s3",{}).update({
            "bronze_path":prefix+"/bronze/trades/streams/"+writer,"silver_path":prefix+"/silver/trades",
            "gold_candles_path":prefix+"/gold/candles_1m","speed_candles_path":prefix+"/streaming/candles_1m/streams/"+writer})
        os.environ.setdefault('BRONZE_STARTING_OFFSETS',starting_offsets('bronze'))
        os.environ.setdefault('SPEED_STARTING_OFFSETS',starting_offsets('speed'))
    return state


def stream_checkpoints():
    """New destination gets new checkpoints; the migration saves source offsets."""
    from storage.s3 import get_storage_mode
    from storage.project_layout import writer_id
    return state_directory()/"checkpoints"/(writer_id() if get_storage_mode()=="s3" else "local")
