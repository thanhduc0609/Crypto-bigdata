"""Portable local settings; credentials stay in the ignored environment file."""
import os
from pathlib import Path
import re
import shutil
import sys
from processing.core.env import load_env_vars

ROOT=Path(__file__).resolve().parents[1]


def run_prefix():
    value=os.getenv("S3_RUN_PREFIX","").strip("/")
    if not re.fullmatch(r"crypto/local-runs/[A-Za-z0-9_-]+",value):
        raise ValueError("Set S3_RUN_PREFIX to crypto/local-runs/<project-run>; shared prefixes are not writable")
    return value


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
        prefix=run_prefix()
        storage_cfg.setdefault("s3",{}).update({
            "bronze_path":prefix+"/bronze/trades","silver_path":prefix+"/silver/trades",
            "gold_candles_path":prefix+"/gold/candles_1m","speed_candles_path":prefix+"/speed/candles_1m"})
    return state


def stream_checkpoints():
    """Reuse an existing compatible checkpoint during migration; never erase it."""
    from storage.s3 import get_storage_mode
    prefix=run_prefix().replace("/","_") if get_storage_mode()=="s3" else "local"
    legacy=ROOT.parent/"local-run/state"/prefix
    return legacy if legacy.exists() else state_directory()/"checkpoints"/prefix
