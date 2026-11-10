"""Load exactly the activated, checksum-verified canonical dataset, never legacy first."""
import hashlib
import io
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from storage.canonical_snapshot import ROOT, load_descriptor


def load_locked_candles(descriptor_path=None, allow_remote=False):
    descriptor=load_descriptor(descriptor_path)
    if descriptor is None:
        raise ValueError("No activated canonical descriptor")
    root=(ROOT/descriptor["local_gold_path"]).resolve()
    if not root.is_relative_to(ROOT/"data/canonical_repairs"):
        raise ValueError("Canonical local path escapes the owned repair directory")
    file=root/"symbol=BTC-USDT/canonical.parquet"
    if file.exists():
        content=file.read_bytes()
    elif allow_remote:
        import boto3
        from botocore.config import Config
        uri=urlparse(descriptor["gold_uri"])
        client=boto3.client("s3",config=Config(connect_timeout=5,read_timeout=30,retries={"max_attempts":1}))
        response=client.get_object(Bucket=uri.netloc,Key=uri.path.lstrip("/")+"/symbol=BTC-USDT/canonical.parquet")
        try:content=response["Body"].read()
        finally:response["Body"].close()
    else:
        raise FileNotFoundError("Locked canonical file absent; no mutable DB/backfill fallback is allowed")
    if hashlib.sha256(content).hexdigest()!=descriptor["gold_sha256"]:
        raise ValueError("Canonical dataset checksum mismatch")
    frame=pq.ParquetFile(io.BytesIO(content)).read().to_pandas()
    if "symbol" not in frame:frame.insert(0,"symbol",descriptor["symbol"])
    stamps=pd.to_datetime(frame.window_start,utc=True)
    expected=pd.date_range(descriptor["requested_start"],descriptor["requested_end"],freq="min")
    if (len(frame)!=descriptor["rows"] or not pd.DatetimeIndex(stamps).astype("datetime64[ns, UTC]").equals(expected)
        or frame.symbol.ne("BTC-USDT").any() or frame.source.ne("historical_rest").any()
        or not frame.is_closed.map(lambda x:isinstance(x,(bool,np.bool_)) and bool(x)).all()):
        raise ValueError("Locked dataset violates identity/continuity/closure rules")
    prices=frame[["open","high","low","close","volume"]].to_numpy(dtype=float)
    if (not np.isfinite(prices).all() or (prices[:,:4]<=0).any() or (prices[:,4]<0).any()
        or (frame.low>frame[["open","close"]].min(axis=1)).any() or (frame.high<frame[["open","close"]].max(axis=1)).any()):
        raise ValueError("Invalid locked OHLCV")
    frame["window_start"]=stamps
    frame["window_end"]=pd.to_datetime(frame.window_end,utc=True)
    if not (frame.window_end-stamps).eq(pd.Timedelta(minutes=1)).all():
        raise ValueError("Locked candle duration differs from one minute")
    return frame,descriptor
