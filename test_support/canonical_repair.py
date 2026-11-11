"""Labelled synthetic capture; never actual market data or live S3."""
import io
import json
from pathlib import Path
from uuid import uuid4
import pandas as pd
from botocore.exceptions import ClientError

from processing.quality.canonical_repair import validated_row
from processing.quality.inventory import write_json,sha256,continuity


def fixture(directory,n=3):
    directory=Path(directory)
    run_id="synthetic-"+uuid4().hex[:10]
    start=pd.Timestamp("2020-01-01T00:00:00Z")
    end=start+pd.Timedelta(minutes=n-1)
    as_of=end+pd.Timedelta(minutes=2)
    plan=dict(run_id=run_id,symbol="BTC-USDT",bar="1m",requested_start=start.isoformat(),requested_end=end.isoformat(),
              source_as_of=as_of.isoformat(),expected_rows=n,endpoint="https://example.test/history",docs="Synthetic fixture only",
              bucket="unit-test-bucket",s3_prefix="crypto/canonical-repairs/"+run_id,ownership_token="fixture-owner",
              legacy_unknown_closed=n,legacy_continuity={"missing":0})
    rows=[[str(int((start+pd.Timedelta(minutes=i)).timestamp()*1000)),"100","110","90","105","2","2","210","1"] for i in range(n)]
    low=int(start.timestamp()*1000);high=int(end.timestamp()*1000)
    path=directory/"raw"/str(low)
    path.mkdir(parents=True,exist_ok=True)
    page=path/"page.json"
    write_json(page,dict(request={"synthetic":True},fetched_at=as_of.isoformat(),response={"code":"0","data":rows}))
    digest=sha256(page)
    records=[]
    for row in rows:
        item=validated_row(row,low,high,as_of)
        item.update(provenance_run_id=run_id,source_page=page.relative_to(directory).as_posix(),source_page_sha256=digest,source_fetched_at=as_of.isoformat())
        records.append(item)
    frame=pd.DataFrame(records)
    frame.to_parquet(path/"confirmed.parquet",index=False)
    legacy=frame.copy();legacy["is_closed"]=None
    legacy.to_parquet(directory/"legacy_gold_before.parquet",index=False)
    write_json(directory/"plan.json",plan)
    write_json(directory/"download.json",dict(chunks=[dict(first_ms=low,last_ms=high,rows=n,
        pages=[dict(path=page.relative_to(directory).as_posix(),sha256=digest,rows=n)])]))
    return plan,frame,rows


class MemoryS3:
    """Only the create/read interface used by immutable repair publication."""
    def __init__(self):
        self.objects={}
        self.calls=[]

    def put_object(self,**kwargs):
        self.calls.append(("put",kwargs["Bucket"],kwargs["Key"]))
        assert kwargs["IfNoneMatch"]=="*"
        key=(kwargs["Bucket"],kwargs["Key"])
        if key in self.objects:
            raise ClientError({"Error":{"Code":"PreconditionFailed"}},"PutObject")
        self.objects[key]=bytes(kwargs["Body"])
        return {"ChecksumSHA256":kwargs["ChecksumSHA256"]}

    def get_object(self,**kwargs):
        self.calls.append(("get",kwargs["Bucket"],kwargs["Key"]))
        return {"Body":io.BytesIO(self.objects[(kwargs["Bucket"],kwargs["Key"])])}
