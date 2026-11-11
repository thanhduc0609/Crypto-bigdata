"""Create-only publication of an owned repair; active pointer changes last."""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path
import time

import pyarrow.parquet as pq

from processing.quality.canonical_repair import ROOT, load_candidate
from processing.quality.inventory import sha256, write_json, now
from storage.canonical_snapshot import DEFAULT_POINTER, load_descriptor
from storage.gold_lock import GoldLock


class ImmutableRepairStore:
    def __init__(self,client,plan):
        self.client,self.bucket,self.prefix,self.run_id=client,plan["bucket"],plan["s3_prefix"].rstrip("/")+"/",plan["run_id"]
        import re
        if not re.fullmatch(r'crypto/canonical-repairs/'+re.escape(self.run_id)+'/',self.prefix):
            raise ValueError("Refusing publication outside an owned repair prefix")
        self.owner=json.dumps(dict(run_id=self.run_id,token=plan["ownership_token"]),sort_keys=True).encode()
        self.put_bytes("_OWNER.json",self.owner)

    def key(self,relative):
        if relative.startswith(("/","\\")) or "\\" in relative or any(x in (".","..","") for x in relative.split("/")):
            raise ValueError("Unsafe repair object key")
        return self.prefix+relative

    def read(self,relative):
        response=self.client.get_object(Bucket=self.bucket,Key=self.key(relative))
        try:return response["Body"].read()
        finally:response["Body"].close()

    def put_bytes(self,relative,body):
        digest=hashlib.sha256(body).hexdigest()
        try:
            response=self.client.put_object(Bucket=self.bucket,Key=self.key(relative),Body=body,
                IfNoneMatch="*",ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode(),
                Metadata={"sha256":digest,"repair-run":self.run_id})
            returned=response.get("ChecksumSHA256")
            if returned and returned!=base64.b64encode(bytes.fromhex(digest)).decode():
                raise ValueError("S3 upload checksum differs")
        except Exception as exc:
            error=getattr(exc,"response",{}).get("Error",{}).get("Code")
            if error not in ("PreconditionFailed","412"):
                raise
            if hashlib.sha256(self.read(relative)).hexdigest()!=digest:
                raise ValueError("Immutable repair key already contains different bytes") from None
        return dict(key=self.key(relative),sha256=digest,bytes=len(body))


def s3_client():
    import boto3
    from botocore.config import Config
    return boto3.client("s3",region_name=os.getenv("AWS_REGION"),config=Config(connect_timeout=5,read_timeout=30,retries={"max_attempts":2}))


def publish(directory,client=None):
    directory=Path(directory)
    plan,candidate,frame=load_candidate(directory)
    client=client or s3_client()
    store=ImmutableRepairStore(client,plan)
    pages=candidate["raw_pages"]
    def upload(item):
        file=directory/item["path"]
        if sha256(file)!=item["sha256"]:
            raise ValueError("REST raw evidence changed before publication")
        return store.put_bytes(item["path"],file.read_bytes())
    with ThreadPoolExecutor(max_workers=6) as pool:
        raw=list(pool.map(upload,pages))
    gold=store.put_bytes(candidate["gold_file"],(directory/candidate["gold_file"]).read_bytes())
    actual=store.read(candidate["gold_file"])
    if hashlib.sha256(actual).hexdigest()!=candidate["gold_sha256"] or pq.ParquetFile(io.BytesIO(actual)).metadata.num_rows!=len(frame):
        raise ValueError("Published Gold read-back verification failed")
    manifest=dict(schema_version=1,state="verified_snapshot",run_id=plan["run_id"],symbol="BTC-USDT",bar="1m",
                  requested_start=plan["requested_start"],requested_end=plan["requested_end"],source_as_of=plan["source_as_of"],
                  rows=len(frame),gold=gold,raw_pages=raw,checks=candidate["checks"],endpoint=plan["endpoint"],docs=plan["docs"],
                  candidate_sha256=sha256(directory/"candidate.json"),policy="Only explicit REST confirm=1; no synthetic/interpolated candles")
    manifest["publication_code_sha256"]={name:sha256(ROOT/name) for name in (
        "processing/quality/canonical_repair.py","warehouse/etl/canonical_repair.py","storage/canonical_publish.py",
        "storage/canonical_snapshot.py","ingestion/historical/candle_parser.py")}
    existing=directory/"published_manifest.json"
    if existing.exists():
        previous=json.loads(existing.read_text(encoding="utf-8"))
        if (previous["run_id"]!=plan["run_id"] or previous["gold"]["sha256"]!=gold["sha256"] or previous["rows"]!=len(frame)):
            raise ValueError("Published run identity differs from current candidate")
        manifest=previous
    content=json.dumps(manifest,ensure_ascii=False,sort_keys=True,indent=2).encode("utf-8")
    store.put_bytes("manifest.json",content)
    if store.read("manifest.json")!=content:
        raise ValueError("Published manifest read-back differs")
    result=dict(run_id=plan["run_id"],rows=len(frame),manifest_uri="s3://"+plan["bucket"]+"/"+store.key("manifest.json"),
                manifest_sha256=hashlib.sha256(content).hexdigest(),gold_uri="s3://"+plan["bucket"]+"/"+plan["s3_prefix"]+"/gold/candles_1m",
                gold_sha256=candidate["gold_sha256"],s3_readback_verified=True,raw_pages=len(raw))
    (directory/"published_manifest.json").write_bytes(content)
    write_json(directory/"publication.json",result)
    return result


def activate(directory,publication,pointer=DEFAULT_POINTER,connect=None):
    """Reverify the committed DB version, then atomically switch the local pointer."""
    from warehouse.etl.canonical_repair import apply_confirmed_snapshot
    directory=Path(directory)
    pointer=Path(pointer)
    plan,candidate,frame=load_candidate(directory)
    expected_manifest="s3://"+plan["bucket"]+"/"+plan["s3_prefix"]+"/manifest.json"
    expected_gold="s3://"+plan["bucket"]+"/"+plan["s3_prefix"]+"/gold/candles_1m"
    if (publication.get("run_id")!=plan["run_id"] or publication.get("rows")!=len(frame)
        or publication.get("gold_sha256")!=candidate["gold_sha256"] or publication.get("manifest_uri")!=expected_manifest
        or publication.get("gold_uri")!=expected_gold or sha256(directory/"published_manifest.json")!=publication.get("manifest_sha256")):
        raise ValueError("Activation descriptor differs from the verified publication")
    with GoldLock(str(pointer),timeout=30):
        kwargs={"connect":connect} if connect is not None else {}
        applied=apply_confirmed_snapshot(frame,plan,publication,**kwargs)
        write_json(directory/"database_applied.json",applied)
        previous=pointer.read_bytes() if pointer.exists() else None
        backup=directory/"previous_pointer_state.json"
        if not backup.exists():
            write_json(backup,dict(existed=previous is not None,descriptor=json.loads(previous) if previous is not None else None))
        descriptor=dict(schema_version=1,state="active",run_id=plan["run_id"],symbol="BTC-USDT",bar="1m",rows=len(frame),
            requested_start=plan["requested_start"],requested_end=plan["requested_end"],source_as_of=plan["source_as_of"],
            manifest_uri=publication["manifest_uri"],manifest_sha256=publication["manifest_sha256"],gold_uri=publication["gold_uri"],
            gold_sha256=publication["gold_sha256"],local_gold_path=(directory/"gold/candles_1m").relative_to(ROOT).as_posix())
        pointer.parent.mkdir(parents=True,exist_ok=True)
        temporary=pointer.with_name(pointer.name+"."+plan["run_id"]+".tmp")
        write_json(temporary,descriptor)
        load_descriptor(temporary)
        for attempt in range(10):
            try:
                os.replace(temporary,pointer)
                break
            except PermissionError:
                if attempt==9:raise
                time.sleep(.1)
        write_json(directory/"activation.json",dict(active=True,activated_utc=now(),pointer=str(pointer),descriptor=descriptor,database=applied))
        return descriptor
