"""Capture committed S3 Bronze objects, cache by ETag + SHA, compact locally.

Raw records are preserved; validation/cleaning still executes in Spark. Never
include orphan Parquet that lacks a Spark file-sink commit.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse
import pyarrow as pa
import pyarrow.parquet as pq
from processing.quality.inventory import committed_files
from runtime.medallion import atomic_json


def capture(client,bucket,bronze_uri,directory,cache):
    prefix=urlparse(bronze_uri).path.strip("/")+"/"
    directory=Path(directory);cache=Path(cache)
    directory.mkdir(parents=True,exist_ok=True);cache.mkdir(parents=True,exist_ok=True)
    listed=[]
    for index,page in enumerate(client.get_paginator("list_objects_v2").paginate(Bucket=bucket,Prefix=prefix)):
        if index>=50:raise ValueError("Bronze scan exceeds bounded 50k-object demo budget")
        listed += page.get("Contents",[])
    meta={}
    for obj in listed:
        match=re.fullmatch(re.escape(prefix)+r"_spark_metadata/(\d+)(\.compact)?",obj["Key"])
        if match:meta[(int(match[1]),bool(match[2]))]=obj
    if not meta:raise ValueError("No Spark commit log: cannot claim a committed Bronze snapshot")
    compact=max((i for i,is_compact in meta if is_compact),default=0)
    end=max(i for i,_ in meta)
    objects=[]
    def download(obj):
        key=obj["Key"];token=hashlib.sha256((key+obj["ETag"]).encode()).hexdigest()
        local=cache/(token+(".parquet" if key.endswith(".parquet") else ".jsonl"))
        receipt=local.with_suffix(local.suffix+".sha256")
        if not local.exists() or not receipt.exists() or hashlib.sha256(local.read_bytes()).hexdigest()!=receipt.read_text():
            response=client.get_object(Bucket=bucket,Key=key,IfMatch=obj["ETag"])
            try:body=response["Body"].read()
            finally:response["Body"].close()
            if len(body)!=obj["Size"]:raise ValueError("Bronze object changed during capture")
            local.write_bytes(body);receipt.write_text(hashlib.sha256(body).hexdigest())
        return dict(key=key,etag=obj["ETag"],bytes=obj["Size"],local=str(local),sha256=receipt.read_text())
    for batch in range(compact,end+1):
        obj=meta.get((batch,batch==compact)) or meta.get((batch,False))
        if obj is None:raise ValueError("Spark commit log has a gap")
        objects.append(download(obj))
    parquet=[o for o in listed if o["Key"].endswith(".parquet")]
    keys,commit=committed_files(objects+[dict(key=o["Key"]) for o in parquet])
    wanted=[o for o in parquet if o["Key"] in keys]
    if sum(o["Size"] for o in wanted)>256*1024**2:raise ValueError("Split Bronze capture larger than 256 MiB explicitly")
    with ThreadPoolExecutor(max_workers=8) as pool:files=list(pool.map(download,wanted))
    tables=[]
    for item in sorted(files,key=lambda o:o["key"]):
        table=pq.ParquetFile(item["local"]).read()
        table=table.append_column("_source_object",pa.array([item["key"]]*len(table),type=pa.string()))
        table=table.append_column("_source_sha256",pa.array([item["sha256"]]*len(table),type=pa.string()))
        tables.append(table)
    if not tables:raise ValueError("Committed Bronze contains no records")
    target=directory/"bronze-input.parquet"
    # Original Spark footer embeds its old schema. After adding lineage fields,
    # retaining that footer makes Spark ignore the newly appended columns.
    combined=pa.concat_tables(tables,promote_options="default").replace_schema_metadata(None)
    pq.write_table(combined,target,coerce_timestamps="us",version="1.0")
    manifest=dict(captured_at=datetime.now(timezone.utc).isoformat(),commit=commit,files=files,
        rows=len(combined),packed_sha256=hashlib.sha256(target.read_bytes()).hexdigest(),path=str(target))
    atomic_json(directory/"source.json",manifest)
    return manifest


def capture_project(client,bucket,directory,cache):
    """Read committed logs separately for every writer/history, then combine raw rows."""
    from storage.project_layout import project_prefix
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    datasets=[]
    for group in ('history','streams'):
        prefix=project_prefix()+'/bronze/trades/'+group+'/'
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=prefix,Delimiter='/'):
            datasets.extend(item['Prefix'] for item in page.get('CommonPrefixes',[]))
    if len(datasets)>64:raise ValueError('More than 64 Bronze datasets; compact history explicitly')
    captures=[];skipped=[]
    for index,prefix in enumerate(sorted(set(datasets))):
        if '/history/' in prefix:
            try:client.head_object(Bucket=bucket,Key=prefix+'_READY.json')
            except Exception as exc:
                if getattr(exc,'response',{}).get('Error',{}).get('Code') in ('NoSuchKey','404','NotFound'):
                    skipped.append(dict(prefix=prefix,reason='Migration not published yet'));continue
                raise
        try:
            result=capture(client,bucket,'s3a://'+bucket+'/'+prefix,directory/f'source-{index}',cache)
            captures.append(result)
        except ValueError as exc:
            if '/streams/' in prefix and str(exc) in ('No Spark commit log: cannot claim a committed Bronze snapshot','Committed Bronze contains no records'):
                skipped.append(dict(prefix=prefix,reason='No committed data yet'))
            else:raise
    if not captures:raise ValueError('No committed project Bronze datasets')
    tables=[pq.ParquetFile(item['path']).read() for item in captures]
    target=directory/'bronze-input.parquet'
    combined=pa.concat_tables(tables,promote_options='default').replace_schema_metadata(None)
    pq.write_table(combined,target,coerce_timestamps='us',version='1.0')
    manifest=dict(captured_at=datetime.now(timezone.utc).isoformat(),
        commit=dict(mode='spark_file_sink_logs',datasets=len(captures),
                    sources=[item['commit'] for item in captures],skipped_uncommitted=skipped),
        files=[file for item in captures for file in item['files']],rows=len(combined),
        packed_sha256=hashlib.sha256(target.read_bytes()).hexdigest(),path=str(target))
    atomic_json(directory/'source.json',manifest)
    return manifest
