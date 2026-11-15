"""Move the old run-scoped S3 dataset into the project's layered layout.

Run .\scripts\setup_env.ps1 -StopDemo first. Copy is create-only and SHA-256 verified. Finish
removes only the inventoried old objects, after an independent runtime audit.
Original bytes are backed up locally outside Git; frozen research bytes stay intact.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlparse,unquote
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT))
from processing.core.env import load_env_vars
from storage.project_layout import migrated_key,writer_id
from runtime.medallion import atomic_json
from runtime.telemetry import checkpoint_offsets

STATE=ROOT/'data/runtime/s3-layout'
PLAN=STATE/'plan.json'


def client():
    import boto3
    from botocore.config import Config
    return boto3.client('s3',region_name=os.getenv('AWS_REGION'),
        config=Config(connect_timeout=5,read_timeout=30,retries={'max_attempts':2},max_pool_connections=16))


def inventory(c,bucket,prefix):
    result=[]
    for index,page in enumerate(c.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=prefix)):
        if index>=100:raise ValueError('Split inventory larger than 100k objects explicitly')
        result.extend(dict(key=o['Key'],size=o['Size'],etag=o['ETag']) for o in page.get('Contents',[]))
    return sorted(result,key=lambda o:o['key'])


def digest(body):return hashlib.sha256(body).hexdigest()


def refresh(c,plan):
    """Add reviewed append-only objects after the other old writers have stopped."""
    if plan['status'] not in ('prepared','copied_and_verified','finishing'):
        raise ValueError('Cannot refresh a completed or retiring migration')
    current=inventory(c,plan['bucket'],plan['source_prefix']+'/')
    actual={o['key']:o for o in current}
    if any(actual.get(o['key'])!=o for o in plan['objects'] if plan['status']!='finishing' or o['key'] in actual):
        raise ValueError('An existing source object changed; retain backup and review it manually')
    if plan['status']=='finishing':
        # A final in-flight batch may append after a retirement pass. Keep its
        # new data and every already-retired receipt, then migrate the delta.
        retired=[row for row in plan['copied'] if row['source'] not in actual]
        plan.setdefault('retired_receipts',[]).extend(retired)
        plan['copied']=[row for row in plan['copied'] if row['source'] in actual]
    previous_keys={old['key'] for old in plan['objects']}
    added=sum(o['key'] not in previous_keys for o in current)
    plan.update(objects=current,status='prepared',pending_source_objects=[])
    atomic_json(PLAN,plan)
    print(f'Refreshed source inventory: {len(current)} objects ({added} appended). No source objects deleted.',flush=True)


def metadata_body(body,plan):
    lines=body.decode('utf-8').splitlines()
    if not lines or lines[0]!='v1':raise ValueError('Unknown Spark metadata version')
    result=['v1']
    for line in lines[1:]:
        record=json.loads(line);uri=urlparse(record['path'])
        if uri.netloc!=plan['bucket']:raise ValueError('Spark metadata references a different bucket')
        key=migrated_key(plan['source_prefix'],unquote(uri.path.lstrip('/')),plan['migration_id'])
        record['path']=uri.scheme+'://'+uri.netloc+'/'+key
        result.append(json.dumps(record,separators=(',',':')))
    return ('\n'.join(result)+'\n').encode()


def prepare(c,source):
    source=source.rstrip('/')
    if not re.fullmatch(r'crypto/local-runs/[A-Za-z0-9_-]+',source):raise ValueError('Select exactly one old run prefix')
    if PLAN.exists():raise ValueError('A storage plan already exists; resume it instead of replacing it')
    bucket=os.environ['S3_BUCKET_NAME'];objects=inventory(c,bucket,source+'/')
    if not objects:raise ValueError('Old prefix is empty')
    migration=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    STATE.mkdir(parents=True,exist_ok=True)
    old=ROOT.parent/'local-run/state'/source.replace('/','_')
    if not old.exists():old=ROOT/'data/runtime/checkpoints'/source.replace('/','_')
    bronze=checkpoint_offsets(old/'bronze')
    if bronze.get('status')!='verified':raise ValueError('No committed old Bronze checkpoint; review Kafka continuity manually')
    # Each file sink needs a new checkpoint at its new destination. Begin from
    # committed source offsets, not latest (which would silently skip downtime).
    offsets={'bronze':bronze['offsets'],'speed':bronze['offsets']}
    plan=dict(schema_version=1,bucket=bucket,source_prefix=source,migration_id=migration,
              status='prepared',objects=objects,bootstrap=offsets,writer_id=writer_id(),
              original_checkpoint=str(old),copied=[])
    mapped=[migrated_key(source,o['key'],migration) for o in objects]
    if len(set(mapped))!=len(mapped):raise ValueError('Destination mapping is not unique')
    (STATE/'environment.before').write_bytes((ROOT/'.env').read_bytes())
    atomic_json(PLAN,plan)
    print(f'Prepared {len(objects)} objects, {sum(o["size"] for o in objects)} bytes. Existing root datasets remain untouched.',flush=True)


def copy_all(c,plan):
    bucket=plan['bucket'];backup=STATE/'original-objects';backup.mkdir(exist_ok=True)
    receipts={row['source']:row for row in plan.get('copied',[])}
    def transfer(item):
        source=item['key'];target=migrated_key(plan['source_prefix'],source,plan['migration_id'])
        local=backup/(hashlib.sha256(source.encode()).hexdigest()+'.bin')
        cached=local.read_bytes() if local.exists() else None
        previous=receipts.get(source)
        if previous and previous['source_etag']==item['etag'] and previous['target']==target:
            if cached is None or digest(cached)!=previous['original_sha256']:
                raise ValueError('Verified backup changed; refusing incremental copy')
            return previous  # finish independently re-reads every destination.
        # Resume our already downloaded single-part objects only when the cache
        # matches the inventoried S3 MD5 ETag. Otherwise re-read with IfMatch.
        if cached is not None and len(cached)==item['size'] and hashlib.md5(cached).hexdigest()==item['etag'].strip('"'):
            original=cached;response={}
        else:
            response=c.get_object(Bucket=bucket,Key=source,IfMatch=item['etag'])
            try:original=response['Body'].read()
            finally:response['Body'].close()
        if len(original)!=item['size']:raise ValueError('Source size changed')
        if local.exists() and digest(local.read_bytes())!=digest(original):raise ValueError('Backup/source differs')
        local.write_bytes(original)
        transformed=bool(re.search(r'/_spark_metadata/\d+(?:\.compact)?$',source))
        body=metadata_body(original,plan) if transformed else original
        try:
            check=c.get_object(Bucket=bucket,Key=target)
        except Exception as exc:
            if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('NoSuchKey','404','NotFound'):raise
            check=None
        if check is None:
            try:
                c.put_object(Bucket=bucket,Key=target,Body=body,IfNoneMatch='*',
                    ContentType=response.get('ContentType','application/octet-stream'),
                    Metadata=response.get('Metadata',{}),ChecksumSHA256=base64.b64encode(hashlib.sha256(body).digest()).decode())
            except Exception as exc:
                if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('PreconditionFailed','412'):raise
            check=c.get_object(Bucket=bucket,Key=target)
        try:actual=check['Body'].read()
        finally:check['Body'].close()
        if digest(actual)!=digest(body):raise ValueError('Destination collision or failed checksum: '+target)
        return dict(source=source,target=target,source_etag=item['etag'],original_sha256=digest(original),
                    sha256=digest(body),size=len(body),metadata_rewritten=transformed,backup=local.name)
    results=[]
    with ThreadPoolExecutor(max_workers=10) as pool:
        for row in pool.map(transfer,plan['objects']):
            results.append(row)
            if len(results)%100==0:print(f'Copied and verified {len(results)}/{len(plan["objects"])}',flush=True)
    current=inventory(c,bucket,plan['source_prefix']+'/')
    expected={o['key']:o for o in plan['objects']};actual={o['key']:o for o in current}
    if any(actual.get(key)!=item for key,item in expected.items()):
        raise ValueError('Existing source objects changed; no source deletion or activation is allowed')
    additions=[o for o in current if o['key'] not in expected]
    if any(not o['key'].startswith(plan['source_prefix']+'/medallion-runs/') for o in additions):
        raise ValueError('Source stream/snapshot changed; stop writers before activating the migration')
    plan['pending_source_objects']=additions
    plan.update(copied=results,status='copied_and_verified');atomic_json(PLAN,plan)
    history=f'crypto/bronze/trades/history/{plan["migration_id"]}/'
    ready=json.dumps(dict(objects=len(results),migration_id=plan['migration_id'])).encode()
    try:c.put_object(Bucket=bucket,Key=history+'_READY.json',Body=ready,IfNoneMatch='*')
    except Exception as exc:
        if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('PreconditionFailed','412'):raise
        response=c.get_object(Bucket=bucket,Key=history+'_READY.json')
        try:existing=response['Body'].read()
        finally:response['Body'].close()
        if json.loads(existing).get('migration_id')!=plan['migration_id']:
            raise ValueError('History publication marker belongs to another migration')
    locations={k:plan[k] for k in ('schema_version','bucket','source_prefix','migration_id')}
    atomic_json(ROOT/'configs/s3_locations.json',locations)
    atomic_json(ROOT/'data/runtime/stream-bootstrap.json',plan['bootstrap'])
    # Reuse the Docker helper's atomic env edit. Remove the deprecated run key;
    # keep all actual credentials and migrated Docker volume/image selections.
    from scripts.demo.docker_environment import update_env
    update_env(ROOT/'.env',{'S3_PROJECT_PREFIX':'crypto'})
    path=ROOT/'.env'
    lines=[line for line in path.read_text(encoding='utf-8').splitlines() if line.split('=',1)[0].strip()!='S3_RUN_PREFIX']
    temporary=path.with_name('.env.storage-migration.tmp')
    temporary.write_text('\n'.join(lines)+'\n',encoding='utf-8');os.replace(temporary,path)
    print(f'Copy verified. Project config activated. Pending newly appended old batch objects: {len(additions)}. Stop old writers before finish.',flush=True)


def finish(c,plan,audit_path):
    if plan['status'] not in ('copied_and_verified','finishing'):raise ValueError('Copy is not verified')
    audit_path=Path(audit_path).resolve();audit_path.relative_to((ROOT/'data/demo_audits').resolve())
    audit=json.loads(audit_path.read_text())
    if not audit.get('passed') or not audit.get('trace',{}).get('raw_source_object','').startswith('crypto/bronze/trades/'):
        raise ValueError('A successful audit of the new project layout is required')
    remaining=inventory(c,plan['bucket'],plan['source_prefix']+'/')
    expected={item['key']:item for item in plan['objects']}
    if any(expected.get(item['key'])!=item for item in remaining) or (plan['status']=='copied_and_verified' and remaining!=plan['objects']):
        raise ValueError('Old source changed after copy; refusing deletion')
    plan['status']='finishing';atomic_json(PLAN,plan)
    def remove(item):
        # Every deletion is bounded to the selected inventoried old source and
        # requires a verified local backup + destination hash + unchanged ETag.
        source=item['source']
        if not source.startswith(plan['source_prefix']+'/'):raise ValueError('Deletion outside source')
        local=STATE/'original-objects'/item['backup']
        if digest(local.read_bytes())!=item['original_sha256']:raise ValueError('Backup checksum changed')
        response=c.get_object(Bucket=plan['bucket'],Key=item['target'])
        try:body=response['Body'].read()
        finally:response['Body'].close()
        if digest(body)!=item['sha256']:raise ValueError('Destination changed; source retained')
        try:c.head_object(Bucket=plan['bucket'],Key=source)
        except Exception as exc:
            if getattr(exc,'response',{}).get('Error',{}).get('Code') in ('NoSuchKey','404','NotFound'):return
            raise
        c.delete_object(Bucket=plan['bucket'],Key=source,IfMatch=item['source_etag'])
    with ThreadPoolExecutor(max_workers=10) as pool:
        for count,_ in enumerate(pool.map(remove,plan['copied']),1):
            if count%100==0:print(f'Retired verified source objects {count}/{len(plan["copied"])}',flush=True)
    if inventory(c,plan['bucket'],plan['source_prefix']+'/'):raise ValueError('Old prefix is not empty')
    plan.update(status='completed',runtime_audit=str(audit_path));atomic_json(PLAN,plan)
    print('Storage relocation complete. Old run folder removed; original bytes remain in the private local backup.',flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['prepare','refresh','copy','finish'])
    p.add_argument('--source-prefix');p.add_argument('--audit',type=Path);args=p.parse_args()
    load_env_vars();c=client()
    if args.action=='prepare':prepare(c,args.source_prefix or os.getenv('S3_RUN_PREFIX',''))
    elif args.action=='refresh':refresh(c,json.loads(PLAN.read_text()))
    elif args.action=='copy':copy_all(c,json.loads(PLAN.read_text()))
    else:
        if args.audit is None:p.error('finish requires --audit of the new layout')
        finish(c,json.loads(PLAN.read_text()),args.audit)


if __name__=='__main__':main()
