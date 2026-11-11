"""Project-level S3 layout and read-only relocation of historical source URIs."""
import json
import os
from pathlib import Path
import re
from urllib.parse import urlparse
from uuid import uuid4
from filelock import FileLock

ROOT = Path(__file__).resolve().parents[1]
LOCATIONS = ROOT / 'configs/s3_locations.json'


def project_prefix():
    value = os.getenv('S3_PROJECT_PREFIX', os.getenv('S3_RUN_PREFIX', 'crypto')).strip('/')
    if value != 'crypto':
        raise ValueError('Set S3_PROJECT_PREFIX=crypto; migrate the old storage layout before starting writers')
    return value


def writer_id():
    """Stable per checkout: two computers never share a Spark file-sink log."""
    value = os.getenv('S3_WRITER_ID')
    if value is None:
        path = ROOT / 'data/runtime/storage-writer.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(path) + '.lock', timeout=10):
            if path.exists():
                value = json.loads(path.read_text())['writer_id']
            else:
                value = 'writer-' + uuid4().hex[:12]
                path.write_text(json.dumps({'writer_id': value}), encoding='utf-8')
    if not re.fullmatch(r'[a-z0-9][a-z0-9_-]{2,47}', value) or 'codex' in value.lower():
        raise ValueError('Invalid project writer identifier')
    return value


def operational_paths(run_id, bucket):
    if not re.fullmatch(r'[0-9]{8}T[0-9]{12}Z-[a-f0-9]{8}', run_id):
        raise ValueError('Invalid batch run identifier')
    base = f's3a://{bucket}/{project_prefix()}'
    return dict(silver=f'{base}/silver/trades/{run_id}', gold=f'{base}/gold/candles_1m/{run_id}',
                manifest=f'{base}/manifests/batches/{run_id}/manifest.json')


def migrated_key(source_prefix, key, migration_id):
    """Map each old key to a meaningful layer; never flatten dataset partitions."""
    source = source_prefix.rstrip('/') + '/'
    if not re.fullmatch(r'crypto/local-runs/[A-Za-z0-9_-]+/', source) or not key.startswith(source):
        raise ValueError('Object is outside the explicitly selected old source')
    if not re.fullmatch(r'[0-9]{8}T[0-9]{12}Z', migration_id):
        raise ValueError('Invalid storage migration identifier')
    rel = key[len(source):]
    if any(part in ('.', '..') for part in rel.split('/')) or '\\' in rel:
        raise ValueError('Unsafe source object key')
    if rel == 'bronze/trades' or rel.startswith('bronze/trades/'):
        return f'crypto/bronze/trades/history/{migration_id}' + rel[len('bronze/trades'):]
    if rel == 'speed/candles_1m' or rel.startswith('speed/candles_1m/'):
        return f'crypto/streaming/candles_1m/history/{migration_id}' + rel[len('speed/candles_1m'):]
    if rel.startswith('medallion-runs/'):
        _, run, *parts = rel.split('/')
        tail = '/'.join(parts)
        if tail == 'silver/trades' or tail.startswith('silver/trades/'):
            return f'crypto/silver/trades/{run}' + tail[len('silver/trades'):]
        if tail == 'gold/candles_1m' or tail.startswith('gold/candles_1m/'):
            return f'crypto/gold/candles_1m/{run}' + tail[len('gold/candles_1m'):]
        return f'crypto/manifests/batches/{run}/' + tail
    return 'crypto/' + rel


def resolve_uri(uri, locations=None):
    """Keep frozen checksums/provenance intact while resolving the new address."""
    if locations is None:
        locations = json.loads(LOCATIONS.read_text()) if LOCATIONS.exists() else {}
    parsed = urlparse(uri)
    if parsed.scheme not in ('s3', 's3a') or parsed.netloc != locations.get('bucket'):
        return uri
    prefix = locations.get('source_prefix', '').rstrip('/')
    if not prefix or not parsed.path.lstrip('/').startswith(prefix + '/'):
        return uri
    target = migrated_key(prefix, parsed.path.lstrip('/'), locations['migration_id'])
    return f'{parsed.scheme}://{parsed.netloc}/{target}'


def starting_offsets(service):
    if service not in ('bronze','speed'):raise ValueError('Unknown streaming service')
    path = ROOT / 'data/runtime/stream-bootstrap.json'
    if path.exists():return json.dumps(json.loads(path.read_text())[service])
    # Other team computers have their own Kafka offsets. Never copy the
    # migration machine's bootstrap or skip their pending records with latest.
    selected=os.getenv('STREAM_RESUME_CHECKPOINT')
    if selected:
        roots=[(ROOT/selected).resolve()]
    else:
        current=writer_id()
        roots=[p for base in (ROOT/'data/runtime/checkpoints',ROOT.parent/'local-run/state') if base.exists()
               for p in base.iterdir() if p.is_dir() and p.name!=current and (p/'bronze/commits').is_dir()]
    if len(roots)>1:raise ValueError('Several old checkpoints found; set STREAM_RESUME_CHECKPOINT to the selected one')
    if not roots:return 'latest'  # Fresh installation, with no previous stream.
    from runtime.telemetry import checkpoint_offsets
    source=checkpoint_offsets(roots[0]/'bronze')
    if source.get('status')!='verified':raise ValueError('Selected Bronze checkpoint has no committed Kafka offset')
    path.parent.mkdir(parents=True,exist_ok=True)
    with FileLock(str(path)+'.lock',timeout=10):
        if not path.exists():
            temporary=path.with_name(path.name+'.'+uuid4().hex+'.tmp')
            temporary.write_text(json.dumps({'bronze':source['offsets'],'speed':source['offsets']}),encoding='utf-8')
            os.replace(temporary,path)
    return json.dumps(json.loads(path.read_text())[service])
