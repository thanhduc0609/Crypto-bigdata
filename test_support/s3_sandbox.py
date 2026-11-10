"""S3 integration-test ownership and scope guards.

Never accepts a configurable production prefix. Each run owns only
crypto/tests/<suite>/<UTC timestamp>-<UUID>/. This is an application guard,
not a replacement for least-privilege IAM credentials.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from uuid import uuid4
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
TEST_ROOT = 'crypto/tests/'


class S3TestSafetyError(ValueError):
    pass


def _relative(value, allow_empty=False):
    if not isinstance(value, str) or '\\' in value or '%' in value or ':' in value or any(ord(c) < 32 for c in value):
        raise S3TestSafetyError('Invalid test object path')
    if not value and allow_empty:
        return ''
    parts = value.rstrip('/').split('/')
    if not value or any(part in ('', '.', '..') for part in parts):
        raise S3TestSafetyError('Empty/absolute/traversal object paths are forbidden')
    return value


def _client_from_environment():
    import boto3
    from botocore.config import Config
    settings = {'region_name': os.getenv('AWS_REGION', 'ap-southeast-1'),
                'config': Config(connect_timeout=10, read_timeout=30, retries={'max_attempts': 2})}
    if os.getenv('S3_ENDPOINT_URL'):
        settings['endpoint_url'] = os.environ['S3_ENDPOINT_URL']
    return boto3.client('s3', **settings)


class _ScopedPaginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, **kwargs):
        self.client._check(kwargs['Bucket'], kwargs.get('Prefix', ''))
        for page in self.client._backend.get_paginator('list_objects_v2').paginate(**kwargs):
            for item in page.get('Contents', []):
                self.client._check(kwargs['Bucket'], item['Key'])
            for item in page.get('CommonPrefixes', []):
                self.client._check(kwargs['Bucket'], item['Prefix'])
            yield page


class ScopedS3Client:
    """Only the explicitly supported object operations; no unrestricted proxy."""
    def __init__(self, owner, backend):
        self.owner, self._backend = owner, backend

    def _check(self, bucket, key, mutating=False):
        self.owner.assert_owned(bucket, key)
        if not self.owner.active:
            raise S3TestSafetyError('The test sandbox is not active')
        if mutating and key == self.owner.marker_key:
            raise S3TestSafetyError('Only the sandbox lifecycle may modify its ownership marker')

    def put_object(self, **kwargs):
        self._check(kwargs['Bucket'], kwargs['Key'], True)
        return self._backend.put_object(**kwargs)

    def get_object(self, **kwargs):
        self._check(kwargs['Bucket'], kwargs['Key'])
        return self._backend.get_object(**kwargs)

    def head_object(self, **kwargs):
        self._check(kwargs['Bucket'], kwargs['Key'])
        return self._backend.head_object(**kwargs)

    def upload_file(self, Filename, Bucket, Key, **kwargs):
        self._check(Bucket, Key, True)
        return self._backend.upload_file(Filename, Bucket, Key, **kwargs)

    def download_file(self, Bucket, Key, Filename, **kwargs):
        self._check(Bucket, Key)
        return self._backend.download_file(Bucket, Key, Filename, **kwargs)

    def list_objects_v2(self, **kwargs):
        self._check(kwargs['Bucket'], kwargs.get('Prefix', ''))
        response = self._backend.list_objects_v2(**kwargs)
        for item in response.get('Contents', []):
            self._check(kwargs['Bucket'], item['Key'])
        for item in response.get('CommonPrefixes', []):
            self._check(kwargs['Bucket'], item['Prefix'])
        return response

    def get_paginator(self, operation):
        if operation != 'list_objects_v2':
            raise S3TestSafetyError('Only scoped object listing is supported')
        return _ScopedPaginator(self)

    def delete_object(self, **kwargs):
        self._check(kwargs['Bucket'], kwargs['Key'], True)
        return self._backend.delete_object(**kwargs)

    def delete_objects(self, **kwargs):
        objects = kwargs.get('Delete', {}).get('Objects', [])
        if not objects or len(objects) > 1000:
            raise S3TestSafetyError('A delete batch must contain 1..1000 owned object keys')
        # Validate the entire batch before sending even the first request.
        for obj in objects:
            self._check(kwargs['Bucket'], obj['Key'], True)
        response = self._backend.delete_objects(**kwargs)
        if response.get('Errors'):
            raise S3TestSafetyError('S3 reported object deletion errors; ownership marker retained')
        return response


class S3TestSandbox:
    def __init__(self, bucket, suite, *, backend=None, local_root=None, keep_objects=False):
        if not isinstance(bucket, str) or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', bucket):
            raise S3TestSafetyError('An explicit valid test bucket is required')
        if not isinstance(suite, str) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,47}', suite):
            raise S3TestSafetyError('Invalid suite identifier')
        self.bucket, self.suite = bucket, suite
        self.run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex
        self.prefix = TEST_ROOT + suite + '/' + self.run_id + '/'
        self.marker_key = self.prefix + '_test_owner.json'
        self._identity = (self.bucket,self.suite,self.run_id,self.prefix,self.marker_key)
        self._token = uuid4().hex
        self._backend = backend
        self.client = None
        self.active = False
        self.keep_objects = keep_objects
        self.local_root = Path(local_root or ROOT/'data/test_runs').resolve()
        self.directory = (self.local_root / suite / self.run_id).resolve()
        if not self.directory.is_relative_to(self.local_root):
            raise S3TestSafetyError('Local workspace escapes the selected test directory')
        self.directory.mkdir(parents=True, exist_ok=False)
        self.record = {'suite':suite, 'run_id':self.run_id, 'bucket':bucket, 'prefix':self.prefix,
                       'started_at':datetime.now(timezone.utc).isoformat(), 'status':'created'}
        self._save()

    @classmethod
    def from_environment(cls, suite, **kwargs):
        from processing.core.env import load_env_vars
        load_env_vars()
        return cls(os.environ.get('S3_BUCKET_NAME', ''), suite, **kwargs)

    def _save(self):
        (self.directory/'result.json').write_text(json.dumps(self.record, indent=2), encoding='utf-8')

    def key(self, relative):
        relative = _relative(relative)
        key = self.prefix + relative
        self.assert_owned(self.bucket, key)
        return key

    def uri(self, relative):
        return 's3a://' + self.bucket + '/' + self.key(relative)

    def assert_owned(self, bucket, key):
        if (self.bucket,self.suite,self.run_id,self.prefix,self.marker_key) != self._identity:
            raise S3TestSafetyError('The sandbox identity cannot be changed after creation')
        expected = TEST_ROOT + self.suite + '/' + self.run_id + '/'
        if self.prefix != expected or not re.fullmatch(r'\d{8}T\d{6}Z-[0-9a-f]{32}', self.run_id):
            raise S3TestSafetyError('Invalid sandbox namespace; production prefixes cannot be substituted')
        if bucket != self.bucket or not isinstance(key, str) or not key.startswith(self.prefix):
            raise S3TestSafetyError('S3 operation outside this exact test run is forbidden')
        _relative(key)
        return key

    def __enter__(self):
        if self.active or self.record['status'] != 'created':
            raise S3TestSafetyError('Sandbox contexts cannot be reused')
        self._backend = self._backend or _client_from_environment()
        marker = {'suite':self.suite, 'run_id':self.run_id, 'owner':self._token}
        # Claim a fresh namespace; never replace another run's marker.
        try:
            self.assert_owned(self.bucket,self.prefix)
            existing=self._backend.list_objects_v2(Bucket=self.bucket,Prefix=self.prefix,MaxKeys=1)
            if existing.get('Contents') or existing.get('CommonPrefixes'):
                raise S3TestSafetyError('Test prefix is not empty; refusing to claim existing objects')
            self._backend.put_object(Bucket=self.bucket, Key=self.marker_key,
                Body=json.dumps(marker).encode(), ContentType='application/json', IfNoneMatch='*')
        except Exception:
            self.record['status']='claim_failed'; self._save()
            raise
        self.active = True
        self.client = ScopedS3Client(self, self._backend)
        self.record['status']='running'; self._save()
        print(f'[TEST S3] s3://{self.bucket}/{self.prefix}', flush=True)
        return self

    def _verify_owner(self):
        if not self.active:
            raise S3TestSafetyError('The test sandbox is not active')
        response = self.client.get_object(Bucket=self.bucket, Key=self.marker_key)
        try:
            owner = json.loads(response['Body'].read())
        finally:
            response['Body'].close()
        if owner != {'suite':self.suite, 'run_id':self.run_id, 'owner':self._token}:
            raise S3TestSafetyError('Ownership marker mismatch; no cleanup is permitted')

    def cleanup(self):
        self._verify_owner()
        keys = []
        for page in self.client.get_paginator('list_objects_v2').paginate(Bucket=self.bucket, Prefix=self.prefix):
            keys.extend(item['Key'] for item in page.get('Contents', []) if item['Key'] != self.marker_key)
        # Validate all pages before deleting anything, including malformed listings.
        for key in keys:
            self.assert_owned(self.bucket, key)
        self._verify_owner()
        for offset in range(0,len(keys),1000):
            self.client.delete_objects(Bucket=self.bucket, Delete={'Objects':[{'Key':key} for key in keys[offset:offset+1000]]})
        self._backend.delete_object(Bucket=self.bucket, Key=self.marker_key)
        remaining = self._backend.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix, MaxKeys=1)
        if remaining.get('Contents'):
            raise S3TestSafetyError('Current objects remain after cleanup')
        self.record['deleted_current_objects']=len(keys)+1
        self.record['cleanup']='empty_current_object_listing'

    def __exit__(self, exc_type, exc, traceback):
        cleanup_error = None
        try:
            if self.keep_objects:
                self.record['cleanup']='retained_for_inspection'
            else:
                self.cleanup()
            self.record['status']='passed' if exc_type is None else 'failed'
        except Exception as error:
            cleanup_error = error
            self.record['status']='cleanup_failed'
            self.record['cleanup_error_type']=type(error).__name__
        finally:
            self.active=False
            self.record['finished_at']=datetime.now(timezone.utc).isoformat()
            self._save()
            print(f'[TEST RESULT] {self.directory / "result.json"}', flush=True)
        if cleanup_error is not None:
            if exc_type is None:
                raise cleanup_error
            if exc is not None and hasattr(exc, 'add_note'):
                exc.add_note('S3 test cleanup also failed; consult the per-run result.json')
        return False

    @contextmanager
    def isolated_storage_paths(self):
        """Apply Spark/storage path overrides only to this test process/context."""
        self._verify_owner()
        from storage.s3 import storage_cfg, get_bronze_trades_path, get_silver_trades_path, get_gold_candles_path, get_speed_candles_path, get_gold_features_path, get_raw_historical_candles_path
        settings = deepcopy(storage_cfg.get('s3', {}))
        settings.update({name:self.key(path) for name,path in {
            'bronze_path':'bronze/trades','silver_path':'silver/trades','gold_candles_path':'gold/candles_1m',
            'gold_features_path':'gold/features','speed_candles_path':'speed/candles_1m',
            'raw_historical_candles_path':'raw/historical_candles_1m'}.items()})
        env = {'STORAGE_DESTINATION':'s3','S3_BUCKET_NAME':self.bucket,'USE_S3_CHECKPOINTS':'false',
               'S3_RAW_PATH':self.uri('bronze/trades'), 'S3_PROCESSED_PATH':self.uri('speed/candles_1m')}
        # A production canonical pointer must never redirect isolated test paths.
        # This absent descriptor lives inside this owned local test directory.
        no_snapshot=self.directory/'no_active_canonical.json'
        if no_snapshot.exists():
            raise S3TestSafetyError('Isolated test descriptor sentinel must remain absent')
        with patch.dict(storage_cfg, {'s3':settings}), patch.dict(os.environ, env), patch('storage.canonical_snapshot.DEFAULT_POINTER',no_snapshot):
            for getter in (get_bronze_trades_path,get_silver_trades_path,get_gold_candles_path,get_speed_candles_path,get_gold_features_path,get_raw_historical_candles_path):
                path=getter()
                prefix='s3a://'+self.bucket+'/'
                if not path.startswith(prefix):
                    raise S3TestSafetyError('Unexpected storage URI')
                self.assert_owned(self.bucket,path[len(prefix):])
            yield
