"""Offline behavioral tests: no AWS access and no shared-prefix deletion."""
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from test_support.s3_sandbox import S3TestSandbox, S3TestSafetyError


class MemoryS3:
    """Minimal object-store implementation with call evidence for safety tests."""
    def __init__(self):
        self.objects={}
        self.calls=[]
        self.poison_listing=False
        self.fail_delete=False
        self.fail_claim=False

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.calls.append(('put',Bucket,Key))
        if self.fail_claim or (kwargs.get('IfNoneMatch')=='*' and (Bucket,Key) in self.objects):
            raise RuntimeError('Conditional claim rejected')
        self.objects[Bucket,Key]=Body.read() if hasattr(Body,'read') else Body
        return {}

    def get_object(self, Bucket, Key, **kwargs):
        self.calls.append(('get',Bucket,Key))
        return {'Body':io.BytesIO(self.objects[Bucket,Key])}

    def head_object(self, Bucket, Key, **kwargs):
        self.calls.append(('head',Bucket,Key))
        return {'ContentLength':len(self.objects[Bucket,Key])}

    def upload_file(self, Filename, Bucket, Key, **kwargs):
        return self.put_object(Bucket=Bucket,Key=Key,Body=Path(Filename).read_bytes())

    def download_file(self, Bucket, Key, Filename, **kwargs):
        self.calls.append(('download',Bucket,Key))
        Path(Filename).write_bytes(self.objects[Bucket,Key])

    def list_objects_v2(self, Bucket, Prefix, **kwargs):
        self.calls.append(('list',Bucket,Prefix))
        matches=sorted(k for b,k in self.objects if b==Bucket and k.startswith(Prefix))
        contents=[]; common=set()
        for key in matches:
            rest=key[len(Prefix):]
            delimiter=kwargs.get('Delimiter')
            if delimiter and delimiter in rest:
                common.add(Prefix+rest.split(delimiter)[0]+delimiter)
            else: contents.append({'Key':key})
        if self.poison_listing:
            contents.append({'Key':'crypto/bronze/trades/DO-NOT-DELETE.parquet'})
        return {'Contents':contents[:kwargs.get('MaxKeys',10000)],
                'CommonPrefixes':[{'Prefix':v} for v in sorted(common)]}

    def get_paginator(self, operation):
        backend=self
        class Paginator:
            def paginate(self, **kwargs):
                page=backend.list_objects_v2(**kwargs)
                rows=page.get('Contents',[])
                for offset in range(0,max(1,len(rows)),600):
                    yield {'Contents':rows[offset:offset+600]}
        return Paginator()

    def delete_object(self, Bucket, Key, **kwargs):
        self.calls.append(('delete',Bucket,Key))
        self.objects.pop((Bucket,Key),None)
        return {}

    def delete_objects(self, Bucket, Delete, **kwargs):
        self.calls.append(('batch_delete',Bucket,[o['Key'] for o in Delete['Objects']]))
        if self.fail_delete:
            return {'Errors':[{'Key':Delete['Objects'][0]['Key'],'Code':'AccessDenied'}]}
        for obj in Delete['Objects']: self.objects.pop((Bucket,obj['Key']),None)
        return {'Deleted':Delete['Objects']}


class S3SafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.fake=MemoryS3()
        self.bucket='test-bucket'
        self.sentinels={('test-bucket','crypto/bronze/trades/keep.parquet'):b'bronze',
                        ('test-bucket','crypto/silver/trades/keep.parquet'):b'silver',
                        ('test-bucket','crypto/models/v2.0.0/model.json'):b'active',
                        ('test-bucket','crypto/tests/another-run/keep'):b'other-run'}
        self.fake.objects.update(self.sentinels)

    def tearDown(self):
        self.temp.cleanup()

    def scope(self, **kwargs):
        return S3TestSandbox(self.bucket,'safety',backend=self.fake,local_root=Path(self.temp.name),**kwargs)

    def assert_sentinels_intact(self):
        for key,value in self.sentinels.items():self.assertEqual(self.fake.objects.get(key),value)

    def test_active_canonical_pointer_cannot_escape_isolated_storage_paths(self):
        from storage.s3 import get_gold_candles_path
        from storage.canonical_snapshot import load_descriptor
        prefix='s3://test-bucket/crypto/local-runs/demo/canonical-repairs/demo'
        pointer=Path(self.temp.name)/'active.json'
        pointer.write_text(json.dumps(dict(schema_version=1,state='active',symbol='BTC-USDT',bar='1m',rows=1,run_id='demo',
            gold_uri=prefix+'/gold/candles_1m',manifest_uri=prefix+'/manifest.json',gold_sha256='a'*64,manifest_sha256='b'*64)),encoding='utf-8')
        with patch('storage.canonical_snapshot.DEFAULT_POINTER',pointer):
            self.assertEqual(load_descriptor()['run_id'],'demo')
            with self.scope() as scope:
                with scope.isolated_storage_paths():
                    self.assertTrue(get_gold_candles_path().startswith(scope.uri('gold/candles_1m')))
                    self.assertIsNone(load_descriptor())
            self.assertEqual(load_descriptor()['run_id'],'demo')

    def test_unique_run_namespaces(self):
        a,b=self.scope(),self.scope()
        self.assertNotEqual(a.prefix,b.prefix)
        self.assertTrue(a.prefix.startswith('crypto/tests/safety/'))
        self.assertEqual(self.fake.calls,[])

    def test_requires_explicit_bucket_and_safe_suite(self):
        for bucket,suite in [('', 'ok'),('test-bucket','../bronze'),('test-bucket','s3://root'),('x','ok')]:
            with self.subTest(bucket=bucket,suite=suite),self.assertRaises(S3TestSafetyError):
                S3TestSandbox(bucket,suite,backend=self.fake,local_root=Path(self.temp.name))
        self.assertEqual(self.fake.calls,[])

    def test_unsafe_relative_paths_rejected(self):
        run=self.scope()
        for value in ('','/crypto/bronze','../bronze','x/../../silver','x//y','x\\..\\y','%2e%2e/bronze','s3://bucket/key','x/./y'):
            with self.subTest(value=value),self.assertRaises(S3TestSafetyError):run.key(value)
        self.assertEqual(self.fake.calls,[])

    def test_shared_prefixes_rejected_before_any_request(self):
        with self.scope() as run:
            baseline=list(self.fake.calls)
            for key in ('','crypto/','crypto/tests/','crypto/bronze/trades/','crypto/silver/trades/','crypto/models/v2.0.0/a',run.prefix[:-1],run.prefix+'../other'):
                with self.subTest(key=key),self.assertRaises(S3TestSafetyError):
                    run.client.delete_object(Bucket=self.bucket,Key=key)
            self.assertEqual(self.fake.calls,baseline)
        self.assert_sentinels_intact()

    def test_cross_bucket_rejected(self):
        with self.scope() as run:
            before=list(self.fake.calls)
            with self.assertRaises(S3TestSafetyError):run.client.put_object(Bucket='other-bucket',Key=run.key('file'),Body=b'x')
            self.assertEqual(before,self.fake.calls)

    def test_all_object_apis_reject_foreign_scope_before_delegation(self):
        with self.scope() as run:
            before=list(self.fake.calls)
            calls=[lambda:run.client.get_object(Bucket=self.bucket,Key='crypto/models/v2.0.0/model.json'),
                   lambda:run.client.head_object(Bucket=self.bucket,Key='crypto/bronze/trades/keep.parquet'),
                   lambda:run.client.upload_file('not-read',self.bucket,'crypto/models/v2.0.0/model.json'),
                   lambda:run.client.download_file(self.bucket,'crypto/models/v2.0.0/model.json','not-written'),
                   lambda:run.client.list_objects_v2(Bucket=self.bucket,Prefix='crypto/'),
                   lambda:list(run.client.get_paginator('list_objects_v2').paginate(Bucket=self.bucket,Prefix=''))]
            for index,call in enumerate(calls):
                with self.subTest(api=index),self.assertRaises(S3TestSafetyError):call()
            self.assertEqual(before,self.fake.calls)

    def test_cannot_substitute_production_root(self):
        with self.scope() as run:
            old=run.prefix
            run.prefix='crypto/'
            try:
                with self.assertRaises(S3TestSafetyError):run.client.delete_object(Bucket=self.bucket,Key='crypto/bronze/trades/keep.parquet')
            finally: run.prefix=old
        self.assert_sentinels_intact()

    def test_mixed_delete_batch_is_rejected_atomically(self):
        with self.scope() as run:
            key=run.key('owned'); run.client.put_object(Bucket=self.bucket,Key=key,Body=b'own')
            before=list(self.fake.calls)
            with self.assertRaises(S3TestSafetyError):
                run.client.delete_objects(Bucket=self.bucket,Delete={'Objects':[{'Key':key},{'Key':'crypto/models/v2.0.0/model.json'}]})
            self.assertEqual(before,self.fake.calls)
            self.assertIn((self.bucket,key),self.fake.objects)

    def test_no_unrestricted_bucket_operations(self):
        with self.scope() as run:
            self.assertFalse(hasattr(run.client,'delete_bucket'))
            with self.assertRaises(S3TestSafetyError):run.client.get_paginator('list_buckets')

    def test_public_client_cannot_remove_or_replace_owner_marker(self):
        with self.scope() as run:
            for method in ('put_object','delete_object'):
                with self.subTest(method=method),self.assertRaises(S3TestSafetyError):
                    getattr(run.client,method)(Bucket=self.bucket,Key=run.marker_key,**({'Body':b'{}'} if method=='put_object' else {}))

    def test_cleanup_only_owns_current_run(self):
        with self.scope() as run:
            key=run.key('bronze/a.parquet')
            run.client.put_object(Bucket=self.bucket,Key=key,Body=b'own')
            self.assertEqual(run.client.get_object(Bucket=self.bucket,Key=key)['Body'].read(),b'own')
        self.assertEqual(self.fake.objects,self.sentinels)
        record=json.loads((run.directory/'result.json').read_text())
        self.assertEqual(record['status'],'passed')
        self.assertEqual(record['deleted_current_objects'],2)

    def test_cleanup_validates_all_pages_before_deletion(self):
        with self.assertRaises(S3TestSafetyError):
            with self.scope() as run:
                run.client.put_object(Bucket=self.bucket,Key=run.key('owned'),Body=b'own')
                self.fake.poison_listing=True
        self.assertFalse(any(c[0] in ('delete','batch_delete') for c in self.fake.calls))
        self.assert_sentinels_intact()

    def test_owner_mismatch_stops_cleanup(self):
        with self.assertRaises(S3TestSafetyError):
            with self.scope() as run:
                self.fake.objects[self.bucket,run.marker_key]=b'{"owner":"different"}'
        self.assertFalse(any(c[0] in ('delete','batch_delete') for c in self.fake.calls))
        self.assert_sentinels_intact()

    def test_cleanup_handles_multiple_delete_batches(self):
        with self.scope() as run:
            for i in range(1005):self.fake.objects[self.bucket,run.key(f'parts/{i}.parquet')]=b'row'
        batches=[c for c in self.fake.calls if c[0]=='batch_delete']
        self.assertEqual([len(c[2]) for c in batches],[1000,5])
        self.assertEqual(self.fake.objects,self.sentinels)

    def test_delete_errors_fail_cleanup_and_preserve_marker(self):
        with self.assertRaises(S3TestSafetyError):
            with self.scope() as run:
                run.client.put_object(Bucket=self.bucket,Key=run.key('file'),Body=b'own')
                self.fake.fail_delete=True
        self.assertIn((self.bucket,run.marker_key),self.fake.objects)
        record=json.loads((run.directory/'result.json').read_text())
        self.assertEqual(record['status'],'cleanup_failed')
        self.assert_sentinels_intact()

    def test_failed_test_is_cleaned_without_masking_failure(self):
        with self.assertRaisesRegex(AssertionError,'expected-test-failure'):
            with self.scope() as run:
                run.client.put_object(Bucket=self.bucket,Key=run.key('file'),Body=b'own')
                raise AssertionError('expected-test-failure')
        self.assertEqual(self.fake.objects,self.sentinels)
        self.assertEqual(json.loads((run.directory/'result.json').read_text())['status'],'failed')

    def test_cleanup_error_does_not_hide_original_test_failure(self):
        with self.assertRaisesRegex(AssertionError,'original failure'):
            with self.scope() as run:
                run.client.put_object(Bucket=self.bucket,Key=run.key('file'),Body=b'own')
                self.fake.fail_delete=True
                raise AssertionError('original failure')
        self.assertEqual(json.loads((run.directory/'result.json').read_text())['status'],'cleanup_failed')
        self.assert_sentinels_intact()

    def test_keep_mode_retains_only_owned_test_data(self):
        with self.scope(keep_objects=True) as run:
            run.client.put_object(Bucket=self.bucket,Key=run.key('file'),Body=b'own')
        self.assertEqual(json.loads((run.directory/'result.json').read_text())['cleanup'],'retained_for_inspection')
        self.assert_sentinels_intact()

    def test_client_is_closed_after_context(self):
        with self.scope() as run:pass
        before=list(self.fake.calls)
        with self.assertRaises(S3TestSafetyError):run.client.put_object(Bucket=self.bucket,Key=run.key('file'),Body=b'x')
        self.assertEqual(before,self.fake.calls)
        with self.assertRaises(S3TestSafetyError):run.__enter__()

    def test_failed_claim_never_attempts_cleanup(self):
        self.fake.fail_claim=True
        run=self.scope()
        with self.assertRaises(RuntimeError):
            with run:pass
        self.assertEqual([c[0] for c in self.fake.calls],['list','put'])
        self.assertEqual(json.loads((run.directory/'result.json').read_text())['status'],'claim_failed')

    def test_preexisting_objects_cannot_be_claimed_or_cleaned(self):
        run=self.scope()
        existing=(self.bucket,run.key('orphan.parquet'))
        self.fake.objects[existing]=b'not-owned'
        with self.assertRaisesRegex(S3TestSafetyError,'not empty'):
            with run:pass
        self.assertEqual(self.fake.objects[existing],b'not-owned')
        self.assertFalse(any(c[0] in ('put','delete','batch_delete') for c in self.fake.calls))

    def test_identity_changes_are_rejected_before_network(self):
        with self.scope() as run:
            original=run.bucket
            before=list(self.fake.calls)
            run.bucket='other-bucket'
            try:
                with self.assertRaises(S3TestSafetyError):
                    run.client.put_object(Bucket=run.bucket,Key=run.prefix+'file',Body=b'x')
                self.assertEqual(self.fake.calls,before)
            finally:run.bucket=original

    def test_storage_overrides_ignore_production_paths_and_restore_state(self):
        from storage.s3 import storage_cfg,get_bronze_trades_path,get_silver_trades_path,get_gold_candles_path,get_speed_candles_path,get_gold_features_path,get_raw_historical_candles_path
        original=deepcopy(storage_cfg)
        with patch.dict(os.environ,{'STORAGE_DESTINATION':'local','S3_RAW_PATH':'s3a://test-bucket/crypto/bronze/trades'}):
            with self.scope() as run:
                with run.isolated_storage_paths():
                    for getter in (get_bronze_trades_path,get_silver_trades_path,get_gold_candles_path,get_speed_candles_path,get_gold_features_path,get_raw_historical_candles_path):
                        self.assertTrue(getter().startswith('s3a://'+self.bucket+'/'+run.prefix))
                self.assertEqual(os.environ['STORAGE_DESTINATION'],'local')
                self.assertEqual(os.environ['S3_RAW_PATH'],'s3a://test-bucket/crypto/bronze/trades')
        self.assertEqual(storage_cfg,original)

    def test_storage_override_restores_after_exception(self):
        from storage.s3 import storage_cfg
        original=deepcopy(storage_cfg)
        with self.scope() as run:
            with self.assertRaisesRegex(RuntimeError,'failure'):
                with run.isolated_storage_paths():raise RuntimeError('failure')
        self.assertEqual(storage_cfg,original)

    def test_integration_modules_do_not_connect_or_start_spark_on_import(self):
        with patch('boto3.client',side_effect=AssertionError('Unexpected AWS call')),patch('processing.core.spark_session.create_spark_session',side_effect=AssertionError('Unexpected Spark session')):
            for filename in ('test_s3.py','test_model_registry.py','test_streaming_medallion_e2e.py'):
                runpy.run_path(str(ROOT/'tests'/filename),run_name='import_safety_only')

    def test_medallion_requires_dedicated_services_before_any_s3_operation(self):
        module=runpy.run_path(str(ROOT/'tests/test_streaming_medallion_e2e.py'),run_name='medallion_safety')
        with patch.dict(os.environ,{'TEST_POSTGRES_DB':'','TEST_KAFKA_TOPIC':''}),patch.object(S3TestSandbox,'from_environment') as factory:
            with self.assertRaises(S3TestSafetyError):module['run_e2e_streaming_test']()
            factory.assert_not_called()

    def test_connection_script_uses_only_scoped_parquet_objects(self):
        module=runpy.run_path(str(ROOT/'tests/test_s3.py'),run_name='connection_safety')
        run=self.scope()
        with patch.object(S3TestSandbox,'from_environment',return_value=run):
            self.assertTrue(module['run_s3_tests']())
        self.assertEqual(self.fake.objects,self.sentinels)

    def test_registry_script_uses_copies_and_exercises_real_integrity_gate(self):
        module=runpy.run_path(str(ROOT/'tests/test_model_registry.py'),run_name='registry_safety')
        run=self.scope()
        with patch.object(S3TestSandbox,'from_environment',return_value=run):
            self.assertTrue(module['run_model_registry_tests']())
        self.assertEqual(self.fake.objects,self.sentinels)
        self.assertEqual(run.record['checks_passed'],5)


if __name__=='__main__':
    unittest.main(verbosity=2)
