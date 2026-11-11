"""Project storage paths, immutable relocation, and create-only S3 migration."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from storage import project_layout as layout
from scripts import organize_s3 as migration


class Missing(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class MemoryS3:
    def __init__(self,objects):self.objects=dict(objects);self.deleted=[]
    def get_paginator(self,name):return self
    def paginate(self,**kwargs):
        if kwargs.get('Delimiter')=='/':
            root=kwargs['Prefix']
            prefixes={root+k[len(root):].split('/')[0]+'/' for k in self.objects if k.startswith(root) and '/' in k[len(root):]}
            yield {'CommonPrefixes':[{'Prefix':p} for p in sorted(prefixes)]};return
        yield {'Contents':[dict(Key=k,Size=len(v),ETag=hashlib.md5(v).hexdigest()) for k,v in self.objects.items() if k.startswith(kwargs['Prefix'])]}
    def head_object(self,**kwargs):
        if kwargs['Key'] not in self.objects:raise Missing('404')
        return dict(ContentLength=len(self.objects[kwargs['Key']]))
    def get_object(self,**kwargs):
        if kwargs['Key'] not in self.objects:raise Missing('NoSuchKey')
        body=self.objects[kwargs['Key']]
        if kwargs.get('IfMatch') and kwargs['IfMatch']!=hashlib.md5(body).hexdigest():raise Missing('PreconditionFailed')
        return dict(Body=io.BytesIO(body),ContentType='application/octet-stream')
    def put_object(self,**kwargs):
        if kwargs.get('IfNoneMatch')=='*' and kwargs['Key'] in self.objects:raise Missing('PreconditionFailed')
        self.objects[kwargs['Key']]=kwargs['Body']
    def delete_object(self,**kwargs):
        self.get_object(**kwargs);self.deleted.append(kwargs['Key']);del self.objects[kwargs['Key']]


class ProjectStorageTests(unittest.TestCase):
    source='crypto/local-runs/previous-team-run'
    migration_id='20261110T092500123456Z'

    def test_root_and_writer_names_are_project_owned(self):
        with patch.dict('os.environ',{'S3_PROJECT_PREFIX':'crypto','S3_WRITER_ID':'writer-unit'}):
            self.assertEqual(layout.project_prefix(),'crypto');self.assertEqual(layout.writer_id(),'writer-unit')
        for value in ('','crypto/../x','other','crypto/tests/x'):
            with patch.dict('os.environ',{'S3_PROJECT_PREFIX':value}):
                with self.assertRaises(ValueError):layout.project_prefix()

    def test_move_maps_layers_and_directory_uris_consistently(self):
        pairs={'bronze/trades/file.parquet':f'crypto/bronze/trades/history/{self.migration_id}/file.parquet',
               'speed/candles_1m/file.parquet':f'crypto/streaming/candles_1m/history/{self.migration_id}/file.parquet',
               'medallion-runs/batch/silver/trades':'crypto/silver/trades/batch',
               'medallion-runs/batch/gold/candles_1m/a.parquet':'crypto/gold/candles_1m/batch/a.parquet',
               'medallion-runs/batch/manifest.json':'crypto/manifests/batches/batch/manifest.json',
               'canonical-repairs/r/manifest.json':'crypto/canonical-repairs/r/manifest.json'}
        for old,new in pairs.items():
            self.assertEqual(layout.migrated_key(self.source,self.source+'/'+old,self.migration_id),new)
        with self.assertRaises(ValueError):layout.migrated_key(self.source,'crypto/models/model.json',self.migration_id)
        with self.assertRaises(ValueError):layout.migrated_key(self.source,self.source+'/../shared',self.migration_id)

    def test_frozen_uri_resolves_without_modifying_provenance_file(self):
        locations=dict(bucket='unit',source_prefix=self.source,migration_id=self.migration_id)
        old='s3://unit/'+self.source+'/canonical-repairs/r/gold/candles_1m'
        self.assertEqual(layout.resolve_uri(old,locations),'s3://unit/crypto/canonical-repairs/r/gold/candles_1m')
        self.assertEqual(layout.resolve_uri(old.replace('unit/','other/'),locations),old.replace('unit/','other/'))

    def test_new_batch_versions_live_under_their_layers(self):
        with patch.dict('os.environ',{'S3_PROJECT_PREFIX':'crypto'}):
            paths=layout.operational_paths('20261110T092500123456Z-1234abcd','unit')
        self.assertIn('/crypto/silver/trades/',paths['silver']);self.assertIn('/crypto/gold/candles_1m/',paths['gold'])
        self.assertIn('/crypto/manifests/batches/',paths['manifest'])

    def make_checkpoint(self,root,name,offset):
        path=root/'data/runtime/checkpoints'/name
        (path/'bronze/commits').mkdir(parents=True);(path/'bronze/offsets').mkdir()
        (path/'bronze/commits/3').write_text('v1\n{}')
        (path/'bronze/offsets/3').write_text('v1\n{}\n'+json.dumps({'crypto-trades':{'0':offset}}))
        return path

    def test_another_machine_uses_its_own_committed_offsets(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(layout,'ROOT',Path(directory)),patch.dict('os.environ',{'S3_WRITER_ID':'writer-unit','STREAM_RESUME_CHECKPOINT':''}):
            root=Path(directory);self.make_checkpoint(root,'previous',77)
            self.assertEqual(json.loads(layout.starting_offsets('bronze')),{'crypto-trades':{'0':77}})
            self.assertEqual(json.loads(layout.starting_offsets('speed')),{'crypto-trades':{'0':77}})

    def test_ambiguous_old_checkpoints_require_explicit_local_selection(self):
        with tempfile.TemporaryDirectory() as directory,patch.object(layout,'ROOT',Path(directory)),patch.dict('os.environ',{'S3_WRITER_ID':'writer-unit','STREAM_RESUME_CHECKPOINT':''}):
            root=Path(directory);self.make_checkpoint(root,'one',11);selected=self.make_checkpoint(root,'two',22)
            with self.assertRaisesRegex(ValueError,'Several old checkpoints'):layout.starting_offsets('bronze')
            with patch.dict('os.environ',{'STREAM_RESUME_CHECKPOINT':str(selected)}):
                self.assertEqual(json.loads(layout.starting_offsets('bronze')),{'crypto-trades':{'0':22}})

    def fixture(self,root):
        root.joinpath('.env').write_text('AWS_SECRET_ACCESS_KEY=synthetic-private-value\nPOSTGRES_USER=existing\nS3_RUN_PREFIX='+self.source+'\n')
        key=self.source+'/bronze/trades/part.parquet'
        meta=self.source+'/bronze/trades/_spark_metadata/0'
        backend=MemoryS3({key:b'raw bytes retained',meta:('v1\n'+json.dumps(dict(path='s3a://unit/'+key,action='add'))+'\n').encode(),'crypto/models/keep.json':b'untouched'})
        plan=dict(schema_version=1,bucket='unit',source_prefix=self.source,migration_id=self.migration_id,objects=migration.inventory(backend,'unit',self.source+'/'),
                  bootstrap={'bronze':{'topic':{'0':42}},'speed':{'topic':{'0':42}}},status='prepared')
        return backend,plan,key

    def test_copy_keeps_raw_bytes_rewrites_only_metadata_and_preserves_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);state=root/'data/runtime/s3-layout';state.mkdir(parents=True)
            backend,plan,key=self.fixture(root)
            with patch.object(migration,'ROOT',root),patch.object(migration,'STATE',state),patch.object(migration,'PLAN',state/'plan.json'):
                migration.copy_all(backend,plan)
            target=layout.migrated_key(self.source,key,self.migration_id)
            self.assertEqual(backend.objects[target],backend.objects[key])
            self.assertEqual(backend.objects['crypto/models/keep.json'],b'untouched')
            self.assertEqual(backend.deleted,[])
            env=root.joinpath('.env').read_text();self.assertIn('synthetic-private-value',env);self.assertIn('S3_PROJECT_PREFIX=crypto',env)
            meta=backend.objects[target.rsplit('/',1)[0]+'/_spark_metadata/0']
            self.assertNotIn(self.source.encode(),meta)
            self.assertEqual(plan['status'],'copied_and_verified')

    def test_destination_collision_never_overwrites_either_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);state=root/'data/runtime/s3-layout';state.mkdir(parents=True)
            backend,plan,key=self.fixture(root)
            target=layout.migrated_key(self.source,key,self.migration_id);backend.objects[target]=b'existing different dataset'
            with patch.object(migration,'ROOT',root),patch.object(migration,'STATE',state),patch.object(migration,'PLAN',state/'plan.json'):
                with self.assertRaisesRegex(ValueError,'collision'):migration.copy_all(backend,plan)
            self.assertEqual(backend.objects[target],b'existing different dataset');self.assertIn(key,backend.objects)
            self.assertFalse(root.joinpath('configs/s3_locations.json').exists());self.assertEqual(backend.deleted,[])

    def test_finish_requires_successful_audit_before_any_deletion(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);folder=root/'data/demo_audits/run';folder.mkdir(parents=True)
            audit=folder/'pipeline-audit.json';audit.write_text(json.dumps({'passed':False}))
            with patch.object(migration,'ROOT',root):
                backend=MemoryS3({})
                with self.assertRaisesRegex(ValueError,'successful audit'):migration.finish(backend,{'status':'copied_and_verified'},audit)
                self.assertEqual(backend.deleted,[])

    def test_finish_deletes_only_verified_old_objects_and_keeps_other_datasets(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);state=root/'data/runtime/s3-layout';state.mkdir(parents=True)
            backend,plan,key=self.fixture(root)
            folder=root/'data/demo_audits/new';folder.mkdir(parents=True)
            audit=folder/'pipeline-audit.json'
            audit.write_text(json.dumps(dict(passed=True,trace={'raw_source_object':'crypto/bronze/trades/streams/writer-unit/a.parquet'})))
            with patch.object(migration,'ROOT',root),patch.object(migration,'STATE',state),patch.object(migration,'PLAN',state/'plan.json'):
                migration.copy_all(backend,plan)
                migration.finish(backend,plan,audit)
            self.assertEqual(plan['status'],'completed')
            self.assertTrue(all(k.startswith(self.source+'/') for k in backend.deleted))
            self.assertIn('crypto/models/keep.json',backend.objects)
            self.assertIn(layout.migrated_key(self.source,key,self.migration_id),backend.objects)
            self.assertTrue(list((state/'original-objects').glob('*.bin')))

    def test_backup_corruption_refuses_source_deletion(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);state=root/'data/runtime/s3-layout';state.mkdir(parents=True)
            backend,plan,key=self.fixture(root)
            folder=root/'data/demo_audits/new';folder.mkdir(parents=True)
            audit=folder/'pipeline-audit.json'
            audit.write_text(json.dumps(dict(passed=True,trace={'raw_source_object':'crypto/bronze/trades/history/unit/a.parquet'})))
            with patch.object(migration,'ROOT',root),patch.object(migration,'STATE',state),patch.object(migration,'PLAN',state/'plan.json'):
                migration.copy_all(backend,plan)
                for file in (state/'original-objects').glob('*.bin'):file.write_bytes(b'tampered')
                with self.assertRaisesRegex(ValueError,'Backup checksum'):migration.finish(backend,plan,audit)
            self.assertEqual(backend.deleted,[])

    def test_inventory_keeps_each_stream_and_batch_version_separate(self):
        from processing.quality.inventory import classify
        self.assertEqual(classify('crypto/bronze/trades/streams/writer-unit/_spark_metadata/0'),('shared','crypto/bronze/trades/streams/writer-unit'))
        self.assertEqual(classify('crypto/gold/candles_1m/20261110T092500123456Z-1234abcd/a.parquet'),('operational_version','crypto/gold/candles_1m/20261110T092500123456Z-1234abcd'))

    def test_refresh_adds_late_batch_objects_and_copy_resumes_verified_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);state=root/'data/runtime/s3-layout';state.mkdir(parents=True)
            backend,plan,key=self.fixture(root)
            with patch.object(migration,'ROOT',root),patch.object(migration,'STATE',state),patch.object(migration,'PLAN',state/'plan.json'):
                migration.copy_all(backend,plan)
                backend.objects[self.source+'/medallion-runs/batch/_owner.json']=b'late completed batch'
                migration.refresh(backend,plan);migration.copy_all(backend,plan)
            self.assertEqual(len(plan['copied']),3);self.assertEqual(plan['pending_source_objects'],[])
            self.assertIn('crypto/manifests/batches/batch/_owner.json',backend.objects)
            self.assertEqual(backend.deleted,[])

    def test_incremental_copy_refuses_corrupt_verified_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);state=root/'data/runtime/s3-layout';state.mkdir(parents=True)
            backend,plan,key=self.fixture(root)
            with patch.object(migration,'ROOT',root),patch.object(migration,'STATE',state),patch.object(migration,'PLAN',state/'plan.json'):
                migration.copy_all(backend,plan)
                for file in (state/'original-objects').glob('*.bin'):file.write_bytes(b'corrupted')
                with self.assertRaisesRegex(ValueError,'Verified backup'):migration.copy_all(backend,plan)
            self.assertEqual(backend.deleted,[])

    def test_changed_destination_blocks_source_retirement(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);state=root/'data/runtime/s3-layout';state.mkdir(parents=True)
            backend,plan,key=self.fixture(root)
            folder=root/'data/demo_audits/new';folder.mkdir(parents=True)
            audit=folder/'pipeline-audit.json'
            audit.write_text(json.dumps(dict(passed=True,trace={'raw_source_object':'crypto/bronze/trades/history/unit/a.parquet'})))
            with patch.object(migration,'ROOT',root),patch.object(migration,'STATE',state),patch.object(migration,'PLAN',state/'plan.json'):
                migration.copy_all(backend,plan)
                for row in plan['copied']:backend.objects[row['target']]=b'changed'
                with self.assertRaisesRegex(ValueError,'Destination changed'):migration.finish(backend,plan,audit)
            self.assertEqual(backend.deleted,[])

    def test_project_capture_combines_independent_logs_and_skips_unpublished_history(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from runtime.bronze_capture import capture_project
        objects={}
        for group,name,offset in [('history','published',1),('streams','writer-unit',2),('history','incomplete',3)]:
            prefix=f'crypto/bronze/trades/{group}/{name}/';key=prefix+'part.parquet'
            sink=io.BytesIO();pq.write_table(pa.table({'raw_payload':['synthetic'],'kafka_offset':[offset]}),sink)
            objects[key]=sink.getvalue()
            objects[prefix+'_spark_metadata/0']=('v1\n'+json.dumps(dict(path='s3a://unit/'+key,action='add'))+'\n').encode()
            if name=='published':objects[prefix+'_READY.json']=b'{}'
        with tempfile.TemporaryDirectory() as directory,patch.dict('os.environ',{'S3_PROJECT_PREFIX':'crypto'}):
            root=Path(directory)
            result=capture_project(MemoryS3(objects),'unit',root/'run',root/'cache')
            frame=pq.ParquetFile(result['path']).read().to_pandas()
        self.assertEqual(result['rows'],2);self.assertEqual(set(frame.kafka_offset),{1,2})
        self.assertEqual(result['commit']['datasets'],2);self.assertEqual(len(result['commit']['skipped_uncommitted']),1)


if __name__=='__main__':unittest.main()
