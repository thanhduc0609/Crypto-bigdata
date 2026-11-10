"""Real isolated S3 model-registry tests, with no active artifact writes.

Run explicitly: python tests/test_model_registry.py
"""
import sys
import shutil
from pathlib import Path
from unittest.mock import patch

project_root=Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0,str(project_root))

from test_support.s3_sandbox import S3TestSandbox


def run_model_registry_tests(*,keep_objects=False):
    import ml.model_registry as registry
    active=project_root/'ml/artifacts'
    before={p.name:registry.calculate_sha256(p) for p in active.iterdir() if p.is_file()}
    try:
        with S3TestSandbox.from_environment('model-registry',keep_objects=keep_objects) as run:
            copies=run.directory/'artifact-copy'; copies.mkdir()
            for name in registry.ARTIFACT_FILENAMES:
                shutil.copy2(active/name,copies/name)
            staging=run.directory/'downloaded'
            prefix=run.key('models')
            version='test-'+run.run_id
            with patch.object(registry,'get_s3_client',return_value=run.client), patch.object(registry,'get_s3_bucket',return_value=run.bucket):
                result=registry.upload_model_artifacts(version=version,artifacts_dir=copies,s3_prefix=prefix)
                assert len(result['uploaded_files'])==len(registry.ARTIFACT_FILENAMES)+1
                assert len(result['checksums'])==len(registry.ARTIFACT_FILENAMES)
                print('[PASS] 1/5: Upload artifact copies and checksum manifest to the owned prefix')
                assert version in registry.list_s3_model_versions(s3_prefix=prefix)
                print('[PASS] 2/5: Discover only this run model registry')
                downloaded=registry.download_model_artifacts(version=version,dest_dir=staging,s3_prefix=prefix,activate=False)
                assert not downloaded['activated'] and len(downloaded['verified_files'])==len(registry.ARTIFACT_FILENAMES)
                print('[PASS] 3/5: Download into this run workspace without activation')
                for name in registry.ARTIFACT_FILENAMES:
                    assert registry.calculate_sha256(active/name)==registry.calculate_sha256(staging/name)
                print('[PASS] 4/5: All staged artifact checksums match the active source')
                # Corrupt OUR uploaded object and exercise the production rejection path.
                model_name='btc_xgboost_model.json'
                run.client.put_object(Bucket=run.bucket,Key=prefix+'/v'+version+'/'+model_name,
                                      Body=(copies/model_name).read_bytes()+b' ')
                rejected=run.directory/'must-not-publish'
                try:
                    registry.download_model_artifacts(version=version,dest_dir=rejected,s3_prefix=prefix,activate=False)
                except ValueError as error:
                    assert 'INTEGRITY FAILURE' in str(error)
                else:
                    raise AssertionError('Production downloader accepted a corrupted artifact')
                assert not rejected.exists(), 'Corrupted download published files before verification'
                print('[PASS] 5/5: Production checksum gate rejects corrupted S3 data before publishing')
                run.record['checks_passed']=5
    finally:
        after={p.name:registry.calculate_sha256(p) for p in active.iterdir() if p.is_file()}
        assert before==after, 'Active artifact directory changed during registry tests'
    print('[PASS] 5/5 model-registry tests; all active files unchanged; '+('test objects retained by request.' if keep_objects else 'owned S3 current objects cleaned up.'))
    return True


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keep',action='store_true',help='Retain only this run prefix for inspection')
    sys.exit(0 if run_model_registry_tests(keep_objects=parser.parse_args().keep) else 1)
