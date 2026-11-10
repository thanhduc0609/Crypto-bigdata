"""Run the three repaired suites against disposable PostgreSQL databases.

Requires the existing local PostgreSQL credentials, including CREATE DATABASE.
Does not read/write S3, use Kafka, or fetch live market data.
"""
import argparse
from datetime import datetime,timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest
from uuid import uuid4

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
FILES=('test_pipeline_e2e.py','test_reconciliation.py','test_inference_reliability.py')


def artifact_hashes():
    return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'ml/artifacts').iterdir() if p.is_file()}


def main(label='run'):
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:12]
    folder=ROOT/'data/test_runs/legacy-reports'/run_id
    folder.mkdir(parents=True)
    before=artifact_hashes()
    records=[]
    for filename in FILES:
        spec=importlib.util.spec_from_file_location('regression_'+filename[:-3],ROOT/'tests'/filename)
        module=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        suite=unittest.defaultTestLoader.loadTestsFromModule(module)
        result=unittest.TextTestRunner(stream=sys.stdout,verbosity=2).run(suite)
        records.append({'suite':filename,'tests':result.testsRun,'failures':len(result.failures),
                        'errors':len(result.errors),'skipped':len(result.skipped),'passed':result.wasSuccessful() and result.testsRun>0})
    after=artifact_hashes()
    source=['ml/predict.py','test_support/postgres_sandbox.py','test_support/candles.py',
            *('tests/'+name for name in FILES)]
    evidence={'label':label,'run_id':run_id,'suites':records,'active_artifacts_unchanged':before==after,
              'source_sha256':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in source},
              'artifact_sha256':after}
    evidence['passed']=all(r['passed'] and not r['skipped'] for r in records) and before==after
    (folder/'summary.json').write_text(json.dumps(evidence,indent=2),encoding='utf-8')
    print('REGRESSION_SUMMARY '+json.dumps(evidence),flush=True)
    print('REGRESSION_REPORT '+str(folder/'summary.json'),flush=True)
    return 0 if evidence['passed'] else 1


if __name__=='__main__':
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8')
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label',default='run')
    sys.exit(main(parser.parse_args().label))
