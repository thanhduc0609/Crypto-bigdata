"""Offline CI suite. DB/S3/live-stream tests remain explicitly opt-in."""
import json
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]
MODULES=[
    'tests.test_orderflow_and_worker','tests.test_prediction_evaluation','tests.test_futures_plan',
    'tests.test_candles_resilience','tests.test_s3_test_safety','tests.test_postgres_test_safety',
    'tests.test_canonical_repair','tests.test_short_term_experiment','tests.test_data_quality_audit',
    'tests.test_long_term_forecast','tests.test_long_term_research','tests.test_demo_runtime',
    'tests.test_research_pages','tests.test_research_artifacts','tests.test_smoke_imports',
    'tests.test_docker_environment',
    'tests.test_project_storage',
]


def main():
    # A pristine checkout has no ignored data; prepare the trusted fixtures first.
    res_script = str(ROOT / 'scripts/quality/research_artifacts.py')
    verify_script = str(ROOT / 'scripts/ml/verify_short_term_experiment.py')
    commands = [[sys.executable, res_script, 'restore'],
                [sys.executable, res_script, 'verify'],
                [sys.executable, '-m', 'unittest', *MODULES, '-q'],
                ['node', '--test', 'tests/long_term_viewport.test.cjs']]
    commands += [['node', '--check', str(p.relative_to(ROOT))] for p in sorted((ROOT / 'web/static/js').glob('*.js'))]
    review = json.loads((ROOT / 'configs/ml_review.json').read_text())
    commands.append([sys.executable, verify_script, 'data/ml_experiments/' + review['run_id']])
    for command in commands:
        print('CHECK: '+' '.join(command),flush=True)
        subprocess.run(command,cwd=ROOT,check=True,timeout=180)


if __name__=='__main__':main()
