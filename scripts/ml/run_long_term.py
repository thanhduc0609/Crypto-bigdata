"""Download multi-year daily data, evaluate and publish a 36-month forecast.

python scripts/run_long_term.py
python scripts/run_long_term.py --offline-from data/long_term/runs/<run-id>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pandas as pd
import yaml
import numpy as np
import sklearn
from filelock import FileLock
from ingestion.historical.okx_daily_backfill import fetch_daily_pages, clean_daily, aggregate_complete_months, utc
from ml.long_term_3y.long_term import build_forecast


def write_json(path, data):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    temporary.replace(path)


def run(args):
    config_path = Path(args.config).resolve()
    config = yaml.safe_load(config_path.read_text(encoding='utf-8'))
    if config['source']['symbol'] != 'BTC-USDT' or config['source']['bar'] != '1Dutc':
        raise ValueError('Only BTC-USDT UTC daily source is supported')
    destination = Path(args.output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    with FileLock(str(destination / 'publish.lock'), timeout=0):
        run_id = utc().strftime('%Y%m%dT%H%M%S%fZ')
        directory = destination / 'runs' / run_id
        directory.mkdir(parents=True)
        if args.offline_from:
            previous = Path(args.offline_from).resolve()
            source = json.loads((previous / 'source.json').read_text(encoding='utf-8'))
            rows = json.loads((previous / 'raw_rows.json').read_text(encoding='utf-8'))
            source = {**source, 'replayed_from': str(previous), 'offline': True}
        else:
            rows, source = fetch_daily_pages(directory / 'raw', start=config['source']['start'])
        write_json(directory / 'raw_rows.json', rows)
        write_json(directory / 'source.json', source)
        daily, quality = clean_daily(rows, start=config['source']['start'], as_of=source['as_of'])
        monthly, excluded = aggregate_complete_months(daily, as_of=source['as_of'])
        daily.to_parquet(directory / 'daily.parquet', index=False)
        monthly.to_parquet(directory / 'monthly.parquet', index=False)
        quality.update(complete_months=len(monthly), excluded_months=excluded)
        write_json(directory / 'quality.json', quality)
        if args.download_only:
            print(json.dumps({'downloaded_run': str(directory), 'daily_rows': len(daily), 'monthly_rows': len(monthly)}), flush=True)
            return directory
        result = build_forecast(monthly, config['forecast'])
        result.update(run_id=run_id, generated_at=utc().isoformat(), source=source, data_quality=quality)
        result['dataset_sha256'] = hashlib.sha256(monthly.to_csv(index=False, float_format='%.12g').encode()).hexdigest()
        result['raw_sha256'] = hashlib.sha256((directory/'raw_rows.json').read_bytes()).hexdigest()
        result['code_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip()
        result['code_has_uncommitted_changes'] = bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT).strip())
        impl_p1 = ROOT / 'ml/long_term_3y/long_term.py' if (ROOT / 'ml/long_term_3y/long_term.py').exists() else ROOT / 'ml/long_term.py'
        impl_p2 = ROOT / 'ingestion/historical/okx_daily_backfill.py'
        impl_p3 = ROOT / 'scripts/ml/run_long_term.py' if (ROOT / 'scripts/ml/run_long_term.py').exists() else ROOT / 'scripts/run_long_term.py'
        result['implementation_sha256'] = hashlib.sha256(b''.join(p.read_bytes() for p in (impl_p1, impl_p2, impl_p3))).hexdigest()
        result['config_sha256'] = hashlib.sha256(config_path.read_bytes()).hexdigest()
        result['runtime_versions'] = {'python':sys.version.split()[0], 'numpy':np.__version__,
                                      'pandas':pd.__version__, 'scikit_learn':sklearn.__version__}
        pd.DataFrame(result['forecast']).to_csv(directory / 'forecast.csv', index=False)
        pd.DataFrame(result['backtest']).to_csv(directory / 'backtest.csv', index=False)
        write_json(directory / 'forecast.json', result)
        if args.upload_s3:
            from processing.core.env import load_env_vars
            load_env_vars()
            import boto3
            bucket = os.environ['S3_BUCKET_NAME']
            # Unique run only; never overwrites the short-term model or shared Gold.
            from storage.project_layout import project_prefix
            prefix = project_prefix() + '/long_term/' + run_id
            client = boto3.client('s3', region_name=os.getenv('AWS_REGION', 'ap-southeast-1'))
            for name in ('raw_rows.json', 'source.json', 'daily.parquet', 'monthly.parquet', 'quality.json', 'forecast.json', 'forecast.csv', 'backtest.csv'):
                client.upload_file(str(directory/name), bucket, prefix+'/'+name)
            result['s3_snapshot'] = {'bucket': bucket, 'prefix': prefix}
            write_json(directory/'forecast.json', result)
            client.upload_file(str(directory/'forecast.json'), bucket, prefix+'/forecast.json')
        # Last operation: publish only a fully successful run via atomic pointer.
        write_json(destination / 'current.json', {'run_id': run_id,
                   'forecast_sha256': hashlib.sha256((directory/'forecast.json').read_bytes()).hexdigest()})
        print(json.dumps({'published_run': str(directory), 'model': result['model'],
                          'daily_rows': len(daily), 'complete_months': len(monthly),
                          'forecast_from': result['forecast'][0]['month'], 'forecast_to': result['forecast'][-1]['month'],
                          'holdout': result['evaluation']['selected'],
                          'beats_baseline_mae': result['evaluation']['beats_baseline_mae']}, ensure_ascii=False), flush=True)
        return directory


if __name__ == '__main__':
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(ROOT/'configs/long_term.yaml'))
    parser.add_argument('--output-dir', default=str(ROOT/'data/long_term'))
    parser.add_argument('--offline-from', help='Replay saved raw rows and original as-of timestamp without downloading OKX data')
    parser.add_argument('--download-only', action='store_true', help='Save history without replacing the published forecast')
    parser.add_argument('--upload-s3', action='store_true', help='Upload immutable files to a unique long_term run prefix')
    run(parser.parse_args())
