"""Real S3/Parquet roundtrip in a fresh owned test prefix. Import is read-only.

Run explicitly: python tests/test_s3.py
This tests boto3 + Parquet, not Spark S3A (covered by the medallion test).
"""
import io
import sys
from datetime import datetime, timezone
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from test_support.s3_sandbox import S3TestSandbox


def run_s3_tests(*, keep_objects=False):
    import pyarrow as pa
    import pyarrow.parquet as pq
    with S3TestSandbox.from_environment('connection', keep_objects=keep_objects) as run:
        timestamp = datetime.now(timezone.utc).isoformat()
        fixtures = {
            'fixtures/trades.parquet': [{'trade_id':'test-'+run.run_id, 'symbol':'BTC-USDT',
                'price':86500.0, 'size':0.05, 'side':'buy', 'timestamp_utc':timestamp, 'is_test':True}],
            'fixtures/candles_1m.parquet': [{'symbol':'BTC-USDT', 'open':86450.0,
                'high':86520.0, 'low':86400.0, 'close':86500.0, 'volume':15.2, 'is_test':True}]
        }
        for relative, rows in fixtures.items():
            table = pa.Table.from_pylist(rows)
            buffer = io.BytesIO(); pq.write_table(table,buffer)
            payload = buffer.getvalue()
            key = run.key(relative)
            run.client.put_object(Bucket=run.bucket,Key=key,Body=payload,
                                  ContentType='application/octet-stream',Metadata={'test-run-id':run.run_id})
            response=run.client.get_object(Bucket=run.bucket,Key=key)
            try: actual=response['Body'].read()
            finally: response['Body'].close()
            assert actual==payload, 'S3 bytes changed during roundtrip'
            assert pq.ParquetFile(io.BytesIO(actual)).read().to_pylist()==rows, 'Parquet rows do not match fixtures'
            print('[PASS] Isolated S3 Parquet roundtrip: '+relative)
        run.record['checks_passed']=2
    print('[PASS] Both datasets verified; '+('test objects retained by request.' if keep_objects else 'owned S3 current objects cleaned up.'))
    return True


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keep',action='store_true',help='Retain only this run prefix for inspection')
    sys.exit(0 if run_s3_tests(keep_objects=parser.parse_args().keep) else 1)
