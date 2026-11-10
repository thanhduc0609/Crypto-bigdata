"""Offline regression tests for real-data validation and temporal separation."""
import hashlib
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
from ingestion.historical.okx_daily_backfill import clean_daily, aggregate_complete_months, fetch_daily_pages
from ml.long_term import build_forecast, forecast_log, uncertainty_band, metrics
from scripts.run_long_term import run
from web.app import app
from web.services.long_term_service import evaluation_summary
from web.services.overview_service import get_candles_data


def raw_rows(start='2024-01-01', end='2024-03-03'):
    return [[str(int(d.timestamp()*1000)), '100','110','90','105','2','210','210','1']
            for d in pd.date_range(start, end, freq='D', tz='UTC')]


def monthly_history(n=108):
    t = np.arange(n)
    return pd.DataFrame({'month': pd.period_range('2017-01', periods=n, freq='M').astype(str),
                         'close': np.exp(8 + .012*t + .25*np.sin(t/7) + .08*np.cos(t/3))})


class DailyHistoryTests(unittest.TestCase):
    def test_duplicates_and_unclosed_are_counted(self):
        rows = raw_rows()
        rows += [rows[0].copy(), [str(int(pd.Timestamp('2024-03-04', tz='UTC').timestamp()*1000)), '1','1','1','1','0','0','0','0']]
        daily, quality = clean_daily(rows, start='2024-01-01', as_of='2024-03-04')
        self.assertEqual(quality['duplicate_rows_removed'], 1)
        self.assertEqual(quality['rejected_rows']['unclosed'], 1)
        self.assertEqual(len(daily), 63)
        self.assertEqual(daily.iloc[0].turnover_usdt, 210)

    def test_invalid_geometry_and_nonfinite_rejected_without_filling(self):
        rows = raw_rows()
        invalid, nan = rows[0].copy(), rows[0].copy()
        invalid[2], nan[4] = '50', 'NaN'
        _, q = clean_daily(rows+[invalid,nan], start='2024-01-01', as_of='2024-03-04')
        self.assertEqual(q['rejected_rows']['invalid_ohlcv'], 1)
        self.assertEqual(q['rejected_rows']['non_finite'], 1)
        self.assertEqual(q['interpolated_rows'], 0)

    def test_missing_day_fails(self):
        rows = raw_rows(); del rows[15]
        with self.assertRaisesRegex(ValueError, 'missing days'):
            clean_daily(rows, start='2024-01-01', as_of='2024-03-04')

    def test_stale_download_fails(self):
        with self.assertRaisesRegex(ValueError, 'stale or truncated'):
            clean_daily(raw_rows(end='2024-03-02'), start='2024-01-01', as_of='2024-03-04')

    def test_conflicting_duplicate_fails(self):
        rows = raw_rows(); conflict = rows[0].copy(); conflict[4] = '106'
        with self.assertRaisesRegex(ValueError, 'Conflicting duplicate'):
            clean_daily(rows+[conflict], start='2024-01-01', as_of='2024-03-04')

    def test_complete_months_and_leap_year(self):
        daily, _ = clean_daily(raw_rows(), start='2024-01-01', as_of='2024-03-04')
        monthly, excluded = aggregate_complete_months(daily, as_of='2024-03-04')
        self.assertEqual(list(monthly.month), ['2024-01','2024-02'])
        self.assertEqual(monthly.iloc[1].days, 29)
        self.assertEqual(monthly.iloc[1].volume_btc, 58)
        self.assertEqual(excluded[0]['month'], '2024-03')

    def test_incomplete_interior_month_fails(self):
        rows = raw_rows(end='2024-04-02')
        daily, _ = clean_daily(rows, start='2024-01-01', as_of='2024-04-03')
        daily = daily[daily.date != pd.Timestamp('2024-02-15', tz='UTC')]
        with self.assertRaisesRegex(ValueError, 'interior month'):
            aggregate_complete_months(daily, as_of='2024-04-03')

    def test_stalled_pagination_fails_instead_of_looping(self):
        session = SimpleNamespace(headers={}, get=lambda *a, **kw: SimpleNamespace(
            raise_for_status=lambda: None, json=lambda: {'code':'0','data':raw_rows()}))
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'did not move backwards'):
                fetch_daily_pages(Path(directory), start='2020-01-01', as_of='2024-03-04', session=session, pause=0)


class ForecastTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.history = monthly_history()
        cls.result = build_forecast(cls.history, {'simulations':500})

    def test_full_horizon_and_future_calendar(self):
        result = self.result
        self.assertEqual(len(result['forecast']), 36)
        self.assertEqual(len(result['backtest']), 36)
        self.assertEqual(result['forecast'][0]['month'], '2026-01')
        self.assertEqual(result['forecast'][-1]['month'], '2028-12')
        self.assertEqual(result['validation']['horizon_months'], 36)
        self.assertEqual(result['validation']['latest_month_available_for_selection'], '2022-12')

    def test_holdout_values_cannot_change_selection_or_holdout_predictions(self):
        changed = self.history.copy()
        changed.loc[changed.index[-36:], 'close'] *= 4
        result = build_forecast(changed, {'simulations':500})
        self.assertEqual(result['model'], self.result['model'])
        self.assertEqual(result['validation'], self.result['validation'])
        for row, previous in zip(result['backtest'], self.result['backtest']):
            self.assertEqual(row['predicted'], previous['predicted'])
            self.assertEqual(row['upper_95'], previous['upper_95'])
        self.assertNotEqual(result['evaluation']['selected']['mae_usdt'], self.result['evaluation']['selected']['mae_usdt'])

    def test_reproducible_positive_nested_intervals(self):
        for row in self.result['forecast']:
            self.assertTrue(0 < row['lower_95'] <= row['lower_80'] <= row['predicted'] <= row['upper_80'] <= row['upper_95'])
        a, _ = uncertainty_band(self.history.close, 'naive_last', 36, simulations=500, seed=7)
        b, _ = uncertainty_band(self.history.close, 'naive_last', 36, simulations=500, seed=7)
        np.testing.assert_array_equal(a, b)

    def test_reject_insufficient_history(self):
        with self.assertRaisesRegex(ValueError, 'at least 96 complete months'):
            build_forecast(monthly_history(95))

    def test_reject_gaps_and_out_of_order(self):
        with self.assertRaisesRegex(ValueError, 'contiguous'):
            build_forecast(self.history.drop(index=50))
        with self.assertRaisesRegex(ValueError, 'contiguous'):
            build_forecast(self.history.iloc[::-1])

    def test_reject_nonfinite_and_wrong_horizon(self):
        with self.assertRaises(ValueError):
            forecast_log([np.nan]*30, 36, 'naive_last')
        with self.assertRaisesRegex(ValueError, '36 months'):
            build_forecast(self.history, {'horizon_months':12})

    def test_baseline_and_metric_math(self):
        np.testing.assert_allclose(np.exp(forecast_log(np.arange(1,31),36,'naive_last')),30)
        result = metrics([100,200],[110,180],[80,90,100])
        self.assertEqual(result['mae_usdt'],15)
        self.assertEqual(result['mase'],1.5)


class PublishedArtifactTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = build_forecast(monthly_history(), {'simulations':500})

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.old_setting = app.config.get('LONG_TERM_DATA_DIR')
        app.config['LONG_TERM_DATA_DIR'] = self.root
        self.client = app.test_client()

    def tearDown(self):
        self.temp.cleanup()
        if self.old_setting is None:
            app.config.pop('LONG_TERM_DATA_DIR',None)
        else:
            app.config['LONG_TERM_DATA_DIR'] = self.old_setting

    def publish(self):
        run_id = '20261108T120000000001Z'
        folder = self.root/'runs'/run_id; folder.mkdir(parents=True)
        raw = json.dumps(self.result).encode()
        (folder/'forecast.json').write_bytes(raw)
        (self.root/'current.json').write_text(json.dumps({'run_id':run_id,'forecast_sha256':hashlib.sha256(raw).hexdigest()}))
        return folder

    def test_api_and_csv_roundtrip(self):
        self.publish()
        response = self.client.get('/api/forecast/long-term')
        self.assertEqual(response.status_code,200)
        self.assertEqual(len(response.json['forecast']),36)
        csv = self.client.get('/api/forecast/long-term/export')
        self.assertEqual(csv.status_code,200)
        self.assertEqual(len(csv.data.decode().splitlines()),37)
        self.assertIn('attachment;',csv.headers['Content-Disposition'])

    def test_comparison_csv_keeps_frozen_selection(self):
        self.publish()
        response=self.client.get('/api/forecast/long-term/evaluation/export')
        self.assertEqual(response.status_code,200)
        self.assertEqual(len(response.data.decode().splitlines()),5)
        self.assertIn('validation_rmsle',response.data.decode().splitlines()[0])
        payload=self.client.get('/api/forecast/long-term').json
        roles=[r for r in payload['reporting']['comparisons'] if r['selected_before_test']]
        self.assertEqual(len(roles),1)
        self.assertEqual(roles[0]['model'],self.result['model'])
        self.assertFalse(payload['reporting']['future_accuracy_known'])

    def test_interval_quality_penalizes_width_and_missed_actual(self):
        result=copy.deepcopy(self.result)
        result['evaluation']['selected']['mae_usdt']=110
        result['evaluation']['baseline']['mae_usdt']=100
        result['backtest']=[{'actual':actual,'lower_80':90,'upper_80':110,'lower_95':90,'upper_95':110} for actual in (100,130)]
        report=evaluation_summary(result)
        self.assertEqual(report['mae_change_vs_baseline_pct'],10)
        band=report['intervals'][1]
        self.assertEqual(band['observed_coverage_pct'],50)
        self.assertEqual(band['mean_width_usdt'],20)
        self.assertAlmostEqual(band['mean_interval_score_usdt'],420)

    def test_empty_backtest_is_rejected(self):
        folder=self.publish()
        bad=copy.deepcopy(self.result); bad['backtest']=[]
        raw=json.dumps(bad).encode()
        (folder/'forecast.json').write_bytes(raw)
        (self.root/'current.json').write_text(json.dumps({'run_id':folder.name,'forecast_sha256':hashlib.sha256(raw).hexdigest()}))
        self.assertEqual(self.client.get('/api/forecast/long-term').status_code,503)

    def test_missing_snapshot_returns_404_without_training(self):
        self.assertEqual(self.client.get('/api/forecast/long-term').status_code,404)
        self.assertEqual(list(self.root.iterdir()),[])

    def test_corrupt_snapshot_returns_503(self):
        folder = self.publish()
        (folder/'forecast.json').write_text('{}')
        self.assertEqual(self.client.get('/api/forecast/long-term').status_code,503)

    def test_traversal_pointer_is_rejected(self):
        (self.root/'current.json').write_text(json.dumps({'run_id':'../../secrets','forecast_sha256':'x'}))
        self.assertEqual(self.client.get('/api/forecast/long-term').status_code,503)

    def test_failed_pipeline_preserves_previous_pointer(self):
        self.publish()
        previous = (self.root/'current.json').read_bytes()
        source = self.root/'broken_source'; source.mkdir()
        (source/'source.json').write_text(json.dumps({'as_of':'2024-03-04'}))
        (source/'raw_rows.json').write_text('[]')
        args = SimpleNamespace(config=str(ROOT/'configs/long_term.yaml'), output_dir=str(self.root),
                               offline_from=str(source), download_only=False, upload_s3=False)
        with self.assertRaisesRegex(ValueError,'No valid'):
            run(args)
        self.assertEqual((self.root/'current.json').read_bytes(),previous)


class CandleConsistencyTests(unittest.TestCase):
    def test_daily_live_candles_use_same_utc_day_as_forecast_source(self):
        urls=[]
        def fetch(url):
            urls.append(url)
            return {'code':'0','data':raw_rows(start='2024-03-01',end='2024-03-01')}
        payload,code=get_candles_data('live','BTC-USDT','1d',10,'https://www.okx.com',lambda:None,fetch)
        self.assertEqual(code,200)
        self.assertIn('bar=1Dutc&',urls[0])
        self.assertEqual(payload['candles'][0]['time']%86400,0)
        self.assertEqual(payload['candles'][0]['close'],105)


if __name__ == '__main__':
    unittest.main(verbosity=2)
