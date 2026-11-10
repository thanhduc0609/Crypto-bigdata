"""Canonical REST/streaming SQL integration regressions on an owned test DB."""
import sys
from datetime import datetime,timezone,timedelta
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from test_support.postgres_sandbox import PostgresRegressionCase
from warehouse.etl.streaming_sink import write_candles_batch,write_historical_candles_batch


class ReconciliationRegressionTests(PostgresRegressionCase):
    suite_name='reconciliation'

    def setUp(self):
        super().setUp()
        start=datetime(2020,1,1,12,0,tzinfo=timezone.utc)
        self.rest={'symbol':'BTC-USDT','window_start':start,'window_end':start+timedelta(minutes=1),
                   'open':80000.0,'high':80500.0,'low':79800.0,'close':80200.0,'volume':10.0,
                   'total_trade_value':801000.0,'source':'historical_rest','is_closed':True}
        self.stream={**self.rest,'open':80010.0,'high':80550.0,'low':79750.0,'close':80300.0,
                     'volume':12.0,'trade_count':140,'buy_volume':7.0,'sell_volume':5.0,
                     'vwap':80220.0,'trade_imbalance':1/6,'total_trade_value':962640.0,
                     'source':'realtime_stream','is_closed':True}

    def snapshot(self):
        return self.query("""SELECT open,high,low,close,volume,total_trade_value,source,is_closed,
            trade_count,buy_volume,sell_volume,vwap,trade_imbalance FROM warehouse.fact_crypto_1m_candles
            WHERE symbol='BTC-USDT' AND window_start=%s""",(self.rest['window_start'],))[0]

    def assert_rest(self,volume=10.0):
        row=self.snapshot()
        self.assertEqual(tuple(map(float,row[:4])),(80000,80500,79800,80200))
        self.assertEqual(float(row[4]),volume)
        self.assertEqual(row[6:8],('historical_rest',True))
        self.assertEqual(row[8:],(None,None,None,None,None))

    def test_01_partial_stream_cannot_overwrite_rest(self):
        write_historical_candles_batch([self.rest])
        original=self.snapshot()
        write_candles_batch([{**self.stream,'volume':1.5,'trade_count':15,'is_closed':False}])
        self.assertEqual(self.snapshot(),original)
        self.assert_rest()

    def test_02_larger_stream_cannot_override_authoritative_rest(self):
        write_historical_candles_batch([self.rest])
        original=self.snapshot()
        write_candles_batch([self.stream])
        self.assertEqual(self.snapshot(),original)
        self.assert_rest()

    def test_03_rest_precedence_is_independent_of_arrival_order(self):
        write_historical_candles_batch([self.rest])
        write_candles_batch([self.stream])
        rest_first=self.snapshot()
        self.db.reset()
        write_candles_batch([self.stream])
        self.assertEqual(self.snapshot()[6],'realtime_stream')
        write_historical_candles_batch([self.rest])
        self.assertEqual(self.snapshot(),rest_first)
        self.assert_rest()

    def test_04_lower_trade_count_does_not_degrade_same_source_stream(self):
        write_candles_batch([self.stream])
        original=self.snapshot()
        write_candles_batch([{**self.stream,'trade_count':15,'volume':1.5,'close':80020.0}])
        self.assertEqual(self.snapshot(),original)
        self.assertEqual(self.snapshot()[8],140)

    def test_05_unclosed_stream_cannot_demote_closed_stream(self):
        write_candles_batch([self.stream])
        original=self.snapshot()
        write_candles_batch([{**self.stream,'trade_count':200,'volume':14,'is_closed':False}])
        self.assertEqual(self.snapshot(),original)
        self.assertTrue(self.snapshot()[7])

    def test_06_same_source_rest_correction_updates_without_volume_heuristic(self):
        write_historical_candles_batch([self.rest])
        correction={**self.rest,'volume':9.0,'total_trade_value':720000.0}
        write_historical_candles_batch([correction])
        self.assert_rest(volume=9)
        self.assertEqual(float(self.snapshot()[5]),720000)

    def test_07_replaying_canonical_record_does_not_duplicate(self):
        for _ in range(3):
            write_historical_candles_batch([self.rest])
            write_candles_batch([self.stream])
        self.assertEqual(self.query('SELECT count(*) FROM warehouse.fact_crypto_1m_candles')[0][0],1)
        self.assert_rest()


def run_reconciliation_tests():
    return unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(ReconciliationRegressionTests)).wasSuccessful()


if __name__=='__main__':
    sys.exit(0 if run_reconciliation_tests() else 1)
