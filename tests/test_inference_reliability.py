"""Inference gates and real PostgreSQL outcome resolution, without target tampering."""
import sys
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
import unittest
import pandas as pd

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from test_support.postgres_sandbox import PostgresRegressionCase
from test_support.candles import canonical_candles,prediction_clock
import ml.predict as predictor
from warehouse.etl.streaming_sink import write_candles_batch,write_historical_candles_batch


class InferenceReliabilityTests(PostgresRegressionCase):
    suite_name='inference'

    def predict(self,minutes_ago=0):
        moment=self.reference_time-timedelta(minutes=minutes_ago)
        candles=canonical_candles(40,moment)
        with prediction_clock(moment):
            result=predictor.run_prediction_detailed(candles,check_duplicate=True)
        self.assertTrue(result['success'],result)
        return result['prediction'],candles

    def target_candle(self,prediction,**changes):
        target=prediction['target_timestamp']
        # Use cent precision so the NUMERIC warehouse roundtrip stays exact.
        price=round(prediction['current_price']+500,2)
        row={'symbol':'BTC-USDT','window_start':target,'window_end':target+timedelta(minutes=1),
             'open':price-20,'high':price+20,'low':price-40,'close':price,'volume':25.0,
             'total_trade_value':price*25,'source':'historical_rest','is_closed':True}
        row.update(changes)
        return row

    def pending_state(self):
        return self.query('SELECT actual_direction,is_correct,actual_future_price FROM warehouse.fact_btc_predictions')[0]

    def test_01_explicit_unclosed_input_is_rejected_not_silently_used(self):
        candles=canonical_candles(40,self.reference_time)
        candles.loc[candles.index[-1],'is_closed']=False
        with prediction_clock(self.reference_time):
            result=predictor.run_prediction_detailed(candles,check_duplicate=False)
        self.assertFalse(result['success'])
        self.assertEqual(result['code'],'UNCLOSED_CANDLE')
        self.assertEqual(self.query('SELECT count(*) FROM warehouse.fact_btc_predictions')[0][0],0)

    def test_02_stream_or_missing_provenance_is_rejected(self):
        base=canonical_candles(40,self.reference_time)
        for candles in (base.assign(source='realtime_stream'),base.drop(columns='source')):
            with self.subTest(columns=list(candles.columns)),prediction_clock(self.reference_time):
                result=predictor.run_prediction_detailed(candles,check_duplicate=False)
                self.assertFalse(result['success'])
                self.assertEqual(result['code'],'UNCLOSED_CANDLE')

    def test_03_short_history_gets_specific_rejection(self):
        with prediction_clock(self.reference_time):
            result=predictor.run_prediction_detailed(canonical_candles(15,self.reference_time),check_duplicate=False)
        self.assertFalse(result['success'])
        self.assertEqual(result['code'],'INSUFFICIENT_HISTORY')

    def test_04_out_of_order_input_preserves_prediction(self):
        candles=canonical_candles(40,self.reference_time)
        shuffled=candles.sample(frac=1,random_state=42).reset_index(drop=True)
        with prediction_clock(self.reference_time):
            a=predictor.run_single_prediction(candles)
            b=predictor.run_single_prediction(shuffled)
        self.assertIsNotNone(a);self.assertIsNotNone(b)
        for key in ('feature_timestamp','target_timestamp','predicted_direction','probability_up','probability_not_up'):
            self.assertEqual(a[key],b[key])
        self.assertEqual(self.query('SELECT count(*) FROM warehouse.fact_btc_predictions')[0][0],1)

    def test_05_clearing_model_cache_keeps_idempotent_persisted_prediction(self):
        prediction,candles=self.predict()
        before=self.query('SELECT * FROM warehouse.fact_btc_predictions')[0]
        with patch.object(predictor,'_cached_model',None),patch.object(predictor,'_cached_features',None),prediction_clock(self.reference_time):
            second=predictor.run_prediction_detailed(candles,check_duplicate=True)
        self.assertTrue(second['success'])
        self.assertTrue(second['already_predicted'])
        self.assertEqual(second['prediction']['feature_timestamp'],prediction['feature_timestamp'])
        self.assertEqual(self.query('SELECT * FROM warehouse.fact_btc_predictions'),[before])

    def test_06_future_target_cannot_be_scored_even_if_input_claims_closed(self):
        prediction,_=self.predict()
        future=pd.DataFrame([self.target_candle(prediction)])
        self.assertGreater(prediction['target_timestamp'],self.reference_time)
        self.assertEqual(predictor.evaluate_unresolved_predictions(future),0)
        self.assertEqual(self.pending_state(),(None,None,None))

    def test_07_mature_exact_confirmed_outcome_is_scored_once(self):
        prediction,_=self.predict(minutes_ago=10)
        target=self.target_candle(prediction)
        self.assertEqual(prediction['target_timestamp']-prediction['feature_timestamp'],timedelta(minutes=5))
        self.assertEqual(self.pending_state(),(None,None,None))
        drift={**target,'window_start':target['window_start']+timedelta(seconds=30)}
        self.assertEqual(predictor.evaluate_unresolved_predictions(pd.DataFrame([drift])),0)
        self.assertEqual(predictor.evaluate_unresolved_predictions(pd.DataFrame([{**target,'is_closed':False}])),0)
        self.assertEqual(self.pending_state(),(None,None,None))
        self.assertEqual(predictor.evaluate_unresolved_predictions(pd.DataFrame([target])),1)
        outcome=self.pending_state()
        self.assertEqual(outcome,(1,prediction['predicted_direction']==1,target['close']))
        self.assertEqual(predictor.evaluate_unresolved_predictions(pd.DataFrame([{**target,'close':target['close']+1000}])),0)
        self.assertEqual(self.pending_state(),outcome)

    def test_08_streaming_dw_target_is_not_authoritative_actual(self):
        prediction,_=self.predict(minutes_ago=10)
        target=self.target_candle(prediction)
        write_candles_batch([{**target,'source':'realtime_stream','trade_count':50}])
        self.assertEqual(predictor.evaluate_unresolved_predictions(),0)
        self.assertEqual(self.pending_state(),(None,None,None))
        write_historical_candles_batch([target])
        self.assertEqual(predictor.evaluate_unresolved_predictions(),1)
        self.assertEqual(self.pending_state()[2],target['close'])

    def test_09_outcome_requires_explicit_provenance(self):
        prediction,_=self.predict(minutes_ago=10)
        target=self.target_candle(prediction)
        missing=pd.DataFrame([target]).drop(columns='source')
        self.assertEqual(predictor.evaluate_unresolved_predictions(missing),0)
        self.assertEqual(self.pending_state(),(None,None,None))
        self.assertEqual(predictor.evaluate_unresolved_predictions(pd.DataFrame([target])),1)

    def test_10_nonfinite_or_nonpositive_actual_price_is_never_scored(self):
        for price in (float('nan'),float('inf'),0.0,-1.0):
            with self.subTest(price=price):
                self.db.reset()
                prediction,_=self.predict(minutes_ago=10)
                target=self.target_candle(prediction,close=price)
                self.assertEqual(predictor.evaluate_unresolved_predictions(pd.DataFrame([target])),0)
                self.assertEqual(self.pending_state(),(None,None,None))


def run_inference_reliability_tests():
    return unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(InferenceReliabilityTests)).wasSuccessful()


if __name__=='__main__':
    sys.exit(0 if run_inference_reliability_tests() else 1)
