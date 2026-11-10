"""Regression of BTC payloads, features, splits, real model and isolated DW.

Synthetic fixtures only; no live OKX/Kafka/S3 calls or model training.
"""
import json
import sys
import time
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import unittest
import numpy as np

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from test_support.postgres_sandbox import PostgresRegressionCase
from test_support.candles import canonical_candles,prediction_clock
from processing.features.feature_engineering import compute_features,add_future_direction_target,FEATURE_COLUMNS
from ml.dataset import create_chronological_splits
from ml.predict import load_prediction_model,run_single_prediction


class PipelineRegressionTests(PostgresRegressionCase):
    suite_name='pipeline'

    def test_01_production_ingestion_callbacks_validate_and_route(self):
        import ingestion.streaming.okx_producer as producer
        fake=MagicMock()
        ws=SimpleNamespace(producer=fake)
        valid={'tradeId':'synthetic-1001','instId':'BTC-USDT','px':'86500.5','sz':'0.25','side':'buy',
               'ts':str(int(self.reference_time.timestamp()*1000))}
        invalid=[{**valid,'tradeId':'missing','sz':None}, {**valid,'px':'-500'},
                 {**valid,'px':'NaN'}, {**valid,'sz':'Infinity'}, {**valid,'side':'unknown'},
                 {**valid,'ts':'0'}, {**valid,'instId':'ETH-USDT'}, {'tradeId':'incomplete'}]
        with patch.object(producer,'get_kafka_producer',side_effect=AssertionError('A real producer must not be created')):
            producer.on_message(ws,json.dumps({'data':[valid,*invalid]}))
        self.assertEqual(fake.send.call_count,1)
        call=fake.send.call_args
        self.assertEqual(call.args,(producer.TOPIC,))
        self.assertEqual(call.kwargs['partition'],0)
        self.assertEqual(call.kwargs['key'],'BTC-USDT')
        self.assertEqual(call.kwargs['value'],{'trade_id':'synthetic-1001','symbol':'BTC-USDT','price':86500.5,
                         'size':0.25,'side':'buy','timestamp':int(valid['ts'])})
        subscription=SimpleNamespace(send=MagicMock())
        producer.on_open(subscription)
        message=json.loads(subscription.send.call_args.args[0])
        self.assertEqual(message,{'op':'subscribe','args':[{'channel':'trades','instId':'BTC-USDT'}]})

    def test_02_features_are_valid_and_do_not_look_ahead(self):
        candles=canonical_candles(120,self.reference_time)
        features=compute_features(candles)
        self.assertTrue(np.isfinite(features[FEATURE_COLUMNS].to_numpy(dtype=float)).all())
        self.assertTrue(features.rsi_14.between(0,100).all())
        self.assertTrue((features.bb_upper>=features.bb_lower).all())
        self.assertAlmostEqual(features.sma_5.iloc[-1],sum(candles.close.iloc[-5:])/5)
        changed=candles.copy()
        changed.loc[80:,'close']*=2
        changed.loc[80:,'high']*=2
        after=compute_features(changed)
        np.testing.assert_allclose(features[FEATURE_COLUMNS].iloc[:80],after[FEATURE_COLUMNS].iloc[:80])

    def test_03_targets_match_exact_future_minutes_and_leave_unknowns(self):
        candles=canonical_candles(120,self.reference_time)
        labeled=add_future_direction_target(candles,horizon_minutes=5)
        self.assertTrue(labeled.target_direction.iloc[-5:].isna().all())
        self.assertTrue(labeled.target_direction.iloc[:-5].isin([0,1]).all())
        expected=(candles.close.iloc[5]-candles.close.iloc[0])/candles.close.iloc[0]
        self.assertAlmostEqual(labeled.future_return.iloc[0],expected)
        self.assertEqual(labeled.target_direction.iloc[0],int(expected>0))
        target_missing=candles.drop(index=30).reset_index(drop=True)
        broken=add_future_direction_target(target_missing,horizon_minutes=5)
        row=broken.loc[broken.window_start==candles.window_start.iloc[25]].iloc[0]
        self.assertTrue(np.isnan(row.target_direction))

    def test_04_chronological_splits_keep_labels_before_next_partition(self):
        split=create_chronological_splits(candles_df=canonical_candles(120,self.reference_time))
        train,val,test=(split[name] for name in ('train_df','val_df','test_df'))
        self.assertTrue(len(train)>len(val)>0)
        self.assertGreater(len(test),0)
        self.assertLess(train.window_start.max()+timedelta(minutes=5),val.window_start.min())
        self.assertLess(val.window_start.max()+timedelta(minutes=5),test.window_start.min())
        self.assertEqual(split['feature_names'],FEATURE_COLUMNS)

    def test_05_real_model_loads_with_compatible_features_and_probabilities(self):
        model,features=load_prediction_model()
        self.assertEqual(features,FEATURE_COLUMNS)
        values=np.zeros((1,len(features)))
        probabilities=model.predict_proba(values)[0]
        self.assertTrue(np.isfinite(probabilities).all())
        self.assertAlmostEqual(float(sum(probabilities)),1,places=4)
        self.assertIn(int(model.predict(values)[0]),(0,1))

    def test_06_inference_accepts_closed_canonical_fixture(self):
        candles=canonical_candles(120,self.reference_time)
        with prediction_clock(self.reference_time):
            result=run_single_prediction(candles)
        self.assertIsNotNone(result)
        self.assertEqual(result['symbol'],'BTC-USDT')
        self.assertIn(result['predicted_direction'],(0,1))
        self.assertGreaterEqual(result['probability_up'],0)
        self.assertLessEqual(result['probability_up'],1)
        self.assertAlmostEqual(result['probability_up']+result['probability_not_up'],1,places=4)
        self.assertEqual(result['feature_timestamp'],candles.window_start.iloc[-1])
        self.assertEqual(result['target_timestamp']-result['feature_timestamp'],timedelta(minutes=5))

    def test_07_real_postgres_persistence_and_query_latency(self):
        with prediction_clock(self.reference_time):
            result=run_single_prediction(canonical_candles(40,self.reference_time))
        self.assertIsNotNone(result)
        start=time.perf_counter()
        row=self.query("""SELECT
            (SELECT count(*) FROM warehouse.fact_btc_predictions),
            (SELECT count(*) FROM warehouse.fact_btc_model_evaluation),
            (SELECT count(*) FROM warehouse.fact_crypto_1m_candles)""")[0]
        latency_ms=(time.perf_counter()-start)*1000
        self.assertEqual(row,(1,0,0))
        stored=self.query('SELECT feature_timestamp,target_timestamp,actual_direction,is_correct FROM warehouse.fact_btc_predictions')[0]
        self.assertEqual(stored[:2],(result['feature_timestamp'],result['target_timestamp']))
        self.assertEqual(stored[2:],(None,None))
        self.assertLess(latency_ms,100,'Existing local query budget: 100ms')
        self.db.record['pipeline_query_ms']=round(latency_ms,3)


def run_all_tests():
    return unittest.TextTestRunner(verbosity=2).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(PipelineRegressionTests)).wasSuccessful()


if __name__=='__main__':
    sys.exit(0 if run_all_tests() else 1)
