"""Synthetic causality, holdout boundaries, parity and snapshot safeguards."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
from ml.locked_dataset import load_locked_candles
from ml.short_term_features import feature_views,build_research_dataset
from ml.short_term_metrics import classification_metrics,baselines,paired_day_bootstrap
from ml.short_term_experiment import cohort,fit_candidate,predict_bundle,estimator,evaluate
from processing.features.feature_engineering import compute_features,COMMON_FEATURE_COLUMNS


def candles(n=500):
    rng=np.random.default_rng(19)
    close=100*np.exp(np.cumsum(rng.normal(0,.002,n)))
    volume=rng.uniform(0,10,n)
    stamps=pd.date_range("2020-01-01",periods=n,freq="min",tz="UTC")
    return pd.DataFrame(dict(window_start=stamps,window_end=stamps+pd.Timedelta(minutes=1),
        symbol="BTC-USDT",open=close*.999,high=close*1.002,low=close*.998,close=close,volume=volume,
        total_trade_value=volume*close,source="historical_rest",is_closed=True))


class FeaturesTests(unittest.TestCase):
    def test_legacy_matches_each_live_120_bar_window(self):
        raw=candles()
        legacy,_=feature_views(raw)
        for row in [119,120,159,300,499]:
            expected=compute_features(raw.iloc[row-119:row+1])[COMMON_FEATURE_COLUMNS].iloc[-1]
            np.testing.assert_allclose(legacy[COMMON_FEATURE_COLUMNS].iloc[row],expected,rtol=1e-8,atol=1e-7)

    def test_normalized_matches_each_live_120_bar_window(self):
        raw=candles()
        _,full=feature_views(raw)
        for row in [119,120,159,300,499]:
            _,single=feature_views(raw.iloc[row-119:row+1])
            np.testing.assert_allclose(full.iloc[row],single.iloc[-1],rtol=1e-6,atol=1e-7)

    def test_future_mutation_cannot_change_past_features(self):
        raw=candles();a,b=feature_views(raw)
        raw.loc[301:,["open","high","low","close","volume","total_trade_value"]]*=9
        c,d=feature_views(raw)
        np.testing.assert_allclose(a[COMMON_FEATURE_COLUMNS].iloc[119:301],c[COMMON_FEATURE_COLUMNS].iloc[119:301])
        np.testing.assert_allclose(b.iloc[119:301],d.iloc[119:301])

    def test_normalized_features_are_price_scale_invariant(self):
        raw=candles();_,a=feature_views(raw)
        raw[["open","high","low","close","total_trade_value"]]*=10
        _,b=feature_views(raw)
        np.testing.assert_allclose(a.iloc[119:],b.iloc[119:],rtol=1e-5,atol=2e-7)

    def test_flat_zero_volume_has_finite_neutral_rsi_without_fake_flow(self):
        raw=candles()
        raw[["open","high","low","close"]]=100.
        raw[["volume","total_trade_value"]]=0.
        _,features=feature_views(raw)
        self.assertTrue(np.isfinite(features.iloc[119:]).all().all())
        self.assertTrue(features.rsi_centered.iloc[119:].eq(0).all())
        self.assertFalse(any("buy" in c or "sell" in c for c in features))

    def test_missing_turnover_is_not_fabricated(self):
        raw=candles();raw.loc[250,"total_trade_value"]=np.nan
        _,features=feature_views(raw)
        self.assertTrue(pd.isna(features.observed_vwap_gap.iloc[250]))

    def test_shared_cohort_and_exact_label_maturity(self):
        raw=candles();frame,views=build_research_dataset(raw)
        self.assertEqual(len(frame),500-119-5)
        self.assertEqual(len(views["legacy"]),len(views["stationary"]))
        self.assertTrue((frame.label_available_at-frame.window_start).eq(pd.Timedelta(minutes=6)).all())
        expected=(raw.close.shift(-5)>raw.close).iloc[119:-5].astype(int).to_numpy()
        np.testing.assert_array_equal(frame.target_direction,expected)

    def test_reject_gaps_duplicates_and_unsorted(self):
        raw=candles()
        for wrong in [raw.drop(index=200),pd.concat([raw,raw.iloc[[100]]]),raw.iloc[::-1]]:
            with self.assertRaises(ValueError):build_research_dataset(wrong)

    def test_purging_requires_target_candle_to_have_closed(self):
        frame,_=build_research_dataset(candles())
        boundary=frame.window_start.iloc[200]
        ids=cohort(frame,end=boundary)
        self.assertLess(frame.label_available_at.iloc[ids].max(),boundary)
        self.assertEqual(frame.window_start.iloc[ids].max(),boundary-pd.Timedelta(minutes=7))


class MetricsTests(unittest.TestCase):
    def test_perfect_and_constant_classification(self):
        perfect=classification_metrics([0,0,1,1],[0,0,1,1],[.1,.2,.8,.9])
        self.assertEqual(perfect["balanced_accuracy"],1)
        self.assertEqual(perfect["confusion_matrix"],[[2,0],[0,2]])
        self.assertEqual(classification_metrics([0,0,0,1],[0,0,0,0])["balanced_accuracy"],.5)

    def test_baseline_frequency_is_fit_only(self):
        raw=compute_features(candles(4))
        pred,p=baselines([0,0,0,1],raw,["train_majority"])["train_majority"]
        self.assertTrue((pred==0).all());self.assertTrue((p==.25).all())

    def test_invalid_probabilities_rejected(self):
        for p in [[-1,.5],[.5,np.inf],[np.nan,.5],[1.1,.3]]:
            with self.assertRaises(ValueError):classification_metrics([0,1],[0,1],p)

    def test_paired_day_interval_deterministic(self):
        dates=pd.Series(pd.date_range("2020-01-01",periods=96,freq="h",tz="UTC"))
        y=np.tile([0,1],48)
        a=paired_day_bootstrap(y,y,1-y,dates,100,42)
        b=paired_day_bootstrap(y,y,1-y,dates,100,42)
        self.assertEqual(a,b);self.assertEqual(a["point"],1);self.assertEqual(a["lower_95"],1)

    def test_reject_too_few_days(self):
        with self.assertRaises(ValueError):paired_day_bootstrap([0,1],[0,1],[1,0],pd.Series(pd.date_range("2020",periods=2,tz="UTC")))


class SnapshotTests(unittest.TestCase):
    def fixture(self,root):
        frame=candles(10)
        file=root/"data/canonical_repairs/test/gold/candles_1m/symbol=BTC-USDT/canonical.parquet"
        file.parent.mkdir(parents=True);frame.to_parquet(file,index=False)
        descriptor=dict(local_gold_path="data/canonical_repairs/test/gold/candles_1m",rows=10,symbol="BTC-USDT",
            requested_start=frame.window_start.iloc[0].isoformat(),requested_end=frame.window_start.iloc[-1].isoformat(),
            gold_sha256=hashlib.sha256(file.read_bytes()).hexdigest())
        return file,descriptor

    def test_valid_local_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();_,d=self.fixture(root)
            with patch("ml.locked_dataset.ROOT",root),patch("ml.locked_dataset.load_descriptor",return_value=d):
                result,_=load_locked_candles();self.assertEqual(len(result),10)

    def test_corruption_never_falls_back(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();file,d=self.fixture(root);file.write_bytes(b"corrupted")
            with patch("ml.locked_dataset.ROOT",root),patch("ml.locked_dataset.load_descriptor",return_value=d):
                with self.assertRaisesRegex(ValueError,"checksum"):load_locked_candles(allow_remote=True)

    def test_escape_path_rejected(self):
        with patch("ml.locked_dataset.load_descriptor",return_value=dict(local_gold_path="../outside")):
            with self.assertRaisesRegex(ValueError,"escapes"):load_locked_candles()

    def test_missing_locked_file_does_not_use_mutable_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();file,d=self.fixture(root);file.unlink()
            with patch("ml.locked_dataset.ROOT",root),patch("ml.locked_dataset.load_descriptor",return_value=d):
                with self.assertRaises(FileNotFoundError):load_locked_candles()

    def test_dataset_does_not_suppress_active_snapshot_error(self):
        from ml.dataset import fetch_historical_btc_candles
        with patch("storage.canonical_snapshot.load_descriptor",return_value={"active":True}),\
             patch("ml.dataset.load_candles_from_lake",side_effect=ValueError("checksum")),\
             patch("ml.dataset.load_candles_from_postgres") as database:
            with self.assertRaisesRegex(ValueError,"checksum"):fetch_historical_btc_candles()
            database.assert_not_called()

    def test_active_snapshot_precedes_archive(self):
        from ml.dataset import load_candles_from_lake
        with patch("storage.canonical_snapshot.load_descriptor",return_value={"active":True}),\
             patch("ml.locked_dataset.load_locked_candles",return_value=(candles(10),{})) as loader:
            self.assertEqual(len(load_candles_from_lake()),10)
            loader.assert_called_once_with(allow_remote=True)

    def test_reject_wrong_source_unclosed_and_invalid_ohlc(self):
        for column,value in [("source","streaming"),("is_closed",False),("low",99999.)]:
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory);file,d=self.fixture(root)
                frame=pd.read_parquet(file);frame.loc[4,column]=value;frame.to_parquet(file,index=False)
                d["gold_sha256"]=hashlib.sha256(file.read_bytes()).hexdigest()
                with patch("ml.locked_dataset.ROOT",root),patch("ml.locked_dataset.load_descriptor",return_value=d):
                    with self.assertRaises(ValueError):load_locked_candles()


class FitTests(unittest.TestCase):
    def test_scaler_fit_excludes_future(self):
        frame,views=build_research_dataset(candles(600))
        boundary=frame.window_start.iloc[250]
        spec=dict(name="test",kind="logistic",features="stationary",C=.1)
        bundle,bounds=fit_candidate(spec,frame,views,boundary,dict(seed=42))
        ids=cohort(frame,end=boundary)
        np.testing.assert_allclose(bundle["estimator"].steps[0][1].mean_,views["stationary"].iloc[ids].mean(),atol=1e-10)
        self.assertLess(pd.Timestamp(bounds["fit"]["max_label_available_at"]),boundary)
        self.assertTrue(np.isfinite(predict_bundle(bundle,views["stationary"].iloc[-5:])).all())

    def test_histogram_model_disables_random_early_stopping(self):
        model=estimator(dict(kind="hist",trees=2,rate=.1,leaves=3,**{"lambda":1}),42)
        self.assertFalse(model.early_stopping)

    def test_platt_uses_separate_purged_calibration_window(self):
        frame,views=build_research_dataset(candles(1000))
        boundary=frame.window_start.iloc[600]
        spec=dict(name="test",kind="xgb",features="stationary",depth=2,trees=5,rate=.05,
                  min_child=1,calibration="platt",**{"lambda":1,"alpha":0})
        bundle,bounds=fit_candidate(spec,frame,views,boundary,dict(seed=42,calibration_days=.1))
        self.assertLess(pd.Timestamp(bounds["fit"]["max_label_available_at"]),pd.Timestamp(bounds["calibration"]["start"]))
        self.assertLess(pd.Timestamp(bounds["calibration"]["max_label_available_at"]),boundary)
        self.assertIsNotNone(bundle["calibrator"])
        self.assertTrue(np.isfinite(predict_bundle(bundle,views["stationary"].iloc[-5:])).all())

    def test_holdout_cannot_be_opened_twice(self):
        with tempfile.TemporaryDirectory() as directory:
            Path(directory,"holdout_access.json").write_text("{}")
            with patch("ml.short_term_experiment.load_data") as loader:
                with self.assertRaisesRegex(ValueError,"already opened"):evaluate(directory)
                loader.assert_not_called()

    def test_changed_source_blocks_holdout_before_loading_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name,value in [("protocol",{}),("selection",{"code_sha256":{}}),("provenance",{})]:
                (root/f"{name}.json").write_text(json.dumps(value))
            with patch("ml.short_term_experiment.load_data") as loader:
                with self.assertRaisesRegex(ValueError,"changed"):evaluate(root)
                loader.assert_not_called()

    def test_changed_candidate_blocks_holdout_before_loading_labels(self):
        from ml.short_term_experiment import fingerprints
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/"candidate.joblib").write_bytes(b"tampered")
            for name,value in [("protocol",{}),("selection",{"code_sha256":fingerprints(),"candidate_sha256":"0"*64}),("provenance",{})]:
                (root/f"{name}.json").write_text(json.dumps(value))
            with patch("ml.short_term_experiment.load_data") as loader:
                with self.assertRaisesRegex(ValueError,"changed"):evaluate(root)
                loader.assert_not_called()


if __name__=="__main__":unittest.main()
