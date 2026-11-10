"""Causality and full-horizon diagnostics for retrospective nested research."""
import json
from pathlib import Path
import unittest
import numpy as np
import pandas as pd
from ml.long_term_research import research,predict_log,origins,interval_metrics


class NestedResearchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.protocol=json.loads((Path(__file__).resolve().parents[1]/"configs/long_term_research.json").read_text())
        cls.protocol["simulations"]=100
        t=np.arange(105)
        cls.monthly=pd.DataFrame(dict(month=pd.period_range("2018-02",periods=105,freq="M").astype(str),close=np.exp(8+.01*t+.2*np.sin(t/6))))
        cls.result=research(cls.monthly,cls.protocol)

    def test_inner_validation_ends_before_outer_future(self):
        for fold in self.result["outer_folds"]:
            for model in fold["validation"]:
                self.assertTrue(all(v["validation_end_exclusive"]<=fold["origin"] for v in model["folds"]))

    def test_exact_full_horizon_and_three_year_breakdown(self):
        self.assertEqual(len(self.result["outer_predictions"]),108)
        self.assertTrue(all(len(f["by_year"])==3 for f in self.result["outer_folds"]))
        self.assertEqual(len(self.result["research_forecast"]),36)

    def test_changed_outer_future_does_not_change_that_fold_selection(self):
        changed=self.monthly.copy();changed.loc[60:,"close"]*=3
        result=research(changed,self.protocol)
        a=self.result["outer_folds"][0];b=result["outer_folds"][0]
        self.assertEqual(a["selected_model"],b["selected_model"]);self.assertEqual(a["validation"],b["validation"])
        old=self.result["outer_predictions"][:36];new=result["outer_predictions"][:36]
        np.testing.assert_array_equal([r["predicted"] for r in old],[r["predicted"] for r in new])
        self.assertNotEqual(a["selected"]["mae_usdt"],b["selected"]["mae_usdt"])

    def test_explicit_prior_exposure_and_overlap(self):
        self.assertFalse(self.result["independent_holdout"]);self.assertTrue(self.result["outer_windows_overlap"])

    def test_regularized_forecasts_are_finite_positive(self):
        for model in self.protocol["candidates"]:
            value=np.exp(predict_log(self.monthly.close.to_numpy()[:60],36,model))
            self.assertTrue(np.isfinite(value).all() and (value>0).all())

    def test_interval_score_penalizes_misses_and_width(self):
        result=interval_metrics(np.array([10.,100.]),np.array([0.,0.]),np.array([20.,20.]),.8)
        self.assertEqual(result["coverage"],.5);self.assertGreater(result["interval_score_usdt"],result["mean_width_usdt"])

    def test_missing_month_rejected(self):
        with self.assertRaises(ValueError):research(self.monthly.drop(index=40),self.protocol)

    def test_insufficient_full_horizon_rejected(self):
        with self.assertRaises(ValueError):origins(59)


if __name__=="__main__":unittest.main()
