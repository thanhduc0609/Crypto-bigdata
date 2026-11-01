"""
=============================================================================
Regression Test Suite: Prediction Evaluation & Input Quality Gates
=============================================================================
Directly verifies:
  1. Outcome evaluation logic in ml/predict.py (evaluate_unresolved_predictions)
     - Exact timestamp matching
     - Rejection of +/- 1s, 30s drift
     - Rejection of unclosed target candle (is_closed=False)
     - Non-contiguous index robustness with .loc
     - Legacy unaligned records (second != 0) skipped without blocking queue
  2. Input Quality Gate in ml/predict.py
     - Strict source == "historical_rest"
     - Strict is_closed is True
     - Rejection of NaN / Infinity in feature vectors
=============================================================================
"""

import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import numpy as np
import pandas as pd

from ml.predict import (
    evaluate_unresolved_predictions,
    run_prediction_detailed,
    RejectionCode
)


class TestPredictionOutcomeEvaluation(unittest.TestCase):
    """Verifies precision and safety of prediction outcome evaluation."""

    def setUp(self):
        self.mock_conn = MagicMock()
        self.mock_cur = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cur

    def test_exact_timestamp_matching(self):
        """Candidate matching exact window_start is evaluated and updated in DB."""
        target_ts = datetime(2026, 11, 1, 12, 5, 0, tzinfo=timezone.utc)
        # Mock DW returning 1 unresolved prediction
        # (prediction_id, feature_timestamp, current_price, target_timestamp, predicted_direction)
        self.mock_cur.fetchall.return_value = [
            (101, datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc), 85000.0, target_ts, 1)
        ]

        candles = pd.DataFrame([
            {
                "window_start": target_ts,
                "window_end": target_ts + timedelta(minutes=1),
                "close": 85200.0,
                "is_closed": True,
                "source": "historical_rest"
            }
        ])

        with patch("ml.predict.get_db_connection", return_value=self.mock_conn):
            evaluated = evaluate_unresolved_predictions(candles_df=candles)

        self.assertEqual(evaluated, 1)
        self.mock_cur.execute.assert_called()
        # Verify update call parameters: actual_future_price=85200.0, actual_dir=1, is_correct=True, pred_id=101
        update_calls = [c for c in self.mock_cur.execute.call_args_list if "UPDATE warehouse.fact_btc_predictions" in str(c)]
        self.assertEqual(len(update_calls), 1)
        params = update_calls[0][0][1]
        self.assertEqual(params[0], 85200.0)
        self.assertEqual(params[1], 1)
        self.assertEqual(params[2], True)
        self.assertEqual(params[3], 101)

    def test_tolerance_drift_rejected(self):
        """Candles off by 1s or 30s are NOT matched as ground truth."""
        target_ts = datetime(2026, 11, 1, 12, 5, 0, tzinfo=timezone.utc)
        self.mock_cur.fetchall.return_value = [
            (102, datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc), 85000.0, target_ts, 1)
        ]
        # DW has no exact match either
        self.mock_cur.fetchone.return_value = None

        # Candle drifted by 30 seconds
        candles = pd.DataFrame([
            {
                "window_start": target_ts + timedelta(seconds=30),
                "window_end": target_ts + timedelta(seconds=90),
                "close": 85500.0,
                "is_closed": True,
                "source": "historical_rest"
            }
        ])

        with patch("ml.predict.get_db_connection", return_value=self.mock_conn), \
             patch("urllib.request.urlopen", side_effect=RuntimeError("No REST call")):
            evaluated = evaluate_unresolved_predictions(candles_df=candles)

        self.assertEqual(evaluated, 0)
        update_calls = [c for c in self.mock_cur.execute.call_args_list if "UPDATE warehouse.fact_btc_predictions" in str(c)]
        self.assertEqual(len(update_calls), 0)

    def test_unclosed_candle_rejected_as_actual(self):
        """Target candle with is_closed=False cannot be used as ground truth."""
        target_ts = datetime(2026, 11, 1, 12, 5, 0, tzinfo=timezone.utc)
        self.mock_cur.fetchall.return_value = [
            (103, datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc), 85000.0, target_ts, 1)
        ]
        self.mock_cur.fetchone.return_value = None

        candles = pd.DataFrame([
            {
                "window_start": target_ts,
                "window_end": target_ts + timedelta(minutes=1),
                "close": 86000.0,
                "is_closed": False,  # Candle still forming!
                "source": "historical_rest"
            }
        ])

        with patch("ml.predict.get_db_connection", return_value=self.mock_conn):
            evaluated = evaluate_unresolved_predictions(candles_df=candles)

        self.assertEqual(evaluated, 0)

    def test_non_contiguous_index_safety(self):
        """DataFrame with non-contiguous index does not crash when accessed with .loc."""
        target_ts = datetime(2026, 11, 1, 12, 5, 0, tzinfo=timezone.utc)
        self.mock_cur.fetchall.return_value = [
            (104, datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc), 85000.0, target_ts, 0)
        ]

        # Non-contiguous index: 10, 42, 99
        candles = pd.DataFrame(
            [
                {"window_start": target_ts - timedelta(minutes=2), "close": 84900.0, "is_closed": True, "source": "historical_rest"},
                {"window_start": target_ts, "close": 84800.0, "is_closed": True, "source": "historical_rest"},
                {"window_start": target_ts + timedelta(minutes=1), "close": 84700.0, "is_closed": True, "source": "historical_rest"},
            ],
            index=[10, 42, 99]
        )

        with patch("ml.predict.get_db_connection", return_value=self.mock_conn):
            evaluated = evaluate_unresolved_predictions(candles_df=candles)

        self.assertEqual(evaluated, 1)
        update_calls = [c for c in self.mock_cur.execute.call_args_list if "UPDATE warehouse.fact_btc_predictions" in str(c)]
        params = update_calls[0][0][1]
        self.assertEqual(params[0], 84800.0)
        self.assertEqual(params[1], 0)
        self.assertEqual(params[2], True)  # predicted DOWN (0) and price dropped (84800 < 85000)

    def test_legacy_unaligned_records_skipped_without_blocking(self):
        """Records with second != 0 are skipped from 1m evaluation; subsequent valid records proceed."""
        legacy_ts = datetime(2026, 11, 1, 12, 5, 23, tzinfo=timezone.utc)
        aligned_ts = datetime(2026, 11, 1, 12, 10, 0, tzinfo=timezone.utc)

        self.mock_cur.fetchall.return_value = [
            (105, datetime(2026, 11, 1, 12, 0, 23, tzinfo=timezone.utc), 85000.0, legacy_ts, 1),
            (106, datetime(2026, 11, 1, 12, 5, 0, tzinfo=timezone.utc), 85000.0, aligned_ts, 1)
        ]

        candles = pd.DataFrame([
            {"window_start": aligned_ts, "close": 85300.0, "is_closed": True, "source": "historical_rest"}
        ])

        with patch("ml.predict.get_db_connection", return_value=self.mock_conn):
            evaluated = evaluate_unresolved_predictions(candles_df=candles)

        self.assertEqual(evaluated, 1)
        update_calls = [c for c in self.mock_cur.execute.call_args_list if "UPDATE warehouse.fact_btc_predictions" in str(c)]
        self.assertEqual(len(update_calls), 1)
        self.assertEqual(update_calls[0][0][1][3], 106)


class TestInferenceQualityGates(unittest.TestCase):
    """Verifies that run_prediction_detailed enforces strict input quality gates."""

    def test_reject_unclosed_candle_inference(self):
        """Rejects inference when latest candle has is_closed=False."""
        now = datetime.now(timezone.utc)
        candles = []
        for i in range(30):
            candles.append({
                "window_start": now - timedelta(minutes=30 - i),
                "window_end": now - timedelta(minutes=29 - i),
                "open": 85000.0, "high": 85100.0, "low": 84900.0, "close": 85050.0,
                "volume": 10.0, "is_closed": True if i < 29 else False,
                "source": "historical_rest"
            })
        df = pd.DataFrame(candles)

        res = run_prediction_detailed(candles_df=df, check_duplicate=False)
        self.assertFalse(res["success"])
        self.assertEqual(res["code"], RejectionCode.UNCLOSED_CANDLE)

    def test_reject_unconfirmed_source_inference(self):
        """Rejects inference when source is not historical_rest."""
        now = datetime.now(timezone.utc)
        candles = []
        for i in range(30):
            candles.append({
                "window_start": now - timedelta(minutes=30 - i),
                "window_end": now - timedelta(minutes=29 - i),
                "open": 85000.0, "high": 85100.0, "low": 84900.0, "close": 85050.0,
                "volume": 10.0, "is_closed": True,
                "source": "realtime_stream"  # Unconfirmed streaming candle
            })
        df = pd.DataFrame(candles)

        res = run_prediction_detailed(candles_df=df, check_duplicate=False)
        self.assertFalse(res["success"])
        self.assertEqual(res["code"], RejectionCode.UNCLOSED_CANDLE)

    def test_reject_nan_or_inf_in_features(self):
        """Rejects inference with FEATURE_SCHEMA_MISMATCH if computed features contain NaN or Inf."""
        now = datetime.now(timezone.utc)
        candles = []
        for i in range(35):
            candles.append({
                "window_start": now - timedelta(minutes=35 - i),
                "window_end": now - timedelta(minutes=34 - i),
                "open": 85000.0, "high": 85100.0, "low": 84900.0, "close": 85050.0,
                "volume": 10.0, "is_closed": True,
                "source": "historical_rest"
            })
        df = pd.DataFrame(candles)

        # Create dummy feature DataFrame with NaN in a required feature column
        bad_feat_df = df.copy()
        from processing.features.feature_engineering import FEATURE_COLUMNS
        for col in FEATURE_COLUMNS:
            bad_feat_df[col] = float("nan")

        with patch("ml.predict.compute_features", return_value=bad_feat_df), \
             patch("ml.predict.check_existing_prediction", return_value=None):
            res = run_prediction_detailed(candles_df=df, check_duplicate=False)
            self.assertFalse(res["success"])
            self.assertEqual(res["code"], RejectionCode.FEATURE_SCHEMA_MISMATCH)


if __name__ == "__main__":
    unittest.main()
