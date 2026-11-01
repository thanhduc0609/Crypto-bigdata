"""
=============================================================================
Regression Test Suite: Order Flow 60s Sliding Window & Prediction Worker
=============================================================================
Directly verifies production components:
  1. RealtimeOrderflowTracker (web/orderflow_realtime.py)
  2. PredictionWorker (ml/prediction_worker.py)
All assertions run against real production code without overriding target logic.
=============================================================================
"""

import math
import time
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

from web.orderflow_realtime import RealtimeOrderflowTracker
from ml.prediction_worker import PredictionWorker
from ml.predict import RejectionCode


class TestOrderflowSlidingWindow(unittest.TestCase):
    """Verifies strict 60-second event time sliding window behavior."""

    def setUp(self):
        self.current_time = 1700000100.0  # Synthetic epoch reference
        self.tracker = RealtimeOrderflowTracker(
            symbol="BTC-USDT",
            window_seconds=60,
            time_provider=lambda: self.current_time
        )
        self.tracker.is_connected = True
        self.tracker.connected_since = datetime.fromtimestamp(self.current_time - 120.0, tz=timezone.utc)

    def _make_trade(self, trade_id: str, offset_seconds: float, size: float = 1.0, side: str = "buy", price: float = 85000.0):
        t_event = self.current_time - offset_seconds
        return {
            "symbol": "BTC-USDT",
            "trade_id": trade_id,
            "price": price,
            "size": size,
            "side": side,
            "event_time": datetime.fromtimestamp(t_event, tz=timezone.utc),
            "received_at": datetime.fromtimestamp(self.current_time, tz=timezone.utc)
        }

    def test_in_order_trades(self):
        """Trades arriving in chronological order are properly included."""
        t1 = self._make_trade("t1", offset_seconds=40.0, size=2.0, side="buy")
        t2 = self._make_trade("t2", offset_seconds=20.0, size=3.0, side="sell")
        self.tracker.add_trade(t1)
        self.tracker.add_trade(t2)

        m = self.tracker.get_orderflow_metrics()
        self.assertEqual(m["trade_count"], 2)
        self.assertAlmostEqual(m["buy_volume"], 2.0)
        self.assertAlmostEqual(m["sell_volume"], 3.0)
        self.assertAlmostEqual(m["total_volume"], 5.0)
        self.assertAlmostEqual(m["buy_ratio"], 0.4)
        self.assertAlmostEqual(m["sell_ratio"], 0.6)
        self.assertAlmostEqual(m["imbalance"], -0.2)
        self.assertEqual(m["state"], "active")

    def test_out_of_order_late_trade_discarded(self):
        """Late arrival trade older than (now - 60s] is discarded even if appended after fresh trade."""
        fresh = self._make_trade("fresh_1", offset_seconds=10.0, size=1.5, side="buy")
        expired_late = self._make_trade("expired_late", offset_seconds=85.0, size=10.0, side="sell")

        self.tracker.add_trade(fresh)
        self.tracker.add_trade(expired_late)

        m = self.tracker.get_orderflow_metrics()
        self.assertEqual(m["trade_count"], 1)
        self.assertAlmostEqual(m["buy_volume"], 1.5)
        self.assertAlmostEqual(m["sell_volume"], 0.0)
        self.assertEqual(m["state"], "active")

    def test_deduplication_by_trade_id(self):
        """Same trade_id pushed multiple times must be counted exactly once."""
        t1 = self._make_trade("dup_trade", offset_seconds=15.0, size=1.0, side="buy")
        t1_dup = self._make_trade("dup_trade", offset_seconds=15.0, size=1.0, side="buy")

        self.tracker.add_trade(t1)
        self.tracker.add_trade(t1_dup)

        m = self.tracker.get_orderflow_metrics()
        self.assertEqual(m["trade_count"], 1)
        self.assertAlmostEqual(m["total_volume"], 1.0)

    def test_future_trade_rejected(self):
        """Trade with event_time > now is rejected from window calculation."""
        future_trade = self._make_trade("future_1", offset_seconds=-10.0, size=5.0, side="buy")
        self.tracker.add_trade(future_trade)

        m = self.tracker.get_orderflow_metrics()
        self.assertEqual(m["trade_count"], 0)
        self.assertEqual(m["total_volume"], 0.0)

    def test_boundary_trade_conditions(self):
        """Trade at exact now - 60s is excluded; trade at now - 59.9s is included."""
        t_boundary_out = self._make_trade("boundary_out", offset_seconds=60.001, size=2.0)
        t_boundary_in = self._make_trade("boundary_in", offset_seconds=59.5, size=3.0)

        self.tracker.add_trade(t_boundary_out)
        self.tracker.add_trade(t_boundary_in)

        m = self.tracker.get_orderflow_metrics()
        self.assertEqual(m["trade_count"], 1)
        self.assertAlmostEqual(m["total_volume"], 3.0)

    def test_advancing_time_prunes_trades(self):
        """Without any new trades, advancing clock past 60s causes existing trades to expire."""
        t = self._make_trade("t_expire", offset_seconds=30.0, size=4.0)
        self.tracker.add_trade(t)

        m1 = self.tracker.get_orderflow_metrics()
        self.assertEqual(m1["trade_count"], 1)

        # Advance clock by 35 seconds (now offset is 65 seconds)
        self.current_time += 35.0
        m2 = self.tracker.get_orderflow_metrics()
        self.assertEqual(m2["trade_count"], 0)
        self.assertEqual(m2["total_volume"], 0.0)
        self.assertIsNone(m2["imbalance"])

    def test_invalid_price_and_size_rejected(self):
        """Trades with NaN, Infinity, negative or zero values are strictly filtered."""
        invalid_cases = [
            {"trade_id": "nan_p", "price": float("nan"), "size": 1.0},
            {"trade_id": "inf_p", "price": float("inf"), "size": 1.0},
            {"trade_id": "zero_p", "price": 0.0, "size": 1.0},
            {"trade_id": "neg_s", "price": 85000.0, "size": -2.0},
            {"trade_id": "nan_s", "price": 85000.0, "size": float("nan")}
        ]
        for c in invalid_cases:
            t = self._make_trade(c["trade_id"], offset_seconds=10.0, size=c["size"], price=c["price"])
            self.tracker.add_trade(t)

        m = self.tracker.get_orderflow_metrics()
        self.assertEqual(m["trade_count"], 0)

        # Valid trade still passes
        valid = self._make_trade("valid_1", offset_seconds=10.0, size=1.0, price=85000.0)
        self.tracker.add_trade(valid)
        m_valid = self.tracker.get_orderflow_metrics()
        self.assertEqual(m_valid["trade_count"], 1)

    def test_disconnected_and_stale_states(self):
        """Tracker reflects disconnected and stale states accurately."""
        self.tracker.is_connected = False
        m_disc = self.tracker.get_orderflow_metrics()
        self.assertEqual(m_disc["state"], "disconnected")

        self.tracker.is_connected = True
        self.tracker.is_reconnecting = True
        m_recon = self.tracker.get_orderflow_metrics()
        self.assertEqual(m_recon["state"], "reconnecting")

    def test_ratio_sum_approximately_one(self):
        """buy_ratio + sell_ratio equals 1.0 when volume > 0."""
        self.tracker.add_trade(self._make_trade("b1", offset_seconds=20.0, size=1.0, side="buy"))
        self.tracker.add_trade(self._make_trade("s1", offset_seconds=15.0, size=3.0, side="sell"))

        m = self.tracker.get_orderflow_metrics()
        self.assertAlmostEqual(m["buy_ratio"] + m["sell_ratio"], 1.0, places=4)
        self.assertAlmostEqual(m["imbalance"], (1.0 - 3.0) / 4.0, places=4)


class TestPredictionWorkerConcurrencyAndSafety(unittest.TestCase):
    """Verifies PredictionWorker concurrency locks, timedelta safety, and exception isolation."""

    def test_no_nameerror_on_perform_cycle(self):
        """perform_cycle must execute cleanly without NameError: timedelta."""
        worker = PredictionWorker(symbol="BTC-USDT", interval_seconds=60)
        with patch("ml.prediction_worker.run_prediction_detailed", return_value={
            "success": False,
            "code": "NO_DATA",
            "message": "No candle data available"
        }), patch("ml.prediction_worker.evaluate_unresolved_predictions", return_value=0):
            result = worker.perform_cycle()
            self.assertIn("status", result)
            self.assertEqual(result["status"], "rejected")
            self.assertEqual(worker.last_rejection_code, "NO_DATA")

    def test_cycle_lock_returns_busy_on_concurrent_call(self):
        """When worker cycle is currently executing, a concurrent call returns BUSY / ALREADY_RUNNING."""
        worker = PredictionWorker(symbol="BTC-USDT", interval_seconds=60)
        # Acquire the cycle lock manually to simulate active cycle
        acquired = worker._cycle_lock.acquire(blocking=False)
        self.assertTrue(acquired)
        try:
            res = worker.perform_cycle(blocking=False)
            self.assertEqual(res["status"], "busy")
            self.assertEqual(res["code"], "ALREADY_RUNNING")
        finally:
            worker._cycle_lock.release()

    def test_evaluation_error_does_not_fail_prediction(self):
        """If evaluate_unresolved_predictions raises an error, prediction cycle still completes."""
        worker = PredictionWorker(symbol="BTC-USDT", interval_seconds=60)
        now_dt = datetime.now(timezone.utc)
        pred_dict = {
            "prediction_id": 999,
            "symbol": "BTC-USDT",
            "predicted_direction": 1,
            "direction_label": "UP",
            "probability_up": 0.65,
            "current_price": 85000.0,
            "feature_timestamp": now_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "target_timestamp": (now_dt + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        with patch("ml.prediction_worker.run_prediction_detailed", return_value={
            "success": True,
            "already_predicted": False,
            "prediction": pred_dict
        }), patch("ml.prediction_worker.evaluate_unresolved_predictions", side_effect=RuntimeError("DW connection lost")):
            res = worker.perform_cycle()
            self.assertEqual(res["status"], "success")
            self.assertEqual(worker.total_predictions, 1)
            self.assertIn("Evaluation warning", worker.last_evaluation_status)

    def test_rejection_code_on_unclosed_candle(self):
        """Rejects unclosed candle with UNCLOSED_CANDLE rejection code."""
        worker = PredictionWorker(symbol="BTC-USDT", interval_seconds=60)
        with patch("ml.prediction_worker.run_prediction_detailed", return_value={
            "success": False,
            "code": "UNCLOSED_CANDLE",
            "message": "Latest candle is not finalized"
        }), patch("ml.prediction_worker.evaluate_unresolved_predictions", return_value=0):
            res = worker.perform_cycle()
            self.assertEqual(res["status"], "rejected")
            self.assertEqual(worker.last_rejection_code, "UNCLOSED_CANDLE")
            self.assertEqual(worker.state, "rejected")

    def test_already_predicted_candle_skipped(self):
        """Skips cleanly when candle was already predicted."""
        worker = PredictionWorker(symbol="BTC-USDT", interval_seconds=60)
        with patch("ml.prediction_worker.run_prediction_detailed", return_value={
            "success": True,
            "already_predicted": True,
            "message": "Already predicted"
        }), patch("ml.prediction_worker.evaluate_unresolved_predictions", return_value=0):
            res = worker.perform_cycle()
            self.assertEqual(res["status"], "success")
            self.assertEqual(worker.state, "waiting_new_candle")
            self.assertEqual(worker.total_predictions, 0)


if __name__ == "__main__":
    unittest.main()
