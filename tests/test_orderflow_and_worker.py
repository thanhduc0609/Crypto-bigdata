"""
=============================================================================
Unit & Integration Tests: Realtime Order Flow, Prediction Worker & VN Time
=============================================================================
"""

import unittest
from datetime import datetime, timezone, timedelta
from web.orderflow_realtime import RealtimeOrderflowTracker
from ml.prediction_worker import PredictionWorker
from ml.predict import RejectionCode, check_existing_prediction


class TestOrderflowRealtime(unittest.TestCase):
    def setUp(self):
        self.tracker = RealtimeOrderflowTracker(symbol="BTC-USDT", window_seconds=60)
        self.tracker.is_connected = True
        self.tracker.connected_since = datetime.now(timezone.utc) - timedelta(seconds=70)

    def test_empty_window_yields_none_imbalance(self):
        """Khi không có giao dịch trong 60s, imbalance và tỷ lệ phải là None, không fake 50/50"""
        metrics = self.tracker.get_orderflow_metrics()
        self.assertEqual(metrics["buy_volume"], 0.0)
        self.assertEqual(metrics["sell_volume"], 0.0)
        self.assertEqual(metrics["total_volume"], 0.0)
        self.assertIsNone(metrics["buy_ratio"])
        self.assertIsNone(metrics["sell_ratio"])
        self.assertIsNone(metrics["imbalance"])
        self.assertEqual(metrics["state"], "no_trades")

    def test_warming_up_state(self):
        """Khi kết nối chưa đủ 60s, state là warming_up"""
        self.tracker.connected_since = datetime.now(timezone.utc) - timedelta(seconds=20)
        metrics = self.tracker.get_orderflow_metrics()
        self.assertEqual(metrics["state"], "warming_up")

    def test_disconnected_state(self):
        """Khi ngắt kết nối, state là disconnected"""
        self.tracker.is_connected = False
        metrics = self.tracker.get_orderflow_metrics()
        self.assertEqual(metrics["state"], "disconnected")

    def test_deduplication_and_sliding_window(self):
        """Khử trùng theo trade_id và loại bỏ giao dịch cũ hơn 60s"""
        now = datetime.now(timezone.utc)
        
        # Thêm giao dịch cũ (70s trước) -> phải bị loại bỏ
        old_trade = {
            "symbol": "BTC-USDT",
            "trade_id": "old_1",
            "price": 60000.0,
            "size": 1.0,
            "side": "buy",
            "event_time": now - timedelta(seconds=70),
            "received_at": now - timedelta(seconds=70)
        }
        
        # Thêm 2 giao dịch hợp lệ
        trade_1 = {
            "symbol": "BTC-USDT",
            "trade_id": "trade_1",
            "price": 60000.0,
            "size": 2.0,
            "side": "buy",
            "event_time": now - timedelta(seconds=30),
            "received_at": now - timedelta(seconds=30)
        }
        trade_2 = {
            "symbol": "BTC-USDT",
            "trade_id": "trade_2",
            "price": 60000.0,
            "size": 1.0,
            "side": "sell",
            "event_time": now - timedelta(seconds=10),
            "received_at": now - timedelta(seconds=10)
        }
        # Thêm trùng trade_1
        trade_1_dup = dict(trade_1)

        self.tracker.trades.extend([old_trade, trade_1, trade_2, trade_1_dup])
        self.tracker.last_trade_event_at = now - timedelta(seconds=10)

        metrics = self.tracker.get_orderflow_metrics()
        self.assertEqual(metrics["state"], "active")
        self.assertEqual(metrics["trade_count"], 2)
        self.assertAlmostEqual(metrics["buy_volume"], 2.0, places=4)
        self.assertAlmostEqual(metrics["sell_volume"], 1.0, places=4)
        self.assertAlmostEqual(metrics["total_volume"], 3.0, places=4)
        self.assertAlmostEqual(metrics["buy_ratio"], 2.0 / 3.0, places=4)
        self.assertAlmostEqual(metrics["sell_ratio"], 1.0 / 3.0, places=4)
        self.assertAlmostEqual(metrics["imbalance"], (2.0 - 1.0) / 3.0, places=4)


class TestPredictionWorkerAndRejection(unittest.TestCase):
    def test_rejection_codes_exist(self):
        """Xác nhận đầy đủ các mã từ chối có cấu trúc"""
        expected_codes = [
            "NO_DATA", "INSUFFICIENT_HISTORY", "STALE_DATA",
            "MISSING_CANDLES", "UNCLOSED_CANDLE", "INVALID_OHLCV",
            "FEATURE_SCHEMA_MISMATCH", "MODEL_UNAVAILABLE",
            "TARGET_EXPIRED", "DATABASE_ERROR"
        ]
        for code in expected_codes:
            self.assertTrue(hasattr(RejectionCode, code), f"Thiếu mã {code}")

    def test_worker_status_structure(self):
        """Worker trả về đầy đủ các trường trạng thái telemetry"""
        worker = PredictionWorker(interval_seconds=15)
        st = worker.get_status()
        self.assertIn("state", st)
        self.assertIn("interval_seconds", st)
        self.assertIn("total_predictions", st)
        self.assertIn("total_evaluations", st)
        self.assertIn("last_rejection_code", st)


class TestVietnamTimezoneFormatting(unittest.TestCase):
    def test_vn_time_conversion(self):
        """Xác nhận múi giờ Việt Nam luôn là UTC+7 không đổi"""
        utc_dt = datetime(2026, 11, 1, 6, 0, 0, tzinfo=timezone.utc)
        vn_tz = timezone(timedelta(hours=7))
        vn_dt = utc_dt.astimezone(vn_tz)
        self.assertEqual(vn_dt.hour, 13)
        self.assertEqual(vn_dt.day, 1)
        self.assertEqual(vn_dt.strftime("%Y-%m-%d %H:%M:%S"), "2026-11-01 13:00:00")


if __name__ == "__main__":
    unittest.main()

