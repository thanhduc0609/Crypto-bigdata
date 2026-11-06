"""
=============================================================================
Unit Test: Futures Plan Service Rules & Integrity Checks
=============================================================================
"""

import unittest
from datetime import datetime, timezone, timedelta
from web.services.futures_plan_service import (
    calculate_15m_atr,
    generate_futures_plan
)


class TestFuturesPlanService(unittest.TestCase):
    def setUp(self):
        self.now_utc = datetime.now(timezone.utc)
        self.valid_future_ts = (self.now_utc + timedelta(minutes=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.expired_ts = (self.now_utc - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def test_calculate_15m_atr(self):
        # 5 sample candles: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
        candles = [
            ["1004", "100", "105", "95", "102", "10", "10", "10", "1"],
            ["1003", "98", "103", "97", "100", "10", "10", "10", "1"],
            ["1002", "95", "99", "94", "98", "10", "10", "10", "1"],
            ["1001", "92", "96", "91", "95", "10", "10", "10", "1"],
            ["1000", "90", "93", "89", "92", "10", "10", "10", "1"],
        ]
        atr = calculate_15m_atr(candles, period=5)
        self.assertIsNotNone(atr)
        self.assertGreater(atr, 0)

        # Empty or single candle returns None
        self.assertIsNone(calculate_15m_atr([]))
        self.assertIsNone(calculate_15m_atr([["1000", "90", "93", "89", "92", "10", "10", "10", "1"]]))

    def test_long_signal_generation(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0,
                "model_version": "2.0.0"
            }
        }
        of_data = {
            "status": "active",
            "imbalance": 0.25,
            "buy_ratio": 0.625,
            "sell_ratio": 0.375,
            "trade_count": 80
        }
        futures_data = {
            "futures_price": 80050.0,
            "atr_15m": 120.0
        }

        plan = generate_futures_plan(pred_data, of_data, futures_data)
        self.assertEqual(plan["action"], "LONG")
        self.assertEqual(plan["instrument"], "BTC-USDT-SWAP")
        self.assertEqual(plan["entry_price"], 80050.0)
        self.assertIsNotNone(plan["stop_loss"])
        self.assertIsNotNone(plan["take_profit"])

        # Strict bounds: SL < Entry < TP
        self.assertLess(plan["stop_loss"], plan["entry_price"])
        self.assertGreater(plan["take_profit"], plan["entry_price"])
        self.assertEqual(plan["risk_reward_ratio"], "1:1.5")
        self.assertEqual(plan["market_data"]["basis_usd"], 50.0)

    def test_short_signal_generation(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.30,
                "probability_not_up": 0.70,
                "direction_label": "NOT_UP",
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0,
                "model_version": "2.0.0"
            }
        }
        of_data = {
            "status": "active",
            "imbalance": -0.35,  # Strong sell pressure <= -0.15
            "buy_ratio": 0.325,
            "sell_ratio": 0.675,
            "trade_count": 95
        }
        futures_data = {
            "futures_price": 79980.0,
            "atr_15m": 150.0
        }

        plan = generate_futures_plan(pred_data, of_data, futures_data)
        self.assertEqual(plan["action"], "SHORT")
        self.assertEqual(plan["instrument"], "BTC-USDT-SWAP")
        self.assertEqual(plan["entry_price"], 79980.0)

        # Strict bounds: TP < Entry < SL
        self.assertLess(plan["take_profit"], plan["entry_price"])
        self.assertGreater(plan["stop_loss"], plan["entry_price"])
        self.assertEqual(plan["risk_reward_ratio"], "1:1.5")

    def test_not_up_does_not_blindly_trigger_short(self):
        # NOT_UP with mild or positive imbalance should yield WAIT (sideways/no selling conviction)
        pred_data = {
            "prediction": {
                "probability_up": 0.40,
                "probability_not_up": 0.60,
                "direction_label": "NOT_UP",
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0
            }
        }
        of_data = {
            "status": "active",
            "imbalance": -0.05,  # Mild imbalance, > -0.15
            "buy_ratio": 0.475,
            "sell_ratio": 0.525
        }
        futures_data = {
            "futures_price": 80000.0,
            "atr_15m": 100.0
        }

        plan = generate_futures_plan(pred_data, of_data, futures_data)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertIn("đi ngang", plan["reason"].lower())

    def test_conflicting_signals_yields_wait(self):
        # UP prediction + Negative imbalance
        pred_data = {
            "prediction": {
                "probability_up": 0.65,
                "probability_not_up": 0.35,
                "direction_label": "UP",
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0
            }
        }
        of_data = {
            "status": "active",
            "imbalance": -0.20,
            "buy_ratio": 0.40,
            "sell_ratio": 0.60
        }
        futures_data = {
            "futures_price": 80000.0,
            "atr_15m": 100.0
        }

        plan = generate_futures_plan(pred_data, of_data, futures_data)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertIn("mâu thuẫn", plan["reason"].lower())

    def test_uncertain_probability_yields_wait(self):
        # Close to 50% (unseparated classes)
        pred_data = {
            "prediction": {
                "probability_up": 0.505,
                "probability_not_up": 0.495,
                "direction_label": "UP",
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0
            }
        }
        of_data = {"status": "active", "imbalance": 0.20}
        futures_data = {"futures_price": 80000.0, "atr_15m": 100.0}

        plan = generate_futures_plan(pred_data, of_data, futures_data)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertIn("chưa phân tách rõ", plan["reason"].lower())

    def test_expired_prediction_yields_wait(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.70,
                "probability_not_up": 0.30,
                "direction_label": "UP",
                "target_timestamp": self.expired_ts,
                "current_price": 80000.0
            }
        }
        of_data = {"status": "active", "imbalance": 0.30}
        futures_data = {"futures_price": 80000.0, "atr_15m": 100.0}

        plan = generate_futures_plan(pred_data, of_data, futures_data)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertIn("hết hiệu lực", plan["reason"].lower())

    def test_missing_data_yields_wait(self):
        # Missing futures price
        plan = generate_futures_plan(None, None, {"futures_price": None, "atr_15m": 100.0})
        self.assertEqual(plan["action"], "CHỜ")
        self.assertIsNone(plan["entry_price"])


if __name__ == "__main__":
    unittest.main()

