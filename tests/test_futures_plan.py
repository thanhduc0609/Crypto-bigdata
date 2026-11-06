"""
=============================================================================
Unit Test: Futures Plan Service Rules, ATR & Data Integrity Checks
=============================================================================
"""

import json
import unittest
from datetime import datetime, timezone, timedelta
from typing import List, Any

from web.services.futures_plan_service import (
    calculate_15m_atr,
    calculate_15m_atr_detailed,
    generate_futures_plan,
    FUTURES_TICKER_MAX_STALENESS_SECONDS,
    FUTURES_TICKER_FUTURE_TOLERANCE_SECONDS,
    PREDICTION_PROB_SUM_TOLERANCE
)


def create_15m_candle(
    start_dt: datetime,
    open_p: float,
    high_p: float,
    low_p: float,
    close_p: float,
    confirm: str = "1"
) -> List[Any]:
    """Tạo 1 nến 15m OKX tiêu chuẩn: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]"""
    ts_ms = str(int(start_dt.timestamp() * 1000))
    return [
        ts_ms,
        str(open_p),
        str(high_p),
        str(low_p),
        str(close_p),
        "10.0",
        "10.0",
        "750000.0",
        str(confirm)
    ]


def build_valid_candles_series(
    end_close_dt: datetime,
    count: int = 15,
    base_price: float = 75000.0,
    confirm: str = "1"
) -> List[List[Any]]:
    """
    Tạo chuỗi nến 15m liên tục, đóng nến gần nhất tại end_close_dt.
    OKX trả về danh sách sắp xếp từ mới nhất về cũ nhất.
    """
    candles = []
    # Nến mới nhất bắt đầu lúc (end_close_dt - 15m)
    for i in range(count):
        start_dt = end_close_dt - timedelta(minutes=(i + 1) * 15)
        p = base_price + (i * 10.0)
        c = create_15m_candle(
            start_dt=start_dt,
            open_p=p,
            high_p=p + 50.0,
            low_p=p - 50.0,
            close_p=p + 10.0,
            confirm=confirm
        )
        candles.append(c)
    return candles


class TestFuturesPlanService(unittest.TestCase):
    def setUp(self):
        # Mốc thời gian cố định phục vụ kiểm thử xác định
        self.now_utc = datetime(2026, 11, 6, 10, 0, 0, tzinfo=timezone.utc)
        self.valid_feature_ts = (self.now_utc - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.valid_future_ts = (self.now_utc + timedelta(minutes=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.expired_ts = (self.now_utc - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.valid_ticker_ts = (self.now_utc - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # =========================================================================
    # 1. KIỂM THỬ TÍNH ATR 15M (B. SỬA TÍNH ATR)
    # =========================================================================

    def test_calculate_15m_atr_valid_series(self):
        # 16 nến 15m hợp lệ đóng nến gần nhất lúc now_utc (cho period=14 cần ít nhất 15 nến)
        candles = build_valid_candles_series(end_close_dt=self.now_utc, count=16)
        atr = calculate_15m_atr(candles, period=14, now_utc=self.now_utc)
        self.assertIsNotNone(atr)
        self.assertGreater(atr, 0.0)

    def test_calculate_15m_atr_hand_calculable(self):
        """
        Kiểm tra độ chính xác của ATR bằng kết quả tính tay:
        Period = 2, cần 3 nến liên tục:
          Nến 0: O=100, H=105, L=95,  C=100
          Nến 1: O=100, H=110, L=98,  C=105 -> TR1 = max(110-98, |110-100|, |98-100|) = max(12, 10, 2) = 12
          Nến 2: O=105, H=108, L=102, C=106 -> TR2 = max(108-102, |108-105|, |102-105|) = max(6, 3, 3) = 6
        ATR = (12 + 6) / 2 = 9.00
        """
        c0_time = self.now_utc - timedelta(minutes=45)
        c1_time = self.now_utc - timedelta(minutes=30)
        c2_time = self.now_utc - timedelta(minutes=15)

        # OKX format (mới nhất trước, cũ nhất sau)
        candles = [
            create_15m_candle(c2_time, 105.0, 108.0, 102.0, 106.0, confirm="1"),
            create_15m_candle(c1_time, 100.0, 110.0, 98.0, 105.0, confirm="1"),
            create_15m_candle(c0_time, 100.0, 105.0, 95.0, 100.0, confirm="1"),
        ]
        atr = calculate_15m_atr(candles, period=2, now_utc=self.now_utc)
        self.assertEqual(atr, 9.0)

    def test_calculate_15m_atr_rejects_all_unclosed_candles(self):
        # 20 nến đều có confirm='0' -> phải trả None, không có fallback nào vượt qua
        candles = build_valid_candles_series(end_close_dt=self.now_utc, count=20, confirm="0")
        atr, reason = calculate_15m_atr_detailed(candles, period=14, now_utc=self.now_utc)
        self.assertIsNone(atr)
        self.assertIn("Không đủ nến đóng xác nhận", reason)

    def test_calculate_15m_atr_rejects_missing_confirm(self):
        candles = build_valid_candles_series(end_close_dt=self.now_utc, count=20)
        for c in candles:
            c[8] = ""  # Thiếu confirm
        atr = calculate_15m_atr(candles, period=14, now_utc=self.now_utc)
        self.assertIsNone(atr)

    def test_calculate_15m_atr_insufficient_candles(self):
        # Với period=14 cần ít nhất 15 nến đóng, 14 nến phải bị từ chối
        candles = build_valid_candles_series(end_close_dt=self.now_utc, count=14)
        atr, reason = calculate_15m_atr_detailed(candles, period=14, now_utc=self.now_utc)
        self.assertIsNone(atr)
        self.assertIn("Không đủ nến đóng", reason)

    def test_calculate_15m_atr_duplicate_timestamp(self):
        candles = build_valid_candles_series(end_close_dt=self.now_utc, count=16)
        # Làm trùng timestamp giữa 2 nến
        candles[1][0] = candles[0][0]
        atr, reason = calculate_15m_atr_detailed(candles, period=14, now_utc=self.now_utc)
        self.assertIsNone(atr)
        self.assertIn("trùng lặp", reason.lower())

    def test_calculate_15m_atr_timestamp_gap(self):
        candles = build_valid_candles_series(end_close_dt=self.now_utc, count=16)
        # Xóa 1 nến ở giữa để tạo khoảng thiếu 30 phút (900000ms != 1800000ms)
        del candles[5]
        atr, reason = calculate_15m_atr_detailed(candles, period=14, now_utc=self.now_utc)
        self.assertIsNone(atr)
        self.assertIn("khoảng trống", reason.lower())

    def test_calculate_15m_atr_stale_candles(self):
        # Nến đóng từ 2 tiếng trước (vượt ATR_15M_MAX_STALENESS_SECONDS = 30m)
        old_time = self.now_utc - timedelta(hours=2)
        candles = build_valid_candles_series(end_close_dt=old_time, count=16)
        atr, reason = calculate_15m_atr_detailed(candles, period=14, now_utc=self.now_utc)
        self.assertIsNone(atr)
        self.assertIn("quá cũ", reason.lower())

    def test_calculate_15m_atr_future_candles(self):
        # Nến đóng ở tương lai + 10 phút (vượt tolerance 60s)
        future_time = self.now_utc + timedelta(minutes=10)
        candles = build_valid_candles_series(end_close_dt=future_time, count=16)
        atr, reason = calculate_15m_atr_detailed(candles, period=14, now_utc=self.now_utc)
        self.assertIsNone(atr)
        self.assertIn("tương lai", reason.lower())

    def test_calculate_15m_atr_invalid_ohlc_geometry(self):
        # 15 nến: làm hỏng 1 nến với Low > Open khiến số nến hợp lệ còn 14 (< 15) -> bị loại
        candles = build_valid_candles_series(end_close_dt=self.now_utc, count=15)
        candles[0][3] = "999999.0"
        atr = calculate_15m_atr(candles, period=14, now_utc=self.now_utc)
        self.assertIsNone(atr)

        # Toàn bộ nến có High < Low -> trả None
        bad_candles = build_valid_candles_series(end_close_dt=self.now_utc, count=15)
        for c in bad_candles:
            c[2] = "50000.0"
            c[3] = "90000.0"
        self.assertIsNone(calculate_15m_atr(bad_candles, period=14, now_utc=self.now_utc))

    # =========================================================================
    # 2. KIỂM THỬ TÍN HIỆU HỢP LỆ VÀ GIỚI HẠN GIÁ (A. LONG / SHORT)
    # =========================================================================

    def test_long_signal_generation_valid(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0,
                "model_version": "2.0.0"
            }
        }
        of_data = {
            "status": "active",
            "is_connected": True,
            "imbalance": 0.25,
            "buy_ratio": 0.625,
            "sell_ratio": 0.375,
            "trade_count": 80
        }
        futures_data = {
            "futures_price": 80050.0,
            "ticker_timestamp": self.valid_ticker_ts,
            "atr_15m": 120.0
        }

        plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
        self.assertEqual(plan["action"], "LONG")
        self.assertEqual(plan["reason_code"], "SUCCESS_LONG")
        self.assertEqual(plan["instrument"], "BTC-USDT-SWAP")
        self.assertEqual(plan["entry_price"], 80050.0)
        self.assertIsNotNone(plan["stop_loss"])
        self.assertIsNotNone(plan["take_profit"])

        # Strict bounds: SL < Entry < TP
        self.assertLess(plan["stop_loss"], plan["entry_price"])
        self.assertGreater(plan["take_profit"], plan["entry_price"])
        self.assertEqual(plan["risk_reward_ratio"], "1:1.5")
        self.assertEqual(plan["market_data"]["basis_usd"], 50.0)

    def test_short_signal_generation_valid(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.30,
                "probability_not_up": 0.70,
                "direction_label": "NOT_UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0,
                "model_version": "2.0.0"
            }
        }
        of_data = {
            "status": "active",
            "is_connected": True,
            "imbalance": -0.35,  # Strong sell pressure <= -0.15
            "buy_ratio": 0.325,
            "sell_ratio": 0.675,
            "trade_count": 95
        }
        futures_data = {
            "futures_price": 79980.0,
            "ticker_timestamp": self.valid_ticker_ts,
            "atr_15m": 150.0
        }

        plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
        self.assertEqual(plan["action"], "SHORT")
        self.assertEqual(plan["reason_code"], "SUCCESS_SHORT")
        self.assertEqual(plan["instrument"], "BTC-USDT-SWAP")
        self.assertEqual(plan["entry_price"], 79980.0)

        # Strict bounds: TP < Entry < SL
        self.assertLess(plan["take_profit"], plan["entry_price"])
        self.assertGreater(plan["stop_loss"], plan["entry_price"])
        self.assertEqual(plan["risk_reward_ratio"], "1:1.5")

    # =========================================================================
    # 3. KIỂM THỬ ORDER FLOW VALIDATION (A.1)
    # =========================================================================

    def test_orderflow_non_active_statuses_rejected(self):
        """Các trạng thái warming_up, incomplete, reconnecting, disconnected, stale, lạ -> CHỜ"""
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0
            }
        }
        futures_data = {
            "futures_price": 80000.0,
            "ticker_timestamp": self.valid_ticker_ts,
            "atr_15m": 100.0
        }

        for st in ["warming_up", "incomplete", "reconnecting", "disconnected", "stale", "unknown_state"]:
            of_data = {
                "status": st,
                "imbalance": 0.30,
                "buy_ratio": 0.65,
                "sell_ratio": 0.35,
                "trade_count": 50
            }
            plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
            self.assertEqual(plan["action"], "CHỜ", f"Status '{st}' must yield CHỜ")
            self.assertEqual(plan["reason_code"], "ORDERFLOW_NOT_ACTIVE")
            self.assertIsNone(plan["entry_price"])
            self.assertIsNone(plan["stop_loss"])

    def test_orderflow_is_connected_false_rejected(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts
            }
        }
        of_data = {
            "status": "active",
            "is_connected": False,
            "imbalance": 0.30,
            "buy_ratio": 0.65,
            "sell_ratio": 0.35,
            "trade_count": 50
        }
        futures_data = {"futures_price": 80000.0, "ticker_timestamp": self.valid_ticker_ts, "atr_15m": 100.0}
        plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertEqual(plan["reason_code"], "ORDERFLOW_DISCONNECTED")

    def test_orderflow_no_trades_rejected(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts
            }
        }
        futures_data = {"futures_price": 80000.0, "ticker_timestamp": self.valid_ticker_ts, "atr_15m": 100.0}

        for tc in [0, -1, None, False]:
            of_data = {
                "status": "active",
                "imbalance": 0.2,
                "buy_ratio": 0.6,
                "sell_ratio": 0.4,
                "trade_count": tc
            }
            plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
            self.assertEqual(plan["action"], "CHỜ", f"trade_count={tc} must yield CHỜ")
            self.assertEqual(plan["reason_code"], "ORDERFLOW_NO_TRADES")

    def test_orderflow_invalid_imbalance(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts
            }
        }
        futures_data = {"futures_price": 80000.0, "ticker_timestamp": self.valid_ticker_ts, "atr_15m": 100.0}

        for imb in [1.5, -2.0, None, True, float("nan"), float("inf"), "0.5"]:
            of_data = {
                "status": "active",
                "imbalance": imb,
                "buy_ratio": 0.5,
                "sell_ratio": 0.5,
                "trade_count": 10
            }
            plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
            self.assertEqual(plan["action"], "CHỜ")
            self.assertEqual(plan["reason_code"], "ORDERFLOW_INVALID_IMBALANCE")

    def test_orderflow_invalid_ratios(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts
            }
        }
        futures_data = {"futures_price": 80000.0, "ticker_timestamp": self.valid_ticker_ts, "atr_15m": 100.0}

        # Sai tổng (0.7 + 0.7 = 1.4 != 1.0)
        of_data_sum = {
            "status": "active",
            "imbalance": 0.0,
            "buy_ratio": 0.7,
            "sell_ratio": 0.7,
            "trade_count": 10
        }
        plan = generate_futures_plan(pred_data, of_data_sum, futures_data, now_utc=self.now_utc)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertEqual(plan["reason_code"], "ORDERFLOW_INVALID_RATIO_SUM")

        # Ratio ngoài [0, 1] hoặc sai kiểu
        for r in [-0.1, 1.2, None, True, float("nan")]:
            of_data_r = {
                "status": "active",
                "imbalance": 0.0,
                "buy_ratio": r,
                "sell_ratio": 0.5,
                "trade_count": 10
            }
            p = generate_futures_plan(pred_data, of_data_r, futures_data, now_utc=self.now_utc)
            self.assertEqual(p["action"], "CHỜ")
            self.assertEqual(p["reason_code"], "ORDERFLOW_INVALID_RATIO")

    # =========================================================================
    # 4. KIỂM THỬ DỮ LIỆU FUTURES (A.2)
    # =========================================================================

    def test_futures_data_stale_from_2020_rejected(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts
            }
        }
        of_data = {
            "status": "active",
            "imbalance": 0.2,
            "buy_ratio": 0.6,
            "sell_ratio": 0.4,
            "trade_count": 20
        }
        futures_data = {
            "futures_price": 80000.0,
            "ticker_timestamp": "2020-01-01T00:00:00Z",  # Năm 2020 (rất cũ)
            "atr_15m": 100.0
        }

        plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertEqual(plan["reason_code"], "FUTURES_DATA_STALE")

    def test_futures_missing_or_naive_timestamp(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts
            }
        }
        of_data = {"status": "active", "imbalance": 0.2, "buy_ratio": 0.6, "sell_ratio": 0.4, "trade_count": 20}

        # Thiếu timestamp
        p1 = generate_futures_plan(pred_data, of_data, {"futures_price": 80000.0, "atr_15m": 100.0}, now_utc=self.now_utc)
        self.assertEqual(p1["action"], "CHỜ")
        self.assertEqual(p1["reason_code"], "MISSING_FUTURES_TIMESTAMP")

        # Timestamp thiếu timezone
        p2 = generate_futures_plan(pred_data, of_data, {"futures_price": 80000.0, "ticker_timestamp": "2026-11-06 10:00:00", "atr_15m": 100.0}, now_utc=self.now_utc)
        self.assertEqual(p2["action"], "CHỜ")
        self.assertEqual(p2["reason_code"], "INVALID_FUTURES_TIMESTAMP")

    def test_futures_future_timestamp_rejected(self):
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts
            }
        }
        of_data = {"status": "active", "imbalance": 0.2, "buy_ratio": 0.6, "sell_ratio": 0.4, "trade_count": 20}
        # Tương lai + 5 phút (> tolerance 15s)
        future_ticker = (self.now_utc + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        p = generate_futures_plan(pred_data, of_data, {"futures_price": 80000.0, "ticker_timestamp": future_ticker, "atr_15m": 100.0}, now_utc=self.now_utc)
        self.assertEqual(p["action"], "CHỜ")
        self.assertEqual(p["reason_code"], "FUTURES_DATA_FUTURE_TIMESTAMP")

    def test_futures_invalid_price(self):
        of_data = {"status": "active", "imbalance": 0.2, "buy_ratio": 0.6, "sell_ratio": 0.4, "trade_count": 20}
        for px in [0, -500, None, True, float("nan"), float("inf"), "80000"]:
            p = generate_futures_plan(None, of_data, {"futures_price": px, "ticker_timestamp": self.valid_ticker_ts, "atr_15m": 100.0}, now_utc=self.now_utc)
            self.assertEqual(p["action"], "CHỜ")
            self.assertEqual(p["reason_code"], "INVALID_FUTURES_PRICE")

    # =========================================================================
    # 5. KIỂM THỬ DỮ LIỆU DỰ BÁO (A.3)
    # =========================================================================

    def test_prediction_invalid_probabilities(self):
        of_data = {"status": "active", "imbalance": 0.2, "buy_ratio": 0.6, "sell_ratio": 0.4, "trade_count": 20}
        futures_data = {"futures_price": 80000.0, "ticker_timestamp": self.valid_ticker_ts, "atr_15m": 100.0}

        # probability_up=5, probability_not_up=-4 (Lỗi tái hiện trong prompt)
        p1 = generate_futures_plan(
            {"probability_up": 5, "probability_not_up": -4, "target_timestamp": self.valid_future_ts, "feature_timestamp": self.valid_feature_ts},
            of_data, futures_data, now_utc=self.now_utc
        )
        self.assertEqual(p1["action"], "CHỜ")
        self.assertEqual(p1["reason_code"], "INVALID_PREDICTION_PROBABILITY")

        # bool không được chấp nhận
        p2 = generate_futures_plan(
            {"probability_up": True, "probability_not_up": False, "target_timestamp": self.valid_future_ts, "feature_timestamp": self.valid_feature_ts},
            of_data, futures_data, now_utc=self.now_utc
        )
        self.assertEqual(p2["action"], "CHỜ")
        self.assertEqual(p2["reason_code"], "INVALID_PREDICTION_PROBABILITY")

        # NaN / Inf
        p3 = generate_futures_plan(
            {"probability_up": float("nan"), "probability_not_up": 0.5, "target_timestamp": self.valid_future_ts, "feature_timestamp": self.valid_feature_ts},
            of_data, futures_data, now_utc=self.now_utc
        )
        self.assertEqual(p3["action"], "CHỜ")
        self.assertEqual(p3["reason_code"], "INVALID_PREDICTION_PROBABILITY")

        # Sai tổng xác suất (0.6 + 0.6 = 1.2)
        p4 = generate_futures_plan(
            {"probability_up": 0.6, "probability_not_up": 0.6, "target_timestamp": self.valid_future_ts, "feature_timestamp": self.valid_feature_ts},
            of_data, futures_data, now_utc=self.now_utc
        )
        self.assertEqual(p4["action"], "CHỜ")
        self.assertEqual(p4["reason_code"], "INVALID_PROBABILITY_SUM")

    def test_prediction_timestamps_and_horizon(self):
        of_data = {"status": "active", "imbalance": 0.2, "buy_ratio": 0.6, "sell_ratio": 0.4, "trade_count": 20}
        futures_data = {"futures_price": 80000.0, "ticker_timestamp": self.valid_ticker_ts, "atr_15m": 100.0}

        # Thiếu feature_timestamp
        p1 = generate_futures_plan(
            {"probability_up": 0.6, "probability_not_up": 0.4, "target_timestamp": self.valid_future_ts},
            of_data, futures_data, now_utc=self.now_utc
        )
        self.assertEqual(p1["action"], "CHỜ")
        self.assertEqual(p1["reason_code"], "MISSING_FEATURE_TIMESTAMP")

        # feature_timestamp >= target_timestamp
        p2 = generate_futures_plan(
            {
                "probability_up": 0.6,
                "probability_not_up": 0.4,
                "feature_timestamp": self.valid_future_ts,
                "target_timestamp": self.valid_feature_ts
            },
            of_data, futures_data, now_utc=self.now_utc
        )
        self.assertEqual(p2["action"], "CHỜ")
        self.assertEqual(p2["reason_code"], "INVALID_TIMESTAMP_RELATION")

        # Khoảng cách không khớp +5m (ví dụ 10m = 600s)
        ten_m_future = (self.now_utc + timedelta(minutes=9)).strftime("%Y-%m-%dT%H:%M:%SZ")
        p3 = generate_futures_plan(
            {
                "probability_up": 0.6,
                "probability_not_up": 0.4,
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": ten_m_future
            },
            of_data, futures_data, now_utc=self.now_utc
        )
        self.assertEqual(p3["action"], "CHỜ")
        self.assertEqual(p3["reason_code"], "INVALID_PREDICTION_HORIZON")

    def test_prediction_expired(self):
        of_data = {"status": "active", "imbalance": 0.2, "buy_ratio": 0.6, "sell_ratio": 0.4, "trade_count": 20}
        futures_data = {"futures_price": 80000.0, "ticker_timestamp": self.valid_ticker_ts, "atr_15m": 100.0}
        old_feat = (self.now_utc - timedelta(minutes=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
        p = generate_futures_plan(
            {
                "probability_up": 0.65,
                "probability_not_up": 0.35,
                "feature_timestamp": old_feat,
                "target_timestamp": self.expired_ts
            },
            of_data, futures_data, now_utc=self.now_utc
        )
        self.assertEqual(p["action"], "CHỜ")
        self.assertEqual(p["reason_code"], "PREDICTION_EXPIRED")

    def test_uncertain_probability_yields_wait(self):
        # Xác suất 50.5% vs 49.5% sát ngưỡng 50%
        pred_data = {
            "prediction": {
                "probability_up": 0.505,
                "probability_not_up": 0.495,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0
            }
        }
        of_data = {"status": "active", "imbalance": 0.20, "buy_ratio": 0.6, "sell_ratio": 0.4, "trade_count": 20}
        futures_data = {"futures_price": 80000.0, "ticker_timestamp": self.valid_ticker_ts, "atr_15m": 100.0}

        plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertEqual(plan["reason_code"], "UNCERTAIN_PROBABILITY")

    def test_conflicting_signals_yields_wait(self):
        # UP prediction + Negative imbalance
        pred_data = {
            "prediction": {
                "probability_up": 0.65,
                "probability_not_up": 0.35,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0
            }
        }
        of_data = {
            "status": "active",
            "imbalance": -0.20,
            "buy_ratio": 0.40,
            "sell_ratio": 0.60,
            "trade_count": 30
        }
        futures_data = {
            "futures_price": 80000.0,
            "ticker_timestamp": self.valid_ticker_ts,
            "atr_15m": 100.0
        }

        plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertEqual(plan["reason_code"], "CONFLICTING_SIGNALS")

    def test_not_up_does_not_blindly_trigger_short(self):
        # NOT_UP nhưng dòng lệnh bán chưa đủ áp lực (> -0.15)
        pred_data = {
            "prediction": {
                "probability_up": 0.40,
                "probability_not_up": 0.60,
                "direction_label": "NOT_UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0
            }
        }
        of_data = {
            "status": "active",
            "imbalance": -0.05,
            "buy_ratio": 0.475,
            "sell_ratio": 0.525,
            "trade_count": 40
        }
        futures_data = {
            "futures_price": 80000.0,
            "ticker_timestamp": self.valid_ticker_ts,
            "atr_15m": 100.0
        }

        plan = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
        self.assertEqual(plan["action"], "CHỜ")
        self.assertEqual(plan["reason_code"], "INSUFFICIENT_SELL_PRESSURE")

    # =========================================================================
    # 6. KIỂM THỬ SERIALIZATION VÀ ĐẦU RA AN TOÀN (A.4)
    # =========================================================================

    def test_json_strict_serialization_no_nan_inf(self):
        """Đảm bảo mọi response (cả khi lỗi) đều serialize thành JSON chuẩn, không chứa NaN/Infinity."""
        pred_data = {
            "prediction": {
                "probability_up": 0.62,
                "probability_not_up": 0.38,
                "direction_label": "UP",
                "feature_timestamp": self.valid_feature_ts,
                "target_timestamp": self.valid_future_ts,
                "current_price": 80000.0
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
            "ticker_timestamp": self.valid_ticker_ts,
            "atr_15m": 120.0
        }

        # 1. Kế hoạch LONG
        plan_long = generate_futures_plan(pred_data, of_data, futures_data, now_utc=self.now_utc)
        raw_json_long = json.dumps(plan_long, allow_nan=False)
        self.assertIn('"action": "LONG"', raw_json_long)

        # 2. Kế hoạch CHỜ do lỗi đầu vào
        plan_wait = generate_futures_plan(None, None, None, now_utc=self.now_utc)
        raw_json_wait = json.dumps(plan_wait, allow_nan=False)
        parsed_wait = json.loads(raw_json_wait)
        self.assertEqual(parsed_wait["action"], "CHỜ")
        self.assertIsNone(plan_wait["entry_price"])
        self.assertIsNone(plan_wait["stop_loss"])
        self.assertIsNone(plan_wait["take_profit"])
        self.assertIsNone(plan_wait["risk_reward_ratio"])


if __name__ == "__main__":
    unittest.main()
