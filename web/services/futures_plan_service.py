"""
=============================================================================
Crypto Big Data - Futures Plan Service (Decision Support)
=============================================================================
Xây dựng kế hoạch giao dịch phái sinh (BTC-USDT-SWAP) dựa trên quy tắc minh bạch:
  1. Tách biệt rõ ràng:
     - Mô hình ML: Huấn luyện trên BTC-USDT Spot (+5m binary direction UP vs NOT_UP).
     - Hợp đồng phái sinh: BTC-USDT-SWAP (Perpetual swap với giá thị trường riêng).
     - Độ lệch Basis: P(Futures) - P(Spot).
  2. Quy tắc kích hoạt kế hoạch (Rule-based confluence):
     - LONG: P(UP) >= 0.55 VÀ Imbalance 60s >= 0.0 (dòng lệnh chủ động đồng thuận).
     - SHORT: P(NOT_UP) >= 0.55 VÀ Imbalance 60s <= -0.15 (quy tắc chiến lược bổ sung:
       mô hình chỉ dự báo Không Tăng, cần dòng lệnh bán áp đảo để xác nhận xu hướng giảm).
     - CHỜ: Khi mô hình hết hạn, thiếu dữ liệu, xác suất sát 50% ([0.48, 0.52]),
       tín hiệu mâu thuẫn (P(UP) cao nhưng bán tháo, hoặc NOT_UP nhưng không có lực bán),
       hoặc mất kết nối API/WebSocket.
  3. Quản lý rủi ro minh bạch:
     - SL / TP tính từ biến động thực tế: 15m ATR từ các nến đã đóng (Simple Moving Average của True Range).
     - Buffer tối thiểu: R = max(1.2 * ATR, Entry * 0.003).
     - LONG: SL = Entry - R, TP = Entry + 1.5R (thỏa SL < Entry < TP).
     - SHORT: SL = Entry + R, TP = Entry - 1.5R (thỏa TP < Entry < SL).
     - Tỷ lệ Lợi nhuận / Rủi ro cố định: 1:1.5.
  4. Ràng buộc an toàn:
     - KHÔNG tự động đặt lệnh, KHÔNG yêu cầu API key giao dịch.
     - KHÔNG gợi ý đòn bẩy hay khối lượng vốn.
     - KHÔNG dùng giá trị 0 hoặc số giả để điền Entry/SL/TP.
=============================================================================
"""

import os
import math
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Callable, List, Tuple


# =============================================================================
# CHÍNH SÁCH KỸ THUẬT VÀ DUNG SAI (CONFIGURABLE TECHNICAL POLICIES)
# Đây là các ngưỡng kiểm soát rủi ro và chất lượng dữ liệu, có thể điều chỉnh qua biến môi trường.
# =============================================================================

# Độ cũ tối đa chấp nhận được của giá Futures (Ticker) từ OKX REST (giây)
FUTURES_TICKER_MAX_STALENESS_SECONDS = int(os.getenv("FUTURES_TICKER_MAX_STALENESS_SECONDS", "60"))

# Dung sai chấp nhận timestamp tương lai do lệch đồng hồ (clock drift) giữa server và sàn (giây)
FUTURES_TICKER_FUTURE_TOLERANCE_SECONDS = int(os.getenv("FUTURES_TICKER_FUTURE_TOLERANCE_SECONDS", "15"))

# Dung sai kiểm tra tổng xác suất dự báo (P(UP) + P(NOT_UP) ≈ 1.0)
PREDICTION_PROB_SUM_TOLERANCE = float(os.getenv("PREDICTION_PROB_SUM_TOLERANCE", "0.005"))

# Tầm dự báo chuẩn (Prediction Horizon) của mô hình Spot BTC-USDT (5 phút = 300 giây)
PREDICTION_HORIZON_SECONDS = int(os.getenv("PREDICTION_HORIZON_SECONDS", "300"))

# Dung sai kiểm tra khoảng cách giữa feature_timestamp và target_timestamp (giây)
PREDICTION_HORIZON_TOLERANCE_SECONDS = int(os.getenv("PREDICTION_HORIZON_TOLERANCE_SECONDS", "30"))

# Dung sai kiểm tra tổng tỷ lệ mua / bán Order Flow (buy_ratio + sell_ratio ≈ 1.0)
ORDERFLOW_RATIO_SUM_TOLERANCE = float(os.getenv("ORDERFLOW_RATIO_SUM_TOLERANCE", "0.01"))

# Độ cũ tối đa chấp nhận được của nến 15m đóng gần nhất tính đến thời điểm hiện tại (giây)
# Một nến 15m sau khi đóng cần tối đa 15 phút (cho nến tiếp theo đang chạy) + 15 phút dung sai sàn = 30 phút (1800s)
ATR_15M_MAX_STALENESS_SECONDS = int(os.getenv("ATR_15M_MAX_STALENESS_SECONDS", "1800"))

# Dung sai timestamp tương lai cho nến đóng (giây)
ATR_15M_FUTURE_TOLERANCE_SECONDS = int(os.getenv("ATR_15M_FUTURE_TOLERANCE_SECONDS", "60"))

# Số nến đóng mặc định dùng để tính ATR (period=14 cần ít nhất 15 nến đóng liên tiếp để tính 14 True Range)
ATR_DEFAULT_PERIOD = 14

# Bước thời gian chuẩn của nến 15 phút tính theo milliseconds
CANDLE_15M_STEP_MS = 15 * 60 * 1000


def _parse_iso_utc(ts_val: Any) -> Optional[datetime]:
    """
    Chuẩn hóa và parse timestamp thành datetime UTC có múi giờ (timezone-aware).
    Từ chối timestamp không có thông tin múi giờ hoặc định dạng không hợp lệ.
    """
    if not ts_val:
        return None
    if isinstance(ts_val, datetime):
        if ts_val.tzinfo is None:
            return None
        return ts_val.astimezone(timezone.utc)
    if isinstance(ts_val, str):
        s = ts_val.strip()
        if not s:
            return None
        clean = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(clean)
            if dt.tzinfo is None:
                return None
            return dt.astimezone(timezone.utc)
        except (ValueError, TypeError):
            return None
    return None


def _is_finite_positive_number(val: Any) -> bool:
    """Kiểm tra giá trị có phải là số thực hữu hạn dương (> 0), loại trừ bool."""
    if isinstance(val, bool):
        return False
    if not isinstance(val, (int, float)):
        return False
    return math.isfinite(val) and val > 0


def _is_finite_probability(val: Any) -> bool:
    """Kiểm tra giá trị có phải là xác suất hợp lệ thuộc [0, 1], loại trừ bool."""
    if isinstance(val, bool):
        return False
    if not isinstance(val, (int, float)):
        return False
    return math.isfinite(val) and 0.0 <= val <= 1.0


def calculate_15m_atr_detailed(
    candles: List[List[Any]],
    period: int = ATR_DEFAULT_PERIOD,
    now_utc: Optional[datetime] = None
) -> Tuple[Optional[float], Optional[str]]:
    """
    Tính Average True Range (ATR) từ danh sách nến 15m của OKX với kiểm định toàn vẹn nghiêm ngặt:
      - Loại bỏ mọi nến có confirm != '1'.
      - Kiểm tra timestamp, OHLC hữu hạn, giá dương và hình học nến Nhật Bản (0 < low <= min(O, C) và max(O, C) <= high).
      - Sắp xếp tăng dần theo timestamp, phát hiện trùng lặp và khoảng thiếu (gap detection).
      - Đảm bảo đủ ít nhất (period + 1) nến đóng liên tiếp để tính đủ period giá trị True Range.
      - Kiểm tra độ mới của nến đóng gần nhất (không quá hạn và không nằm trong tương lai).
      - Phương pháp: Simple Moving Average (SMA) của True Range qua 'period' nến.
    
    Trả về: (atr_value, rejection_reason). Nếu thành công, rejection_reason là None.
    """
    if not candles or not isinstance(candles, (list, tuple)):
        return None, "Dữ liệu nến 15m rỗng hoặc không đúng định dạng mảng."

    if period <= 0:
        return None, f"Chu kỳ ATR ({period}) không hợp lệ (phải > 0)."

    # 1. Lọc và chuẩn hóa các nến thỏa mãn confirm == '1' và hình học OHLC
    valid_closed = []
    for c in candles:
        if not isinstance(c, (list, tuple)) or len(c) < 9:
            continue

        # Chỉ chấp nhận nến có xác nhận đóng chính thức từ OKX
        confirm_str = str(c[8]).strip() if c[8] is not None else ""
        if confirm_str != "1":
            continue

        try:
            ts_raw = int(c[0])
            if ts_raw <= 0:
                continue
            ts_ms = ts_raw * 1000 if ts_raw < 100_000_000_000 else ts_raw

            open_p = float(c[1])
            high_p = float(c[2])
            low_p = float(c[3])
            close_p = float(c[4])
        except (ValueError, TypeError, OverflowError):
            continue

        # Kiểm tra tính hữu hạn và số dương
        if not (math.isfinite(open_p) and math.isfinite(high_p) and math.isfinite(low_p) and math.isfinite(close_p)):
            continue
        if open_p <= 0 or high_p <= 0 or low_p <= 0 or close_p <= 0:
            continue

        # Kiểm tra hình học nến: Low <= min(Open, Close) và High >= max(Open, Close)
        if not (low_p <= min(open_p, close_p) and max(open_p, close_p) <= high_p):
            continue

        valid_closed.append({
            "ts": ts_ms,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p
        })

    # Cần tối thiểu (period + 1) nến đóng để tính period giá trị True Range
    min_required_candles = period + 1
    if len(valid_closed) < min_required_candles:
        return None, f"Không đủ nến đóng xác nhận (confirm='1'): cần tối thiểu {min_required_candles} nến, hiện có {len(valid_closed)}."

    # 2. Sắp xếp các nến theo thứ tự thời gian tăng dần (cũ nhất -> mới nhất)
    sorted_candles = sorted(valid_closed, key=lambda x: x["ts"])

    # 3. Kiểm tra tính liên tục: không trùng lặp timestamp
    seen_timestamps = set()
    for c in sorted_candles:
        if c["ts"] in seen_timestamps:
            return None, f"Phát hiện timestamp trùng lặp trong chuỗi nến 15m ({c['ts']})."
        seen_timestamps.add(c["ts"])

    # Kiểm tra không có khoảng thiếu (gap)
    for i in range(1, len(sorted_candles)):
        diff_ms = sorted_candles[i]["ts"] - sorted_candles[i - 1]["ts"]
        if diff_ms != CANDLE_15M_STEP_MS:
            return None, f"Phát hiện khoảng trống thời gian (gap) trong chuỗi nến 15m (khoảng cách {diff_ms}ms != {CANDLE_15M_STEP_MS}ms)."

    # 4. Kiểm tra độ mới của nến đóng gần nhất
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    elif now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)

    latest_candle = sorted_candles[-1]
    latest_start_dt = datetime.fromtimestamp(latest_candle["ts"] / 1000.0, tz=timezone.utc)
    latest_close_dt = latest_start_dt + timedelta(minutes=15)

    # Từ chối nếu nến đóng nằm trong tương lai vượt dung sai cho phép
    future_drift = (latest_close_dt - now_utc).total_seconds()
    if future_drift > ATR_15M_FUTURE_TOLERANCE_SECONDS:
        return None, f"Nến 15m gần nhất có thời gian đóng trong tương lai vượt dung sai ({future_drift:.0f}s > {ATR_15M_FUTURE_TOLERANCE_SECONDS}s)."

    # Từ chối nếu nến đóng gần nhất quá cũ
    age_seconds = (now_utc - latest_close_dt).total_seconds()
    if age_seconds > ATR_15M_MAX_STALENESS_SECONDS:
        return None, f"Nến 15m gần nhất đã quá cũ ({age_seconds:.0f}s > {ATR_15M_MAX_STALENESS_SECONDS}s)."

    # 5. Tính chuỗi True Range
    true_ranges: List[float] = []
    for i in range(1, len(sorted_candles)):
        curr_h = sorted_candles[i]["high"]
        curr_l = sorted_candles[i]["low"]
        prev_c = sorted_candles[i - 1]["close"]

        tr = max(
            curr_h - curr_l,
            abs(curr_h - prev_c),
            abs(curr_l - prev_c)
        )
        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None, f"Không đủ giá trị True Range (cần {period}, tính được {len(true_ranges)})."

    # Lấy chính xác 'period' giá trị True Range gần nhất và tính trung bình số học (SMA)
    sample = true_ranges[-period:]
    atr_val = sum(sample) / float(len(sample))
    return round(atr_val, 2), None


def calculate_15m_atr(
    candles: List[List[Any]],
    period: int = ATR_DEFAULT_PERIOD,
    now_utc: Optional[datetime] = None
) -> Optional[float]:
    """
    Tính Average True Range (ATR) từ danh sách nến 15m của OKX.
    Giữ nguyên signature tương thích ngược với các caller hiện hữu.
    """
    atr, _ = calculate_15m_atr_detailed(candles, period=period, now_utc=now_utc)
    return atr


def fetch_futures_market_data(
    okx_rest_url: str = "https://www.okx.com",
    fetch_okx_json: Optional[Callable[[str], Any]] = None,
    now_utc: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Lấy giá ticker thời gian thực và nến 15m của hợp đồng BTC-USDT-SWAP từ OKX REST API công khai.
    """
    if fetch_okx_json is None:
        import urllib.request
        import json

        def default_fetch(url: str):
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Accept": "application/json"
                    }
                )
                with urllib.request.urlopen(req, timeout=4) as resp:
                    if resp.status == 200:
                        return json.loads(resp.read().decode("utf-8"))
            except Exception as e:
                print(f"[!] Lỗi gọi OKX REST ({url}): {e}")
                return None
            return None

        fetch_okx_json = default_fetch

    # 1. Lấy ticker BTC-USDT-SWAP
    ticker_url = f"{okx_rest_url}/api/v5/market/ticker?instId=BTC-USDT-SWAP"
    ticker_res = fetch_okx_json(ticker_url)
    futures_price = None
    ticker_ts = None

    if ticker_res and isinstance(ticker_res, dict) and ticker_res.get("data"):
        item = ticker_res["data"][0]
        try:
            p_val = float(item.get("last", 0))
            if math.isfinite(p_val) and p_val > 0:
                futures_price = p_val
            ts_val = item.get("ts")
            if ts_val:
                ts_int = int(ts_val)
                ticker_ts = datetime.fromtimestamp(ts_int / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            pass

    # 2. Lấy nến 15m để tính ATR (yêu cầu limit=30 để đảm bảo đủ ít nhất 15 nến đóng)
    candles_url = f"{okx_rest_url}/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=15m&limit=30"
    candles_res = fetch_okx_json(candles_url)
    atr_15m = None

    if candles_res and isinstance(candles_res, dict) and candles_res.get("data"):
        raw_candles = candles_res["data"]
        atr_15m = calculate_15m_atr(raw_candles, period=ATR_DEFAULT_PERIOD, now_utc=now_utc)

    return {
        "futures_price": futures_price,
        "ticker_timestamp": ticker_ts,
        "atr_15m": atr_15m
    }


def _validate_futures_data(
    futures_data: Optional[Dict[str, Any]],
    now_utc: datetime
) -> Tuple[bool, str, str, Optional[float], Optional[datetime]]:
    """
    Kiểm tra tính hợp lệ và độ tin cậy của dữ liệu giá Futures:
      - Giá phải là số hữu hạn và lớn hơn 0.
      - Có ticker_timestamp hợp lệ, có timezone và được chuẩn hóa UTC.
      - Dữ liệu chưa vượt quá giới hạn độ cũ.
      - Timestamp tương lai vượt dung sai cho phép phải bị từ chối.
    
    Trả về: (is_valid, reason_code, reason_message, futures_price, ticker_dt)
    """
    if not futures_data or not isinstance(futures_data, dict):
        return False, "MISSING_FUTURES_DATA", "Chưa có dữ liệu thị trường hợp đồng BTC-USDT-SWAP từ OKX REST.", None, None

    futures_price = futures_data.get("futures_price")
    if not _is_finite_positive_number(futures_price):
        return False, "INVALID_FUTURES_PRICE", f"Giá hợp đồng Futures ({futures_price}) không hợp lệ (phải là số hữu hạn > 0).", None, None

    ticker_ts_raw = futures_data.get("ticker_timestamp")
    if not ticker_ts_raw:
        return False, "MISSING_FUTURES_TIMESTAMP", "Thiếu mốc thời gian (ticker_timestamp) của dữ liệu giá Futures.", None, None

    ticker_dt = _parse_iso_utc(ticker_ts_raw)
    if not ticker_dt:
        return False, "INVALID_FUTURES_TIMESTAMP", f"Mốc thời gian ticker_timestamp ({ticker_ts_raw}) không hợp lệ hoặc thiếu thông tin múi giờ (timezone).", None, None

    # Kiểm tra timestamp tương lai (clock drift)
    future_drift = (ticker_dt - now_utc).total_seconds()
    if future_drift > FUTURES_TICKER_FUTURE_TOLERANCE_SECONDS:
        return (
            False,
            "FUTURES_DATA_FUTURE_TIMESTAMP",
            f"Mốc thời gian giá Futures ({ticker_ts_raw}) vượt quá thời gian hiện tại ({future_drift:.0f}s > {FUTURES_TICKER_FUTURE_TOLERANCE_SECONDS}s).",
            None,
            None
        )

    # Kiểm tra độ cũ (staleness)
    age_seconds = (now_utc - ticker_dt).total_seconds()
    if age_seconds > FUTURES_TICKER_MAX_STALENESS_SECONDS:
        return (
            False,
            "FUTURES_DATA_STALE",
            f"Dữ liệu giá Futures đã quá cũ ({age_seconds:.0f}s > {FUTURES_TICKER_MAX_STALENESS_SECONDS}s). Cần kiểm tra lại kết nối ticker sàn.",
            None,
            None
        )

    return True, "FUTURES_VALID", "Dữ liệu Futures hợp lệ.", float(futures_price), ticker_dt


def _validate_prediction_data(
    prediction_data: Optional[Dict[str, Any]],
    now_utc: datetime
) -> Tuple[bool, str, str, Dict[str, Any]]:
    """
    Kiểm tra tính toàn vẹn và độ tin cậy của dữ liệu dự báo ML:
      - probability_up và probability_not_up phải là số hữu hạn thuộc [0, 1].
      - Tổng xác suất xấp xỉ 1 theo dung sai rõ ràng (PREDICTION_PROB_SUM_TOLERANCE).
      - Không nhận bool như một giá trị xác suất.
      - target_timestamp hợp lệ và chưa hết hạn.
      - Kiểm tra feature_timestamp và quan hệ với target_timestamp theo horizon thực tế của dự án.
      - Dự báo thiếu hoặc có thời gian bất hợp lý phải trả CHỜ.
      - Không tự chỉnh xác suất lỗi về phạm vi hợp lệ để tiếp tục tạo lệnh.
    
    Trả về: (is_valid, reason_code, reason_message, parsed_context)
    """
    if not prediction_data or not isinstance(prediction_data, dict):
        return False, "PREDICTION_DATA_MISSING", "Chưa có dữ liệu dự báo hướng giá từ Data Warehouse.", {}

    pred = (
        prediction_data.get("prediction")
        if isinstance(prediction_data, dict) and "prediction" in prediction_data
        else prediction_data
    )
    if not pred or not isinstance(pred, dict):
        return False, "PREDICTION_DATA_MISSING", "Cấu trúc dữ liệu dự báo hướng giá không hợp lệ.", {}

    prob_up = pred.get("probability_up")
    prob_not_up = pred.get("probability_not_up")

    # Kiểm tra kiểu và miền giá trị của xác suất (từ chối bool, NaN, Inf, ngoài [0, 1])
    if not _is_finite_probability(prob_up):
        return (
            False,
            "INVALID_PREDICTION_PROBABILITY",
            f"Xác suất probability_up ({prob_up}) không hợp lệ (phải là số thực trong [0, 1], không chấp nhận bool/NaN/Inf).",
            {}
        )
    if not _is_finite_probability(prob_not_up):
        return (
            False,
            "INVALID_PREDICTION_PROBABILITY",
            f"Xác suất probability_not_up ({prob_not_up}) không hợp lệ (phải là số thực trong [0, 1], không chấp nhận bool/NaN/Inf).",
            {}
        )

    prob_up_f = float(prob_up)
    prob_not_up_f = float(prob_not_up)
    prob_sum = prob_up_f + prob_not_up_f
    if abs(prob_sum - 1.0) > PREDICTION_PROB_SUM_TOLERANCE:
        return (
            False,
            "INVALID_PROBABILITY_SUM",
            f"Tổng xác suất mô hình (P(UP)={prob_up_f:.4f} + P(NOT_UP)={prob_not_up_f:.4f} = {prob_sum:.4f}) sai lệch vượt dung sai cho phép ({PREDICTION_PROB_SUM_TOLERANCE}).",
            {}
        )

    # Kiểm tra mốc thời gian mục tiêu (target_timestamp)
    target_ts_raw = pred.get("target_timestamp")
    if not target_ts_raw:
        return False, "MISSING_TARGET_TIMESTAMP", "Thiếu mốc thời gian mục tiêu (target_timestamp) của dự báo.", {}

    target_dt = _parse_iso_utc(target_ts_raw)
    if not target_dt:
        return False, "INVALID_TARGET_TIMESTAMP", f"Mốc thời gian mục tiêu ({target_ts_raw}) không hợp lệ hoặc thiếu thông tin múi giờ (timezone).", {}

    # Kiểm tra mốc thời gian nến đầu vào (feature_timestamp)
    feat_ts_raw = pred.get("feature_timestamp")
    if not feat_ts_raw:
        return False, "MISSING_FEATURE_TIMESTAMP", "Thiếu mốc thời gian nến đầu vào (feature_timestamp) của dự báo.", {}

    feat_dt = _parse_iso_utc(feat_ts_raw)
    if not feat_dt:
        return False, "INVALID_FEATURE_TIMESTAMP", f"Mốc thời gian nến đầu vào ({feat_ts_raw}) không hợp lệ hoặc thiếu thông tin múi giờ (timezone).", {}

    # Kiểm tra quan hệ thời gian giữa feature và target
    if feat_dt >= target_dt:
        return False, "INVALID_TIMESTAMP_RELATION", f"Thời điểm nến đầu vào ({feat_ts_raw}) không trước thời điểm mục tiêu ({target_ts_raw}).", {}

    horizon_diff = (target_dt - feat_dt).total_seconds()
    if abs(horizon_diff - PREDICTION_HORIZON_SECONDS) > PREDICTION_HORIZON_TOLERANCE_SECONDS:
        return (
            False,
            "INVALID_PREDICTION_HORIZON",
            f"Khoảng cách giữa feature và target ({horizon_diff:.0f}s) không khớp với chu kỳ dự báo +5m ({PREDICTION_HORIZON_SECONDS}s ± {PREDICTION_HORIZON_TOLERANCE_SECONDS}s).",
            {}
        )

    # Kiểm tra nến đầu vào không được nằm trong tương lai vượt dung sai
    if (feat_dt - now_utc).total_seconds() > FUTURES_TICKER_FUTURE_TOLERANCE_SECONDS:
        return False, "PREDICTION_FEATURE_IN_FUTURE", f"Thời điểm nến đầu vào ({feat_ts_raw}) nằm trong tương lai vượt dung sai cho phép.", {}

    # Kiểm tra thời hạn hiệu lực của dự báo
    seconds_remaining = int((target_dt - now_utc).total_seconds())
    if seconds_remaining <= 0:
        return (
            False,
            "PREDICTION_EXPIRED",
            f"Dự báo gần nhất đã hết hiệu lực mục tiêu (+5m) vào {target_ts_raw}. Chờ chu kỳ suy luận mới.",
            {
                "target_timestamp": target_ts_raw,
                "time_remaining_seconds": 0
            }
        )

    # Kiểm tra phân tách xác suất sát ngưỡng 50%
    if 0.48 <= prob_up_f <= 0.52:
        return (
            False,
            "UNCERTAIN_PROBABILITY",
            f"Xác suất mô hình ({prob_up_f:.2%} UP vs {prob_not_up_f:.2%} NOT_UP) sát ngưỡng 50%, hai lớp chưa phân tách rõ.",
            {
                "probability_up": prob_up_f,
                "probability_not_up": prob_not_up_f,
                "direction_label": pred.get("direction_label"),
                "target_timestamp": target_ts_raw,
                "time_remaining_seconds": max(0, seconds_remaining)
            }
        )

    parsed_ctx = {
        "probability_up": prob_up_f,
        "probability_not_up": prob_not_up_f,
        "direction_label": pred.get("direction_label"),
        "target_timestamp": target_ts_raw,
        "feature_timestamp": feat_ts_raw,
        "model_version": pred.get("model_version"),
        "spot_price": pred.get("current_price"),
        "time_remaining_seconds": max(0, seconds_remaining)
    }
    return True, "PREDICTION_VALID", "Dữ liệu dự báo hợp lệ.", parsed_ctx


def _validate_orderflow_data(
    orderflow_data: Optional[Dict[str, Any]]
) -> Tuple[bool, str, str, Dict[str, Any]]:
    """
    Kiểm tra tính toàn vẹn và trạng thái hoạt động của dòng lệnh Order Flow:
      - Chỉ cho phép tạo kế hoạch khi order flow có trạng thái 'active'.
      - Các trạng thái khác hoặc không xác định phải trả CHỜ, kèm lý do ngắn gọn.
      - is_connected không được là false nếu trường này có trong response.
      - Có giao dịch trong cửa sổ (trade_count > 0).
      - Imbalance hữu hạn, thuộc [-1, 1].
      - Buy/sell ratio hữu hạn, thuộc [0, 1], tổng xấp xỉ 1.
      - Không dùng giá trị mặc định để biến dữ liệu thiếu thành hợp lệ.
    
    Trả về: (is_valid, reason_code, reason_message, parsed_of_context)
    """
    if not orderflow_data or not isinstance(orderflow_data, dict):
        return False, "ORDERFLOW_DATA_MISSING", "Chưa có dữ liệu dòng lệnh Order Flow thời gian thực.", {}

    of_status = orderflow_data.get("status")
    if not of_status:
        of_status = orderflow_data.get("state")

    parsed_context = {
        "status": of_status,
        "imbalance": orderflow_data.get("imbalance"),
        "buy_ratio": orderflow_data.get("buy_ratio"),
        "sell_ratio": orderflow_data.get("sell_ratio"),
        "trade_count": orderflow_data.get("trade_count")
    }

    # 1. Chỉ chấp nhận trạng thái 'active'
    if of_status != "active":
        status_label = of_status or "không xác định"
        status_text = orderflow_data.get("status_text")
        reason_desc = f" ({status_text})" if status_text else ""
        return False, "ORDERFLOW_NOT_ACTIVE", f"Dòng lệnh Order Flow chưa ở trạng thái hoạt động (trạng thái: {status_label}){reason_desc}.", parsed_context

    # 2. Kiểm tra kết nối nếu có trường is_connected
    if "is_connected" in orderflow_data and orderflow_data["is_connected"] is False:
        return False, "ORDERFLOW_DISCONNECTED", "Dòng lệnh Order Flow bị mất kết nối tới sàn (is_connected=False).", parsed_context

    # 3. Phải có giao dịch trong cửa sổ (trade_count > 0)
    trade_count = orderflow_data.get("trade_count")
    if trade_count is None or isinstance(trade_count, bool) or not isinstance(trade_count, (int, float)) or trade_count <= 0:
        return False, "ORDERFLOW_NO_TRADES", "Không có giao dịch nào trong cửa sổ dòng lệnh 60 giây (trade_count <= 0).", parsed_context

    # 4. Kiểm tra imbalance hữu hạn trong [-1, 1]
    imbalance = orderflow_data.get("imbalance")
    if (
        imbalance is None
        or isinstance(imbalance, bool)
        or not isinstance(imbalance, (int, float))
        or not math.isfinite(imbalance)
        or not (-1.0 <= imbalance <= 1.0)
    ):
        return False, "ORDERFLOW_INVALID_IMBALANCE", f"Chỉ số Order Flow Imbalance ({imbalance}) không hợp lệ (phải là số hữu hạn trong [-1, 1]).", parsed_context

    # 5. Kiểm tra buy_ratio và sell_ratio hữu hạn trong [0, 1] và tổng ≈ 1
    buy_ratio = orderflow_data.get("buy_ratio")
    sell_ratio = orderflow_data.get("sell_ratio")

    if (
        buy_ratio is None
        or isinstance(buy_ratio, bool)
        or not isinstance(buy_ratio, (int, float))
        or not math.isfinite(buy_ratio)
        or not (0.0 <= buy_ratio <= 1.0)
    ):
        return False, "ORDERFLOW_INVALID_RATIO", f"Tỷ lệ Mua Order Flow ({buy_ratio}) không hợp lệ (phải thuộc [0, 1]).", parsed_context

    if (
        sell_ratio is None
        or isinstance(sell_ratio, bool)
        or not isinstance(sell_ratio, (int, float))
        or not math.isfinite(sell_ratio)
        or not (0.0 <= sell_ratio <= 1.0)
    ):
        return False, "ORDERFLOW_INVALID_RATIO", f"Tỷ lệ Bán Order Flow ({sell_ratio}) không hợp lệ (phải thuộc [0, 1]).", parsed_context

    ratio_sum = float(buy_ratio) + float(sell_ratio)
    if abs(ratio_sum - 1.0) > ORDERFLOW_RATIO_SUM_TOLERANCE:
        return (
            False,
            "ORDERFLOW_INVALID_RATIO_SUM",
            f"Tổng tỷ lệ Mua/Bán Order Flow ({ratio_sum:.4f}) không xấp xỉ 1 (dung sai {ORDERFLOW_RATIO_SUM_TOLERANCE}).",
            parsed_context
        )

    parsed_context["imbalance"] = float(imbalance)
    parsed_context["buy_ratio"] = float(buy_ratio)
    parsed_context["sell_ratio"] = float(sell_ratio)
    parsed_context["trade_count"] = int(trade_count)

    return True, "ORDERFLOW_VALID", "Dòng lệnh Order Flow hợp lệ.", parsed_context


def generate_futures_plan(
    prediction_data: Optional[Dict[str, Any]],
    orderflow_data: Optional[Dict[str, Any]],
    futures_data: Optional[Dict[str, Any]] = None,
    okx_rest_url: str = "https://www.okx.com",
    fetch_okx_json: Optional[Callable[[str], Any]] = None,
    now_utc: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Sinh kế hoạch giao dịch phái sinh BTC-USDT-SWAP theo quy tắc minh bạch:
      - Kiểm định dữ liệu đầu vào nghiêm ngặt trước khi tạo LONG/SHORT.
      - ATR chỉ sử dụng nến đã đóng confirm='1', liên tục và không bị cũ.
      - Trả action='CHỜ' khi dữ liệu thiếu, quá cũ hoặc chưa đủ điều kiện.
      - entry_price, stop_loss, take_profit, risk_reward_ratio được set là null khi CHỜ.
      - Đảm bảo JSON serialize an toàn, không chứa NaN hay Infinity.
    """
    disclaimer_text = (
        "Kế hoạch giao dịch mang tính tham khảo thử nghiệm quy tắc, kết hợp mô hình XGBoost Spot và dòng lệnh Order Flow. "
        "Không phải lời khuyên đầu tư tài chính. Quản trị vốn độc lập."
    )

    # Chuẩn hóa mốc thời gian đánh giá hiện tại
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    elif now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    else:
        now_utc = now_utc.astimezone(timezone.utc)

    base_response: Dict[str, Any] = {
        "status": "success",
        "instrument": "BTC-USDT-SWAP",
        "instrument_type": "Perpetual Swap (Futures)",
        "underlying_spot": "BTC-USDT",
        "action": "CHỜ",
        "action_label": "CHỜ (WAIT)",
        "entry_price": None,
        "stop_loss": None,
        "take_profit": None,
        "risk_reward_ratio": None,
        "valid_until": None,
        "time_remaining_seconds": None,
        "reason_code": "INITIALIZING",
        "reason": "",
        "disclaimer": disclaimer_text,
        "market_data": {
            "futures_price": None,
            "spot_price": None,
            "basis_usd": None,
            "atr_15m": None,
            "atr_source": "OKX 15m closed candles"
        },
        "model_context": {},
        "orderflow_context": {}
    }

    # 1. Lấy dữ liệu futures nếu chưa được truyền vào
    if futures_data is None:
        futures_data = fetch_futures_market_data(okx_rest_url, fetch_okx_json, now_utc=now_utc)

    # 2. Kiểm tra dữ liệu thị trường Futures (Giá, Ticker Timestamp, Độ cũ)
    is_valid_f, code_f, msg_f, futures_price, _ = _validate_futures_data(futures_data, now_utc)
    if not is_valid_f or futures_price is None:
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason_code"] = code_f
        base_response["reason"] = msg_f
        return base_response

    base_response["market_data"]["futures_price"] = futures_price

    # 3. Kiểm tra dữ liệu dự báo ML (Xác suất, Horizon, Expiration, 50% Uncertainty)
    is_valid_p, code_p, msg_p, pred_ctx = _validate_prediction_data(prediction_data, now_utc)
    if not is_valid_p:
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason_code"] = code_p
        base_response["reason"] = msg_p
        if pred_ctx:
            base_response["valid_until"] = pred_ctx.get("target_timestamp")
            base_response["time_remaining_seconds"] = pred_ctx.get("time_remaining_seconds")
            base_response["model_context"] = {
                k: pred_ctx[k] for k in ("direction_label", "probability_up", "probability_not_up", "target_timestamp")
                if k in pred_ctx
            }
        return base_response

    base_response["model_context"] = {
        "direction_label": pred_ctx.get("direction_label"),
        "probability_up": pred_ctx.get("probability_up"),
        "probability_not_up": pred_ctx.get("probability_not_up"),
        "target_timestamp": pred_ctx.get("target_timestamp"),
        "model_version": pred_ctx.get("model_version")
    }
    base_response["valid_until"] = pred_ctx.get("target_timestamp")
    base_response["time_remaining_seconds"] = pred_ctx.get("time_remaining_seconds")

    spot_price = pred_ctx.get("spot_price")
    if _is_finite_positive_number(spot_price):
        spot_p_float = float(spot_price)
        base_response["market_data"]["spot_price"] = spot_p_float
        base_response["market_data"]["basis_usd"] = round(futures_price - spot_p_float, 2)

    # 4. Kiểm tra dòng lệnh Order Flow (Status 'active', is_connected, Imbalance, Ratio, Trade count)
    is_valid_of, code_of, msg_of, of_ctx = _validate_orderflow_data(orderflow_data)
    base_response["orderflow_context"] = of_ctx
    if not is_valid_of:
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason_code"] = code_of
        base_response["reason"] = msg_of
        return base_response

    # 5. Kiểm tra ATR 15m phục vụ tính Stop Loss / Take Profit
    atr_15m = futures_data.get("atr_15m")
    if not _is_finite_positive_number(atr_15m):
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason_code"] = "INSUFFICIENT_ATR_DATA"
        base_response["reason"] = "Dữ liệu nến 15m không đủ hoặc không hợp lệ để tính biên độ biến động (ATR) cho Stop Loss / Take Profit."
        return base_response

    atr_val = round(float(atr_15m), 2)
    base_response["market_data"]["atr_15m"] = atr_val

    # 6. Đánh giá tín hiệu Confluence giữa mô hình Spot và dòng lệnh Order Flow
    prob_up = pred_ctx["probability_up"]
    prob_not_up = pred_ctx["probability_not_up"]
    imbalance = of_ctx["imbalance"]
    buy_ratio = of_ctx["buy_ratio"]
    sell_ratio = of_ctx["sell_ratio"]

    # Khoảng dừng lỗ R = max(1.2 * ATR, 0.3% giá vào)
    entry_price = round(futures_price, 1)
    min_buffer = entry_price * 0.003
    r_distance = max(1.2 * atr_val, min_buffer)

    # 6.1 Quy tắc LONG: P(UP) >= 0.55 VÀ Imbalance >= 0.0
    if prob_up >= 0.55:
        if imbalance < 0.0:
            base_response["action"] = "CHỜ"
            base_response["action_label"] = "CHỜ (WAIT)"
            base_response["reason_code"] = "CONFLICTING_SIGNALS"
            base_response["reason"] = (
                f"Tín hiệu mâu thuẫn: Mô hình Spot dự báo Tăng (P(UP)={prob_up:.1%}) "
                f"nhưng dòng lệnh 60s đang chịu áp lực bán (Imbalance={imbalance:+.1%})."
            )
            return base_response

        # Thỏa mãn điều kiện LONG
        sl = round(entry_price - r_distance, 1)
        tp = round(entry_price + (1.5 * r_distance), 1)

        # Kiểm tra tính toàn vẹn: SL < Entry < TP
        if not (sl < entry_price < tp):
            base_response["action"] = "CHỜ"
            base_response["action_label"] = "CHỜ (WAIT)"
            base_response["reason_code"] = "INVALID_PRICE_BOUNDS"
            base_response["reason"] = f"Lỗi tính toán giới hạn giá: Không thỏa mãn SL ({sl}) < Entry ({entry_price}) < TP ({tp})."
            return base_response

        base_response["action"] = "LONG"
        base_response["action_label"] = "MUA (LONG)"
        base_response["entry_price"] = entry_price
        base_response["stop_loss"] = sl
        base_response["take_profit"] = tp
        base_response["risk_reward_ratio"] = "1:1.5"
        base_response["reason_code"] = "SUCCESS_LONG"
        base_response["reason"] = (
            f"Đồng thuận xu hướng Tăng: Mô hình Spot dự báo UP (P={prob_up:.1%}) "
            f"cùng dòng lệnh mua chủ động 60s (Imbalance={imbalance:+.1%}, Buy Ratio={buy_ratio:.1%})."
        )
        return base_response

    # 6.2 Quy tắc SHORT: P(NOT_UP) >= 0.55 VÀ Imbalance <= -0.15
    if prob_not_up >= 0.55:
        if imbalance > -0.15:
            base_response["action"] = "CHỜ"
            base_response["action_label"] = "CHỜ (WAIT)"
            base_response["reason_code"] = "INSUFFICIENT_SELL_PRESSURE"
            base_response["reason"] = (
                f"Mô hình báo Không Tăng (P={prob_not_up:.1%}) nhưng dòng lệnh chưa đủ áp lực bán mạnh "
                f"(Imbalance={imbalance:+.1%} > -15%). Thị trường có khả năng đi ngang tích lũy."
            )
            return base_response

        # Thỏa mãn điều kiện SHORT
        sl = round(entry_price + r_distance, 1)
        tp = round(entry_price - (1.5 * r_distance), 1)

        # Kiểm tra tính toàn vẹn: TP < Entry < SL
        if not (tp < entry_price < sl):
            base_response["action"] = "CHỜ"
            base_response["action_label"] = "CHỜ (WAIT)"
            base_response["reason_code"] = "INVALID_PRICE_BOUNDS"
            base_response["reason"] = f"Lỗi tính toán giới hạn giá: Không thỏa mãn TP ({tp}) < Entry ({entry_price}) < SL ({sl})."
            return base_response

        base_response["action"] = "SHORT"
        base_response["action_label"] = "BÁN (SHORT)"
        base_response["entry_price"] = entry_price
        base_response["stop_loss"] = sl
        base_response["take_profit"] = tp
        base_response["risk_reward_ratio"] = "1:1.5"
        base_response["reason_code"] = "SUCCESS_SHORT"
        base_response["reason"] = (
            f"Xác nhận vị thế Bán theo quy tắc chiến lược: Mô hình Spot báo Không Tăng (P={prob_not_up:.1%}) "
            f"kết hợp dòng lệnh bán áp đảo 60s (Imbalance={imbalance:+.1%} <= -15%, Sell Ratio={sell_ratio:.1%})."
        )
        return base_response

    # 6.3 Không đủ điều kiện kích hoạt
    base_response["action"] = "CHỜ"
    base_response["action_label"] = "CHỜ (WAIT)"
    base_response["reason_code"] = "INSUFFICIENT_CONDITIONS"
    base_response["reason"] = f"Không đủ điều kiện vào lệnh (P(UP)={prob_up:.1%}, P(NOT_UP)={prob_not_up:.1%}). Giữ vị thế quan sát."
    return base_response
