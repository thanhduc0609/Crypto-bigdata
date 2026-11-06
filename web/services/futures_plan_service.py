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
     - SL / TP tính từ biến động thực tế: 15m ATR từ các nến đã đóng.
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

import math
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable, List, Tuple


def calculate_15m_atr(candles: List[List[Any]], period: int = 14) -> Optional[float]:
    """
    Tính Average True Range (ATR) từ danh sách nến 15m của OKX.
    Cấu trúc nến OKX: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
    Chỉ lấy các nến đã đóng hoàn tất (confirm == '1' hoặc bỏ nến đầu tiên đang chạy).
    """
    if not candles or len(candles) < 2:
        return None

    # Lọc các nến đã đóng: nếu nến có trường confirm=='0' (đang chạy), ta loại bỏ
    closed_candles = []
    for c in candles:
        if len(c) > 8 and str(c[8]) == "0":
            continue  # Nến chưa đóng
        try:
            high = float(c[2])
            low = float(c[3])
            close = float(c[4])
            if high >= low > 0 and close > 0:
                closed_candles.append((high, low, close))
        except (ValueError, TypeError, IndexError):
            continue

    if len(closed_candles) < 3:
        # Nếu không có cờ confirm chuẩn, ta lấy từ nến index 1 trở đi (bỏ nến đang chạy)
        closed_candles = []
        for c in candles[1:]:
            try:
                high = float(c[2])
                low = float(c[3])
                close = float(c[4])
                if high >= low > 0 and close > 0:
                    closed_candles.append((high, low, close))
            except (ValueError, TypeError, IndexError):
                continue

    if len(closed_candles) < 2:
        return None

    # OKX trả về nến sắp xếp từ mới nhất đến cũ nhất. Đảo ngược để tính theo thứ tự thời gian.
    chrono_candles = list(reversed(closed_candles))

    true_ranges: List[float] = []
    for i in range(1, len(chrono_candles)):
        curr_high, curr_low, _ = chrono_candles[i]
        _, _, prev_close = chrono_candles[i - 1]

        tr = max(
            curr_high - curr_low,
            abs(curr_high - prev_close),
            abs(curr_low - prev_close)
        )
        true_ranges.append(tr)

    if not true_ranges:
        return None

    # Lấy tối đa 'period' giá trị True Range gần nhất
    sample = true_ranges[-period:]
    return round(sum(sample) / len(sample), 2)


def fetch_futures_market_data(
    okx_rest_url: str = "https://www.okx.com",
    fetch_okx_json: Optional[Callable[[str], Any]] = None
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
            futures_price = float(item.get("last", 0))
            ts_val = item.get("ts")
            if ts_val:
                ticker_ts = datetime.fromtimestamp(int(ts_val) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError):
            pass

    # 2. Lấy nến 15m để tính ATR
    candles_url = f"{okx_rest_url}/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=15m&limit=20"
    candles_res = fetch_okx_json(candles_url)
    atr_15m = None

    if candles_res and isinstance(candles_res, dict) and candles_res.get("data"):
        raw_candles = candles_res["data"]
        atr_15m = calculate_15m_atr(raw_candles, period=14)

    return {
        "futures_price": futures_price,
        "ticker_timestamp": ticker_ts,
        "atr_15m": atr_15m
    }


def generate_futures_plan(
    prediction_data: Optional[Dict[str, Any]],
    orderflow_data: Optional[Dict[str, Any]],
    futures_data: Optional[Dict[str, Any]] = None,
    okx_rest_url: str = "https://www.okx.com",
    fetch_okx_json: Optional[Callable[[str], Any]] = None
) -> Dict[str, Any]:
    """
    Sinh kế hoạch giao dịch phái sinh BTC-USDT-SWAP theo quy tắc minh bạch.
    Trả về dict chuẩn với action: 'LONG' | 'SHORT' | 'CHỜ'.
    """
    disclaimer_text = (
        "Kế hoạch giao dịch mang tính tham khảo thử nghiệm quy tắc, kết hợp mô hình XGBoost Spot và dòng lệnh Order Flow. "
        "Không phải lời khuyên đầu tư tài chính. Quản trị vốn độc lập."
    )

    base_response = {
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
        futures_data = fetch_futures_market_data(okx_rest_url, fetch_okx_json)

    futures_price = futures_data.get("futures_price")
    atr_15m = futures_data.get("atr_15m")
    base_response["market_data"]["futures_price"] = futures_price
    base_response["market_data"]["atr_15m"] = atr_15m

    # 2. Kiểm tra dữ liệu giá Futures
    if not futures_price or futures_price <= 0:
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason"] = "Không thể lấy giá hợp đồng BTC-USDT-SWAP từ OKX REST."
        return base_response

    # 3. Phân tích ngữ cảnh dự báo (Prediction context)
    pred = (prediction_data.get("prediction") if isinstance(prediction_data, dict) and "prediction" in prediction_data
            else prediction_data)

    if not pred or not isinstance(pred, dict):
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason"] = "Chưa có dữ liệu dự báo hướng giá từ Data Warehouse."
        return base_response

    prob_up = pred.get("probability_up")
    prob_not_up = pred.get("probability_not_up")
    direction_label = pred.get("direction_label")
    target_ts_str = pred.get("target_timestamp")
    spot_price = pred.get("current_price")

    base_response["model_context"] = {
        "direction_label": direction_label,
        "probability_up": prob_up,
        "probability_not_up": prob_not_up,
        "target_timestamp": target_ts_str,
        "model_version": pred.get("model_version")
    }
    base_response["valid_until"] = target_ts_str

    if spot_price and spot_price > 0:
        base_response["market_data"]["spot_price"] = spot_price
        base_response["market_data"]["basis_usd"] = round(futures_price - spot_price, 2)

    # Kiểm tra thời hạn hiệu lực của dự báo
    now_utc = datetime.now(timezone.utc)
    target_dt = None
    if target_ts_str:
        try:
            # Hỗ trợ cả định dạng kết thúc Z và offset
            clean_ts = target_ts_str.replace("Z", "+00:00")
            target_dt = datetime.fromisoformat(clean_ts)
            if target_dt.tzinfo is None:
                target_dt = target_dt.replace(tzinfo=timezone.utc)
        except Exception:
            target_dt = None

    if target_dt:
        seconds_remaining = int((target_dt - now_utc).total_seconds())
        base_response["time_remaining_seconds"] = max(0, seconds_remaining)
        if seconds_remaining <= 0:
            base_response["action"] = "CHỜ"
            base_response["action_label"] = "CHỜ (WAIT)"
            base_response["reason"] = f"Dự báo gần nhất đã hết hiệu lực mục tiêu (+5m) vào {target_ts_str}. Chờ chu kỳ suy luận mới."
            return base_response
    else:
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason"] = "Thiếu mốc thời gian mục tiêu (target_timestamp) của dự báo."
        return base_response

    # Kiểm tra tính toàn vẹn xác suất
    if prob_up is None or prob_not_up is None:
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason"] = "Thiếu xác suất dự báo từ mô hình."
        return base_response

    # Kiểm tra phân tách xác suất (sát ngưỡng 50%)
    if 0.48 <= prob_up <= 0.52:
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason"] = f"Xác suất mô hình ({prob_up:.2%} UP vs {prob_not_up:.2%} NOT_UP) sát ngưỡng 50%, hai lớp chưa phân tách rõ."
        return base_response

    # 4. Phân tích ngữ cảnh dòng lệnh (Order Flow context)
    of = orderflow_data if isinstance(orderflow_data, dict) else {}
    of_status = of.get("status", of.get("state", "disconnected"))
    imbalance = of.get("imbalance")
    buy_ratio = of.get("buy_ratio")
    sell_ratio = of.get("sell_ratio")
    trade_count = of.get("trade_count", 0)

    base_response["orderflow_context"] = {
        "status": of_status,
        "imbalance": imbalance,
        "buy_ratio": buy_ratio,
        "sell_ratio": sell_ratio,
        "trade_count": trade_count
    }

    # Nếu Order Flow mất kết nối hoặc dữ liệu cũ
    if of_status in ("disconnected", "stale"):
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason"] = f"Dòng lệnh Order Flow thời gian thực không khả dụng ({of.get('status_text', of_status)})."
        return base_response

    # Kiểm tra ATR để tính SL/TP
    if not atr_15m or atr_15m <= 0:
        base_response["action"] = "CHỜ"
        base_response["action_label"] = "CHỜ (WAIT)"
        base_response["reason"] = "Dữ liệu nến 15m không đủ để tính biên độ biến động (ATR) cho Stop Loss / Take Profit."
        return base_response

    # 5. Đánh giá quy tắc tín hiệu
    # Quy tắc LONG: Mô hình UP (P(UP) >= 0.55) VÀ Imbalance >= 0.0
    # Quy tắc SHORT: Mô hình NOT_UP (P(NOT_UP) >= 0.55) VÀ Imbalance <= -0.15
    action = "CHỜ"
    reason = ""

    if prob_up >= 0.55:
        if imbalance is not None and imbalance < 0.0:
            action = "CHỜ"
            reason = (
                f"Tín hiệu mâu thuẫn: Mô hình Spot dự báo Tăng (P(UP)={prob_up:.1%}) "
                f"nhưng dòng lệnh 60s đang chịu áp lực bán (Imbalance={imbalance:+.1%})."
            )
        elif imbalance is not None and imbalance >= 0.0:
            action = "LONG"
            reason = (
                f"Đồng thuận xu hướng Tăng: Mô hình Spot dự báo UP (P={prob_up:.1%}) "
                f"cùng dòng lệnh mua chủ động 60s (Imbalance={imbalance:+.1%}, Buy Ratio={buy_ratio:.1%})."
            )
        else:
            action = "CHỜ"
            reason = f"Mô hình dự báo Tăng (P={prob_up:.1%}) nhưng chưa đủ dữ liệu dòng lệnh xác nhận."
    elif prob_not_up >= 0.55:
        if imbalance is not None and imbalance <= -0.15:
            action = "SHORT"
            reason = (
                f"Xác nhận vị thế Bán theo quy tắc chiến lược: Mô hình Spot báo Không Tăng (P={prob_not_up:.1%}) "
                f"kết hợp dòng lệnh bán áp đảo 60s (Imbalance={imbalance:+.1%} <= -15%, Sell Ratio={sell_ratio:.1%})."
            )
        elif imbalance is not None and imbalance > -0.15:
            action = "CHỜ"
            reason = (
                f"Mô hình báo Không Tăng (P={prob_not_up:.1%}) nhưng dòng lệnh chưa đủ áp lực bán mạnh "
                f"(Imbalance={imbalance:+.1%} > -15%). Thị trường có khả năng đi ngang tích lũy."
            )
        else:
            action = "CHỜ"
            reason = f"Mô hình báo Không Tăng (P={prob_not_up:.1%}) nhưng thiếu dữ liệu dòng lệnh xác nhận."
    else:
        action = "CHỜ"
        reason = f"Không đủ điều kiện vào lệnh (P(UP)={prob_up:.1%}, P(NOT_UP)={prob_not_up:.1%}). Giữ vị thế quan sát."

    # 6. Tính toán điểm Entry, Stop Loss, Take Profit nếu có lệnh
    entry_price = round(futures_price, 1)
    # Khoảng dừng lỗ R = max(1.2 * ATR, 0.3% giá vào)
    min_buffer = entry_price * 0.003
    r_distance = max(1.2 * atr_15m, min_buffer)

    if action == "LONG":
        sl = round(entry_price - r_distance, 1)
        tp = round(entry_price + (1.5 * r_distance), 1)

        # Kiểm tra tính toàn vẹn: SL < Entry < TP
        if not (sl < entry_price < tp):
            action = "CHỜ"
            reason = f"Lỗi tính toán giới hạn giá: Không thỏa mãn SL ({sl}) < Entry ({entry_price}) < TP ({tp})."
        else:
            base_response["action"] = "LONG"
            base_response["action_label"] = "MUA (LONG)"
            base_response["entry_price"] = entry_price
            base_response["stop_loss"] = sl
            base_response["take_profit"] = tp
            base_response["risk_reward_ratio"] = "1:1.5"
            base_response["reason"] = reason
            return base_response

    elif action == "SHORT":
        sl = round(entry_price + r_distance, 1)
        tp = round(entry_price - (1.5 * r_distance), 1)

        # Kiểm tra tính toàn vẹn: TP < Entry < SL
        if not (tp < entry_price < sl):
            action = "CHỜ"
            reason = f"Lỗi tính toán giới hạn giá: Không thỏa mãn TP ({tp}) < Entry ({entry_price}) < SL ({sl})."
        else:
            base_response["action"] = "SHORT"
            base_response["action_label"] = "BÁN (SHORT)"
            base_response["entry_price"] = entry_price
            base_response["stop_loss"] = sl
            base_response["take_profit"] = tp
            base_response["risk_reward_ratio"] = "1:1.5"
            base_response["reason"] = reason
            return base_response

    base_response["action"] = "CHỜ"
    base_response["action_label"] = "CHỜ (WAIT)"
    base_response["reason"] = reason
    return base_response

