"""
=============================================================================
Crypto Big Data - Real-time 60s Order Flow Imbalance Tracker
=============================================================================
Thu thập và duy trì luồng giao dịch thời gian thực cho BTC-USDT:
  - Cửa sổ trượt 60 giây chính xác theo event time UTC: (now - 60s, now].
  - Lọc từng giao dịch theo event time, không phụ thuộc thứ tự nhận (out-of-order safe).
  - Loại bỏ triệt để giao dịch hết hạn và giao dịch có timestamp ở tương lai.
  - Khử trùng theo (symbol, trade_id) trong tập hợp lệ.
  - Kiểm tra tính hợp lệ: giá và size phải hữu hạn (> 0), từ chối NaN/Inf.
  - Hỗ trợ mockable time_provider phục vụ kiểm thử không phụ thuộc sleep.
  - Phân biệt rõ các trạng thái: warming_up, active, no_trades, stale, disconnected, reconnecting, incomplete.
  - Khi volume = 0 hoặc mất kết nối: tỉ lệ và imbalance là None, không tự hiện 50/50.
=============================================================================
"""

import sys
import json
import time
import math
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Callable, List
import websocket


class RealtimeOrderflowTracker:
    def __init__(
        self,
        symbol: str = "BTC-USDT",
        window_seconds: int = 60,
        time_provider: Optional[Callable[[], float]] = None,
        max_buffer_size: int = 10000
    ):
        self.symbol = symbol
        self.window_seconds = window_seconds
        self.time_provider = time_provider if time_provider is not None else time.time
        self.max_buffer_size = max_buffer_size

        # Bounded trade storage
        self.trades: deque = deque(maxlen=max_buffer_size)
        self.lock = threading.Lock()

        self.is_connected = False
        self.is_reconnecting = False
        self.connected_since: Optional[datetime] = None
        self.last_trade_event_at: Optional[datetime] = None
        self.last_message_at: Optional[datetime] = None
        self.buffer_overflow = False

        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._ws_app: Optional[websocket.WebSocketApp] = None

    def start(self):
        """Khởi động daemon thread thu thập giao dịch"""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="RealtimeOrderflowThread", daemon=True)
        self._thread.start()
        print(f"[*] RealtimeOrderflowTracker started for {self.symbol} (60s sliding window).")

    def stop(self):
        """Dừng daemon thread"""
        self._stop_event.set()
        if self._ws_app:
            try:
                self._ws_app.close()
            except Exception:
                pass
        self.is_connected = False
        self.is_reconnecting = False

    def _run_loop(self):
        """Vòng lặp kết nối WebSocket tự động reconnect khi gặp sự cố"""
        while not self._stop_event.is_set():
            try:
                self._connect_and_listen()
            except Exception as e:
                self.is_connected = False
                self.is_reconnecting = True
                print(f"[!] RealtimeOrderflow WebSocket error: {e}")

            if not self._stop_event.is_set():
                time.sleep(3)

    def _connect_and_listen(self):
        def on_open(ws):
            self.is_connected = True
            self.is_reconnecting = False
            now_sec = self.time_provider()
            self.connected_since = datetime.fromtimestamp(now_sec, tz=timezone.utc)
            sub_msg = {
                "op": "subscribe",
                "args": [{"channel": "trades", "instId": self.symbol}]
            }
            ws.send(json.dumps(sub_msg))
            print(f"[+] RealtimeOrderflow WebSocket connected to OKX trades ({self.symbol}).")

        def on_message(ws, message):
            now_sec = self.time_provider()
            self.last_message_at = datetime.fromtimestamp(now_sec, tz=timezone.utc)
            try:
                data = json.loads(message)
                if "data" in data and isinstance(data["data"], list):
                    for item in data["data"]:
                        self.process_raw_trade_item(item)
            except Exception:
                pass

        def on_error(ws, error):
            self.is_connected = False
            self.is_reconnecting = True
            print(f"[!] RealtimeOrderflow WebSocket stream error: {error}")

        def on_close(ws, close_status_code, close_msg):
            self.is_connected = False
            self.is_reconnecting = True
            print(f"[-] RealtimeOrderflow WebSocket closed ({close_status_code}: {close_msg}). Reconnecting...")

        self._ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        self._ws_app.run_forever(ping_interval=20, ping_timeout=10)

    def add_trade(self, trade_or_item: Dict[str, Any]) -> bool:
        """Helper to add trade either in raw OKX format or normalized dict."""
        return self.process_raw_trade_item(trade_or_item)

    def process_raw_trade_item(self, item: Dict[str, Any], symbol: Optional[str] = None) -> bool:
        """
        Xác thực và nạp một bản ghi giao dịch đơn lẻ vào bộ đệm:
          - Ký hiệu giao dịch phải là BTC-USDT.
          - trade_id không được rỗng.
          - Giá và kích thước phải là số thực hữu hạn, dương (> 0).
          - Side phải là 'buy' hoặc 'sell'.
        """
        target_symbol = symbol or item.get("instId") or item.get("symbol") or self.symbol
        if target_symbol != self.symbol:
            return False

        try:
            trade_id = str(item.get("tradeId") or item.get("trade_id") or "").strip()
            if not trade_id:
                return False

            px_raw = item.get("px") if "px" in item else item.get("price")
            sz_raw = item.get("sz") if "sz" in item else item.get("size")
            if px_raw is None or sz_raw is None:
                return False
            px = float(px_raw)
            sz = float(sz_raw)
            if not (math.isfinite(px) and px > 0 and math.isfinite(sz) and sz > 0):
                return False

            side = str(item.get("side", "")).strip().lower()
            if side not in ("buy", "sell"):
                return False

            if "ts" in item:
                ts_ms = int(item["ts"])
                event_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            elif "event_time" in item:
                et = item["event_time"]
                if isinstance(et, (int, float)):
                    event_time = datetime.fromtimestamp(et, tz=timezone.utc)
                elif isinstance(et, datetime):
                    event_time = et if et.tzinfo else et.replace(tzinfo=timezone.utc)
                else:
                    return False
            else:
                return False

            with self.lock:
                now_sec = self.time_provider()
                now_utc = datetime.fromtimestamp(now_sec, tz=timezone.utc)
                cutoff_time = now_utc - timedelta(seconds=self.window_seconds)

                # Kiểm tra tràn bộ đệm làm mất giao dịch còn trong cửa sổ
                if len(self.trades) >= self.max_buffer_size:
                    oldest = self.trades[0]
                    if oldest["event_time"] > cutoff_time:
                        self.buffer_overflow = True

                self.trades.append({
                    "trade_id": trade_id,
                    "symbol": self.symbol,
                    "price": px,
                    "size": sz,
                    "side": side,
                    "event_time": event_time,
                    "value": px * sz
                })
                if self.last_trade_event_at is None or event_time > self.last_trade_event_at:
                    self.last_trade_event_at = event_time
            return True
        except (ValueError, KeyError, TypeError):
            return False

    def get_orderflow_metrics(self) -> Dict[str, Any]:
        """
        Tính toán cán cân mua/bán trong cửa sổ 60 giây trượt theo event time UTC:
          - Biên cửa sổ: (now - 60s, now].
          - Lọc từng giao dịch độc lập, không giả định thứ tự đến.
          - Loại bỏ giao dịch quá hạn (<= now - 60s) và giao dịch tương lai (> now).
          - Khử trùng lặp theo trade_id trong tập hợp lệ.
          - Tính toán trên snapshot nhất quán được chụp trong lock.
        """
        now_sec = self.time_provider()
        now_utc = datetime.fromtimestamp(now_sec, tz=timezone.utc)
        cutoff_time = now_utc - timedelta(seconds=self.window_seconds)

        with self.lock:
            # 1. Chụp snapshot nhất quán và dọn các giao dịch đã quá hạn
            raw_snapshot = list(self.trades)

            # Lọc lại bộ đệm lưu trữ để giải phóng bộ nhớ khi thời gian tiến lên
            retained_buffer = [t for t in self.trades if t["event_time"] > cutoff_time]
            self.trades = deque(retained_buffer, maxlen=self.max_buffer_size)

        # 2. Lọc từng giao dịch theo biên cửa sổ: (now - 60s, now]
        # Bỏ giao dịch quá hạn (<= cutoff_time) và giao dịch tương lai (> now_utc)
        valid_window_trades = []
        seen_ids = set()

        for t in raw_snapshot:
            ev_time = t["event_time"]
            if ev_time <= cutoff_time or ev_time > now_utc:
                continue

            tid = t["trade_id"]
            if tid in seen_ids:
                continue
            seen_ids.add(tid)
            valid_window_trades.append(t)

        # 3. Tính toán các chỉ số khối lượng
        buy_volume = sum(t["size"] for t in valid_window_trades if t["side"] == "buy")
        sell_volume = sum(t["size"] for t in valid_window_trades if t["side"] == "sell")
        total_volume = buy_volume + sell_volume
        trade_count = len(valid_window_trades)

        if total_volume > 0:
            buy_ratio = round(buy_volume / total_volume, 4)
            sell_ratio = round(sell_volume / total_volume, 4)
            imbalance = round((buy_volume - sell_volume) / total_volume, 4)
        else:
            buy_ratio = None
            sell_ratio = None
            imbalance = None

        # 4. Xác định trạng thái kết nối & độ tin cậy dữ liệu
        uptime_seconds = (now_utc - self.connected_since).total_seconds() if self.connected_since and self.is_connected else 0

        if not self.is_connected:
            status = "disconnected"
            status_text = "Mất kết nối luồng giao dịch"
        elif self.is_reconnecting:
            status = "reconnecting"
            status_text = "Đang kết nối lại luồng giao dịch (cửa sổ có thể gián đoạn)"
        elif self.last_message_at and (now_utc - self.last_message_at).total_seconds() > 15:
            status = "stale"
            status_text = "Dữ liệu có dấu hiệu gián đoạn (> 15s không có tín hiệu)"
        elif uptime_seconds < self.window_seconds:
            status = "warming_up"
            status_text = f"Đang tích lũy cửa sổ ({int(uptime_seconds)}s / {self.window_seconds}s)"
        elif self.buffer_overflow:
            status = "incomplete"
            status_text = "Tràn bộ đệm, dữ liệu cửa sổ có thể chưa đầy đủ"
        elif trade_count == 0:
            status = "no_trades"
            status_text = "Không có giao dịch trong 60 giây qua"
        else:
            status = "active"
            status_text = "Đang nhận dữ liệu thời gian thực"

        coverage_sec = min(self.window_seconds, max(0, int(uptime_seconds))) if self.is_connected else 0

        return {
            "symbol": self.symbol,
            "source": "okx_public_ws",
            "source_label": "OKX WebSocket Trực tiếp",
            "status": status,
            "state": status,
            "status_text": status_text,
            "is_connected": self.is_connected,
            "window_start": cutoff_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_end": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "calculated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "latest_event_at": self.last_trade_event_at.strftime("%Y-%m-%dT%H:%M:%SZ") if self.last_trade_event_at else None,
            "coverage_seconds": coverage_sec,
            "trade_count": trade_count,
            "buy_volume": round(buy_volume, 4) if total_volume > 0 else 0.0,
            "sell_volume": round(sell_volume, 4) if total_volume > 0 else 0.0,
            "total_volume": round(total_volume, 4) if total_volume > 0 else 0.0,
            "buy_ratio": buy_ratio,
            "sell_ratio": sell_ratio,
            "imbalance": imbalance
        }


# Global singleton tracker instance
_global_orderflow_tracker: Optional[RealtimeOrderflowTracker] = None
_orderflow_init_lock = threading.Lock()


def get_orderflow_tracker(
    symbol: str = "BTC-USDT",
    window_seconds: int = 60,
    autostart: bool = False
) -> RealtimeOrderflowTracker:
    """
    Trả về thể hiện RealtimeOrderflowTracker singleton.
    Mặc định autostart=False để import module không tự mở kết nối mạng.
    """
    global _global_orderflow_tracker
    with _orderflow_init_lock:
        if _global_orderflow_tracker is None:
            _global_orderflow_tracker = RealtimeOrderflowTracker(symbol=symbol, window_seconds=window_seconds)
            if autostart:
                _global_orderflow_tracker.start()
        return _global_orderflow_tracker
