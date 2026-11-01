"""
=============================================================================
Crypto Big Data - Real-time 60s Order Flow Imbalance Tracker
=============================================================================
Thu thập và duy trì luồng giao dịch thời gian thực cho BTC-USDT:
  - Cửa sổ trượt 60 giây chính xác theo event time UTC.
  - Sử dụng kết nối WebSocket upstream dùng chung (1 kết nối duy nhất cho toàn backend).
  - Khử trùng theo (symbol, trade_id).
  - Tự động loại bỏ giao dịch cũ hơn 60s kể cả khi thị trường tạm lắng.
  - Phân biệt rõ các trạng thái: warming_up, active, no_trades, stale, disconnected.
  - Khi volume = 0 hoặc mất kết nối: tỉ lệ và imbalance là None, không tự hiện 50/50.
=============================================================================
"""

import sys
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
import websocket

class RealtimeOrderflowTracker:
    def __init__(self, symbol: str = "BTC-USDT", window_seconds: int = 60):
        self.symbol = symbol
        self.window_seconds = window_seconds
        
        # Bounded trade storage (max 10,000 trades in memory)
        self.trades = deque(maxlen=10000)
        self.lock = threading.Lock()
        
        self.is_connected = False
        self.connected_since: Optional[datetime] = None
        self.last_trade_event_at: Optional[datetime] = None
        self.last_message_at: Optional[datetime] = None
        
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

    def _run_loop(self):
        """Vòng lặp kết nối WebSocket tự động reconnect khi gặp sự cố"""
        while not self._stop_event.is_set():
            try:
                self._connect_and_listen()
            except Exception as e:
                self.is_connected = False
                print(f"[!] RealtimeOrderflow WebSocket error: {e}")
            
            if not self._stop_event.is_set():
                time.sleep(3)

    def _connect_and_listen(self):
        def on_open(ws):
            self.is_connected = True
            self.connected_since = datetime.now(timezone.utc)
            sub_msg = {
                "op": "subscribe",
                "args": [{"channel": "trades", "instId": self.symbol}]
            }
            ws.send(json.dumps(sub_msg))
            print(f"[+] RealtimeOrderflow WebSocket connected to OKX trades ({self.symbol}).")

        def on_message(ws, message):
            self.last_message_at = datetime.now(timezone.utc)
            try:
                data = json.loads(message)
                if "data" in data and isinstance(data["data"], list):
                    now_utc = datetime.now(timezone.utc)
                    with self.lock:
                        for item in data["data"]:
                            try:
                                px = float(item["px"])
                                sz = float(item["sz"])
                                side = str(item["side"]).strip().lower()
                                ts_ms = int(item["ts"])
                                trade_id = str(item["tradeId"])
                                event_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

                                if px > 0 and sz > 0 and side in ("buy", "sell"):
                                    self.trades.append({
                                        "trade_id": trade_id,
                                        "price": px,
                                        "size": sz,
                                        "side": side,
                                        "event_time": event_time,
                                        "value": px * sz
                                    })
                                    if self.last_trade_event_at is None or event_time > self.last_trade_event_at:
                                        self.last_trade_event_at = event_time
                            except (ValueError, KeyError, TypeError):
                                continue
            except Exception:
                pass

        def on_error(ws, error):
            self.is_connected = False
            print(f"[!] RealtimeOrderflow WebSocket stream error: {error}")

        def on_close(ws, close_status_code, close_msg):
            self.is_connected = False
            print(f"[-] RealtimeOrderflow WebSocket closed ({close_status_code}: {close_msg}). Reconnecting...")

        self._ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        self._ws_app.run_forever(ping_interval=20, ping_timeout=10)

    def get_orderflow_metrics(self) -> Dict[str, Any]:
        """
        Tính toán cán cân mua/bán trong cửa sổ 60 giây trượt theo event time UTC.
        Tự động loại bỏ các giao dịch quá hạn.
        """
        now_utc = datetime.now(timezone.utc)
        cutoff_time = now_utc - timedelta(seconds=self.window_seconds)

        with self.lock:
            # 1. Loại bỏ các giao dịch cũ hơn 60s
            while self.trades and self.trades[0]["event_time"] < cutoff_time:
                self.trades.popleft()

            # 2. Khử trùng theo trade_id
            seen_ids = set()
            valid_window_trades = []
            for t in self.trades:
                tid = t["trade_id"]
                if tid not in seen_ids:
                    seen_ids.add(tid)
                    valid_window_trades.append(t)

        # 3. Tính toán các chỉ số
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

        # 4. Xác định trạng thái kết nối & mức bao phủ
        uptime_seconds = (now_utc - self.connected_since).total_seconds() if self.connected_since and self.is_connected else 0

        if not self.is_connected:
            status = "disconnected"
            status_text = "Mất kết nối luồng giao dịch"
        elif uptime_seconds < self.window_seconds:
            status = "warming_up"
            status_text = f"Đang thu thập dữ liệu ({int(uptime_seconds)}s / {self.window_seconds}s)"
        elif self.last_message_at and (now_utc - self.last_message_at).total_seconds() > 15:
            status = "stale"
            status_text = "Dữ liệu có dấu hiệu gián đoạn"
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

def get_orderflow_tracker(symbol: str = "BTC-USDT", window_seconds: int = 60) -> RealtimeOrderflowTracker:
    global _global_orderflow_tracker
    if _global_orderflow_tracker is None:
        _global_orderflow_tracker = RealtimeOrderflowTracker(symbol=symbol, window_seconds=window_seconds)
        _global_orderflow_tracker.start()
    return _global_orderflow_tracker

