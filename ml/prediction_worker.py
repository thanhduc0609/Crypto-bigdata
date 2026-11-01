"""
=============================================================================
Crypto Big Data - Machine Learning: Background Prediction & Evaluation Worker
=============================================================================
Thực hiện tác vụ dự báo và đối chiếu tự động chạy ngầm:
  - Kiểm tra nến mới đã đóng mỗi 15 giây.
  - Mỗi feature_timestamp chỉ suy luận duy nhất một lần.
  - Tự động đối chiếu kết quả thực tế khi nến mục tiêu đã đóng (target + 60s).
  - Không chạy trùng lặp; an toàn trong môi trường đa luồng / web process.
=============================================================================
"""

import sys
import time
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ml.predict import (
    run_prediction_detailed,
    evaluate_unresolved_predictions,
    get_last_prediction_rejection
)

class PredictionWorker:
    def __init__(self, check_interval_seconds: int = 15, interval_seconds: Optional[int] = None):
        if interval_seconds is not None:
            check_interval_seconds = interval_seconds
        self.check_interval_seconds = check_interval_seconds
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Telemetry & Status
        self.last_check_at: Optional[datetime] = None
        self.last_success_at: Optional[datetime] = None
        self.last_candle_timestamp: Optional[datetime] = None
        self.last_predicted_candle_timestamp: Optional[datetime] = None
        self.state: str = "idle"  # "idle" | "inferring" | "rejected" | "waiting_new_candle"
        self.last_rejection_code: Optional[str] = None
        self.last_rejection_message: Optional[str] = None
        self.total_predictions: int = 0
        self.total_evaluations: int = 0

    def start(self):
        with self._lock:
            if self.running or (self._thread and self._thread.is_alive()):
                return
            self.running = True
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, name="BackgroundPredictionWorker", daemon=True)
            self._thread.start()
            print(f"[*] Background Prediction & Evaluation Worker started (interval: {self.check_interval_seconds}s).")

    def stop(self):
        with self._lock:
            self.running = False
            self._stop_event.set()

    def _run_loop(self):
        while not self._stop_event.is_set():
            try:
                self.perform_cycle()
            except Exception as e:
                print(f"[!] PredictionWorker unhandled exception: {e}")
                self.state = "error"
                self.last_rejection_code = "WORKER_ERROR"
                self.last_rejection_message = str(e)

            # Sleep with interruptible wait
            self._stop_event.wait(self.check_interval_seconds)

    def perform_cycle(self) -> Dict[str, Any]:
        """Thực thi một chu kỳ kiểm tra nến mới, suy luận và đối chiếu kết quả"""
        now_utc = datetime.now(timezone.utc)
        self.last_check_at = now_utc

        # 1. Thực hiện suy luận dự báo
        self.state = "inferring"
        res = run_prediction_detailed(check_duplicate=True)

        pred_data = res.get("prediction") or {}
        feat_ts = pred_data.get("feature_timestamp")
        if feat_ts:
            if isinstance(feat_ts, str):
                try:
                    feat_ts = datetime.fromisoformat(feat_ts.replace("Z", "+00:00"))
                except Exception:
                    pass
            self.last_candle_timestamp = feat_ts

        if res.get("success"):
            if res.get("already_predicted"):
                self.state = "waiting_new_candle"
                self.last_rejection_code = None
                self.last_rejection_message = "Nến 1 phút gần nhất đã được dự báo. Đang chờ nến tiếp theo đóng cửa."
                if feat_ts:
                    self.last_predicted_candle_timestamp = feat_ts
            else:
                self.state = "waiting_new_candle"
                self.last_success_at = now_utc
                self.last_rejection_code = None
                self.last_rejection_message = None
                self.total_predictions += 1
                if feat_ts:
                    self.last_predicted_candle_timestamp = feat_ts
        else:
            self.state = "rejected"
            self.last_rejection_code = res.get("code")
            self.last_rejection_message = res.get("message")

        # Ghi log ngắn gọn cho mỗi lần kiểm tra theo quy định
        now_vn = now_utc.astimezone(timezone(timedelta(hours=7)))
        check_time_str = now_vn.strftime("%H:%M:%S")
        candle_str = self.last_candle_timestamp.strftime("%H:%M:%S") if self.last_candle_timestamp else "—"
        pred_str = self.last_predicted_candle_timestamp.strftime("%H:%M:%S") if self.last_predicted_candle_timestamp else "—"

        if res.get("success") and not res.get("already_predicted"):
            p = res.get("prediction", {})
            dir_lbl = p.get("direction_label", "—")
            prob = round(float(p.get("probability_up", 0.0)) * 100, 1)
            px = p.get("current_price", 0.0)
            print(f"[WORKER {check_time_str}] Nến: {candle_str} | Dự báo: {pred_str} | DỰ BÁO THÀNH CÔNG: {dir_lbl} (P(UP)={prob}%, Giá={px:,.2f} USDT)")
        elif res.get("already_predicted"):
            print(f"[WORKER {check_time_str}] Nến: {candle_str} | Dự báo: {pred_str} | Đang chờ nến mới (nến gần nhất đã xử lý)")
        else:
            print(f"[WORKER {check_time_str}] Nến: {candle_str} | Dự báo: {pred_str} | TỪ CHỐI [{self.last_rejection_code}]: {self.last_rejection_message}")

        # 2. Tự động đối chiếu các dự báo chưa có kết quả
        try:
            evaluated = evaluate_unresolved_predictions()
            if evaluated > 0:
                self.total_evaluations += evaluated
        except Exception as eval_err:
            print(f"[!] Note on automated outcome evaluation: {eval_err}")

        return self.get_status()

    def get_status(self) -> Dict[str, Any]:
        """Trả về trạng thái chi tiết của worker phục vụ API và UI"""
        return {
            "worker_running": self.running,
            "check_interval_seconds": self.check_interval_seconds,
            "interval_seconds": self.check_interval_seconds,
            "state": self.state,
            "last_check_at": self.last_check_at.strftime("%Y-%m-%dT%H:%M:%SZ") if self.last_check_at else None,
            "last_success_at": self.last_success_at.strftime("%Y-%m-%dT%H:%M:%SZ") if self.last_success_at else None,
            "last_candle_at": self.last_candle_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") if self.last_candle_timestamp else None,
            "last_predicted_candle_at": self.last_predicted_candle_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") if self.last_predicted_candle_timestamp else None,
            "last_rejection_code": self.last_rejection_code,
            "last_rejection_message": self.last_rejection_message,
            "total_predictions": self.total_predictions,
            "total_evaluations": self.total_evaluations
        }

# Global singleton worker instance
_global_prediction_worker: Optional[PredictionWorker] = None
_worker_init_lock = threading.Lock()

def get_prediction_worker(check_interval_seconds: int = 15, interval_seconds: Optional[int] = None) -> PredictionWorker:
    global _global_prediction_worker
    if interval_seconds is not None:
        check_interval_seconds = interval_seconds
    with _worker_init_lock:
        if _global_prediction_worker is None:
            _global_prediction_worker = PredictionWorker(check_interval_seconds=check_interval_seconds)
            _global_prediction_worker.start()
        return _global_prediction_worker

