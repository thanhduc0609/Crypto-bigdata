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
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Optional

try:
    from filelock import FileLock, Timeout
except ImportError:
    class Timeout(Exception):
        """Fallback Timeout exception when filelock package is not installed."""
        pass

    class FileLock:
        """Fallback process/thread lock when filelock package is not installed."""
        def __init__(self, lock_file: str, timeout: float = 0.1):
            self.lock_file = lock_file
            self.timeout = timeout
            self._thread_lock = threading.Lock()

        def acquire(self, timeout: Optional[float] = None):
            t = self.timeout if timeout is None else timeout
            acquired = self._thread_lock.acquire(blocking=True, timeout=t if t > 0 else -1)
            if not acquired:
                raise Timeout(f"Timeout acquiring lock on {self.lock_file}")
            return self

        def release(self):
            if self._thread_lock.locked():
                self._thread_lock.release()

        def __enter__(self):
            return self.acquire()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.release()

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from ml.realtime.predict import (
    run_prediction_detailed,
    evaluate_unresolved_predictions,
    get_last_prediction_rejection
)

WORKER_LOCK_FILE = project_root / "ml" / "prediction_worker.lock"


def _format_iso_utc(dt: Optional[datetime]) -> Optional[str]:
    """Serialize datetime to ISO UTC format ending with Z, ensuring timezone awareness."""
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class PredictionWorker:
    def __init__(self, check_interval_seconds: int = 15, interval_seconds: Optional[int] = None, symbol: str = "BTC-USDT"):
        if interval_seconds is not None:
            check_interval_seconds = interval_seconds
        self.symbol = symbol
        self.check_interval_seconds = check_interval_seconds
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lifecycle_lock = threading.Lock()
        self._cycle_lock = threading.Lock()
        self._file_lock = FileLock(str(WORKER_LOCK_FILE), timeout=0.1)
        self._has_file_lock = False

        # Telemetry & Status
        self.last_check_at: Optional[datetime] = None
        self.last_success_at: Optional[datetime] = None
        self.last_candle_timestamp: Optional[datetime] = None
        self.last_predicted_candle_timestamp: Optional[datetime] = None
        self.state: str = "idle"  # "idle" | "inferring" | "rejected" | "waiting_new_candle" | "error"
        self.last_rejection_code: Optional[str] = None
        self.last_rejection_message: Optional[str] = None
        self.last_evaluation_status: Optional[str] = None
        self.total_predictions: int = 0
        self.total_evaluations: int = 0

    def start(self) -> bool:
        """
        Khởi động worker trong luồng nền daemon.
        Sử dụng filelock để đảm bảo trên 1 máy chỉ có tối đa 1 worker chạy vòng lặp.
        """
        with self._lifecycle_lock:
            if self.running or (self._thread and self._thread.is_alive()):
                return True

            # Thử lấy filelock ở mức process
            try:
                self._file_lock.acquire(timeout=0.1)
                self._has_file_lock = True
            except Timeout:
                print(f"[WARN] PredictionWorker: Tiến trình worker khác đang hoạt động trên máy (lock: {WORKER_LOCK_FILE.name}). Bỏ qua chạy nền.")
                self.state = "blocked_by_external_worker"
                return False
            except Exception as e:
                print(f"[!] Warning acquiring worker lock file: {e}")
                self._has_file_lock = False

            self.running = True
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, name="BackgroundPredictionWorker", daemon=True)
            self._thread.start()
            print(f"[*] Background Prediction & Evaluation Worker started (interval: {self.check_interval_seconds}s).")
            return True

    def stop(self, timeout: float = 2.0):
        """Dừng worker an toàn, giải phóng khóa mà không gây deadlock khi join."""
        self._stop_event.set()
        with self._lifecycle_lock:
            self.running = False

        if self._has_file_lock:
            try:
                self._file_lock.release()
                self._has_file_lock = False
            except Exception:
                pass

        if self._thread and self._thread.is_alive():
            if threading.current_thread() != self._thread:
                self._thread.join(timeout=timeout)

    def _run_loop(self):
        while not self._stop_event.is_set():
            try:
                self.perform_cycle(blocking=True)
            except Exception as e:
                print(f"[!] PredictionWorker unhandled exception: {e}")
                self.state = "error"
                self.last_rejection_code = "WORKER_ERROR"
                self.last_rejection_message = str(e)

            # Sleep with interruptible wait
            self._stop_event.wait(self.check_interval_seconds)

    def perform_cycle(self, blocking: bool = False) -> Dict[str, Any]:
        """
        Thực thi một chu kỳ kiểm tra nến mới, suy luận và đối chiếu kết quả.
        Khóa riêng cho toàn bộ chu kỳ ngăn các lệnh thủ công/tự động chạy chồng chéo.
        """
        acquired = self._cycle_lock.acquire(blocking=blocking)
        if not acquired:
            return {
                "status": "busy",
                "code": "ALREADY_RUNNING",
                "message": "Chu kỳ suy luận khác đang thực thi, vui lòng thử lại sau.",
                "worker": self.get_status()
            }

        try:
            now_utc = datetime.now(timezone.utc)
            self.last_check_at = now_utc

            # 1. Thực hiện suy luận dự báo
            self.state = "inferring"
            try:
                res = run_prediction_detailed(check_duplicate=True)
            except Exception as inf_err:
                self.state = "error"
                self.last_rejection_code = "INFERENCE_EXCEPTION"
                self.last_rejection_message = str(inf_err)
                res = {"success": False, "code": "INFERENCE_EXCEPTION", "message": str(inf_err)}

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
            elif self.state != "error":
                self.state = "rejected"
                self.last_rejection_code = res.get("code")
                self.last_rejection_message = res.get("message")

            # Ghi log ngắn gọn cho mỗi lần kiểm tra (an toàn format)
            try:
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
            except Exception as log_err:
                print(f"[!] Warning formatting worker cycle log: {log_err}")

            # 2. Tự động đối chiếu các dự báo chưa có kết quả (tách biệt lỗi, không làm hỏng kết quả dự báo)
            try:
                evaluated = evaluate_unresolved_predictions()
                if evaluated > 0:
                    self.total_evaluations += evaluated
                    self.last_evaluation_status = f"Evaluated {evaluated} records"
                else:
                    self.last_evaluation_status = "No records ready for evaluation"
            except Exception as eval_err:
                print(f"[!] Warning on automated outcome evaluation: {eval_err}")
                self.last_evaluation_status = f"Evaluation warning: {eval_err}"

            return {
                "status": "success" if res.get("success") else "rejected",
                "prediction_result": res,
                "worker": self.get_status()
            }
        finally:
            self._cycle_lock.release()

    def get_status(self) -> Dict[str, Any]:
        """Trả về trạng thái chi tiết của worker phục vụ API và UI với timestamp chuẩn UTC (Z)"""
        return {
            "worker_running": self.running,
            "check_interval_seconds": self.check_interval_seconds,
            "interval_seconds": self.check_interval_seconds,
            "state": self.state,
            "last_check_at": _format_iso_utc(self.last_check_at),
            "last_success_at": _format_iso_utc(self.last_success_at),
            "last_candle_at": _format_iso_utc(self.last_candle_timestamp),
            "last_predicted_candle_at": _format_iso_utc(self.last_predicted_candle_timestamp),
            "last_rejection_code": self.last_rejection_code,
            "last_rejection_message": self.last_rejection_message,
            "last_evaluation_status": self.last_evaluation_status,
            "total_predictions": self.total_predictions,
            "total_evaluations": self.total_evaluations
        }


# Global singleton worker instance
_global_prediction_worker: Optional[PredictionWorker] = None
_worker_init_lock = threading.Lock()


def get_prediction_worker(
    check_interval_seconds: int = 15,
    interval_seconds: Optional[int] = None,
    autostart: bool = False
) -> PredictionWorker:
    """
    Trả về thể hiện PredictionWorker singleton.
    Mặc định autostart=False để việc import module không tự ý chạy background thread.
    """
    global _global_prediction_worker
    if interval_seconds is not None:
        check_interval_seconds = interval_seconds
    with _worker_init_lock:
        if _global_prediction_worker is None:
            _global_prediction_worker = PredictionWorker(check_interval_seconds=check_interval_seconds)
            if autostart:
                _global_prediction_worker.start()
        return _global_prediction_worker
