"""
=============================================================================
Crypto Big Data - Storage Layer: Exclusive Gold Write Lock
=============================================================================
Provides cross-process mutual exclusion for Gold layer mutations.
Guarantees:
  - Exclusive access acquired BEFORE reading existing Gold state.
  - OS-level atomic file locking (msvcrt on Windows, fcntl on POSIX).
  - Uses `filelock` when available, with built-in standard library fallback.
  - Configurable timeout; raises GoldLockError immediately on timeout/failure.
  - No check-then-create race conditions (no exists() -> touch()).
  - No arbitrary age-based lock breaking.
  - Only the lock owner can release the lock.

SCOPE AND LIMITATION:
  This lock coordinates processes running on the local host (or shared filesystem).
  It does NOT provide distributed locking across multi-node independent S3 compute nodes
  (which would require a centralized lock coordinator such as AWS DynamoDB Lock Client).
=============================================================================
"""

import os
import sys
import time
from pathlib import Path
from typing import Optional

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

try:
    import filelock
    HAS_FILELOCK = True
except ImportError:
    HAS_FILELOCK = False

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


class GoldLockError(RuntimeError):
    """Raised when an exclusive Gold write lock cannot be acquired or maintained."""
    pass


class GoldLock:
    """
    Exclusive OS-level write lock protecting Gold read-modify-write workflows.
    Works seamlessly with either the `filelock` library or built-in OS primitives.
    """

    def __init__(self, target_path: str, timeout: float = 30.0, timeout_seconds: Optional[float] = None):
        self.target_path = str(target_path)
        effective_timeout = timeout_seconds if timeout_seconds is not None else timeout
        self.timeout = float(effective_timeout)
        self.is_s3 = self.target_path.startswith("s3a://") or self.target_path.startswith("s3://")

        if self.is_s3:
            lock_dir = project_root / "data" / "locks"
            safe_name = self.target_path.replace("s3a://", "").replace("s3://", "").replace("/", "_")
        else:
            p = Path(self.target_path)
            lock_dir = p.parent / ".locks"
            safe_name = p.name.replace("=", "_")

        lock_dir.mkdir(parents=True, exist_ok=True)
        self.lock_file = lock_dir / f"{safe_name}.lock"
        self._is_owner = False
        self._fd = None

        if HAS_FILELOCK:
            self._lock = filelock.FileLock(str(self.lock_file), timeout=self.timeout)
        else:
            self._lock = None

    def acquire(self):
        """Acquires exclusive lock or raises GoldLockError."""
        if HAS_FILELOCK and self._lock is not None:
            try:
                self._lock.acquire(timeout=self.timeout)
                self._is_owner = True
                return self
            except filelock.Timeout as te:
                raise GoldLockError(
                    f"Timed out after {self.timeout}s waiting for exclusive Gold lock at {self.lock_file}. "
                    "Another job is actively reading/publishing Gold."
                ) from te
            except Exception as e:
                raise GoldLockError(f"Failed to acquire exclusive Gold lock at {self.lock_file}: {e}") from e
        else:
            # Standard Library Fallback (msvcrt on Windows, fcntl on POSIX)
            start_time = time.time()
            poll_interval = 0.05
            while True:
                try:
                    self._fd = open(self.lock_file, "a+")
                    if sys.platform == "win32":
                        msvcrt.locking(self._fd.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._is_owner = True
                    return self
                except (IOError, OSError):
                    if self._fd is not None:
                        try:
                            self._fd.close()
                        except Exception:
                            pass
                        self._fd = None

                    elapsed = time.time() - start_time
                    if elapsed >= self.timeout:
                        raise GoldLockError(
                            f"Timed out after {self.timeout}s waiting for exclusive Gold lock at {self.lock_file}. "
                            "Another job is actively reading/publishing Gold."
                        )
                    time.sleep(poll_interval)

    def release(self):
        """Releases exclusive lock only if held by this owner."""
        if not self._is_owner:
            raise GoldLockError(
                f"Cannot release lock: not the lock owner or lock not acquired ({self.lock_file})"
            )
        try:
            if HAS_FILELOCK and self._lock is not None:
                self._lock.release()
            else:
                if self._fd is not None:
                    try:
                        if sys.platform == "win32":
                            msvcrt.locking(self._fd.fileno(), msvcrt.LK_UNLCK, 1)
                        else:
                            fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
                    finally:
                        self._fd.close()
                        self._fd = None
            self._is_owner = False
        except Exception as e:
            raise GoldLockError(f"Error releasing exclusive Gold lock at {self.lock_file}: {e}") from e

    @property
    def is_owner(self) -> bool:
        return self._is_owner

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._is_owner:
            self.release()
