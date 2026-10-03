"""One maintenance worker with bounded admission and duplicate suppression."""
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from time import monotonic
from shared.shutdown import background_work, is_shutdown_requested

logger = logging.getLogger(__name__)


class MaintenanceExecutor:
    def __init__(self, capacity=8):
        if capacity < 1:
            raise ValueError("Maintenance capacity must be positive")
        self._capacity = capacity
        self._pending = set()
        self._lock = Lock()
        self._closed = False
        self._stop = Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="maintenance")

    def submit(self, key, action):
        with self._lock:
            if self._closed or key in self._pending or len(self._pending) >= self._capacity:
                logger.warning("Maintenance submission skipped job=%s closed=%s duplicate=%s pending=%s",
                               key, self._closed, key in self._pending, len(self._pending))
                return False
            self._pending.add(key)
            try:
                future = self._executor.submit(self._run, key, action, monotonic())
            except BaseException:
                self._pending.remove(key)
                raise
        future.add_done_callback(lambda _: self._release(key))
        return True

    def _run(self, key, action, submitted_at):
        started = monotonic()
        logger.info("Maintenance started job=%s queue_wait_s=%.3f", key, started - submitted_at)
        try:
            with background_work(self._stop):
                if is_shutdown_requested():
                    raise KeyboardInterrupt()
                action()
        except KeyboardInterrupt:
            logger.info("Maintenance interrupted job=%s", key)
        except Exception:
            logger.exception("Maintenance failed job=%s duration_s=%.3f", key, monotonic() - started)
        else:
            logger.info("Maintenance completed job=%s duration_s=%.3f", key, monotonic() - started)

    def _release(self, key):
        with self._lock:
            self._pending.discard(key)

    def shutdown(self):
        with self._lock:
            self._closed = True
            self._stop.set()
        # Cancel queued work; let the active job finish its transaction safely.
        self._executor.shutdown(wait=True, cancel_futures=True)
