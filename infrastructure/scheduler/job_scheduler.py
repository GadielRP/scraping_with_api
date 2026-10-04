"""Clock dispatch only; jobs execute on assigned bounded workers."""

import logging
from threading import Event, Lock, Thread
from time import monotonic
from shared.execution_context import Priority
import schedule
from .contracts import Admission

logger = logging.getLogger(__name__)


class JobScheduler:
    def __init__(self, executors):
        self.executors = executors
        self.clock = schedule.Scheduler()
        self._stop = Event()
        self._thread = None
        self._retry = {}
        self._retry_lock = Lock()
        self._retry_capacity = sum(executor.capacity for executor in executors.values())
        for executor in executors.values():
            executor.on_deferred = self.defer

    def _remember(self, request, delay):
        with self._retry_lock:
            if self._stop.is_set():
                return
            if request.key not in self._retry and len(self._retry) >= self._retry_capacity:
                logger.error("Job retry capacity exhausted job=%s", request.key)
                return
            existing = self._retry.get(request.key)
            if not existing or request.scheduled_at >= existing[0].scheduled_at:
                self._retry[request.key] = (request, monotonic() + delay)

    def defer(self, request):
        # Critical work is recomputed by the next tick; only maintenance resumes
        # the exact date/slot it could not finish.
        if request.priority == Priority.MAINTENANCE:
            self._remember(request, 60)

    def dispatch(self, request):
        status = self.executors[request.priority].submit(request)
        if status == Admission.FULL:
            self._remember(request, 1)
        else:
            with self._retry_lock:
                self._retry.pop(request.key, None)
        return status

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = Thread(target=self._run, name="job-dispatch", daemon=True)
        self._thread.start()

    def _run(self):
        while not self._stop.wait(0.25):
            try:
                self.clock.run_pending()
                with self._retry_lock:
                    due = [request for request, when in self._retry.values() if when <= monotonic()]
                for request in due:
                    self.dispatch(request)
            except Exception:
                logger.exception("Clock dispatch failed")

    def stop(self, timeout=30):
        deadline = monotonic() + max(0, timeout)
        self._stop.set()
        for executor in self.executors.values():
            executor.stop_admission()
        if self._thread:
            self._thread.join(timeout=max(0, deadline - monotonic()))
        with self._retry_lock:
            self._retry.clear()
        stopped = not self._thread or not self._thread.is_alive()
        for executor in self.executors.values():
            stopped = executor.shutdown(timeout=max(0, deadline - monotonic())) and stopped
        return stopped
