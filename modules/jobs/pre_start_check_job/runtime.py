"""Dependencies and synchronized state owned by the pre-start use case."""

from contextvars import copy_context
from dataclasses import replace
import logging
from threading import Event, Lock, Thread, current_thread
from time import monotonic
from infrastructure.settings.job_execution import JobExecutionSettings
from infrastructure.runtime.reporting_exclusion import reporting_exclusion
from shared.execution_context import (
    ExecutionContext,
    Priority,
    current_execution,
    execution_scope,
    check_execution_budget,
    WorkDeferred,
)
from .budget import MissingOddsCooldown

logger = logging.getLogger(__name__)


class RescheduledEvents:
    def __init__(self, ttl_seconds=600):
        self.ttl_seconds = ttl_seconds
        self._entries = {}
        self._lock = Lock()

    def add(self, event_id):
        with self._lock:
            self._entries[event_id] = monotonic() + self.ttl_seconds

    def __contains__(self, event_id):
        with self._lock:
            expires = self._entries.get(event_id, 0)
            return expires > monotonic()

    def cleanup(self):
        now = monotonic()
        with self._lock:
            self._entries = {key: expiry for key, expiry in self._entries.items() if expiry > now}


class OddsPortalWorkerState:
    """Own one browser cycle until it actually ends, including busy/empty ticks."""

    def __init__(self):
        self._thread = None
        self._lock = Lock()
        self._stop = Event()
        self._closed = False

    @property
    def active_thread(self):
        with self._lock:
            return self._thread

    def launch(self, action):
        context = current_execution()
        context = (
            replace(context, stop=self._stop)
            if context
            else ExecutionContext("oddsportal", Priority.PRE_START, self._stop)
        )

        def run():
            try:
                with execution_scope(context):
                    check_execution_budget()
                    action()
            except (KeyboardInterrupt, WorkDeferred):
                logger.info("OddsPortal worker stopped before completing its cycle")
            finally:
                try:
                    with self._lock:
                        if self._thread is current_thread():
                            self._thread = None
                finally:
                    activity.__exit__(None, None, None)

        # Reserve before starting the thread; scraping can outlive its parent job.
        activity = reporting_exclusion.pre_start()
        activity.__enter__()
        launched = False
        try:
            with self._lock:
                if self._closed or (self._thread and self._thread.is_alive()):
                    return None
                thread = Thread(
                    target=copy_context().run,
                    args=(run,),
                    name="oddsportal-cycle",
                    daemon=True,
                )
                self._thread = thread
                thread.start()
                launched = True
                return thread
        finally:
            if not launched:
                activity.__exit__(None, None, None)

    def stop_admission(self):
        with self._lock:
            self._closed = True
            self._stop.set()

    def close(self, timeout=30):
        self.stop_admission()
        with self._lock:
            thread = self._thread
        if thread:
            thread.join(timeout=max(0, timeout))
            if thread.is_alive():
                logger.error("OddsPortal cycle did not stop within shutdown grace")
                return False
        return True


class PreStartRuntime:
    def __init__(self, event_repo, settings=None):
        settings = settings or JobExecutionSettings()
        self.event_repo = event_repo
        self.recently_rescheduled = RescheduledEvents()
        self.oddsportal = OddsPortalWorkerState()
        self.missing_odds = MissingOddsCooldown(
            settings.missing_odds_cooldown_seconds, settings.missing_odds_capacity
        )
