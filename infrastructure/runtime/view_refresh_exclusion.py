"""Keep materialized-view refreshes separate from pre-start work in this process."""

from contextlib import contextmanager
from threading import Condition

from shared.execution_context import WorkDeferred, check_execution_budget


class ViewRefreshExclusion:
    def __init__(self):
        self._condition = Condition()
        self._pre_start = 0
        self._refreshing = False

    @contextmanager
    def pre_start(self):
        # Count waiting work too, so another view cannot overtake a waiting job.
        with self._condition:
            self._pre_start += 1
        try:
            with self._condition:
                while self._refreshing:
                    check_execution_budget()
                    self._condition.wait(timeout=0.1)
            check_execution_budget()
            yield
        finally:
            with self._condition:
                self._pre_start -= 1
                self._condition.notify_all()

    @contextmanager
    def refresh(self, *, critical_pending=None):
        with self._condition:
            if self._pre_start or self._refreshing or (critical_pending and critical_pending()):
                raise WorkDeferred("View refresh deferred: pre-start work or refresh active")
            self._refreshing = True
        try:
            yield
        finally:
            with self._condition:
                self._refreshing = False
                self._condition.notify_all()


view_refresh_exclusion = ViewRefreshExclusion()
