"""One worker, bounded queue and explicit coalescence; no unbounded futures."""

from collections import OrderedDict
import logging
from threading import Condition, Event, Thread
from time import monotonic
from shared.execution_context import ExecutionContext, execution_scope, WorkDeferred
from shared.runtime_observability import observe_operation
from shared.temporal import utc_now
from .contracts import Admission

logger = logging.getLogger(__name__)


class SerialExecutor:
    def __init__(self, name, capacity=8, coalesce_active=False):
        if capacity < 1:
            raise ValueError("Executor capacity must be positive")
        self.name, self.capacity = name, capacity
        self.coalesce_active = coalesce_active
        self.on_deferred = None
        self._condition = Condition()
        self._queue = OrderedDict()
        self._active = None
        self._closed = False
        self._stop = Event()
        self._thread = Thread(target=self._run, name=name, daemon=True)
        self._thread.start()

    def submit(self, request):
        with self._condition:
            if self._closed:
                status = Admission.CLOSED
            elif request.expires_at and utc_now() >= request.expires_at:
                status = Admission.EXPIRED
            elif request.key == self._active and not self.coalesce_active:
                status = Admission.COALESCED
            elif request.key in self._queue:
                self._queue[request.key] = (request, monotonic())
                status = Admission.COALESCED
            elif len(self._queue) + int(self._active is not None) >= self.capacity:
                status = Admission.FULL
            else:
                self._queue[request.key] = (request, monotonic())
                self._condition.notify()
                status = Admission.ACCEPTED
        logger.info(
            "Job admission channel=%s job=%s scheduled_at=%s status=%s",
            self.name,
            request.key,
            request.scheduled_at.isoformat(),
            status.value,
        )
        return status

    def _run(self):
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._closed or self._queue)
                if self._closed:
                    return
                key, (request, admitted) = self._queue.popitem(last=False)
                self._active = key
            started = monotonic()
            try:
                if request.expires_at and utc_now() >= request.expires_at:
                    logger.warning(
                        "Job expired channel=%s job=%s scheduled_at=%s",
                        self.name,
                        key,
                        request.scheduled_at,
                    )
                    continue
                deadline = started + request.budget_seconds if request.budget_seconds else None
                if request.expires_at:
                    expiry_deadline = started + max(
                        0, (request.expires_at - utc_now()).total_seconds()
                    )
                    deadline = min(deadline, expiry_deadline) if deadline else expiry_deadline
                context = ExecutionContext(key, request.priority, self._stop, deadline)
                logger.info(
                    "Job started channel=%s job=%s run_id=%s queue_wait_s=%.3f dispatch_lag_s=%.3f",
                    self.name,
                    key,
                    context.run_id,
                    started - admitted,
                    max(0, (utc_now() - request.scheduled_at).total_seconds()),
                )
                with execution_scope(context), observe_operation(key):
                    request.action()
            except WorkDeferred as exc:
                logger.warning("Job deferred channel=%s job=%s reason=%s", self.name, key, exc)
                if self.on_deferred:
                    self.on_deferred(request)
            except KeyboardInterrupt:
                logger.info("Job interrupted channel=%s job=%s", self.name, key)
            except Exception:
                logger.exception("Job failed channel=%s job=%s", self.name, key)
            else:
                logger.info(
                    "Job completed channel=%s job=%s duration_s=%.3f",
                    self.name,
                    key,
                    monotonic() - started,
                )
            finally:
                with self._condition:
                    self._active = None
                    self._condition.notify_all()

    def stop_admission(self):
        with self._condition:
            self._closed = True
            self._queue.clear()
            self._stop.set()
            self._condition.notify_all()

    def shutdown(self, timeout=30):
        self.stop_admission()
        self._thread.join(timeout=max(0, timeout))
        stopped = not self._thread.is_alive()
        if not stopped:
            logger.error("Worker did not stop within shutdown grace channel=%s", self.name)
        return stopped
