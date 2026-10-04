"""Execution identity, priority and cooperative cancellation, scoped to a run."""

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import IntEnum
from threading import Event
from time import monotonic
from uuid import uuid4


class Priority(IntEnum):
    CLOSING = 0
    PRE_START = 1
    MAINTENANCE = 2


class WorkDeferred(Exception):
    """A unit can be retried after its deadline or resource budget is exhausted."""


@dataclass(frozen=True)
class ExecutionContext:
    job: str
    priority: Priority
    stop: Event = field(default_factory=Event)
    deadline: float | None = None
    run_id: str = field(default_factory=lambda: uuid4().hex)


_CURRENT = ContextVar("execution_context", default=None)


def current_execution():
    return _CURRENT.get()


def current_priority():
    context = current_execution()
    return context.priority if context else Priority.PRE_START


def check_execution_budget():
    from shared.shutdown import is_shutdown_requested

    if is_shutdown_requested():
        raise KeyboardInterrupt()
    context = current_execution()
    if context and context.deadline is not None and monotonic() >= context.deadline:
        raise WorkDeferred(f"Execution deadline reached job={context.job}")


def request_timeout(default):
    """Network work cannot reserve more time than the run has left."""
    check_execution_budget()
    context = current_execution()
    if context and context.deadline is not None:
        return max(0.001, min(default, context.deadline - monotonic()))
    return default


def wait_interruptibly(seconds):
    end = monotonic() + seconds
    context = current_execution()
    stop = context.stop if context else Event()
    while monotonic() < end:
        check_execution_budget()
        stop.wait(min(0.1, end - monotonic()))


@contextmanager
def execution_scope(context):
    token = _CURRENT.set(context)
    try:
        yield context
    finally:
        _CURRENT.reset(token)
