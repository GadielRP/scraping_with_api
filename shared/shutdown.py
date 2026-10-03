"""Process-wide shutdown coordination helpers."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from contextvars import ContextVar

_shutdown_requested = threading.Event()
_background_stop = ContextVar('background_stop', default=None)


@contextmanager
def background_work(stop_event):
    """Scope cooperative cancellation and request priority to this worker."""
    token = _background_stop.set(stop_event)
    try:
        yield
    finally:
        _background_stop.reset(token)


def is_background_work() -> bool:
    return _background_stop.get() is not None


def request_shutdown() -> None:
    """Mark the current process as shutting down."""
    _shutdown_requested.set()


def clear_shutdown_request() -> None:
    """Clear any prior shutdown request flag."""
    _shutdown_requested.clear()


def is_shutdown_requested() -> bool:
    """Return True when the process should stop as soon as possible."""
    local_stop = _background_stop.get()
    return _shutdown_requested.is_set() or (local_stop is not None and local_stop.is_set())
