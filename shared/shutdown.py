"""Process-wide shutdown coordination helpers."""

from __future__ import annotations

import threading

_shutdown_requested = threading.Event()


def request_shutdown():
    _shutdown_requested.set()


def clear_shutdown_request():
    _shutdown_requested.clear()


def is_shutdown_requested():
    from shared.execution_context import current_execution
    context = current_execution()
    return _shutdown_requested.is_set() or bool(context and context.stop.is_set())
