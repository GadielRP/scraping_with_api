"""Process-wide shutdown coordination helpers."""

from __future__ import annotations

import threading

_shutdown_requested = threading.Event()


def request_shutdown() -> None:
    """Mark the current process as shutting down."""
    _shutdown_requested.set()


def clear_shutdown_request() -> None:
    """Clear any prior shutdown request flag."""
    _shutdown_requested.clear()


def is_shutdown_requested() -> bool:
    """Return True when the process should stop as soon as possible."""
    return _shutdown_requested.is_set()
