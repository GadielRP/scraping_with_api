"""Small, calculation-only audit vocabulary shared by the independent pillars."""

from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar

SIGNAL_STATUSES = frozenset({"COMPUTED", "BLOCKED", "ERROR"})
PILLAR_STATUSES = frozenset({"ACTIVE", "INSUFFICIENT_DATA", "ERROR", "SKIPPED"})
SIGNAL_REASONS = frozenset(
    {
        "MISSING_INPUT",
        "INCOMPATIBLE_CONTRACT",
        "INSUFFICIENT_OBSERVATIONS",
        "AMBIGUOUS_CANDIDATE",
        "INVALID_VALUE",
        "MISSING_POPULATION_FILTER",
        "MISSING_ENDPOINT",
        "NOT_APPLICABLE",
        "NON_CONTIGUOUS_GAP",
        "ZERO_DENOMINATOR",
        "UNDEFINED_METRIC",
        "CALCULATION_ERROR",
        "HISTORICAL_LOOKUP_ERROR",
        "CONTEXT_PREPARATION_ERROR",
        "INPUT_AFTER_AS_OF",
    }
)


def signal_diagnostics(signals):
    counts = Counter(signal["status"] for signal in signals)
    return {
        "total_signals": len(signals),
        "computed_signals": counts["COMPUTED"],
        "blocked_signals": counts["BLOCKED"],
        "error_signals": counts["ERROR"],
        "reasons": dict(
            Counter(
                signal["reason"]
                for signal in signals
                if signal["status"] != "COMPUTED" and signal.get("reason")
            )
        ),
    }


_trace = ContextVar("pillar_calculation_trace", default=None)
_scope = ContextVar("pillar_calculation_scope", default="global")


@contextmanager
def capture_calculations():
    """Collect explicit quantitative steps; never inspect frames or local objects."""
    steps = {}
    token = _trace.set(steps)
    try:
        yield steps
    finally:
        _trace.reset(token)


@contextmanager
def calculation_scope(name):
    parent = _scope.get()
    token = _scope.set(name if parent == "global" else f"{parent}.{name}")
    try:
        yield
    finally:
        _scope.reset(token)


def record_calculation(name, **values):
    trace = _trace.get()
    if trace is not None:
        trace.setdefault(_scope.get(), []).append({"operation": name, **values})
