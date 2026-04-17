"""Process 1 package for the dual-process alert system."""

from .candidate_search import AlertMatch, Process1CandidateSearch
from .evaluator import AlertPrediction, Process1Evaluator

__all__ = [
    "AlertEngine",
    "AlertMatch",
    "AlertPrediction",
    "Process1CandidateSearch",
    "Process1Evaluator",
    "alert_engine",
]


def __getattr__(name):
    if name in {"AlertEngine", "alert_engine"}:
        from .engine import AlertEngine, alert_engine

        globals()["AlertEngine"] = AlertEngine
        globals()["alert_engine"] = alert_engine
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
