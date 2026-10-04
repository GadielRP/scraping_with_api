"""Result eligibility policies are independent of SQL and scheduler timing."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from infrastructure.settings import Config
from shared.temporal import local_day_bounds_utc, utc_now
from shared.execution_context import WorkDeferred


class ResultOutcome(str, Enum):
    PERSISTED = "persisted"
    DELETED = "deleted"
    DEFERRED = "deferred_unresolved"
    BUDGET_DEFERRED = "deferred_budget"
    MISSING_MAPPING = "missing_mapping"
    AMBIGUOUS_MAPPING = "ambiguous_mapping"
    PROVIDER_ERROR = "provider_or_parse_error"
    CONFLICT = "persistence_conflict"


class ResultBatchDeferred(WorkDeferred):
    """A partial page has committed work that must appear in the run's counters."""

    def __init__(self, reason, stats):
        super().__init__(str(reason))
        self.stats = dict(stats)


@dataclass(frozen=True)
class ResultSelection:
    start: datetime | None = None
    end: datetime | None = None
    sport_cutoffs: dict[str, datetime] = field(default_factory=dict)
    default_cutoff: datetime | None = None


def result_selection(target_date):
    if target_date is not None:
        start, end = local_day_bounds_utc(target_date, Config.TIMEZONE)
        return ResultSelection(start, end)
    now = utc_now()
    return ResultSelection(
        sport_cutoffs={
            "Football": now - timedelta(hours=2.5),
            "Futsal": now - timedelta(hours=2.5),
            "Tennis": now - timedelta(hours=4),
            "Baseball": now - timedelta(hours=4),
        },
        default_cutoff=now - timedelta(hours=3),
    )
