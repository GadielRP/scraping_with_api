"""Sequential discipline maintenance with keyset continuation and bounded execution."""
from dataclasses import replace
import logging
from time import monotonic

from infrastructure.runtime.resource_budget import check_maintenance_capacity
from infrastructure.settings.job_execution import JobExecutionSettings
from modules.competition.tracked_competitions import is_tracked_competition
from modules.discipline.contracts import DisciplineEndpoint, Outcome
from shared.execution_context import (
    ExecutionContext, Priority, WorkDeferred, check_execution_budget,
    current_execution, execution_scope,
)

logger = logging.getLogger(__name__)


def run_discipline_collection(service, repository, settings, *, selection=None,
                              limit=None, commit=False, on_event=None):
    if selection is None:
        raise ValueError("An explicit discipline selection is required")
    maximum = settings.max_events if limit is None else limit
    if maximum < 1:
        raise ValueError("limit must be positive")
    stats = dict.fromkeys(("scanned", "skipped_untracked", "updated", "unchanged",
                          "unavailable", "invalid", "deferred", "mapping_conflict",
                          "failed", "statistics_attempted", "statistics_used"), 0)
    stats.update(last_processed_event_id=selection.after_event_id, dry_run=not commit, limit_reached=False)
    parent = current_execution() or ExecutionContext("discipline", Priority.MAINTENANCE)
    deadline = monotonic() + settings.budget_seconds
    context = replace(parent, deadline=min(parent.deadline, deadline) if parent.deadline is not None else deadline)
    try:
        with execution_scope(context):
            for batch in repository.iter_candidates(selection, JobExecutionSettings().event_read_batch_size):
                for event in batch:
                    if settings.tracked_competitions_only and not is_tracked_competition(
                        event.competition_id
                    ):
                        stats["skipped_untracked"] += 1
                        logger.info(
                            "Discipline skipped untracked competition event_id=%s competition_id=%s",
                            event.event_id,
                            event.competition_id,
                        )
                        continue
                    if stats["scanned"] >= maximum:
                        stats["limit_reached"] = True
                        return stats
                    check_execution_budget()
                    check_maintenance_capacity()
                    try:
                        report = service.process(event, commit=commit)
                    except WorkDeferred:
                        raise
                    except Exception:
                        logger.exception("Discipline failed event_id=%s", event.event_id)
                        outcome = Outcome.FAILED
                    else:
                        outcome = report.outcome
                        acquisition = report.acquisition
                        if acquisition is not None:
                            stats["statistics_attempted"] += int(acquisition.statistics_attempted)
                            stats["statistics_used"] += int(any(t.endpoint == DisciplineEndpoint.STATISTICS for t in acquisition.totals))
                            logger.info("Discipline totals event_id=%s statistics_attempted=%s statistics_reason=%s totals=%s",
                                        event.event_id, acquisition.statistics_attempted, acquisition.statistics_reason,
                                        [(t.type.value, t.side.value, t.count, t.endpoint.value) for t in acquisition.totals])
                        if on_event is not None:
                            on_event(event, report)
                        logger.info("Discipline event_id=%s outcome=%s incidents=%s reason=%s dry_run=%s",
                                    event.event_id, outcome.value,
                                    len(report.acquisition.incidents) if report.acquisition else 0,
                                    report.acquisition.reason if report.acquisition else None, not commit)
                    stats["scanned"] += 1
                    stats[outcome.value] += 1
                    stats["last_processed_event_id"] = event.event_id
    except WorkDeferred:
        stats["deferred"] += 1
        raise
    finally:
        logger.info("Discipline summary=%s", stats)
    return stats
