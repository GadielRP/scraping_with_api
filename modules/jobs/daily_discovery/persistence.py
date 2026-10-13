"""Normalize one filtered daily batch before opening event write transactions."""

from collections import Counter
from dataclasses import dataclass, field
import logging

from infrastructure.persistence.repositories import EventRepository
from modules.jobs.discovery.filters import SofascoreEventAdmission, sofascore_start_time_filter_reason
from shared.execution_context import WorkDeferred, check_execution_budget
from shared.temporal import utc_now

logger = logging.getLogger(__name__)


@dataclass
class DailyWriteSummary:
    persisted: int = 0
    inserted: int = 0
    updated: int = 0
    blocked_by_discard_memory: int = 0
    failed: int = 0
    filter_reasons: Counter[str] = field(default_factory=Counter)

    @property
    def filtered(self) -> int:
        return sum(self.filter_reasons.values())


def persist_daily_events(client, events: list[SofascoreEventAdmission], run_store=None):
    """Normalize an admitted batch, rechecking only elapsed kickoff time."""
    check_execution_budget()
    summary = DailyWriteSummary()
    eligible = []
    current = utc_now()
    for admission in events:
        reason = sofascore_start_time_filter_reason(admission.raw_event, now=current)
        if reason:
            summary.filter_reasons[reason] += 1
        elif not admission.raw_event.get("id"):
            summary.failed += 1
        else:
            eligible.append(admission)
    if not eligible:
        return summary
    blocked = EventRepository.discarded_source_ids(
        "sofascore", [admission.raw_event["id"] for admission in eligible],
    )
    if blocked:
        logger.info(
            "Discovery discard memory blocked: source=sofascore phase=before_normalization "
            "unique_ids=%s source_event_ids=%s",
            len(blocked),
            sorted(blocked),
        )
    normalized = []
    for admission in eligible:
        check_execution_budget()
        raw = admission.raw_event
        if str(raw["id"]) in blocked:
            summary.blocked_by_discard_memory += 1
            continue
        try:
            data = client.normalize_event_payload(
                raw, discovery_source="daily_discovery", resolved_round=admission.round_name,
            )
            if not data:
                raise ValueError("Invalid normalized event")
            normalized.append(data)
        except WorkDeferred:
            raise
        except Exception:
            logger.exception("Cannot normalize daily event %s", raw["id"])
            summary.failed += 1
    if not normalized:
        return summary
    result = EventRepository.batch_upsert_events(normalized)
    if run_store is not None:
        run_store.record_mapping(result.events)
    summary.persisted = len(result.events)
    summary.inserted = result.inserted
    summary.updated = result.updated
    summary.blocked_by_discard_memory += len(result.discarded)
    summary.failed += len(result.errors)
    return summary
