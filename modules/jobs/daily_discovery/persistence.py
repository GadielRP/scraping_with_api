"""Normalize one filtered daily batch before opening event write transactions."""

from dataclasses import dataclass
import logging

from infrastructure.persistence.repositories import EventRepository
from modules.jobs.discovery.filters import (
    filter_sofascore_events, load_tracked_source_competitions, UNRESOLVED_SCOPE,
)
from shared.execution_context import WorkDeferred, check_execution_budget

logger = logging.getLogger(__name__)


@dataclass
class DailyWriteSummary:
    persisted: int = 0
    inserted: int = 0
    updated: int = 0
    discarded: int = 0
    failed: int = 0
    filtered: int = 0


def persist_daily_events(client, events, run_store=None, *, tracked_competitions=UNRESOLVED_SCOPE):
    """Revalidate admission before normalization and event writes."""
    check_execution_budget()
    if tracked_competitions is UNRESOLVED_SCOPE:
        tracked_competitions = load_tracked_source_competitions("sofascore")
    summary = DailyWriteSummary()
    eligible = filter_sofascore_events(events, tracked_competitions)
    summary.filtered = len(events) - len(eligible)
    eligible = [raw for raw in eligible if raw.get("id")]
    summary.failed = len(events) - summary.filtered - len(eligible)
    blocked = EventRepository.discarded_source_ids("sofascore", [raw["id"] for raw in eligible])
    if blocked:
        logger.info(
            "Discovery discard memory blocked: source=sofascore phase=before_normalization "
            "unique_ids=%s source_event_ids=%s",
            len(blocked),
            sorted(blocked),
        )
    normalized = []
    for raw in eligible:
        check_execution_budget()
        if str(raw["id"]) in blocked:
            summary.discarded += 1
            continue
        try:
            data = client.normalize_event_payload(raw, discovery_source="daily_discovery")
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
    summary.discarded += len(result.discarded)
    summary.failed += len(result.errors)
    return summary
