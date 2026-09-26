"""Results collection jobs."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple, Union

from infrastructure.persistence.repositories import (
    EventRepository,
    EventSourceMappingRepository,
    ResultRepository,
)
from infrastructure.settings import Config
from modules.observations import sport_observation_service
from modules.sofascore import api_client
from shared.temporal import now_in_timezone

logger = logging.getLogger(__name__)


def _collect_results_for_events(events: List, job_name: str = "Results Collection") -> Dict[str, int]:
    stats = {"updated": 0, "skipped": 0, "failed": 0, "deleted": 0}
    source_event_ids = EventSourceMappingRepository.get_source_event_ids_by_event_ids(
        [event.id for event in events],
        "sofascore",
    )
    deferred_deletion_event_ids: set[int] = set()
    results_to_upsert: List[Tuple[int, Dict]] = []
    events_for_observations: List[Tuple[object, Dict]] = []

    for event in events:
        try:
            if ResultRepository.get_result_by_event_id(event.id):
                logger.info("Results exist for event %s, skipping", event.id)
                stats["skipped"] += 1
                continue

            source_event_id = source_event_ids.get(event.id)
            if source_event_id is None:
                raise ValueError(
                    f"Missing SofaScore source mapping for event_id={event.id}"
                )

            # Previous-day/date collection: stale not_started events are a minority
            # of zombie fixtures that never update on SofaScore. Policy lives
            # here (caller), not in the shared results parser.
            result_data = api_client.get_event_results(
                int(source_event_id),
                canonical_event_id=event.id,
                deferred_deletion_event_ids=deferred_deletion_event_ids,
                on_not_started="delete",
            )
            if not result_data:
                if event.id not in deferred_deletion_event_ids:
                    stats["failed"] += 1
                continue

            results_to_upsert.append((event.id, result_data))
            events_for_observations.append((event, result_data))
        except Exception as exc:
            logger.error("Error in %s for event %s: %s", job_name, event.id, exc)
            stats["failed"] += 1

    if results_to_upsert:
        upserted_count = ResultRepository.batch_upsert_results(results_to_upsert)
        stats["updated"] = upserted_count
        logger.info(
            "%s: batch upserted %s result(s)",
            job_name,
            upserted_count,
        )
        for event, r_data in events_for_observations:
            try:
                sport_observation_service.process_result_observations(event, r_data)
            except Exception as obs_exc:
                logger.error("Error processing observations for event %s: %s", event.id, obs_exc)

    if deferred_deletion_event_ids:
        requested_deletions = len(deferred_deletion_event_ids)
        stats["deleted"] = int(
            EventRepository.batch_delete_events(
                sorted(deferred_deletion_event_ids)
            )
            or 0
        )
        failed_deletions = max(0, requested_deletions - stats["deleted"])
        stats["failed"] += failed_deletions
        logger.info(
            "%s batch deletion completed: requested=%s deleted=%s failed=%s",
            job_name,
            requested_deletions,
            stats["deleted"],
            failed_deletions,
        )

    return stats


def run_results_collection_for_date(
    target_date: Optional[Union[date, str]] = None,
    job_name: Optional[str] = None,
) -> Dict[str, int]:
    """Collect results for events on a specific target date (defaults to previous day).

    Following SOLID design principles, this serves as the unified engine for date-based
    results collection, shared by scheduled midnight sync and ad-hoc CLI executions.
    """
    if target_date is None:
        resolved_date = now_in_timezone(Config.TIMEZONE).date() - timedelta(days=1)
        job_label = job_name or "Results Collection (previous day)"
    elif isinstance(target_date, str):
        resolved_date = date.fromisoformat(target_date)
        job_label = job_name or f"Results Collection ({resolved_date})"
    else:
        resolved_date = target_date
        job_label = job_name or f"Results Collection ({resolved_date})"

    logger.info("Starting %s for date: %s", job_label, resolved_date)
    try:
        events = EventRepository.get_events_by_date(resolved_date)
        if not events:
            logger.info("No events found for %s (%s)", resolved_date, job_label)
            return {"updated": 0, "skipped": 0, "failed": 0, "deleted": 0}

        logger.info("Processing %s events from %s (%s)", len(events), resolved_date, job_label)
        stats = _collect_results_for_events(events, job_label)
        logger.info(
            "%s completed: %s updated, %s skipped, %s deleted, %s failed",
            job_label,
            stats["updated"],
            stats["skipped"],
            stats["deleted"],
            stats["failed"],
        )
        return stats
    except Exception as exc:
        logger.exception("Error in %s for %s: %s", job_label, resolved_date, exc)
        return {"updated": 0, "skipped": 0, "failed": 0, "deleted": 0}


def run_results_collection_previous_day() -> Dict[str, int]:
    """Scheduled entrypoint for collecting previous-day results."""
    return run_results_collection_for_date(
        target_date=None,
        job_name="Results Collection (previous day)",
    )


def run_results_collection_all_finished() -> Dict[str, int]:
    logger.info("Starting Results Collection (all finished)")
    try:
        events = EventRepository.get_all_finished_events()
        if not events:
            logger.info("No finished events found")
            return {"updated": 0, "skipped": 0, "failed": 0, "deleted": 0}

        logger.info("Processing %s finished events", len(events))
        stats = _collect_results_for_events(events, "Results Collection (all finished)")
        logger.info(
            "Results Collection (all finished) completed: %s updated, %s skipped, %s deleted, %s failed",
            stats["updated"],
            stats["skipped"],
            stats["deleted"],
            stats["failed"],
        )
        return stats
    except Exception as exc:
        logger.exception("Results Collection (all finished) failed: %s", exc)
        return {"updated": 0, "skipped": 0, "failed": 0, "deleted": 0}
