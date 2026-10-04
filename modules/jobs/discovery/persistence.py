"""Discovery admission policies and bounded event/odds persistence."""

import logging

from infrastructure.persistence.repositories import EventRepository, EventSourceMappingRepository
from infrastructure.settings.job_execution import JobExecutionSettings
from modules.competition.discovery_scope import (
    filter_tracked_source_events,
    load_tracked_source_competitions,
    UNRESOLVED_SCOPE,
)
from modules.jobs.discovery_filters import filter_supported_sofascore_events
from modules.odds_ingestion import MarketOddsIngestionService
from shared.batching import chunks
from shared.execution_context import WorkDeferred, check_execution_budget
from .fetching import fetch_event_odds

logger = logging.getLogger(__name__)


def _source_id(event):
    return str(event.get("event", event)["id"])


def _eligible_events(events, tracked_competitions):
    supported = filter_supported_sofascore_events(events)
    eligible = filter_tracked_source_events(supported, tracked_competitions)
    if not eligible:
        return []
    blocked = EventRepository.discarded_source_ids("sofascore", [_source_id(e) for e in eligible])
    return [event for event in eligible if _source_id(event) not in blocked]


def _persist_event_odds(events, odds_map, save_odds, run_store):
    """Record confirmed identities once, then ingest odds through the provider adapter."""
    result = EventRepository.batch_upsert_events(events)
    if run_store is not None:
        run_store.record_mapping(result.events)
    processed = 0
    for event in events:
        check_execution_budget()
        source_id = _source_id(event)
        db_event = result.events.get(source_id)
        odds = odds_map.get(source_id) or odds_map.get(int(source_id))
        if db_event is None or not odds:
            continue
        try:
            saved = save_odds(db_event.id, odds, source="sofascore")
            if saved.markets_saved > 0 or saved.dual_process_market_available:
                processed += 1
            else:
                logger.debug(
                    "Discovery odds skipped source_event_id=%s reason=%s", source_id, saved.reason
                )
        except WorkDeferred:
            raise
        except Exception:
            logger.exception("Cannot persist discovery odds source_event_id=%s", source_id)
    return processed, len(events) - processed


def persist_events(
    events, discovery_source=None, *, tracked_competitions=UNRESOLVED_SCOPE, run_store=None
):
    """Persist eligible events regardless of odds availability."""
    if tracked_competitions is UNRESOLVED_SCOPE:
        tracked_competitions = load_tracked_source_competitions("sofascore")
    processed = skipped = 0
    for batch in chunks(events, JobExecutionSettings().event_read_batch_size):
        check_execution_budget()
        eligible = _eligible_events(batch, tracked_competitions)
        if not eligible:
            skipped += len(batch)
            continue
        result = EventRepository.batch_upsert_events(eligible)
        if run_store is not None:
            run_store.record_mapping(result.events)
        processed += len(result.events)
        skipped += len(batch) - len(result.events)
    logger.info(
        "%s events processed: persisted=%s skipped=%s", discovery_source, processed, skipped
    )
    return processed, skipped


def persist_events_with_odds(
    events,
    odds_map,
    discovery_source=None,
    *,
    tracked_competitions=UNRESOLVED_SCOPE,
    run_store=None
):
    """Persist feed events even without odds; ingest available dropping/winning odds entries."""
    if tracked_competitions is UNRESOLVED_SCOPE:
        tracked_competitions = load_tracked_source_competitions("sofascore")
    processed = skipped = 0
    for batch in chunks(events, JobExecutionSettings().event_read_batch_size):
        check_execution_budget()
        eligible = _eligible_events(batch, tracked_competitions)
        skipped += len(batch) - len(eligible)
        if not eligible:
            continue
        saved, failed = _persist_event_odds(
            eligible,
            odds_map,
            MarketOddsIngestionService.save_from_dropping_odds_map_entry,
            run_store,
        )
        processed += saved
        skipped += failed
    logger.info("%s events with odds processed=%s skipped=%s", discovery_source, processed, skipped)
    return processed, skipped


def fetch_and_persist_events_with_odds(
    events,
    discovery_source=None,
    max_workers=5,
    *,
    tracked_competitions=UNRESOLVED_SCOPE,
    run_store=None
):
    """Fetch odds first and admit only events with a successful odds response."""
    if tracked_competitions is UNRESOLVED_SCOPE:
        tracked_competitions = load_tracked_source_competitions("sofascore")
    processed = skipped = 0
    for batch in chunks(events, JobExecutionSettings().event_read_batch_size):
        check_execution_budget()
        eligible = _eligible_events(batch, tracked_competitions)
        skipped += len(batch) - len(eligible)
        if not eligible:
            continue
        fetched = fetch_event_odds(eligible, max_workers=max_workers)
        if fetched.endpoint_missing_source_event_ids:
            mappings = EventSourceMappingRepository.get_event_ids_by_sofascore_ids(
                list(map(str, fetched.endpoint_missing_source_event_ids))
            )
            EventSourceMappingRepository.mark_odds_unavailable(mappings.values(), "sofascore")
        logger.info(
            "Discovery odds source=%s available=%s missing_endpoint=%s empty=%s temporary_failure=%s",
            discovery_source,
            len(fetched.odds_by_source_event_id),
            len(fetched.endpoint_missing_source_event_ids),
            len(fetched.empty_source_event_ids),
            len(fetched.failed_source_event_ids),
        )
        valid = [
            event for event in eligible if _source_id(event) in fetched.odds_by_source_event_id
        ]
        skipped += len(eligible) - len(valid)
        if not valid:
            continue
        saved, failed = _persist_event_odds(
            valid,
            fetched.odds_by_source_event_id,
            MarketOddsIngestionService.save_from_sofascore_response,
            run_store,
        )
        processed += saved
        skipped += failed
    return processed, skipped
