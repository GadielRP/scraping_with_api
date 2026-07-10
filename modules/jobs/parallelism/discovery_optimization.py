"""Optimization helpers for discovery jobs."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from infrastructure.persistence.repositories import EventRepository
from infrastructure.persistence.repositories import EventSourceMappingRepository
from modules.odds_ingestion import MarketOddsIngestionService
from modules.sofascore import api_client

logger = logging.getLogger(__name__)


def _event_payload(event_data: Dict) -> Dict:
    return event_data.get("event", event_data)


def _event_id(event_data: Dict) -> int:
    return _event_payload(event_data)["id"]


def parallel_team_event_fetching(team_ids: List[int], max_workers: int = 5) -> List[Dict]:
    """Fetch nearest events for multiple teams in parallel."""

    def fetch_team_event(team_id: int) -> Optional[Dict]:
        try:
            event_response = api_client.get_nearest_event_for_team(team_id)
            if not event_response:
                logger.debug("No nearest event found for team %s", team_id)
                return None

            event_data = api_client.normalize_event_payload(event_response, discovery_source="team_streaks")
            if not event_data:
                logger.debug("Failed to structure event data for team %s", team_id)
                return None

            logger.debug("Fetched event %s for team %s", _event_payload(event_data).get("id"), team_id)
            return event_data
        except Exception as exc:
            logger.debug("Error processing team %s: %s", team_id, exc)
            return None

    team_events = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_team = {executor.submit(fetch_team_event, team_id): team_id for team_id in team_ids}
        for future in as_completed(future_to_team):
            event_data = future.result()
            if event_data:
                team_events.append(event_data)

    return team_events


def parallel_odds_checking(
    events: List[Dict],
    max_workers: int = 5,
    no_retry_on_404: bool = True,
) -> Tuple[Dict[str, Dict], List[int]]:
    """Check odds availability for multiple events in parallel."""

    def check_event_odds(event_data: Dict) -> Tuple[str, Optional[Dict]]:
        sofascore_event_id = str(_event_id(event_data))
        odds_data = api_client.get_event_final_odds(sofascore_event_id, no_retry_on_404=no_retry_on_404)
        if not odds_data:
            return sofascore_event_id, None

        return sofascore_event_id, odds_data

    events_with_odds = {}
    events_to_delete = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_event = {executor.submit(check_event_odds, event_data): event_data for event_data in events}
        for future in as_completed(future_to_event):
            try:
                event_id, odds_data = future.result()
                if odds_data is None:
                    events_to_delete.append(int(event_id))
                else:
                    events_with_odds[event_id] = odds_data
            except Exception as exc:
                event_data = future_to_event[future]
                logger.debug("Error checking odds for event %s: %s", _event_payload(event_data).get("id"), exc)
                events_to_delete.append(int(_event_id(event_data)))

    return events_with_odds, events_to_delete


def batch_upsert_events(events: List[Dict]) -> int:
    """Upsert multiple events efficiently."""
    upserted_count = 0
    for event_data in events:
        try:
            event = EventRepository.upsert_event(event_data)
            if event:
                upserted_count += 1
        except Exception as exc:
            logger.debug("Error upserting event %s: %s", _event_payload(event_data).get("id"), exc)
    return upserted_count


def batch_process_odds(events_with_odds: Dict[str, Dict], events: List[Dict]) -> Tuple[int, int]:
    """Process odds data for multiple events efficiently."""
    processed_count = 0
    skipped_count = 0

    for event_data in events:
        sofascore_event_id = str(_event_id(event_data))
        odds_response = events_with_odds.get(sofascore_event_id) or events_with_odds.get(int(sofascore_event_id))
        if not odds_response:
            continue

        try:
            db_event = EventRepository.upsert_event(event_data)
            if not db_event:
                logger.debug("Failed to upsert event %s before saving odds", sofascore_event_id)
                skipped_count += 1
                continue

            ingestion_result = MarketOddsIngestionService.save_from_event_odds_response(
                db_event.id,
                odds_response,
                source="parallel_odds_checking",
            )
            if ingestion_result.markets_saved <= 0 and not ingestion_result.dual_process_market_available:
                logger.debug("Failed to save market odds for event %s: %s", sofascore_event_id, ingestion_result.reason)
                skipped_count += 1
                continue

            processed_count += 1
        except Exception as exc:
            logger.debug("Error processing event %s: %s", sofascore_event_id, exc)
            skipped_count += 1

    return processed_count, skipped_count


def process_with_batch_cleanup(
    events: List[Dict],
    discovery_source: str = None,
    max_workers: int = 5,
) -> Tuple[int, int]:
    """Complete pipeline for processing events with batch deletion optimization."""
    if not events:
        return 0, 0

    batch_upsert_events(events)
    events_with_odds, events_to_delete = parallel_odds_checking(events, max_workers=max_workers)

    if events_to_delete:
        canonical_event_ids = []
        for external_event_id in events_to_delete:
            canonical_event_id = EventSourceMappingRepository.get_event_id_by_source(
                "sofascore",
                str(external_event_id),
            )
            if canonical_event_id is not None:
                canonical_event_ids.append(canonical_event_id)
            else:
                logger.debug("Could not resolve canonical event_id for external SofaScore event %s", external_event_id)

        deleted_count = EventRepository.batch_delete_events(canonical_event_ids)
        logger.info("Batch deleted %s %s events without odds", deleted_count, discovery_source)

    processed_count, skipped_count = batch_process_odds(events_with_odds, events)
    skipped_count += len(events_to_delete)
    return processed_count, skipped_count


def process_odds_first(
    events: List[Dict],
    discovery_source: str = None,
    max_workers: int = 5,
) -> Tuple[int, int]:
    """Check odds BEFORE upserting. Only persist events that have valid odds.

    This avoids the insert-then-delete pattern and eliminates the need for
    orphaned season cleanup queries entirely.
    """
    if not events:
        return 0, 0

    # Step 1: Check odds in parallel (no DB writes yet)
    events_with_odds, events_without_odds_ids = parallel_odds_checking(events, max_workers=max_workers)

    if events_without_odds_ids:
        logger.info(
            "Skipped %s %s events without odds (never persisted to DB)",
            len(events_without_odds_ids),
            discovery_source,
        )

    # Step 2: Filter to only events that have odds
    valid_events = [e for e in events if str(_event_id(e)) in events_with_odds]

    if not valid_events:
        logger.info("No %s events had valid odds, nothing to persist", discovery_source)
        return 0, len(events)

    # Step 3: Upsert only valid events (the ones with odds)
    upserted = batch_upsert_events(valid_events)
    logger.info(
        "Upserted %s/%s %s events (pre-filtered by odds availability)",
        upserted,
        len(events),
        discovery_source,
    )

    # Step 4: Process odds for those events
    processed_count, skipped_count = batch_process_odds(events_with_odds, valid_events)
    skipped_count += len(events_without_odds_ids)

    return processed_count, skipped_count


def process_with_parallel_db_ops(
    events: List[Dict],
    odds_map: Dict,
    discovery_source: str = None,
    max_workers: int = 5,
) -> Tuple[int, int]:
    """Process events with pre-fetched odds using parallel database operations."""

    def process_single_event(event_data: Dict) -> Tuple[bool, str]:
        try:
            sofascore_event_id = str(_event_id(event_data))

            event = EventRepository.upsert_event(event_data)
            if not event:
                return False, f"Failed to upsert event {sofascore_event_id}"

            odds_map_entry = odds_map.get(sofascore_event_id) or odds_map.get(str(sofascore_event_id)) or odds_map.get(int(sofascore_event_id))
            if not odds_map_entry:
                return False, f"No odds data found for event {sofascore_event_id}"

            ingestion_result = MarketOddsIngestionService.save_from_dropping_odds_map_entry(
                event.id,
                odds_map_entry,
                source=discovery_source or "dropping_odds",
            )
            if ingestion_result.markets_saved <= 0 and not ingestion_result.dual_process_market_available:
                return False, f"Failed to save market odds for event {sofascore_event_id}: {ingestion_result.reason}"

            return True, f"Successfully processed event {sofascore_event_id}"
        except Exception as exc:
            return False, f"Error processing event {_event_payload(event_data).get('id')}: {exc}"

    processed_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_event = {executor.submit(process_single_event, event_data): event_data for event_data in events}
        for future in as_completed(future_to_event):
            try:
                success, reason = future.result()
                if success:
                    processed_count += 1
                else:
                    logger.debug(reason)
                    skipped_count += 1
            except Exception as exc:
                event_data = future_to_event[future]
                logger.error("Exception processing event %s: %s", _event_payload(event_data).get("id"), exc)
                skipped_count += 1

    return processed_count, skipped_count


def process_events_only(
    events: List[Dict],
    discovery_source: str = None,
    max_workers: int = 10,
) -> Tuple[int, int]:
    """Process events without fetching odds."""
    if not events:
        return 0, 0

    upserted_count = batch_upsert_events(events)
    logger.info("%s events processed: %s/%s events upserted", discovery_source, upserted_count, len(events))
    return upserted_count, len(events) - upserted_count


def process_with_aggressive_parallel(
    events: List[Dict],
    discovery_source: str = None,
    max_workers: int = 10,
) -> Tuple[int, int]:
    """Aggressive optimization mode with more workers."""
    logger.warning("AGGRESSIVE MODE: Using %s workers for %s", max_workers, discovery_source)
    return process_with_batch_cleanup(events, discovery_source, max_workers)

