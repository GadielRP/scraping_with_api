"""Optimization helpers for discovery jobs."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from infrastructure.persistence.repositories import EventRepository
from infrastructure.persistence.repositories import EventSourceMappingRepository
from modules.odds_ingestion import MarketOddsIngestionService
from modules.odds_ingestion.fetch_result import OddsFetchStatus
from modules.sofascore import api_client
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher

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


@dataclass
class ParallelOddsFetchSummary:
    """Typed outcomes from fetching SofaScore odds concurrently."""

    odds_by_source_event_id: Dict[str, Dict] = field(default_factory=dict)
    endpoint_missing_source_event_ids: set[int] = field(default_factory=set)
    empty_source_event_ids: set[int] = field(default_factory=set)
    failed_source_event_ids: set[int] = field(default_factory=set)


def fetch_event_odds_in_parallel(
    events: List[Dict],
    max_workers: int = 5,
    *,
    odds_fetcher: SofaScoreOddsFetcher | None = None,
) -> ParallelOddsFetchSummary:
    """Fetch odds without treating temporary failures as missing endpoints."""
    fetcher = odds_fetcher or SofaScoreOddsFetcher(api_client)

    def fetch_event_odds(event_data: Dict):
        sofascore_event_id = str(_event_id(event_data))
        return sofascore_event_id, fetcher.fetch_odds(int(sofascore_event_id))

    summary = ParallelOddsFetchSummary()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_event = {
            executor.submit(fetch_event_odds, event_data): event_data
            for event_data in events
        }
        for future in as_completed(future_to_event):
            try:
                source_event_id, fetch_result = future.result()
                numeric_source_event_id = int(source_event_id)
                if fetch_result.status is OddsFetchStatus.SUCCESS:
                    summary.odds_by_source_event_id[source_event_id] = fetch_result.payload
                elif fetch_result.status is OddsFetchStatus.ENDPOINT_NOT_FOUND:
                    summary.endpoint_missing_source_event_ids.add(numeric_source_event_id)
                else:
                    summary.empty_source_event_ids.add(numeric_source_event_id)
            except Exception as exc:
                event_data = future_to_event[future]
                source_event_id = int(_event_id(event_data))
                summary.failed_source_event_ids.add(source_event_id)
                logger.warning(
                    "Temporary failure fetching SofaScore odds for source_event_id=%s: %s",
                    source_event_id,
                    exc,
                )

    return summary


def _mark_missing_sofascore_odds(
    source_event_ids: set[int],
) -> int:
    """Resolve existing canonical mappings and persist confirmed odds 404s in bulk."""
    if not source_event_ids:
        return 0

    canonical_ids_by_source_id = EventSourceMappingRepository.get_event_ids_by_sofascore_ids(
        [str(event_id) for event_id in source_event_ids]
    )
    return EventSourceMappingRepository.mark_odds_unavailable(
        canonical_ids_by_source_id.values(),
        "sofascore",
    )


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

            ingestion_result = MarketOddsIngestionService.save_from_sofascore_response(
                db_event.id,
                odds_response,
                source="secondary_discovery",
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

    fetch_summary = fetch_event_odds_in_parallel(events, max_workers=max_workers)
    _mark_missing_sofascore_odds(fetch_summary.endpoint_missing_source_event_ids)

    skipped_before_persistence = (
        len(fetch_summary.endpoint_missing_source_event_ids)
        + len(fetch_summary.empty_source_event_ids)
        + len(fetch_summary.failed_source_event_ids)
    )
    if skipped_before_persistence:
        logger.info(
            "Skipped %s %s events before persistence "
            "(missing_endpoint=%s empty=%s temporary_failure=%s)",
            skipped_before_persistence,
            discovery_source,
            len(fetch_summary.endpoint_missing_source_event_ids),
            len(fetch_summary.empty_source_event_ids),
            len(fetch_summary.failed_source_event_ids),
        )

    valid_events = [
        event
        for event in events
        if str(_event_id(event)) in fetch_summary.odds_by_source_event_id
    ]

    if not valid_events:
        logger.info("No %s events had valid odds, nothing to persist", discovery_source)
        return 0, len(events)

    upserted = batch_upsert_events(valid_events)
    logger.info(
        "Upserted %s/%s %s events (pre-filtered by odds availability)",
        upserted,
        len(events),
        discovery_source,
    )

    processed_count, skipped_count = batch_process_odds(
        fetch_summary.odds_by_source_event_id,
        valid_events,
    )
    skipped_count += skipped_before_persistence

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

