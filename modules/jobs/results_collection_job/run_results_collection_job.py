"""Results collection jobs."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from infrastructure.persistence.repositories import (
    EventRepository,
    EventSourceMappingRepository,
    ResultRepository,
)
from modules.odds_ingestion import MarketOddsIngestionService
from modules.observations import sport_observation_service
from modules.sofascore import api_client
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher

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


def run_results_collection_previous_day() -> None:
    logger.info("Starting Results Collection (previous day)")
    try:
        yesterday = datetime.now() - timedelta(days=1)
        events = EventRepository.get_events_by_date(yesterday)
        if not events:
            logger.info("No events found from previous day")
            return

        logger.info("Processing %s events from previous day", len(events))
        stats = _collect_results_for_events(events, "Results Collection (previous day)")
        logger.info(
            "Results Collection (previous day) completed: %s updated, %s skipped, %s deleted, %s failed",
            stats["updated"],
            stats["skipped"],
            stats["deleted"],
            stats["failed"],
        )
    except Exception as exc:
        logger.exception("Results Collection (previous day) failed: %s", exc)


def run_results_collection_all_finished() -> None:
    logger.info("Starting Results Collection (all finished)")
    try:
        events = EventRepository.get_all_finished_events()
        if not events:
            logger.info("No finished events found")
            return

        logger.info("Processing %s finished events", len(events))
        stats = _collect_results_for_events(events, "Results Collection (all finished)")
        logger.info(
            "Results Collection (all finished) completed: %s updated, %s skipped, %s deleted, %s failed",
            stats["updated"],
            stats["skipped"],
            stats["deleted"],
            stats["failed"],
        )
    except Exception as exc:
        logger.exception("Results Collection (all finished) failed: %s", exc)


def run_results_collection_for_date(target_date) -> None:
    logger.info("Starting results collection for date: %s", target_date)
    try:
        events = EventRepository.get_events_by_date(target_date)
        if not events:
            logger.info("No events found for %s", target_date)
            return

        source_states = EventSourceMappingRepository.get_odds_source_states(
            [event.id for event in events],
            ["sofascore"],
        )
        odds_fetcher = SofaScoreOddsFetcher(api_client)
        missing_odds_event_ids: set[int] = set()
        odds_updated_count = 0
        for event_data in events:
            try:
                source_state = source_states.get(event_data.id, {}).get("sofascore")
                if source_state is None:
                    logger.warning("Missing SofaScore mapping for event %s", event_data.id)
                    continue
                if not source_state.has_odds:
                    logger.debug(
                        "Skipping final odds for event %s: endpoint marked unavailable",
                        event_data.id,
                    )
                    continue

                fetch_result = odds_fetcher.fetch_odds(
                    int(source_state.source_event_id),
                    event_data.slug,
                )
                if fetch_result.endpoint_missing:
                    missing_odds_event_ids.add(event_data.id)
                    continue

                final_odds_response = fetch_result.payload
                if not final_odds_response:
                    logger.debug("No final odds response for event %s", event_data.id)
                    continue

                ingestion_result = MarketOddsIngestionService.save_from_sofascore_response(
                    event_data.id,
                    final_odds_response,
                    source="sofascore",
                )
                if ingestion_result.markets_saved > 0 or ingestion_result.dual_process_market_available:
                    odds_updated_count += 1
                    logger.info("Final market odds updated for %s vs %s", event_data.home_team, event_data.away_team)
                else:
                    logger.warning("Failed to save final market odds for event %s: %s", event_data.id, ingestion_result.reason)
            except Exception as exc:
                logger.warning("Error updating odds for event %s: %s", event_data.id, exc)

        EventSourceMappingRepository.mark_odds_unavailable(
            missing_odds_event_ids,
            "sofascore",
        )
        logger.info("Final market odds updated for %s/%s events", odds_updated_count, len(events))
        logger.info("Processing %s events from %s", len(events), target_date)
        stats = _collect_results_for_events(events, f"Results Collection ({target_date})")
        logger.info(
            "Results collection for %s completed: %s updated, %s skipped, %s deleted, %s failed",
            target_date,
            stats["updated"],
            stats["skipped"],
            stats["deleted"],
            stats["failed"],
        )
    except Exception as exc:
        logger.error("Error in results collection for %s: %s", target_date, exc)
