"""Results collection jobs."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List

from infrastructure.persistence.repositories import EventRepository, ResultRepository
from modules.odds_ingestion import MarketOddsIngestionService
from modules.observations import sport_observation_service
from modules.sofascore import api_client

logger = logging.getLogger(__name__)


def _collect_results_for_events(events: List, job_name: str = "Results Collection") -> Dict[str, int]:
    stats = {"updated": 0, "skipped": 0, "failed": 0}

    for event in events:
        try:
            if ResultRepository.get_result_by_event_id(event.id):
                logger.info("Results exist for event %s, skipping", event.id)
                stats["skipped"] += 1
                continue

            result_data = api_client.get_event_results(event.id)
            if not result_data:
                stats["failed"] += 1
                continue

            if ResultRepository.upsert_result(event.id, result_data):
                stats["updated"] += 1
                logger.info(
                    "%s: %s = %s-%s, Winner: %s",
                    job_name,
                    event.id,
                    result_data["home_score"],
                    result_data["away_score"],
                    result_data["winner"],
                )
                sport_observation_service.process_result_observations(event, result_data)
        except Exception as exc:
            logger.error("Error in %s for event %s: %s", job_name, event.id, exc)
            stats["failed"] += 1

    return stats


def run_results_collection_previous_day() -> None:
    logger.info("Starting Job E: Results collection for finished events")
    try:
        yesterday = datetime.now() - timedelta(days=1)
        events = EventRepository.get_events_by_date(yesterday)
        if not events:
            logger.info("No events found from previous day")
            return

        logger.info("Processing %s events from previous day", len(events))
        stats = _collect_results_for_events(events, "Job E")
        logger.info("Job E completed: %s updated, %s skipped, %s failed", stats["updated"], stats["skipped"], stats["failed"])
    except Exception as exc:
        logger.error("Error in Job E: %s", exc)


def run_results_collection_all_finished() -> None:
    logger.info("Starting Job E2: Comprehensive results collection")
    try:
        events = EventRepository.get_all_finished_events()
        if not events:
            logger.info("No finished events found")
            return

        logger.info("Processing %s finished events", len(events))
        stats = _collect_results_for_events(events, "Job E2")
        logger.info("Job E2 completed: %s updated, %s skipped, %s failed", stats["updated"], stats["skipped"], stats["failed"])
    except Exception as exc:
        logger.error("Error in Job E2: %s", exc)


def run_results_collection_for_date(target_date) -> None:
    logger.info("Starting results collection for date: %s", target_date)
    try:
        events = EventRepository.get_events_by_date(target_date)
        if not events:
            logger.info("No events found for %s", target_date)
            return

        odds_updated_count = 0
        for event_data in events:
            try:
                final_odds_response = api_client.get_event_final_odds(event_data.id, event_data.slug)
                if not final_odds_response:
                    logger.debug("No final odds response for event %s", event_data.id)
                    continue

                ingestion_result = MarketOddsIngestionService.save_from_event_odds_response(
                    event_data.id,
                    final_odds_response,
                    source="results_collection_for_date",
                )
                if ingestion_result.markets_saved > 0 or ingestion_result.dual_process_market_available:
                    odds_updated_count += 1
                    logger.info("Final market odds updated for %s vs %s", event_data.home_team, event_data.away_team)
                else:
                    logger.warning("Failed to save final market odds for event %s: %s", event_data.id, ingestion_result.reason)
            except Exception as exc:
                logger.warning("Error updating odds for event %s: %s", event_data.id, exc)

        logger.info("Final market odds updated for %s/%s events", odds_updated_count, len(events))
        logger.info("Processing %s events from %s", len(events), target_date)
        stats = _collect_results_for_events(events, f"Results Collection ({target_date})")
        logger.info(
            "Results collection for %s completed: %s updated, %s skipped, %s failed",
            target_date,
            stats["updated"],
            stats["skipped"],
            stats["failed"],
        )
    except Exception as exc:
        logger.error("Error in results collection for %s: %s", target_date, exc)
