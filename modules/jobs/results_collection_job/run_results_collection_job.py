"""Results collection jobs."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Dict, List

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event
from infrastructure.persistence.repositories import EventRepository, OddsRepository, ResultRepository
from modules.prediction import prediction_logger
from modules.jobs.pre_start_check_job.odds_extraction import extract_final_odds_from_response
from modules.sofascore import api_client
from sport_observations import sport_observations_manager

logger = logging.getLogger(__name__)


def _collect_results_for_events(events: List, job_name: str = "Results Collection") -> Dict[str, int]:
    stats = {"updated": 0, "skipped": 0, "failed": 0}

    for event in events:
        try:
            if ResultRepository.get_result_by_event_id(event.id):
                logger.info(f"Results exist for event {event.id}, skipping")
                stats["skipped"] += 1
                continue

            result_data = api_client.get_event_results(event.id)
            if not result_data:
                stats["failed"] += 1
                continue

            if ResultRepository.upsert_result(event.id, result_data):
                stats["updated"] += 1
                logger.info(
                    f"✅ {job_name}: {event.id} = {result_data['home_score']}-{result_data['away_score']}, Winner: {result_data['winner']}"
                )
                sport_observations_manager.process_event_observations(event, result_data)
        except Exception as exc:
            logger.error(f"Error in {job_name} for event {event.id}: {exc}")
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

        logger.info(f"Processing {len(events)} events from previous day")
        stats = _collect_results_for_events(events, "Job E")
        logger.info(f"Job E completed: {stats['updated']} updated, {stats['skipped']} skipped, {stats['failed']} failed")
    except Exception as exc:
        logger.error(f"Error in Job E: {exc}")


def run_results_collection_all_finished() -> None:
    logger.info("Starting Job E2: Comprehensive results collection")
    try:
        events = EventRepository.get_all_finished_events()
        if not events:
            logger.info("No finished events found")
            return

        logger.info(f"Processing {len(events)} finished events")
        stats = _collect_results_for_events(events, "Job E2")
        logger.info(f"Job E2 completed: {stats['updated']} updated, {stats['skipped']} skipped, {stats['failed']} failed")
    except Exception as exc:
        logger.error(f"Error in Job E2: {exc}")


def run_results_collection_for_date(target_date) -> None:
    logger.info(f"Starting results collection for date: {target_date}")
    try:
        events = EventRepository.get_events_by_date(target_date)
        if not events:
            logger.info(f"No events found for {target_date}")
            return

        odds_updated_count = 0
        for event_data in events:
            try:
                final_odds_response = api_client.get_event_final_odds(event_data.id, event_data.slug)
                if final_odds_response:
                    final_odds_data = extract_final_odds_from_response(final_odds_response, initial_odds_extraction=True)
                    if final_odds_data:
                        upserted_id = OddsRepository.upsert_event_odds(event_data.id, final_odds_data)
                        if upserted_id:
                            snapshot = OddsRepository.create_odds_snapshot(event_data.id, final_odds_data)
                            if snapshot:
                                odds_updated_count += 1
                                logger.info(f"✅ Final odds updated for {event_data.home_team} vs {event_data.away_team}")

                            try:
                                from infrastructure.persistence.repositories import MarketRepository

                                MarketRepository.save_markets_from_response(event_data.id, final_odds_response)
                            except Exception as market_exc:
                                logger.warning(f"Error saving markets to DB for event {event_data.id}: {market_exc}")
                        else:
                            logger.warning(f"Failed to update final odds for event {event_data.id}")
                    else:
                        logger.warning(f"No final odds data extracted for event {event_data.id}")
                else:
                    logger.debug(f"No final odds response for event {event_data.id}")
            except Exception as exc:
                logger.warning(f"Error updating odds for event {event_data.id}: {exc}")

        logger.info(f"📊 Final odds updated for {odds_updated_count}/{len(events)} events")
        logger.info(f"Processing {len(events)} events from {target_date}")
        stats = _collect_results_for_events(events, f"Results Collection ({target_date})")
        logger.info(
            f"Results collection for {target_date} completed: {stats['updated']} updated, {stats['skipped']} skipped, {stats['failed']} failed"
        )
    except Exception as exc:
        logger.error(f"Error in results collection for {target_date}: {exc}")
