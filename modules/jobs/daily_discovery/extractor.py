"""Daily discovery extractor for SofaScore scheduled events."""

from __future__ import annotations

import logging
from typing import Dict, List

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from modules.sofascore import api_client as default_api_client
from shared.odds_utils import validate_odds_data

from .constants import DEFAULT_DAILY_DISCOVERY_SPORTS
from .filters import filter_events_present_in_odds_feed, filter_events_starting_after_threshold
from .odds_parser import parse_today_odds_response
from .persistence import persist_event_with_odds

logger = logging.getLogger(__name__)


class DailyDiscoveryExtractor:
    """Extract and persist today's events with odds."""

    def __init__(self, api_client=None):
        self.api_client = api_client or default_api_client

    def discover_events_for_date(self, date: str, sports: List[str] | None = None) -> Dict[str, int]:
        if sports is None:
            sports = DEFAULT_DAILY_DISCOVERY_SPORTS

        logger.info("Starting daily discovery for date: %s", date)

        total_events_processed = 0
        total_events_inserted = 0
        total_odds_inserted = 0

        try:
            for sport in sports:
                try:
                    logger.info("Processing %s...", sport)
                    logger.info("Fetching today's %s odds...", sport)
                    odds_response = self.api_client.get_today_sport_events_odds_response(date, sport)

                    if not odds_response:
                        logger.warning("No odds response for %s, skipping", sport)
                        DailyDiscoveryRepository.update_sport_status(date, sport, "failed")
                        continue

                    odds_map = parse_today_odds_response(odds_response)
                    if not odds_map:
                        logger.info("No events with odds found for %s", sport)
                        continue

                    odds_event_ids = set(odds_map.keys())
                    logger.info("Found %s %s events with odds", len(odds_event_ids), sport)

                    logger.info("Fetching today's %s events...", sport)
                    events_response = self.api_client.get_today_sport_events_response(date, sport)

                    if not events_response:
                        logger.warning("No events response for %s, skipping", sport)
                        DailyDiscoveryRepository.update_sport_status(date, sport, "failed")
                        continue

                    filtered_events = filter_events_present_in_odds_feed(events_response, odds_event_ids)
                    if not filtered_events:
                        logger.info("No matching %s events found after filtering", sport)
                        continue

                    upcoming_events = filter_events_starting_after_threshold(filtered_events, min_minutes_away=10)
                    if not upcoming_events:
                        logger.info("No upcoming %s events found after time filtering", sport)
                        continue

                    logger.info("Processing %s %s events...", len(upcoming_events), sport)
                    sport_events_inserted = 0
                    sport_odds_inserted = 0

                    for event in upcoming_events:
                        event_id = event.get("id")
                        if not event_id:
                            continue

                        event_odds = odds_map.get(event_id)
                        if not event_odds:
                            logger.warning("No odds found for event %s, skipping", event_id)
                            continue

                        success = persist_event_with_odds(self.api_client, event, event_odds)
                        if success:
                            sport_events_inserted += 1
                            if validate_odds_data(event_odds):
                                sport_odds_inserted += 1

                    DailyDiscoveryRepository.update_sport_status(date, sport, "completed")
                    logger.info(
                        "%s completed: %s/%s events inserted, %s with odds",
                        sport,
                        sport_events_inserted,
                        len(upcoming_events),
                        sport_odds_inserted,
                    )

                    total_events_processed += len(upcoming_events)
                    total_events_inserted += sport_events_inserted
                    total_odds_inserted += sport_odds_inserted
                except Exception as exc:
                    logger.error("Error processing %s: %s", sport, exc)
                    DailyDiscoveryRepository.update_sport_status(date, sport, "failed")
                    continue

            logger.info(
                "Daily discovery completed for all sports: %s/%s events inserted, %s with odds",
                total_events_inserted,
                total_events_processed,
                total_odds_inserted,
            )

            return {
                "events_processed": total_events_processed,
                "events_inserted": total_events_inserted,
                "odds_inserted": total_odds_inserted,
            }
        except Exception as exc:
            logger.error("Error in discover_events_for_date: %s", exc)
            return {"events_processed": 0, "events_inserted": 0, "odds_inserted": 0}
