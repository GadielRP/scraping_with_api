"""Daily discovery extractor for SofaScore scheduled events."""

from __future__ import annotations

import logging
from typing import Dict, List

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from modules.sofascore import api_client as default_api_client

from .constants import DEFAULT_DAILY_DISCOVERY_SPORTS
from .filters import filter_events_present_in_odds_feed, filter_events_starting_after_threshold
from .odds_parser import parse_today_market_odds_response
from .persistence import persist_event_with_odds

logger = logging.getLogger(__name__)


class DailyDiscoveryExtractor:
    """Extract and persist today's events with odds."""

    def __init__(self, api_client=None):
        self.api_client = api_client or default_api_client

    def discover_events_for_date(
        self,
        date: str,
        sports: List[str] | None = None,
        run_slot: str | None = None,
    ) -> Dict[str, int]:
        if sports is None:
            sports = DEFAULT_DAILY_DISCOVERY_SPORTS

        normalized_run_slot = (run_slot or "AM").strip().upper()
        if normalized_run_slot not in {"AM", "PM"}:
            logger.warning(
                "Daily discovery extractor received invalid run_slot=%s; defaulting to AM.",
                run_slot,
            )
            normalized_run_slot = "AM"
        elif run_slot is None:
            logger.warning(
                "Daily discovery extractor invoked without run_slot; defaulting to AM for backward compatibility."
            )

        logger.info("Starting daily discovery for date: %s", date)

        total_events_processed = 0
        total_events_inserted = 0
        total_odds_inserted = 0

        try:
            for sport in sports:
                try:
                    logger.info("Processing %s...", sport)
                    logger.info("Fetching today's %s odds...", sport)
                    odds_map = {}
                    try:
                        odds_response = self.api_client.get_today_sport_events_odds_response(date, sport)
                        if odds_response:
                            odds_map = parse_today_market_odds_response(odds_response) or {}
                        else:
                            logger.warning("No odds response for %s, proceeding to fetch events without odds", sport)
                    except Exception as exc:
                        logger.warning("Failed to fetch odds for %s: %s. Will proceed without odds.", sport, exc)

                    odds_event_ids = set(odds_map.keys())
                    logger.info("Found %s %s events with odds in the feed", len(odds_event_ids), sport)

                    logger.info("Fetching today's %s scheduled tournaments...", sport)

                    unique_tournament_ids = []
                    page = 1
                    failed = False

                    while True:
                        page_response = self.api_client.get_today_sport_events_response(date, sport, page)

                        if not page_response:
                            if page == 1:
                                failed = True
                            break

                        scheduled = page_response.get("scheduled", [])
                        if not scheduled:
                            break

                        for item in scheduled:
                            tz_count = item.get("timezoneEventCount", {})
                            total_events_in_tz = sum(tz_count.values()) if tz_count else 0
                            if tz_count and total_events_in_tz == 0:
                                continue

                            ut = item.get("tournament", {}).get("uniqueTournament", {})
                            ut_id = ut.get("id")
                            if ut_id and ut_id not in unique_tournament_ids:
                                unique_tournament_ids.append(ut_id)

                        if not page_response.get("hasNextPage", False):
                            break

                        page += 1

                    if failed:
                        logger.warning("No tournaments response for %s, skipping", sport)
                        DailyDiscoveryRepository.update_sport_status(date, normalized_run_slot, sport, "failed")
                        continue

                    logger.info("Found %d unique tournaments for %s. Fetching events...", len(unique_tournament_ids), sport)

                    all_events = []
                    for ut_id in unique_tournament_ids:
                        try:
                            ut_events_response = self.api_client.get_unique_tournament_scheduled_events(ut_id, date)
                            if ut_events_response and "events" in ut_events_response:
                                all_events.extend(ut_events_response["events"])
                        except Exception as exc:
                            logger.warning("Failed to fetch events for tournament %s: %s", ut_id, exc)

                    if not all_events:
                        logger.info("No %s events found", sport)
                        DailyDiscoveryRepository.update_sport_status(date, normalized_run_slot, sport, "completed")
                        continue

                    # Determine which events have not started yet (using our standard min_minutes_away=10 threshold)
                    upcoming_events = filter_events_starting_after_threshold(all_events, min_minutes_away=10)
                    upcoming_event_ids = {e["id"] for e in upcoming_events if e.get("id")}

                    logger.info("Processing %s %s events...", len(all_events), sport)
                    sport_events_inserted = 0
                    sport_odds_inserted = 0

                    for event in all_events:
                        event_id = event.get("id")
                        if not event_id:
                            continue

                        # Only persist odds if the event has not started yet and is present in the odds feed
                        has_odds_data = event_id in odds_event_ids and event_id in upcoming_event_ids
                        event_odds = odds_map.get(event_id) if has_odds_data else None

                        success = persist_event_with_odds(self.api_client, event, event_odds)
                        if success:
                            sport_events_inserted += 1
                            if event_odds:
                                sport_odds_inserted += 1

                    DailyDiscoveryRepository.update_sport_status(date, normalized_run_slot, sport, "completed")
                    logger.info(
                        "%s completed: %s/%s events inserted, %s with odds",
                        sport,
                        sport_events_inserted,
                        len(all_events),
                        sport_odds_inserted,
                    )

                    total_events_processed += len(all_events)
                    total_events_inserted += sport_events_inserted
                    total_odds_inserted += sport_odds_inserted
                except Exception as exc:
                    logger.error("Error processing %s: %s", sport, exc)
                    DailyDiscoveryRepository.update_sport_status(date, normalized_run_slot, sport, "failed")
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
