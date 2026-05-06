"""Dropping odds discovery job."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from infrastructure.persistence.repositories import EventRepository, OddsRepository
from modules.sofascore import api_client
from modules.jobs.parallelism import filter_upcoming_events, process_with_parallel_db_ops

logger = logging.getLogger(__name__)


def run_discover_dropping_odds() -> None:
    """Discover events from dropping odds and persist them."""
    logger.info("Starting Job A: Event Discovery with Odds Processing")

    dropping_sports = [
        "football",
        "basketball",
        "volleyball",
        "american-football",
        "ice-hockey",
        "darts",
        "baseball",
        "rugby",
    ]

    processed_event_ids = set()
    total_processed = 0
    total_skipped = 0

    try:
        logger.info("Step 1: Fetching /dropping/all endpoint")
        response_all = api_client.get_dropping_odds_with_odds_and_events_response()
        if response_all:
            # to save dropping/all events in json format
            # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # json_filename = os.path.join("debug", f"debug_discovery_all_{timestamp}.json")
            # try:
            #     os.makedirs("debug", exist_ok=True)
            #     with open(json_filename, "w", encoding="utf-8") as handle:
            #         json.dump(response_all, handle, indent=2, ensure_ascii=False)
            # except Exception as exc:
            #     logger.warning(f"Failed to save JSON debug file: {exc}")

            events_all, odds_map_all = api_client.extract_events_and_odds_from_dropping_response(
                response_all,
                odds_extraction=True,
                discovery_source="dropping_odds",
            )

            if events_all:
                logger.info(f"Found {len(events_all)} events in /dropping/all endpoint")
                events_all = filter_upcoming_events(events_all)
                if events_all:
                    processed_count, skipped_count = process_with_parallel_db_ops(
                        events_all,
                        odds_map_all,
                        discovery_source="dropping_odds",
                        max_workers=10,
                    )
                    total_processed += processed_count
                    total_skipped += skipped_count
                    for event in events_all:
                        processed_event_ids.add(event["id"])
                    logger.info(
                        f"/dropping/all completed: processed {processed_count}/{len(events_all)} events, skipped {skipped_count} events"
                    )
            else:
                logger.warning("No events found in /dropping/all endpoint")
        else:
            logger.error("Failed to get dropping odds with odds data from /dropping/all")

        logger.info(f"Step 2: Fetching and processing {len(dropping_sports)} individual sports")
        for sport in dropping_sports:
            try:
                logger.info(f"Fetching dropping odds for sport: {sport}")
                response_sport = api_client.get_dropping_odds_with_odds_and_events_response(sport=sport)
                if not response_sport:
                    logger.warning(f"No response for sport {sport}, skipping")
                    continue

                events_sport, odds_map_sport = api_client.extract_events_and_odds_from_dropping_response(
                    response_sport,
                    odds_extraction=True,
                    discovery_source="dropping_odds",
                )
                if not events_sport:
                    logger.info(f"No events found for sport {sport}")
                    continue

                events_sport = filter_upcoming_events(events_sport)
                if not events_sport:
                    logger.info(f"No upcoming events for sport {sport} after filtering")
                    continue

                new_events = [event for event in events_sport if event["id"] not in processed_event_ids]
                skipped_duplicates = len(events_sport) - len(new_events)
                if skipped_duplicates > 0:
                    logger.info(
                        f"Sport {sport}: Skipping {skipped_duplicates} duplicate events already processed from /dropping/all"
                    )

                if not new_events:
                    logger.info(f"Sport {sport}: All events already processed, skipping")
                    continue

                new_odds_map = {
                    str(event_id): odds_data
                    for event_id, odds_data in odds_map_sport.items()
                    if int(event_id) in [event["id"] for event in new_events]
                }

                logger.info(f"Sport {sport}: Processing {len(new_events)} new events (skipped {skipped_duplicates} duplicates)")
                processed_count, skipped_count = process_with_parallel_db_ops(
                    new_events,
                    new_odds_map,
                    discovery_source="dropping_odds",
                    max_workers=10,
                )

                total_processed += processed_count
                total_skipped += skipped_count
                for event in new_events:
                    processed_event_ids.add(event["id"])

                logger.info(f"Sport {sport} completed: processed {processed_count}/{len(new_events)} events, skipped {skipped_count} events")
            except Exception as exc:
                logger.error(f"Error processing sport {sport}: {exc}")

        logger.info(f"Job A completed: Total processed {total_processed} events, total skipped {total_skipped} events")
        logger.info(f"Total unique events processed: {len(processed_event_ids)}")
    except Exception as exc:
        logger.error(f"Error in Job A: {exc}")
