"""Filtering helpers for daily discovery events."""

from __future__ import annotations

import logging
from typing import Dict, List

from shared.timezone_utils import get_local_now_aware

logger = logging.getLogger(__name__)


def filter_events_present_in_odds_feed(events_response: Dict, odds_event_ids: set[int]) -> List[Dict]:
    try:
        if not events_response or "events" not in events_response:
            logger.warning("No events found in response")
            return []

        all_events = events_response["events"]
        filtered_events = [event for event in all_events if event.get("id") and event.get("id") in odds_event_ids]
        logger.info("Filtered %s events with odds out of %s total events", len(filtered_events), len(all_events))
        return filtered_events
    except Exception as exc:
        logger.error("Error filtering events: %s", exc)
        return []


def filter_events_starting_after_threshold(events: List[Dict], min_minutes_away: int = 10) -> List[Dict]:
    try:
        if not events:
            return []

        current_time = get_local_now_aware()
        current_timestamp = int(current_time.timestamp())
        min_start_timestamp = current_timestamp + (min_minutes_away * 60)

        upcoming_events = []
        excluded_count = 0
        for event in events:
            start_timestamp = event.get("startTimestamp")
            if not start_timestamp:
                logger.debug("Event %s has no startTimestamp, skipping", event.get("id", "unknown"))
                excluded_count += 1
                continue

            if start_timestamp >= min_start_timestamp:
                upcoming_events.append(event)
            else:
                event_id = event.get("id", "unknown")
                time_diff_minutes = (start_timestamp - current_timestamp) / 60
                logger.debug(
                    "Filtered out event %s: starts in %.1f minutes (< %s min threshold)",
                    event_id,
                    time_diff_minutes,
                    min_minutes_away,
                )
                excluded_count += 1

        logger.info(
            "Filtered %s upcoming events (excluded %s events that already started or are starting soon)",
            len(upcoming_events),
            excluded_count,
        )
        return upcoming_events
    except Exception as exc:
        logger.error("Error filtering upcoming events: %s", exc)
        return events
