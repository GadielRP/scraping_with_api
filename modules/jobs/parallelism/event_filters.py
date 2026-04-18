"""Shared helpers for filtering discovery events."""

from __future__ import annotations

import logging
from typing import Dict, List

from shared.timezone_utils import get_local_now_aware

logger = logging.getLogger(__name__)


def filter_upcoming_events(events: List[Dict], min_minutes_away: int = 10) -> List[Dict]:
    """Keep only events that start at least ``min_minutes_away`` minutes from now."""
    if not events:
        return []

    try:
        current_time = get_local_now_aware()
        current_timestamp = int(current_time.timestamp())
        min_start_timestamp = current_timestamp + (min_minutes_away * 60)

        upcoming_events = []
        filtered_count = 0

        for event in events:
            start_timestamp = event.get("startTimestamp")
            if not start_timestamp:
                logger.debug("Event %s has no startTimestamp, skipping", event.get("id", "unknown"))
                filtered_count += 1
                continue

            if start_timestamp >= min_start_timestamp:
                upcoming_events.append(event)
                continue

            event_id = event.get("id", "unknown")
            time_diff_minutes = (start_timestamp - current_timestamp) / 60
            if time_diff_minutes < 0:
                logger.debug(
                    "Filtered out event %s: already started (%.1f minutes ago)",
                    event_id,
                    abs(time_diff_minutes),
                )
            else:
                logger.debug(
                    "Filtered out event %s: starts in %.1f minutes (< %s min threshold)",
                    event_id,
                    time_diff_minutes,
                    min_minutes_away,
                )
            filtered_count += 1

        if filtered_count > 0:
            logger.info(
                "Filtered %s upcoming events (excluded %s events that already started or are starting soon)",
                len(upcoming_events),
                filtered_count,
            )

        return upcoming_events
    except Exception as exc:
        logger.error("Error filtering upcoming events: %s", exc)
        return events

