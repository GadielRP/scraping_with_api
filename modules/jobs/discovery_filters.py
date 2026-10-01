"""Shared discovery event and start-time filters."""

from __future__ import annotations

import logging
from typing import Dict, Iterable, List

from infrastructure.settings import Config
from modules.sports.catalog import is_supported_sofascore_event
from shared.temporal import now_in_timezone

logger = logging.getLogger(__name__)


def filter_supported_sofascore_events(events: Iterable[Dict] | None) -> List[Dict]:
    return [event for event in events or () if is_supported_sofascore_event(event)]


def filter_upcoming_events(events: List[Dict], min_minutes_away: int = 10) -> List[Dict]:
    """Keep supported events starting at least ``min_minutes_away`` from now."""
    if not events:
        return []

    try:
        current_timestamp = int(now_in_timezone(Config.TIMEZONE).timestamp())
        minimum_start = current_timestamp + min_minutes_away * 60
        upcoming = []
        rejected_unsupported_sport = 0
        rejected_missing_start_timestamp = 0
        rejected_invalid_start_timestamp = 0
        rejected_start_too_soon_or_started = 0
        rejected_invalid_event_payload = 0

        for event in events:
            if not isinstance(event, dict):
                rejected_invalid_event_payload += 1
                logger.debug(
                    "Upcoming filter rejected event source_event_id=unknown "
                    "reason=invalid_event_payload"
                )
                continue
            payload = event.get("event", event)
            if not isinstance(payload, dict):
                rejected_invalid_event_payload += 1
                logger.debug(
                    "Upcoming filter rejected event source_event_id=unknown "
                    "reason=invalid_event_payload"
                )
                continue
            if not is_supported_sofascore_event(event):
                rejected_unsupported_sport += 1
                logger.debug(
                    "Upcoming filter rejected event source_event_id=%s sport=%s "
                    "reason=unsupported_sport",
                    payload.get("id"),
                    payload.get("sport"),
                )
                continue
            raw_start_timestamp = payload.get("startTimestamp")
            if raw_start_timestamp is None:
                rejected_missing_start_timestamp += 1
                logger.debug(
                    "Upcoming filter rejected event source_event_id=%s "
                    "reason=missing_start_timestamp",
                    payload.get("id"),
                )
                continue
            try:
                start_timestamp = int(raw_start_timestamp)
            except (TypeError, ValueError, OverflowError):
                rejected_invalid_start_timestamp += 1
                logger.debug(
                    "Upcoming filter rejected event source_event_id=%s start_timestamp=%r "
                    "reason=invalid_start_timestamp",
                    payload.get("id"),
                    raw_start_timestamp,
                )
                continue
            if start_timestamp >= minimum_start:
                upcoming.append(event)
            else:
                rejected_start_too_soon_or_started += 1
                logger.debug(
                    "Upcoming filter rejected event source_event_id=%s start_timestamp=%s "
                    "minimum_start_timestamp=%s reason=start_too_soon_or_started",
                    payload.get("id"),
                    start_timestamp,
                    minimum_start,
                )

        logger.info(
            "Evaluated upcoming events input=%s kept=%s "
            "rejected_unsupported_sport=%s rejected_missing_start_timestamp=%s "
            "rejected_invalid_start_timestamp=%s rejected_start_too_soon_or_started=%s "
            "rejected_invalid_event_payload=%s minimum_minutes_away=%s",
            len(events),
            len(upcoming),
            rejected_unsupported_sport,
            rejected_missing_start_timestamp,
            rejected_invalid_start_timestamp,
            rejected_start_too_soon_or_started,
            rejected_invalid_event_payload,
            min_minutes_away,
        )
        return upcoming
    except Exception as exc:
        logger.exception(
            "Upcoming event evaluation failed; rejecting all candidates "
            "reason=upcoming_filter_error error=%s",
            exc,
        )
        return []


__all__ = [
    "filter_supported_sofascore_events",
    "filter_upcoming_events",
]
