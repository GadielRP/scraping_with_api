"""Shared helpers for filtering discovery events."""

from __future__ import annotations

import logging
from typing import Collection, Dict, List

from infrastructure.settings import Config
from shared.temporal import now_in_timezone

logger = logging.getLogger(__name__)

_SUPPORTED_SPORTS_CONFIG: tuple[str, ...] | None = None
_SUPPORTED_SPORT_KEYS: frozenset[str] = frozenset()


def _event_payload(event: Dict) -> Dict:
    return event.get("event", event)


def _sport_key(value: object) -> str:
    """Normalize sport labels and slugs to one comparison key."""
    if isinstance(value, dict):
        value = value.get("name") or value.get("slug")
    normalized = " ".join(str(value or "").replace("-", " ").replace("_", " ").split()).casefold()
    return {"soccer": "football", "hockey": "ice hockey"}.get(normalized, normalized)


def _supported_sport_keys(supported_sports: Collection[str] | None = None) -> frozenset[str]:
    global _SUPPORTED_SPORTS_CONFIG, _SUPPORTED_SPORT_KEYS

    if supported_sports is None:
        configured = tuple(Config.SUPPORTED_SPORTS or ())
        if configured != _SUPPORTED_SPORTS_CONFIG:
            _SUPPORTED_SPORTS_CONFIG = configured
            _SUPPORTED_SPORT_KEYS = frozenset(
                key for sport in configured if (key := _sport_key(sport))
            )
        return _SUPPORTED_SPORT_KEYS

    return frozenset(
        key for sport in supported_sports or () if (key := _sport_key(sport))
    )


def is_supported_sport_name(
    sport: object,
    *,
    supported_sports: Collection[str] | None = None,
) -> bool:
    """Return whether a sport name/slug is supported by runtime configuration."""
    return _sport_key(sport) in _supported_sport_keys(supported_sports)


def is_supported_sport(
    event: Dict | None,
    *,
    supported_sports: Collection[str] | None = None,
) -> bool:
    """Recognize supported sport fields across raw and normalized payloads."""
    if not isinstance(event, dict):
        return False

    payload = _event_payload(event)
    sport = payload.get("sport") or payload.get("sportName") or payload.get("sport_name")
    if sport is None:
        tournament = payload.get("tournament") or {}
        category = tournament.get("category") or {}
        sport = category.get("sport")
    return is_supported_sport_name(sport, supported_sports=supported_sports)


def filter_upcoming_events(events: List[Dict], min_minutes_away: int = 10) -> List[Dict]:
    """Keep only events that start at least ``min_minutes_away`` minutes from now."""
    if not events:
        return []

    try:
        current_time = now_in_timezone(Config.TIMEZONE)
        current_timestamp = int(current_time.timestamp())
        min_start_timestamp = current_timestamp + (min_minutes_away * 60)

        upcoming_events = []
        filtered_count = 0
        unsupported_count = 0

        for event in events:
            event_payload = _event_payload(event)
            event_id = event_payload.get("id", "unknown")
            if not is_supported_sport(event):
                unsupported_count += 1
                continue
            start_timestamp = event_payload.get("startTimestamp")
            if not start_timestamp:
                logger.debug("Event %s has no startTimestamp, skipping", event_id)
                filtered_count += 1
                continue

            if start_timestamp >= min_start_timestamp:
                upcoming_events.append(event)
                continue

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

        if unsupported_count or filtered_count > 0:
            logger.info(
                "Filtered %s upcoming events (unsupported_sport=%s, too_soon_or_started=%s)",
                len(upcoming_events),
                unsupported_count,
                filtered_count,
            )

        return upcoming_events
    except Exception as exc:
        logger.error("Error filtering upcoming events: %s", exc)
        return events
