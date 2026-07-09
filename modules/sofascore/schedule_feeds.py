"""Schedule and live feed helpers for SofaScore.
    dates are in format YYYY-MM-DD."""

from __future__ import annotations

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def get_today_sport_events_response(client, date: str, sport: str, page: int = 1):
    logger.info("✈️ Fetching scheduled tournaments for %s on %s (page %d)", sport, date, page)
    return client._request_json(f"/sport/{sport}/scheduled-tournaments/{date}/page/{page}")


def get_today_sport_events_odds_response(client, date: str, sport: str):
    logger.info("✈️ Fetching scheduled odds for %s on %s", sport, date)
    return client._request_json(f"/sport/{sport}/odds/1/{date}")


def get_live_events_response_per_sport(client, sport: str) -> Optional[Dict]:
    response = client._request_json(f"/sport/{sport}/events/live")
    if not response or "events" not in response:
        return None
    return response


def get_unique_tournament_scheduled_events(client, unique_tournament_id: int | str, date: str):
    logger.info("✈️ Fetching scheduled events for tournament %s on %s", unique_tournament_id, date)
    return client._request_json(f"/unique-tournament/{unique_tournament_id}/scheduled-events/{date}")
