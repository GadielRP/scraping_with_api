"""Schedule and live feed helpers for SofaScore.
    dates are in format YYYY-MM-DD."""

from __future__ import annotations

import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def get_today_sport_events_response(client, date: str, sport: str, page: int = 1):
    endpoint = f"/sport/{sport}/scheduled-tournaments/{date}/page/{page}"
    logger.info(f"✈️ Fetching scheduled tournaments {sport} on {date} (page {page}). Endpoint: {endpoint}")
    return client.request_json_or_none(endpoint)


def get_today_sport_events_odds_response(client, date: str, sport: str):
    endpoint = f"/sport/{sport}/odds/1/{date}"
    logger.info(f"✈️ Fetching scheduled odds {sport} on {date}. Endpoint: {endpoint}")
    return client.request_json_or_none(endpoint)


def get_live_events_response_per_sport(client, sport: str) -> Optional[Dict]:
    endpoint = f"/sport/{sport}/events/live"
    logger.info(f"✈️ Fetching live events {sport}. Endpoint: {endpoint}")
    response = client.request_json_or_none(endpoint)
    if not response or "events" not in response:
        return None
    return response


def get_unique_tournament_scheduled_events(client, unique_tournament_id: int | str, date: str):
    endpoint = f"/unique-tournament/{unique_tournament_id}/scheduled-events/{date}"
    logger.info(f"✈️ Fetching scheduled events for tournament {unique_tournament_id} on {date}. Endpoint: {endpoint}")
    return client.request_json_or_none(endpoint)
