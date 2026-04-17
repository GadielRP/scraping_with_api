"""Winning odds discovery job."""

from __future__ import annotations

import logging

from optimization import filter_upcoming_events
from sofascore_api import api_client

logger = logging.getLogger(__name__)


def run_winning_odds():
    response = api_client.get_winning_odds_events()
    if not response:
        logger.error("Failed to get winning odds events")
        return [], {}

    events, odds_map = api_client.extract_events_and_odds_from_dropping_response(
        response,
        odds_extraction=True,
        discovery_source="winning_odds",
    )
    events = filter_upcoming_events(events)
    if not events:
        logger.warning("No events found in winning odds events")
    return events, odds_map
