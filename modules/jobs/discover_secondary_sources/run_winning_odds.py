"""Winning odds discovery job."""

from __future__ import annotations

import logging

from modules.sofascore import api_client
from modules.competition.discovery_scope import load_tracked_source_competitions
from modules.jobs.discovery_filters import filter_upcoming_events

logger = logging.getLogger(__name__)


def run_winning_odds(tracked_competitions=None):
    if tracked_competitions is None:
        tracked_competitions = load_tracked_source_competitions("sofascore")
    if tracked_competitions is not None and not tracked_competitions:
        return [], {}
    response = api_client.get_winning_odds_events()
    if not response:
        logger.error("Failed to get winning odds events")
        return [], {}

    events, odds_map = api_client.extract_events_and_odds_from_dropping_response(
        response,
        odds_extraction=True,
        discovery_source="winning_odds",
        tracked_competitions=tracked_competitions,
    )
    events = filter_upcoming_events(events)
    if not events:
        logger.warning("No events found in winning odds events")
    return events, odds_map
