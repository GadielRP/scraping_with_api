"""H2H discovery job."""

from __future__ import annotations

import logging

from modules.sofascore import api_client
from modules.competition.discovery_scope import load_tracked_source_competitions, UNRESOLVED_SCOPE
from modules.jobs.discovery_filters import filter_upcoming_events

logger = logging.getLogger(__name__)


def run_top_h2h(tracked_competitions=UNRESOLVED_SCOPE):
    if tracked_competitions is UNRESOLVED_SCOPE:
        tracked_competitions = load_tracked_source_competitions("sofascore")
    if tracked_competitions is not None and not tracked_competitions:
        return []
    response = api_client.get_h2h_events()
    if not response:
        logger.error("Failed to get h2h events")
        return []

    events, _ = api_client.extract_events_and_odds_from_dropping_response(
        response,
        odds_extraction=False,
        discovery_source="top_h2h",
        tracked_competitions=tracked_competitions,
    )
    events = filter_upcoming_events(events)
    if not events:
        logger.warning("No events found in h2h events")
    return events
