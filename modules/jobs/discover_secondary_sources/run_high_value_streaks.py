"""High value streaks discovery job."""

from __future__ import annotations

import logging

from modules.sofascore import api_client
from modules.competition.discovery_scope import load_tracked_source_competitions, UNRESOLVED_SCOPE
from modules.jobs.discovery_filters import filter_upcoming_events

logger = logging.getLogger(__name__)


def run_high_value_streaks(tracked_competitions=UNRESOLVED_SCOPE):
    if tracked_competitions is UNRESOLVED_SCOPE:
        tracked_competitions = load_tracked_source_competitions("sofascore")
    if tracked_competitions is not None and not tracked_competitions:
        return [], []
    response = api_client.get_high_value_streaks_events()
    if not response:
        logger.error("Failed to get high value streaks events")
        return [], []

    extracted_events, extracted_events_h2h = api_client.extract_events_from_high_value_streaks(
        response,
        tracked_competitions=tracked_competitions,
    )

    normalized_response = {"events": extracted_events}
    normalized_response_h2h = {"events": extracted_events_h2h}
    events, _ = api_client.extract_events_and_odds_from_dropping_response(
        normalized_response,
        odds_extraction=False,
        discovery_source="high_value_streaks",
        tracked_competitions=tracked_competitions,
    )
    events_h2h, _ = api_client.extract_events_and_odds_from_dropping_response(
        normalized_response_h2h,
        odds_extraction=False,
        discovery_source="high_value_streaks_h2h",
        tracked_competitions=tracked_competitions,
    )

    events = filter_upcoming_events(events)
    events_h2h = filter_upcoming_events(events_h2h)
    if not events and not events_h2h:
        logger.warning("No events found in high value streaks events")
    return events, events_h2h
