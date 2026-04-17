"""High value streaks discovery job."""

from __future__ import annotations

import logging

from modules.sofascore import api_client
from modules.jobs.parallelism import filter_upcoming_events

logger = logging.getLogger(__name__)


def run_high_value_streaks():
    response = api_client.get_high_value_streaks_events()
    if not response:
        logger.error("Failed to get high value streaks events")
        return [], []

    extracted_events, extracted_events_h2h = api_client.extract_events_from_high_value_streaks(response)
    if not extracted_events:
        logger.warning("No events found in high value streaks events")
        return [], []

    normalized_response = {"events": extracted_events}
    normalized_response_h2h = {"events": extracted_events_h2h}
    events, _ = api_client.extract_events_and_odds_from_dropping_response(
        normalized_response,
        odds_extraction=False,
        discovery_source="high_value_streaks",
    )
    events_h2h, _ = api_client.extract_events_and_odds_from_dropping_response(
        normalized_response_h2h,
        odds_extraction=False,
        discovery_source="high_value_streaks_h2h",
    )

    return filter_upcoming_events(events), filter_upcoming_events(events_h2h)
