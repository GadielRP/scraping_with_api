"""H2H discovery job."""

from __future__ import annotations

import logging

from modules.sofascore import api_client
from modules.jobs.parallelism import filter_upcoming_events

logger = logging.getLogger(__name__)


def run_top_h2h():
    response = api_client.get_h2h_events()
    if not response:
        logger.error("Failed to get h2h events")
        return []

    events, _ = api_client.extract_events_and_odds_from_dropping_response(
        response,
        odds_extraction=False,
        discovery_source="top_h2h",
    )
    events = filter_upcoming_events(events)
    if not events:
        logger.warning("No events found in h2h events")
    return events
