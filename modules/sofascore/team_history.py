"""Team history helpers for SofaScore feeds."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from shared.timezone_utils import get_local_now_aware

logger = logging.getLogger(__name__)





def get_nearest_event_for_team(client, team_id: int) -> Optional[Dict]:
    response = client._request_json(f"/team/{team_id}/events/next/0")
    if not response:
        return None

    events = response.get("events", [])
    if not events:
        return None

    now_ts = get_local_now_aware().timestamp()
    future_events = [event for event in events if event.get("startTimestamp", 0) >= now_ts]

    if not future_events:
        events.sort(key=lambda event: abs(event.get("startTimestamp", 0) - now_ts))
        return events[0]

    nearest_event = min(future_events, key=lambda event: event.get("startTimestamp", float("inf")))
    return nearest_event


def get_team_last_results_response(
    client,
    team_id: int,
    is_tennis_singles: bool = False,
    is_tennis_doubles: bool = False,
    fetch_index: int = 0,
) -> Optional[Dict]:
    if fetch_index < 0:
        fetch_index = 0

    fetch_number_display = fetch_index + 1

    if is_tennis_singles:
        endpoint = f"/team/{team_id}/events/singles/last/{fetch_index}"
        logger.info("Fetching %s (attempt %s) last singles events for player %s", endpoint, fetch_number_display, team_id)
    elif is_tennis_doubles:
        endpoint = f"/team/{team_id}/events/doubles/last/{fetch_index}"
        logger.info("Fetching %s (attempt %s) last doubles events for player %s", endpoint, fetch_number_display, team_id)
    else:
        endpoint = f"/team/{team_id}/events/last/{fetch_index}"
        logger.info("Fetching %s (attempt %s) last events for team %s", endpoint, fetch_number_display, team_id)

    response = client._request_json(endpoint)
    if not response or "events" not in response:
        logger.error("No results found for team %s", team_id)
        return None
    return response
