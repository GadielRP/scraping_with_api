"""Team streaks discovery job."""

from __future__ import annotations

import logging

from modules.sofascore import api_client
from modules.competition.discovery_scope import load_tracked_source_competitions, UNRESOLVED_SCOPE
from modules.jobs.discovery.fetching import fetch_nearest_team_events

logger = logging.getLogger(__name__)


def run_team_streaks(tracked_competitions=UNRESOLVED_SCOPE):
    if tracked_competitions is UNRESOLVED_SCOPE:
        tracked_competitions = load_tracked_source_competitions("sofascore")
    if tracked_competitions is not None and not tracked_competitions:
        return []
    response = api_client.get_team_streaks_events()
    if not response:
        logger.error("Failed to get team streaks events")
        return []

    team_ids = get_team_ids_from_team_streaks(response)
    if not team_ids:
        logger.warning("No team IDs found in team streaks response")
        return []

    logger.info(f"Found {len(team_ids)} teams in team streaks response")
    return fetch_nearest_team_events(
        team_ids,
        max_workers=10,
        tracked_competitions=tracked_competitions,
    )


def get_team_ids_from_team_streaks(response: dict) -> list[int]:
    team_ids: list[int] = []
    for item in response.get("topTeamStreaks", []):
        team = item.get("team", {})
        team_id = team.get("id")
        if team_id is not None:
            team_ids.append(team_id)
    return team_ids
