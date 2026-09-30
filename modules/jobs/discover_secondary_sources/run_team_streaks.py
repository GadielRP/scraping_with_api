"""Team streaks discovery job."""

from __future__ import annotations

import logging

from modules.sofascore import api_client
from modules.competition.discovery_scope import load_tracked_source_competitions
from modules.jobs.parallelism import parallel_team_event_fetching

logger = logging.getLogger(__name__)


def run_team_streaks(tracked_competitions=None):
    if tracked_competitions is None:
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
    return parallel_team_event_fetching(
        team_ids,
        max_workers=10,
        tracked_competitions=tracked_competitions,
    )

def get_team_ids_from_team_streaks(response: Dict) -> List[int]:
    team_ids: List[int] = []
    for item in response.get("topTeamStreaks", []):
        team = item.get("team", {})
        team_id = team.get("id")
        if team_id is not None:
            team_ids.append(team_id)
    return team_ids
