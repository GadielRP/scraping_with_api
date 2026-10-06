"""SofaScore secondary discovery feeds; shared admission precedes normalization."""

import logging
from infrastructure.settings import discovery as settings
from modules.sofascore import api_client
from modules.jobs.discovery.filters import load_tracked_source_competitions, UNRESOLVED_SCOPE
from modules.jobs.discovery.fetching import fetch_nearest_team_events

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

    if not events and not events_h2h:
        logger.warning("No events found in high value streaks events")
    return events, events_h2h


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
        max_workers=settings.SOFASCORE.team_event_workers,
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
    if not events:
        logger.warning("No events found in h2h events")
    return events


def run_winning_odds(tracked_competitions=UNRESOLVED_SCOPE):
    if tracked_competitions is UNRESOLVED_SCOPE:
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
    if not events:
        logger.warning("No events found in winning odds events")
    return events, odds_map
