"""Event detail and synchronization helpers for SofaScore."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from infrastructure.persistence.repositories import EventRepository, SeasonRepository
from modules.observations import sport_observation_service
from modules.observations.sofascore_extractor import extract_observations_from_sofascore_response

from .event_normalizer import get_event_information
from .exceptions import SofaScoreNotFoundException, SofaScoreRateLimitException
from .results_parser import extract_results_from_response

logger = logging.getLogger(__name__)


def fetch_event_response(client, event_id: int, delete_event_on_404: bool = True) -> Optional[Dict]:
    endpoint = f"/event/{event_id}"
    try:
        return client._make_request(endpoint)
    except SofaScoreNotFoundException:
        if delete_event_on_404:
            deleted = EventRepository.batch_delete_events([event_id])
            if deleted:
                logger.info("Deleted event %s after 404 response", event_id)
            else:
                logger.warning("Failed to delete event %s after 404 response", event_id)
        return None
    except SofaScoreRateLimitException:
        logger.warning("Rate limited while fetching event %s", event_id)
        return None


def get_event_details(client, event_id: int) -> Optional[Dict]:
    response = fetch_event_response(client, event_id, delete_event_on_404=True)
    if not response or "event" not in response:
        return None
    return response["event"]


def update_event_information_from_response(response: Dict) -> bool:
    try:
        if not response or "event" not in response:
            logger.warning("No event data in response for information update")
            return False

        event_response = response["event"]
        event_data = get_event_information(event_response, discovery_source="results_sync")
        if not event_data or not event_data.get("id"):
            logger.warning("Could not extract event information from response")
            return False

        event_data.pop("discovery_source", None)
        updated_event = EventRepository.upsert_event(event_data)
        if updated_event:
            logger.info(
                "Event information updated for event %s from results sync (season_id=%s, round=%s, season_year=%s)",
                event_data["id"],
                event_data.get("season_id"),
                event_data.get("round"),
                event_data.get("season_year"),
            )
            return True

        logger.warning("Failed to update event information for event %s", event_data.get("id"))
        return False
    except Exception as exc:
        logger.error("Error updating event information from response: %s", exc)
        return False


def _extract_observations_from_response(response: Dict) -> Optional[List[Dict]]:
    return extract_observations_from_sofascore_response(response)


def extract_observations_from_response(response: Dict) -> Optional[List[Dict]]:
    """Public wrapper for observation extraction."""
    return _extract_observations_from_response(response)


def _extract_metadata_snapshot(response: Dict) -> Optional[Dict]:
    try:
        if not response or "event" not in response:
            return None

        event_data = response["event"]
        home_team = event_data.get("homeTeam", {})
        away_team = event_data.get("awayTeam", {})
        tournament = event_data.get("tournament", {})
        season_data = event_data.get("season", {})

        season_year_raw = season_data.get("year")
        season_year = SeasonRepository._parse_year(season_year_raw) if season_year_raw is not None else None

        observations = _extract_observations_from_response(response) or []
        if not any(observation.get("type") == "rankings" for observation in observations):
            home_ranking = home_team.get("ranking")
            away_ranking = away_team.get("ranking")
            if home_ranking is not None or away_ranking is not None:
                observations.append(
                    {
                        "type": "rankings",
                        "home_ranking": home_ranking,
                        "away_ranking": away_ranking,
                    }
                )

        return {
            "home_team_id": home_team.get("id"),
            "away_team_id": away_team.get("id"),
            "home_team_ranking": home_team.get("ranking"),
            "away_team_ranking": away_team.get("ranking"),
            "tournament_id": tournament.get("id"),
            "tournament_name": tournament.get("name"),
            "competition_slug": tournament.get("uniqueTournament", {}).get("slug"),
            "season_id": str(season_data.get("id", "")) if season_data.get("id") else None,
            "season_name": season_data.get("name"),
            "season_year": season_year,
            "observations": observations,
        }
    except Exception as exc:
        logger.warning("Error extracting metadata snapshot: %s", exc)
        return None


def get_event_results(
    client,
    event_id: int,
    update_time: bool = False,
    update_court_type: bool = False,
    minutes_until_start: int = 0,
    update_event_info: bool = True,
    return_snapshot: bool = False,
    current_start_time=None,
) -> Optional[Dict]:
    try:
        if update_court_type:
            logger.info("Fetching /event/%s endpoint to update court type", event_id)
        elif update_time:
            logger.info("Fetching /event/%s endpoint to update time", event_id)
        elif return_snapshot and update_time==False:
            logger.info("Fetching /event/%s endpoint to get metadata snapshot (timestamp correction bypassed)", event_id)
        else:
            logger.info("Fetching event results for event %s", event_id)

        response = fetch_event_response(client, event_id, delete_event_on_404=True)
        if not response:
            logger.warning("No response received for event %s", event_id)
            return None

        if update_event_info and not update_court_type:
            update_event_information_from_response(response)

        if update_court_type:
            event_data = response.get("event", {})
            home_team_ranking = event_data.get("homeTeam", {}).get("ranking")
            away_team_ranking = event_data.get("awayTeam", {}).get("ranking")
            return [
                {
                    "type": "ground_type",
                    "value": sport_observation_service.extract_and_save_tennis_ground_type(event_id, response),
                },
                {
                    "type": "rankings",
                    "home_ranking": home_team_ranking,
                    "away_ranking": away_team_ranking,
                },
            ]

        if update_time:
            logger.info("Checking and updating starting time for event %s", event_id)
            event_data = response.get("event", {})
            start_timestamp = event_data.get("startTimestamp")
            if start_timestamp is None:
                logger.warning("No startTimestamp found in API response for event %s", event_id)
                if return_snapshot:
                    return None, None
                return None

            timing_result = client.check_and_update_starting_time(
                event_id,
                start_timestamp,
                send_alert=True,
                current_starting_time=current_start_time,
            )

            if return_snapshot:
                return timing_result, _extract_metadata_snapshot(response)
            return timing_result

        elif return_snapshot and update_time==False:
            logger.info("Fetching metadata snapshot for event %s (timestamp correction bypassed)", event_id)
            return True, _extract_metadata_snapshot(response)

        result = extract_results_from_response(response)
        if isinstance(result, dict) and result.get("_canceled"):
            deleted = EventRepository.batch_delete_events([event_id])
            if deleted:
                logger.info("Deleted canceled event %s", event_id)
            else:
                logger.warning("Failed to delete canceled event %s", event_id)
            return None

        return result
    except Exception as exc:
        logger.error("Error fetching event results for %s: %s", event_id, exc)
        return None
