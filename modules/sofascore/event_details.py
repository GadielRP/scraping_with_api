"""Event detail and synchronization helpers for SofaScore."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from infrastructure.persistence.repositories import EventRepository, SeasonRepository, EventSourceMappingRepository
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
            canonical_event_id = EventSourceMappingRepository.get_event_id_by_source("sofascore", str(event_id))
            if canonical_event_id is None:
                logger.warning("Could not resolve canonical event_id for SofaScore event %s after 404", event_id)
                return None

            deleted = EventRepository.batch_delete_events([canonical_event_id])
            if deleted:
                logger.info("Deleted canonical event %s after 404 response for SofaScore event %s", canonical_event_id, event_id)
            else:
                logger.warning("Failed to delete canonical event %s after 404 response", canonical_event_id)
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
        event_payload = event_data.get("event", event_data) if event_data else {}
        if not event_payload or not event_payload.get("id"):
            logger.warning("Could not extract event information from response")
            return False

        event_payload.pop("discovery_source", None)
        updated_event = EventRepository.upsert_event(event_data)
        if updated_event:
            logger.info(
                "Event information updated for event %s from results sync (season_id=%s, round=%s, season_year=%s)",
                event_payload["id"],
                event_payload.get("season_id"),
                event_payload.get("round"),
                event_payload.get("season_year"),
            )
            return True

        logger.warning("Failed to update event information for event %s", event_payload.get("id"))
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
        unique_tournament = tournament.get("uniqueTournament", {})
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
            "unique_tournament_id": unique_tournament.get("id"),
            "unique_tournament_name": unique_tournament.get("name"),
            "competition_slug": unique_tournament.get("slug"),
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
    canonical_event_id: int | None = None,
) -> Optional[Dict]:
    try:
        if update_court_type:
            logger.info("✈️ Fetching /event/%s endpoint to update court type", event_id)
        elif update_time:
            logger.info("⏱️ Fetching /event/%s endpoint to update time", event_id)
        elif return_snapshot and update_time==False:
            logger.info("✈️ Fetching /event/%s endpoint to get metadata snapshot (timestamp correction bypassed)", event_id)
        else:
            logger.info("✈️ Fetching event results for event %s", event_id)

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
            observation_event_id = canonical_event_id
            if observation_event_id is None:
                observation_event_id = EventSourceMappingRepository.get_event_id_by_source(
                    "sofascore",
                    str(event_id),
                )
            ground_type = None
            if observation_event_id is None:
                logger.warning(
                    "Skipping tennis ground-type persistence: canonical event ID was not resolved for SofaScore event %s",
                    event_id,
                )
            else:
                ground_type = sport_observation_service.extract_and_save_tennis_ground_type(
                    observation_event_id,
                    response,
                )
            return [
                {
                    "type": "ground_type",
                    "value": ground_type,
                },
                {
                    "type": "rankings",
                    "home_ranking": home_team_ranking,
                    "away_ranking": away_team_ranking,
                },
            ]

        if update_time:
            logger.info("🔎 Checking and updating starting time for event %s", event_id)
            event_data = response.get("event", {})
            start_timestamp = event_data.get("startTimestamp")
            if start_timestamp is None:
                logger.warning("No startTimestamp found in API response for event %s", event_id)
                if return_snapshot:
                    return None, None
                return None

            timing_event_id = canonical_event_id
            if timing_event_id is None:
                timing_event_id = EventSourceMappingRepository.get_event_id_by_source(
                    "sofascore",
                    str(event_id),
                )

            if timing_event_id is None:
                logger.warning(
                    "Skipping time update: canonical event ID was not resolved for SofaScore event %s",
                    event_id,
                )
                if return_snapshot:
                    return None, _extract_metadata_snapshot(response)
                return None

            timing_result = client.check_and_update_starting_time(
                timing_event_id,
                start_timestamp,
                send_alert=True,
                current_starting_time=current_start_time,
            )

            if return_snapshot:
                return timing_result, _extract_metadata_snapshot(response)
            return timing_result

        elif return_snapshot and update_time==False:
            logger.info("Parsing metadata snapshot for event %s (timestamp correction bypassed)", event_id)
            return True, _extract_metadata_snapshot(response)

        result = extract_results_from_response(response)
        if isinstance(result, dict) and result.get("_canceled"):
            canonical_event_id = EventSourceMappingRepository.get_event_id_by_source("sofascore", str(event_id))
            if canonical_event_id is None:
                logger.warning("Could not resolve canonical event_id for canceled SofaScore event %s", event_id)
                return None

            deleted = EventRepository.batch_delete_events([canonical_event_id])
            if deleted:
                logger.info("Deleted canceled canonical event %s for SofaScore event %s", canonical_event_id, event_id)
            else:
                logger.warning("Failed to delete canceled canonical event %s", canonical_event_id)
            return None

        return result
    except Exception as exc:
        logger.error("Error fetching event results for %s: %s", event_id, exc)
        return None
