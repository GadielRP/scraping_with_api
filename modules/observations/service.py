"""Service for reading, persisting and formatting sports observations."""

from __future__ import annotations

import logging
from typing import Optional

from infrastructure.persistence.repositories import ObservationRepository

from .sofascore_extractor import extract_observations_from_sofascore_response
from .tennis import format_tennis_ground_type

logger = logging.getLogger(__name__)


class SportObservationService:
    def __init__(self, observation_repo: ObservationRepository | None = None):
        self.observation_repo = observation_repo or ObservationRepository()

    def event_has_observations(self, event_id: int) -> bool:
        try:
            observations = self.observation_repo.get_all_observations(event_id)
            has_observations = len(observations) > 0
            if has_observations:
                logger.info("Event %s already has %s observations - skipping API call", event_id, len(observations))
            return has_observations
        except Exception as exc:
            logger.warning("Error checking observations for event %s: %s", event_id, exc)
            return False

    def _observation_rows(
        self,
        event_id: int,
        observations: list[dict],
        fallback_sport: str | None = None,
    ) -> list[dict]:
        rows: list[dict] = []
        for observation in observations or []:
            observation_type = observation.get("type")
            
            # Skip in-memory-only observations that are not persisted to database.
            if observation_type == "rankings":
                continue
                
            observation_value = observation.get("value")
            sport = observation.get("sport") or fallback_sport or "Unknown"
            if not observation_type or observation_value is None:
                logger.info("Invalid observation data: %s", observation)
                continue
            rows.append(
                {
                    "event_id": event_id,
                    "sport": sport,
                    "observation_type": observation_type,
                    "observation_value": observation_value,
                }
            )
        return rows

    def save_observations_for_event(
        self,
        event_id: int,
        observations: list[dict],
        fallback_sport: str | None = None,
    ) -> str | None:
        try:
            rows = self._observation_rows(event_id, observations, fallback_sport)
            if not rows:
                return None
            saved = self.observation_repo.upsert_observations(rows)
            if not saved:
                return None
            return next(
                (
                    row["observation_value"]
                    for row in rows
                    if row["observation_type"] == "ground_type"
                ),
                None,
            )
        except Exception as exc:
            logger.warning("Error saving observations for event %s: %s", event_id, exc)
            return None

    def save_observations_for_events(
        self,
        events: list[tuple[int, list[dict], str | None]],
    ) -> int:
        rows: list[dict] = []
        try:
            for event_id, observations, fallback_sport in events:
                rows.extend(
                    self._observation_rows(event_id, observations, fallback_sport)
                )
            if not rows:
                return 0
            return self.observation_repo.upsert_observations(rows) or 0
        except Exception as exc:
            logger.warning("Error saving observations for %s events: %s", len(events), exc)
            return 0

    def observations_for_events(
        self,
        event_ids: list[int],
    ) -> dict[int, list[dict]]:
        return self.observation_repo.get_observations_for_events(event_ids)

    def extract_and_save_tennis_ground_type(self, event_id: int, api_response: dict) -> str | None:
        try:
            logger.info("Extracting ground type for tennis event %s", event_id)
            observations = extract_observations_from_sofascore_response(api_response)
            if not observations:
                logger.info("No observations found for tennis event %s", event_id)
                return None
            return self.save_observations_for_event(event_id, observations, fallback_sport="Tennis")
        except Exception as exc:
            logger.warning("Error extracting ground type for tennis event %s: %s", event_id, exc)
            return None

    def process_result_observations(self, event, result_data: dict) -> None:
        try:
            observations = result_data.get("observations")
            if not observations:
                logger.debug("No observations found for event %s", event.id)
                return

            self.save_observations_for_event(event.id, observations, fallback_sport=getattr(event, "sport", None))
        except Exception as exc:
            logger.warning("Error processing observations for event %s: %s", event.id, exc)

    def format_event_observation_summary(self, event_id: int, sport: str) -> Optional[str]:
        try:
            if not event_id or not sport:
                return None

            if str(sport).lower() not in {"tennis", "tennis doubles"}:
                return None

            ground_type_obs = self.observation_repo.get_observation(event_id, "ground_type")
            if ground_type_obs and ground_type_obs.observation_value:
                return format_tennis_ground_type(ground_type_obs.observation_value)

            return format_tennis_ground_type(None)
        except Exception as exc:
            logger.warning("Error getting sport-specific info for event %s: %s", event_id, exc)
            return None

    def format_candidate_observation_summary(self, candidate_event_id: int, candidate_sport: str) -> Optional[str]:
        try:
            return self.format_event_observation_summary(candidate_event_id, candidate_sport)
        except Exception as exc:
            logger.warning("Error formatting sport info for candidate %s: %s", candidate_event_id, exc)
            return None

    def has_observations_for_event(self, event_id: int) -> bool:
        return self.event_has_observations(event_id)

    def extract_tennis_ground_type(self, event_id: int, api_response: dict) -> str | None:
        return self.extract_and_save_tennis_ground_type(event_id, api_response)

    def process_event_observations(self, event, result_data: dict) -> None:
        self.process_result_observations(event, result_data)

    def get_sport_specific_info(self, event_id: int, sport: str) -> Optional[str]:
        return self.format_event_observation_summary(event_id, sport)

    def format_sport_info_for_candidates(self, candidate_event_id: int, candidate_sport: str) -> Optional[str]:
        return self.format_candidate_observation_summary(candidate_event_id, candidate_sport)


sport_observation_service = SportObservationService()
sport_observations_manager = sport_observation_service

__all__ = [
    "SportObservationService",
    "sport_observation_service",
    "sport_observations_manager",
]
