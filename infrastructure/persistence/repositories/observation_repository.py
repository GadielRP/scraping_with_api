import logging
from typing import List, Optional, Dict

from sqlalchemy import and_

from infrastructure.persistence.models import EventObservation
from infrastructure.persistence.database import db_manager
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class ObservationRepository:
    """Repository for event observation-related database operations"""

    @staticmethod
    def _upsert_in_session(
        session,
        event_id: int,
        sport: str,
        observation_type: str,
        observation_value: str,
        *,
        existing: Optional[EventObservation] = None,
        skip_lookup: bool = False,
    ) -> EventObservation:
        observation = existing
        if observation is None and not skip_lookup:
            observation = session.query(EventObservation).filter(
                and_(
                    EventObservation.event_id == event_id,
                    EventObservation.observation_type == observation_type,
                )
            ).first()

        if observation:
            observation.observation_value = observation_value
            observation.sport = sport
            observation.updated_at = get_local_now()
            logger.debug("Updated observation %s for event %s", observation_type, event_id)
            return observation

        observation = EventObservation(
            event_id=event_id,
            observation_type=observation_type,
            observation_value=observation_value,
            sport=sport,
        )
        session.add(observation)
        logger.debug("Created new observation %s for event %s", observation_type, event_id)
        return observation

    @staticmethod
    def upsert_observation(event_id: int, sport: str, observation_type: str, observation_value: str) -> Optional[EventObservation]:
        """
        Insert or update an event observation.
        FAIL-SAFE: Returns None on any error, doesn't break main flow.
        """
        try:
            with db_manager.get_session() as session:
                return ObservationRepository._upsert_in_session(
                    session,
                    event_id,
                    sport,
                    observation_type,
                    observation_value,
                )
        except Exception as e:
            logger.warning(f"Error upserting observation {observation_type} for event {event_id}: {e}")
            # FAIL-SAFE: Return None, don't break main processing
            return None

    @staticmethod
    def upsert_observations(rows: list[Dict]) -> int:
        """Insert or update many observations in a single session.

        Each row is ``{event_id, sport, observation_type, observation_value}``.
        FAIL-SAFE: Returns 0 on any error.
        """
        if not rows:
            return 0

        try:
            with db_manager.get_session() as session:
                event_ids = {row["event_id"] for row in rows}
                observation_types = {row["observation_type"] for row in rows}
                existing_by_key = {
                    (observation.event_id, observation.observation_type): observation
                    for observation in session.query(EventObservation).filter(
                        EventObservation.event_id.in_(event_ids),
                        EventObservation.observation_type.in_(observation_types),
                    ).all()
                }
                saved = 0
                for row in rows:
                    key = (row["event_id"], row["observation_type"])
                    observation = ObservationRepository._upsert_in_session(
                        session,
                        row["event_id"],
                        row["sport"],
                        row["observation_type"],
                        row["observation_value"],
                        existing=existing_by_key.get(key),
                        skip_lookup=True,
                    )
                    existing_by_key[key] = observation
                    saved += 1
                return saved
        except Exception as exc:
            logger.warning("Error upserting %s observations: %s", len(rows), exc)
            return 0

    @staticmethod
    def get_observation(event_id: int, observation_type: str) -> Optional[EventObservation]:
        """
        Get a specific observation for an event.
        FAIL-SAFE: Returns None if not found or on error.
        """
        try:
            with db_manager.get_session() as session:
                return session.query(EventObservation).filter(
                    and_(
                        EventObservation.event_id == event_id,
                        EventObservation.observation_type == observation_type
                    )
                ).first()
        except Exception as e:
            logger.warning(f"Error getting observation {observation_type} for event {event_id}: {e}")
            # FAIL-SAFE: Return None, don't break main processing
            return None

    @staticmethod
    def get_observations_for_events(
        event_ids: List[int],
    ) -> Dict[int, List[Dict]]:
        """Load all observations for many events in a single session.

        FAIL-SAFE: Returns an empty dict on error.
        """
        unique_ids = {event_id for event_id in event_ids or [] if event_id is not None}
        if not unique_ids:
            return {}
        try:
            with db_manager.get_session() as session:
                rows = (
                    session.query(EventObservation)
                    .filter(EventObservation.event_id.in_(unique_ids))
                    .all()
                )
                by_event: Dict[int, List[Dict]] = {}
                for row in rows:
                    by_event.setdefault(row.event_id, []).append(
                        {
                            "type": row.observation_type,
                            "value": row.observation_value,
                            "sport": row.sport,
                        }
                    )
                return by_event
        except Exception as exc:
            logger.warning(
                "Error getting observations for %s events: %s",
                len(unique_ids),
                exc,
            )
            return {}

    @staticmethod
    def get_all_observations(event_id: int) -> List[EventObservation]:
        """
        Get all observations for an event.
        FAIL-SAFE: Returns empty list on error.
        """
        try:
            with db_manager.get_session() as session:
                return session.query(EventObservation).filter(
                    EventObservation.event_id == event_id
                ).all()
        except Exception as e:
            logger.warning(f"Error getting observations for event {event_id}: {e}")
            # FAIL-SAFE: Return empty list, don't break main processing
            return []

    @staticmethod
    def get_observations_by_type(observation_type: str, sport: str = None) -> List[EventObservation]:
        """
        Get all observations of a specific type, optionally filtered by sport.
        FAIL-SAFE: Returns empty list on error.
        """
        try:
            with db_manager.get_session() as session:
                query = session.query(EventObservation).filter(
                    EventObservation.observation_type == observation_type
                )

                if sport:
                    query = query.filter(EventObservation.sport == sport)

                return query.all()
        except Exception as e:
            logger.warning(f"Error getting observations by type {observation_type}: {e}")
            # FAIL-SAFE: Return empty list, don't break main processing
            return []
