import logging
from typing import List, Optional, Dict

from sqlalchemy import and_

from infrastructure.persistence.models import EventObservation
from infrastructure.persistence.database import db_manager
from timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class ObservationRepository:
    """Repository for event observation-related database operations"""

    @staticmethod
    def upsert_observation(event_id: int, sport: str, observation_type: str, observation_value: str) -> Optional[EventObservation]:
        """
        Insert or update an event observation.
        FAIL-SAFE: Returns None on any error, doesn't break main flow.
        """
        try:
            with db_manager.get_session() as session:
                # Check if observation exists
                observation = session.query(EventObservation).filter(
                    and_(
                        EventObservation.event_id == event_id,
                        EventObservation.observation_type == observation_type
                    )
                ).first()

                if observation:
                    # Update existing observation
                    observation.observation_value = observation_value
                    observation.sport = sport
                    observation.updated_at = get_local_now()
                    logger.debug(f"Updated observation {observation_type} for event {event_id}")
                else:
                    # Create new observation
                    observation = EventObservation(
                        event_id=event_id,
                        observation_type=observation_type,
                        observation_value=observation_value,
                        sport=sport
                    )
                    session.add(observation)
                    logger.debug(f"Created new observation {observation_type} for event {event_id}")

                return observation

        except Exception as e:
            logger.warning(f"Error upserting observation {observation_type} for event {event_id}: {e}")
            # FAIL-SAFE: Return None, don't break main processing
            return None

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
