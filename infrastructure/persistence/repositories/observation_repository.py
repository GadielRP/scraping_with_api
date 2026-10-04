import logging
from typing import List, Optional, Dict

from sqlalchemy import and_

from infrastructure.persistence.models import EventObservation
from infrastructure.persistence.database import db_manager
from shared.temporal import utc_now

logger = logging.getLogger(__name__)


class ObservationRepository:
    """Repository for event observation-related database operations"""

    @staticmethod
    def write_observations(session, rows):
        """Batch upsert in the caller's transaction; failures propagate."""
        if not rows:
            return 0
        if session.get_bind().dialect.name == 'postgresql':
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        keyed = {(row['event_id'], row['observation_type']): row for row in rows}
        statement = insert(EventObservation).values(list(keyed.values()))
        session.execute(statement.on_conflict_do_update(
            index_elements=['event_id', 'observation_type'], set_={
                'sport': statement.excluded.sport,
                'observation_value': statement.excluded.observation_value,
                'updated_at': utc_now()}))
        return len(keyed)

    @staticmethod
    def upsert_observations(rows):
        """Commit bounded writes; optional-failure policy belongs to ObservationService."""
        from shared.batching import chunks
        from infrastructure.settings import Config
        saved = 0
        for batch in chunks(rows, Config.EVENT_WRITE_BATCH_SIZE):
            with db_manager.get_session() as session:
                saved += ObservationRepository.write_observations(session, batch)
        return saved

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
