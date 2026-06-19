import logging
from typing import List, Optional

from sqlalchemy.orm import Session

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, EventSourceMapping

logger = logging.getLogger(__name__)


class EventSourceMappingRepository:
    """Repository for canonical event to external source ID mappings."""

    @staticmethod
    def _normalize_source(source: str) -> str:
        return str(source or "").strip().lower()

    @staticmethod
    def _normalize_source_event_id(source_event_id) -> str:
        return str(source_event_id).strip()

    @staticmethod
    def _normalize_optional_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @staticmethod
    def get_event_id_by_source(source: str, source_event_id: str) -> Optional[int]:
        """Return the canonical event_id for a source + external event ID."""
        normalized_source = EventSourceMappingRepository._normalize_source(source)
        normalized_source_event_id = EventSourceMappingRepository._normalize_source_event_id(source_event_id)

        if not normalized_source or not normalized_source_event_id:
            return None

        try:
            with db_manager.get_session() as session:
                mapping = (
                    session.query(EventSourceMapping)
                    .filter(
                        EventSourceMapping.source == normalized_source,
                        EventSourceMapping.source_event_id == normalized_source_event_id,
                    )
                    .first()
                )
                return mapping.event_id if mapping else None
        except Exception as exc:
            logger.error(
                "Error resolving canonical event_id for source=%s source_event_id=%s: %s",
                normalized_source,
                normalized_source_event_id,
                exc,
            )
            return None

    @staticmethod
    def get_source_event_id(event_id: int, source: str) -> Optional[str]:
        """Return the external source event ID for a canonical event."""
        normalized_source = EventSourceMappingRepository._normalize_source(source)
        if not normalized_source:
            return None

        try:
            with db_manager.get_session() as session:
                mapping = (
                    session.query(EventSourceMapping)
                    .filter(
                        EventSourceMapping.event_id == event_id,
                        EventSourceMapping.source == normalized_source,
                    )
                    .first()
                )
                return mapping.source_event_id if mapping else None
        except Exception as exc:
            logger.error(
                "Error resolving source event id for event_id=%s source=%s: %s",
                event_id,
                normalized_source,
                exc,
            )
            return None

    @staticmethod
    def resolve_required_source_event_id(event_id: int, source: str) -> str:
        """Resolve an external source event ID or fail fast."""
        normalized_source = EventSourceMappingRepository._normalize_source(source)
        source_event_id = EventSourceMappingRepository.get_source_event_id(event_id, normalized_source)
        if source_event_id is None:
            raise ValueError(f"Missing source mapping for event_id={event_id}, source={normalized_source}")
        return source_event_id

    @staticmethod
    def _upsert_mapping_in_session(
        session: Session,
        event_id: int,
        source: str,
        source_event_id: str,
        source_sport_id: Optional[str] = None,
        source_tournament_id: Optional[str] = None,
        source_season_id: Optional[str] = None,
        match_method: str = "direct",
        confidence: Optional[float] = None,
        raw_external_providers: Optional[dict] = None,
    ) -> EventSourceMapping:
        normalized_source = EventSourceMappingRepository._normalize_source(source)
        normalized_source_event_id = EventSourceMappingRepository._normalize_source_event_id(source_event_id)

        if not normalized_source:
            raise ValueError("source is required for EventSourceMappingRepository.upsert_mapping")
        if not normalized_source_event_id:
            raise ValueError("source_event_id is required for EventSourceMappingRepository.upsert_mapping")

        event_exists = session.query(Event.id).filter(Event.id == event_id).first()
        if not event_exists:
            raise ValueError(f"Cannot create mapping for missing event_id={event_id}")

        mapping = (
            session.query(EventSourceMapping)
            .filter(
                EventSourceMapping.source == normalized_source,
                EventSourceMapping.source_event_id == normalized_source_event_id,
            )
            .first()
        )

        if mapping:
            if mapping.event_id != event_id:
                logger.warning(
                    "Existing mapping for source=%s source_event_id=%s points to event_id=%s; requested event_id=%s. Keeping existing canonical event.",
                    normalized_source,
                    normalized_source_event_id,
                    mapping.event_id,
                    event_id,
                )

            if source_sport_id is not None:
                mapping.source_sport_id = str(source_sport_id).strip() or None
            if source_tournament_id is not None:
                mapping.source_tournament_id = str(source_tournament_id).strip() or None
            if source_season_id is not None:
                mapping.source_season_id = str(source_season_id).strip() or None
            if match_method is not None:
                mapping.match_method = match_method
            if confidence is not None:
                mapping.confidence = confidence
            if raw_external_providers is not None:
                mapping.raw_external_providers = raw_external_providers

            logger.debug(
                "Updated mapping for event_id=%s source=%s source_event_id=%s",
                mapping.event_id,
                normalized_source,
                normalized_source_event_id,
            )
            return mapping

        mapping = EventSourceMapping(
            event_id=event_id,
            source=normalized_source,
            source_event_id=normalized_source_event_id,
            source_sport_id=EventSourceMappingRepository._normalize_optional_text(source_sport_id),
            source_tournament_id=EventSourceMappingRepository._normalize_optional_text(source_tournament_id),
            source_season_id=EventSourceMappingRepository._normalize_optional_text(source_season_id),
            match_method=match_method or "direct",
            confidence=confidence,
            raw_external_providers=raw_external_providers,
        )
        session.add(mapping)
        session.flush()
        logger.debug(
            "Created mapping for event_id=%s source=%s source_event_id=%s",
            event_id,
            normalized_source,
            normalized_source_event_id,
        )
        return mapping

    @staticmethod
    def upsert_mapping(
        event_id: int,
        source: str,
        source_event_id: str,
        source_sport_id: Optional[str] = None,
        source_tournament_id: Optional[str] = None,
        source_season_id: Optional[str] = None,
        match_method: str = "direct",
        confidence: Optional[float] = None,
        raw_external_providers: Optional[dict] = None,
        session: Optional[Session] = None,
    ) -> EventSourceMapping:
        """Insert or update a source mapping in an idempotent way."""
        try:
            if session is not None:
                return EventSourceMappingRepository._upsert_mapping_in_session(
                    session=session,
                    event_id=event_id,
                    source=source,
                    source_event_id=source_event_id,
                    source_sport_id=source_sport_id,
                    source_tournament_id=source_tournament_id,
                    source_season_id=source_season_id,
                    match_method=match_method,
                    confidence=confidence,
                    raw_external_providers=raw_external_providers,
                )

            with db_manager.get_session() as session:
                return EventSourceMappingRepository._upsert_mapping_in_session(
                    session=session,
                    event_id=event_id,
                    source=source,
                    source_event_id=source_event_id,
                    source_sport_id=source_sport_id,
                    source_tournament_id=source_tournament_id,
                    source_season_id=source_season_id,
                    match_method=match_method,
                    confidence=confidence,
                    raw_external_providers=raw_external_providers,
                )
        except Exception as exc:
            logger.error(
                "Error upserting event source mapping for event_id=%s source=%s source_event_id=%s: %s",
                event_id,
                source,
                source_event_id,
                exc,
            )
            raise

    @staticmethod
    def get_mappings_for_event(event_id: int) -> List[EventSourceMapping]:
        """Return all source mappings for a canonical event."""
        try:
            with db_manager.get_session() as session:
                return (
                    session.query(EventSourceMapping)
                    .filter(EventSourceMapping.event_id == event_id)
                    .order_by(EventSourceMapping.source.asc(), EventSourceMapping.source_event_id.asc())
                    .all()
                )
        except Exception as exc:
            logger.error("Error getting source mappings for event_id=%s: %s", event_id, exc)
            return []
