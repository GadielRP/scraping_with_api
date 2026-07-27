import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional

from sqlalchemy.orm import Session

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, EventSourceMapping
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EventOddsSourceState:
    """Source identity and odds availability for one canonical event."""
    event_id: int
    source: str
    source_event_id: str
    has_odds: bool
    source_sport_id: str | None = None


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
    def _normalize_event_ids(values: Iterable[int]) -> set[int]:
        event_ids: set[int] = set()
        for value in values or []:
            if isinstance(value, bool):
                continue
            try:
                event_id = int(value)
            except (TypeError, ValueError):
                continue
            if event_id > 0:
                event_ids.add(event_id)
        return event_ids

    @staticmethod
    def get_event_id_by_source(
        source: str,
        source_event_id: str,
        session: Optional[Session] = None,
    ) -> Optional[int]:
        """Return the canonical event_id for a source + external event ID."""
        normalized_source = EventSourceMappingRepository._normalize_source(source)
        normalized_source_event_id = EventSourceMappingRepository._normalize_source_event_id(source_event_id)

        if not normalized_source or not normalized_source_event_id:
            return None

        try:
            def _lookup(scoped_session: Session) -> Optional[int]:
                mapping = (
                    scoped_session.query(EventSourceMapping)
                    .filter(
                        EventSourceMapping.source == normalized_source,
                        EventSourceMapping.source_event_id == normalized_source_event_id,
                    )
                    .first()
                )
                return mapping.event_id if mapping else None

            if session is not None:
                return _lookup(session)

            with db_manager.get_session() as scoped_session:
                return _lookup(scoped_session)
        except Exception as exc:
            logger.error(
                "Error resolving canonical event_id for source=%s source_event_id=%s: %s",
                normalized_source,
                normalized_source_event_id,
                exc,
            )
            return None

    @staticmethod
    def get_event_ids_by_source_event_ids(
        source: str,
        source_event_ids: list[str],
        session: Optional[Session] = None,
    ) -> dict[str, int]:
        """Return canonical IDs in one query, reusing the supplied transaction."""
        normalized_source = EventSourceMappingRepository._normalize_source(source)
        normalized_ids = {
            EventSourceMappingRepository._normalize_source_event_id(value)
            for value in (source_event_ids or [])
            if str(value or "").strip()
        }
        if not normalized_source or not normalized_ids:
            return {}

        def _lookup(scoped_session: Session) -> dict[str, int]:
            rows = (
                scoped_session.query(
                    EventSourceMapping.source_event_id,
                    EventSourceMapping.event_id,
                )
                .filter(
                    EventSourceMapping.source == normalized_source,
                    EventSourceMapping.source_event_id.in_(normalized_ids),
                )
                .all()
            )
            return {str(source_event_id): event_id for source_event_id, event_id in rows}

        try:
            if session is not None:
                return _lookup(session)
            with db_manager.get_session() as scoped_session:
                return _lookup(scoped_session)
        except Exception as exc:
            logger.error(
                "Error resolving batch event IDs for source=%s count=%s: %s",
                normalized_source,
                len(normalized_ids),
                exc,
            )
            raise

    @staticmethod
    def get_event_ids_by_sofascore_ids(
        sofascore_ids: list[str],
        session: Optional[Session] = None,
    ) -> dict[str, int]:
        """Return canonical event IDs for SofaScore IDs in one query."""
        return EventSourceMappingRepository.get_event_ids_by_source_event_ids(
            source="sofascore",
            source_event_ids=sofascore_ids,
            session=session,
        )

    @staticmethod
    def get_source_event_ids_by_event_ids(
        event_ids: list[int],
        source: str,
        session: Optional[Session] = None,
    ) -> dict[int, str]:
        """Return source event IDs for canonical event IDs in one query."""
        normalized_source = EventSourceMappingRepository._normalize_source(source)
        normalized_ids: set[int] = set()
        for event_id in event_ids or []:
            if event_id is None or not str(event_id).strip():
                continue
            try:
                normalized_ids.add(int(event_id))
            except (TypeError, ValueError):
                logger.warning(
                    "Ignoring invalid canonical event ID in batch source lookup: %r",
                    event_id,
                )
        if not normalized_source or not normalized_ids:
            return {}

        def _lookup(scoped_session: Session) -> dict[int, str]:
            rows = (
                scoped_session.query(
                    EventSourceMapping.event_id,
                    EventSourceMapping.source_event_id,
                )
                .filter(
                    EventSourceMapping.source == normalized_source,
                    EventSourceMapping.event_id.in_(normalized_ids),
                )
                .all()
            )
            return {
                int(event_id): str(source_event_id)
                for event_id, source_event_id in rows
                if source_event_id is not None and str(source_event_id).strip()
            }

        try:
            if session is not None:
                return _lookup(session)
            with db_manager.get_session() as scoped_session:
                return _lookup(scoped_session)
        except Exception as exc:
            logger.error(
                "Error resolving batch source event IDs for source=%s count=%s: %s",
                normalized_source,
                len(normalized_ids),
                exc,
            )
            raise

    @classmethod
    def get_odds_source_states(
        cls,
        event_ids: Iterable[int],
        sources: Iterable[str],
        *,
        session: Session | None = None,
    ) -> dict[int, dict[str, EventOddsSourceState]]:
        """Load provider IDs and odds availability in one query."""
        normalized_event_ids = cls._normalize_event_ids(event_ids)
        normalized_sources: set[str] = set()
        for source in sources or []:
            normalized_source = cls._normalize_source(source)
            if normalized_source:
                normalized_sources.add(normalized_source)
        if not normalized_event_ids or not normalized_sources:
            return {}

        def _load(scoped_session: Session) -> dict[int, dict[str, EventOddsSourceState]]:
            rows = (
                scoped_session.query(
                    EventSourceMapping.event_id,
                    EventSourceMapping.source,
                    EventSourceMapping.source_event_id,
                    EventSourceMapping.has_odds,
                    EventSourceMapping.source_sport_id,
                )
                .filter(
                    EventSourceMapping.event_id.in_(normalized_event_ids),
                    EventSourceMapping.source.in_(normalized_sources),
                )
                .all()
            )
            states: dict[int, dict[str, EventOddsSourceState]] = {}
            for event_id, source, source_event_id, has_odds, source_sport_id in rows:
                normalized_source = cls._normalize_source(source)
                states.setdefault(int(event_id), {})[normalized_source] = EventOddsSourceState(
                    event_id=int(event_id),
                    source=normalized_source,
                    source_event_id=str(source_event_id),
                    has_odds=bool(has_odds),
                    source_sport_id=cls._normalize_optional_text(source_sport_id),
                )
            return states

        if session is not None:
            return _load(session)
        with db_manager.get_session() as scoped_session:
            return _load(scoped_session)

    @classmethod
    def mark_odds_unavailable(
        cls,
        event_ids: Iterable[int],
        source: str,
        *,
        session: Session | None = None,
    ) -> int:
        """Persist confirmed missing odds endpoints in one idempotent update."""
        normalized_event_ids = cls._normalize_event_ids(event_ids)
        normalized_source = cls._normalize_source(source)
        if not normalized_event_ids or not normalized_source:
            return 0

        def _update(scoped_session: Session) -> int:
            updated_count = (
                scoped_session.query(EventSourceMapping)
                .filter(
                    EventSourceMapping.event_id.in_(normalized_event_ids),
                    EventSourceMapping.source == normalized_source,
                    EventSourceMapping.has_odds.is_(True),
                )
                .update(
                    {
                        EventSourceMapping.has_odds: False,
                        EventSourceMapping.updated_at: get_local_now(),
                    },
                    synchronize_session=False,
                )
            )
            logger.info(
                "Marked odds unavailable source=%s requested_events=%s updated_mappings=%s",
                normalized_source,
                len(normalized_event_ids),
                updated_count,
            )
            return int(updated_count or 0)

        if session is not None:
            return _update(session)
        with db_manager.get_session() as scoped_session:
            return _update(scoped_session)

    @staticmethod
    def get_source_event_id(event_id: int, source: str, session: Optional[Session] = None) -> Optional[str]:
        """Return the external source event ID for a canonical event."""
        normalized_source = EventSourceMappingRepository._normalize_source(source)
        if not normalized_source:
            return None

        try:
            def _lookup(scoped_session: Session) -> Optional[str]:
                mapping = (
                    scoped_session.query(EventSourceMapping)
                    .filter(
                        EventSourceMapping.event_id == event_id,
                        EventSourceMapping.source == normalized_source,
                    )
                    .first()
                )
                return mapping.source_event_id if mapping else None

            if session is not None:
                return _lookup(session)

            with db_manager.get_session() as session:
                return _lookup(session)
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

            logger.info(
                "Updated event source mapping source=%s source_event_id=%s -> event_id=%s",
                normalized_source,
                normalized_source_event_id,
                mapping.event_id,
            )
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
        logger.info(
            "Created event source mapping source=%s source_event_id=%s -> event_id=%s",
            normalized_source,
            normalized_source_event_id,
            event_id,
        )
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
