import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable, List, Optional

from sqlalchemy import inspect, text, tuple_
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

    PARTICIPANT_LINK_COLUMNS = ("participant_home_id", "participant_away_id")
    PARTICIPANT_LINK_FOREIGN_KEYS = (
        (
            "fk_event_source_mappings_participant_home_id",
            ("participant_home_id",),
            "ALTER TABLE event_source_mappings ADD CONSTRAINT "
            "fk_event_source_mappings_participant_home_id "
            "FOREIGN KEY (participant_home_id) "
            "REFERENCES participants(participant_id) ON DELETE SET NULL",
        ),
        (
            "fk_event_source_mappings_participant_away_id",
            ("participant_away_id",),
            "ALTER TABLE event_source_mappings ADD CONSTRAINT "
            "fk_event_source_mappings_participant_away_id "
            "FOREIGN KEY (participant_away_id) "
            "REFERENCES participants(participant_id) ON DELETE SET NULL",
        ),
    )
    PARTICIPANT_LINK_INDEXES = (
        "CREATE INDEX IF NOT EXISTS "
        "idx_event_source_mappings_participant_home_id "
        "ON event_source_mappings (participant_home_id)",
        "CREATE INDEX IF NOT EXISTS "
        "idx_event_source_mappings_participant_away_id "
        "ON event_source_mappings (participant_away_id)",
    )

    @staticmethod
    def ensure_participant_link_schema() -> None:
        """Ensure participant foreign keys and indexes exist on source mappings."""
        try:
            inspector = inspect(db_manager.engine)
            if "event_source_mappings" not in set(inspector.get_table_names()):
                return

            EventSourceMappingRepository._ensure_participant_link_columns(inspector)
            inspector = inspect(db_manager.engine)
            EventSourceMappingRepository._ensure_participant_link_foreign_keys(inspector)
            EventSourceMappingRepository._ensure_participant_link_indexes()
            logger.info("Event source mapping participant schema is ready")
        except Exception:
            logger.exception("Event source mapping participant schema migration failed")

    @staticmethod
    def _ensure_participant_link_columns(inspector) -> None:
        existing_columns = {
            column["name"]
            for column in inspector.get_columns("event_source_mappings")
        }
        with db_manager.get_session() as session:
            for column_name in EventSourceMappingRepository.PARTICIPANT_LINK_COLUMNS:
                if column_name in existing_columns:
                    continue
                session.execute(
                    text(
                        "ALTER TABLE event_source_mappings "
                        f"ADD COLUMN {column_name} INTEGER"
                    )
                )
                logger.info("Added event_source_mappings.%s", column_name)
            session.commit()

    @staticmethod
    def _ensure_participant_link_foreign_keys(inspector) -> None:
        if db_manager.engine.dialect.name != "postgresql":
            return

        existing_fk_columns = {
            tuple(constraint.get("constrained_columns") or [])
            for constraint in inspector.get_foreign_keys("event_source_mappings")
        }
        with db_manager.get_session() as session:
            for constraint_name, constrained_columns, statement in (
                EventSourceMappingRepository.PARTICIPANT_LINK_FOREIGN_KEYS
            ):
                if constrained_columns in existing_fk_columns:
                    continue
                try:
                    with session.begin_nested():
                        session.execute(text(statement))
                    logger.info("Added FK constraint %s", constraint_name)
                except Exception as exc:
                    logger.debug(
                        "FK constraint %s may already exist or be equivalent: %s",
                        constraint_name,
                        exc,
                    )
            session.commit()

    @staticmethod
    def _ensure_participant_link_indexes() -> None:
        with db_manager.get_session() as session:
            for statement in EventSourceMappingRepository.PARTICIPANT_LINK_INDEXES:
                session.execute(text(statement))
            session.commit()

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
        participant_home_id: Optional[int] = None,
        participant_away_id: Optional[int] = None,
        match_method: Optional[str] = "direct",
        confidence: Optional[float] = None,
        raw_external_providers: Optional[dict] = None,
    ) -> EventSourceMapping:
        key = (
            EventSourceMappingRepository._normalize_source(source),
            EventSourceMappingRepository._normalize_source_event_id(source_event_id),
        )
        return EventSourceMappingRepository.upsert_mappings(
            session=session,
            mappings_data=[
                {
                    "event_id": event_id,
                    "source": source,
                    "source_event_id": source_event_id,
                    "source_sport_id": source_sport_id,
                    "source_tournament_id": source_tournament_id,
                    "source_season_id": source_season_id,
                    "participant_home_id": participant_home_id,
                    "participant_away_id": participant_away_id,
                    "match_method": match_method,
                    "confidence": confidence,
                    "raw_external_providers": raw_external_providers,
                }
            ],
        )[key]

    @staticmethod
    def _normalize_mapping_data(mapping_data: dict) -> tuple[tuple[str, str], dict]:
        source = EventSourceMappingRepository._normalize_source(mapping_data.get("source"))
        source_event_id = EventSourceMappingRepository._normalize_source_event_id(
            mapping_data.get("source_event_id")
        )
        if not source:
            raise ValueError("source is required for EventSourceMappingRepository.upsert_mapping")
        if not source_event_id:
            raise ValueError("source_event_id is required for EventSourceMappingRepository.upsert_mapping")

        try:
            event_id = int(mapping_data.get("event_id"))
        except (TypeError, ValueError) as exc:
            raise ValueError("event_id is required for EventSourceMappingRepository.upsert_mapping") from exc

        confidence = mapping_data.get("confidence")
        if confidence is not None:
            try:
                confidence = Decimal(str(confidence))
            except InvalidOperation as exc:
                raise ValueError(f"Invalid mapping confidence={confidence}") from exc

        normalized = {
            "event_id": event_id,
            "source": source,
            "source_event_id": source_event_id,
            "source_sport_id": EventSourceMappingRepository._normalize_optional_text(
                mapping_data.get("source_sport_id")
            ),
            "source_tournament_id": EventSourceMappingRepository._normalize_optional_text(
                mapping_data.get("source_tournament_id")
            ),
            "source_season_id": EventSourceMappingRepository._normalize_optional_text(
                mapping_data.get("source_season_id")
            ),
            "participant_home_id": mapping_data.get("participant_home_id"),
            "participant_away_id": mapping_data.get("participant_away_id"),
            "match_method": mapping_data.get("match_method"),
            "confidence": confidence,
            "raw_external_providers": mapping_data.get("raw_external_providers"),
        }
        return (source, source_event_id), normalized

    @staticmethod
    def _apply_mapping_updates(mapping: EventSourceMapping, mapping_data: dict) -> bool:
        changed = False
        for attr in (
            "source_sport_id",
            "source_tournament_id",
            "source_season_id",
            "participant_home_id",
            "participant_away_id",
            "match_method",
            "confidence",
            "raw_external_providers",
        ):
            value = mapping_data.get(attr)
            if value is not None and getattr(mapping, attr) != value:
                setattr(mapping, attr, value)
                changed = True
        if changed:
            mapping.updated_at = get_local_now()
        return changed

    @staticmethod
    def upsert_mappings(
        session: Session,
        mappings_data: Iterable[dict],
    ) -> dict[tuple[str, str], EventSourceMapping]:
        """Insert or update source mappings with two preload queries per batch."""
        normalized_by_key: dict[tuple[str, str], dict] = {}
        for mapping_data in mappings_data or ():
            key, normalized = EventSourceMappingRepository._normalize_mapping_data(mapping_data)
            previous = normalized_by_key.get(key)
            if previous is not None and previous["event_id"] != normalized["event_id"]:
                logger.warning(
                    "Batch contains conflicting canonical events for source=%s source_event_id=%s; "
                    "keeping event_id=%s and ignoring event_id=%s",
                    key[0],
                    key[1],
                    previous["event_id"],
                    normalized["event_id"],
                )
                continue
            normalized_by_key[key] = normalized

        if not normalized_by_key:
            return {}

        requested_event_ids = {values["event_id"] for values in normalized_by_key.values()}
        existing_event_ids = {
            int(row[0])
            for row in session.query(Event.id).filter(Event.id.in_(requested_event_ids)).all()
        }
        missing_event_ids = requested_event_ids - existing_event_ids
        if missing_event_ids:
            missing = ", ".join(str(event_id) for event_id in sorted(missing_event_ids))
            raise ValueError(f"Cannot create mappings for missing event_id(s)={missing}")

        keys = list(normalized_by_key)
        existing_mappings = (
            session.query(EventSourceMapping)
            .filter(tuple_(EventSourceMapping.source, EventSourceMapping.source_event_id).in_(keys))
            .all()
        )
        mappings_by_key = {
            (mapping.source, mapping.source_event_id): mapping
            for mapping in existing_mappings
        }

        new_mapping_rows: list[dict] = []
        for key, mapping_data in normalized_by_key.items():
            mapping = mappings_by_key.get(key)
            if mapping is not None:
                if mapping.event_id != mapping_data["event_id"]:
                    logger.warning(
                        "Existing mapping for source=%s source_event_id=%s points to event_id=%s; "
                        "requested event_id=%s. Keeping existing canonical event.",
                        key[0],
                        key[1],
                        mapping.event_id,
                        mapping_data["event_id"],
                    )
                changed = EventSourceMappingRepository._apply_mapping_updates(mapping, mapping_data)
                logger.debug(
                    "%s event source mapping source=%s source_event_id=%s -> event_id=%s",
                    "Updated" if changed else "Unchanged",
                    key[0],
                    key[1],
                    mapping.event_id,
                )
                continue

            new_mapping_rows.append(
                {
                    **mapping_data,
                    "match_method": mapping_data.get("match_method") or "direct",
                }
            )

        if new_mapping_rows:
            dialect_name = session.get_bind().dialect.name
            if dialect_name in {"postgresql", "sqlite"}:
                session.flush()
                if dialect_name == "postgresql":
                    from sqlalchemy.dialects.postgresql import insert
                else:
                    from sqlalchemy.dialects.sqlite import insert

                statement = insert(EventSourceMapping).values(new_mapping_rows)
                statement = statement.on_conflict_do_nothing(
                    index_elements=["source", "source_event_id"],
                )
                session.execute(statement)
                new_keys = [
                    (row["source"], row["source_event_id"])
                    for row in new_mapping_rows
                ]
                persisted = (
                    session.query(EventSourceMapping)
                    .filter(
                        tuple_(
                            EventSourceMapping.source,
                            EventSourceMapping.source_event_id,
                        ).in_(new_keys)
                    )
                    .all()
                )
                mappings_by_key.update(
                    {
                        (mapping.source, mapping.source_event_id): mapping
                        for mapping in persisted
                    }
                )
            else:
                new_mappings = [
                    EventSourceMapping(**mapping_data)
                    for mapping_data in new_mapping_rows
                ]
                session.add_all(new_mappings)
                mappings_by_key.update(
                    {
                        (mapping.source, mapping.source_event_id): mapping
                        for mapping in new_mappings
                    }
                )
        session.flush()
        for mapping_data in new_mapping_rows:
            logger.info(
                "Persisted event source mapping source=%s source_event_id=%s -> event_id=%s",
                mapping_data["source"],
                mapping_data["source_event_id"],
                mapping_data["event_id"],
            )
        return mappings_by_key

    @staticmethod
    def upsert_mapping(
        event_id: int,
        source: str,
        source_event_id: str,
        source_sport_id: Optional[str] = None,
        source_tournament_id: Optional[str] = None,
        source_season_id: Optional[str] = None,
        participant_home_id: Optional[int] = None,
        participant_away_id: Optional[int] = None,
        match_method: Optional[str] = "direct",
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
                    participant_home_id=participant_home_id,
                    participant_away_id=participant_away_id,
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
                    participant_home_id=participant_home_id,
                    participant_away_id=participant_away_id,
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
