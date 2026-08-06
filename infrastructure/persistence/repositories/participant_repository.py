import logging
from collections.abc import Iterable

from sqlalchemy import tuple_
from sqlalchemy.orm import Session

from infrastructure.persistence.models import Participant
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)

ParticipantKey = tuple[str, int]


class ParticipantRepository:
    """Repository for normalized event participants."""

    @staticmethod
    def _normalize_participant_data(participant_data: dict) -> tuple[ParticipantKey, dict] | None:
        if not participant_data:
            return None

        source = str(participant_data.get("source") or "sofascore").strip().lower()
        try:
            source_participant_id = int(participant_data.get("source_participant_id"))
        except (TypeError, ValueError):
            logger.warning("Skipping participant because source_participant_id is missing or invalid")
            return None

        name = str(participant_data.get("name") or "").strip()
        if not name:
            logger.warning(
                "Skipping participant %s:%s because name is missing",
                source,
                source_participant_id,
            )
            return None

        normalized = {
            "source": source,
            "source_participant_id": source_participant_id,
            "name": name,
            "slug": participant_data.get("slug"),
            "short_name": participant_data.get("short_name"),
            "code_name": participant_data.get("code_name"),
        }
        return (source, source_participant_id), normalized

    @staticmethod
    def upsert_participants(
        session: Session,
        participants_data: Iterable[dict],
    ) -> dict[ParticipantKey, Participant]:
        """Insert or update a participant batch with one preload query.

        Duplicate source IDs are collapsed before touching the database. Existing
        rows are updated only when source data actually changed, and new rows are
        flushed together so callers can immediately use their generated IDs.
        """
        normalized_by_key: dict[ParticipantKey, dict] = {}
        for participant_data in participants_data or ():
            normalized = ParticipantRepository._normalize_participant_data(participant_data)
            if normalized is None:
                continue
            key, values = normalized
            previous = normalized_by_key.get(key)
            if previous is None:
                normalized_by_key[key] = values
                continue
            # Preserve useful values when a duplicate fixture has a sparse payload.
            for field, value in values.items():
                if value is not None:
                    previous[field] = value

        if not normalized_by_key:
            return {}

        keys = list(normalized_by_key)
        existing = (
            session.query(Participant)
            .filter(
                tuple_(Participant.source, Participant.source_participant_id).in_(keys)
            )
            .all()
        )
        participants_by_key = {
            (participant.source, int(participant.source_participant_id)): participant
            for participant in existing
        }

        new_participant_rows: list[dict] = []
        for key, participant_data in normalized_by_key.items():
            participant = participants_by_key.get(key)
            if participant is not None:
                ParticipantRepository._apply_updates(participant, participant_data)
                continue

            new_participant_rows.append(participant_data)

        if new_participant_rows:
            dialect_name = session.get_bind().dialect.name
            if dialect_name in {"postgresql", "sqlite"}:
                # Flush changed existing rows, then let the database resolve a
                # concurrent insert through the unique source/external-ID key.
                session.flush()
                if dialect_name == "postgresql":
                    from sqlalchemy.dialects.postgresql import insert
                else:
                    from sqlalchemy.dialects.sqlite import insert

                statement = insert(Participant).values(new_participant_rows)
                statement = statement.on_conflict_do_nothing(
                    index_elements=["source", "source_participant_id"],
                )
                session.execute(statement)
                new_keys = [
                    (row["source"], row["source_participant_id"])
                    for row in new_participant_rows
                ]
                persisted = (
                    session.query(Participant)
                    .filter(
                        tuple_(Participant.source, Participant.source_participant_id).in_(new_keys)
                    )
                    .all()
                )
                participants_by_key.update(
                    {
                        (participant.source, int(participant.source_participant_id)): participant
                        for participant in persisted
                    }
                )
            else:
                new_participants = [
                    Participant(**participant_data)
                    for participant_data in new_participant_rows
                ]
                session.add_all(new_participants)
                participants_by_key.update(
                    {
                        (participant.source, int(participant.source_participant_id)): participant
                        for participant in new_participants
                    }
                )
        session.flush()
        return participants_by_key

    @staticmethod
    def upsert_participant(session: Session, participant_data: dict) -> Participant | None:
        """Upsert one participant through the shared batch implementation."""
        participants = ParticipantRepository.upsert_participants(session, [participant_data])
        return next(iter(participants.values()), None)

    @staticmethod
    def _apply_updates(participant: Participant, participant_data: dict) -> bool:
        changed = False
        for attr in ("name", "slug", "short_name", "code_name"):
            value = participant_data.get(attr)
            if value is not None and getattr(participant, attr) != value:
                setattr(participant, attr, value)
                changed = True
        if changed:
            participant.updated_at = get_local_now()
        return changed
