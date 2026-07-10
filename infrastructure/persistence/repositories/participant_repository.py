import logging
from typing import Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from infrastructure.persistence.models import Participant
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class ParticipantRepository:
    """Repository for normalized event participants."""

    @staticmethod
    def upsert_participant(session: Session, participant_data: Dict) -> Optional[Participant]:
        if not participant_data:
            return None

        source = participant_data.get("source") or "sofascore"
        source_participant_id = participant_data.get("source_participant_id")
        if source_participant_id is None:
            logger.warning("Skipping participant upsert because source_participant_id is missing")
            return None

        name = participant_data.get("name")
        if not name:
            logger.warning("Skipping participant %s:%s because name is missing", source, source_participant_id)
            return None

        participant = (
            session.query(Participant)
            .filter(
                Participant.source == source,
                Participant.source_participant_id == source_participant_id,
            )
            .first()
        )

        if participant:
            ParticipantRepository._apply_updates(participant, participant_data)
            return participant

        participant = Participant(
            source=source,
            source_participant_id=source_participant_id,
            name=name,
            slug=participant_data.get("slug"),
            short_name=participant_data.get("short_name"),
            code_name=participant_data.get("code_name"),
        )

        try:
            with session.begin_nested():
                session.add(participant)
                session.flush()
            return participant
        except IntegrityError:
            participant = (
                session.query(Participant)
                .filter(
                    Participant.source == source,
                    Participant.source_participant_id == source_participant_id,
                )
                .first()
            )
            if participant:
                ParticipantRepository._apply_updates(participant, participant_data)
            return participant

    @staticmethod
    def _apply_updates(participant: Participant, participant_data: Dict) -> None:
        for attr in ("name", "slug", "short_name", "code_name"):
            value = participant_data.get(attr)
            if value is not None:
                setattr(participant, attr, value)
        participant.updated_at = get_local_now()
