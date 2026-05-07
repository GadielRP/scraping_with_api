import logging
from typing import Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from infrastructure.persistence.models import Competition
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class CompetitionRepository:
    """Repository for normalized event competitions."""

    @staticmethod
    def upsert_competition(session: Session, competition_data: Dict) -> Optional[Competition]:
        if not competition_data:
            return None

        source = competition_data.get("source") or "sofascore"
        source_tournament_id = competition_data.get("source_tournament_id")
        if source_tournament_id is None:
            logger.warning("Skipping competition upsert because source_tournament_id is missing")
            return None

        canonical_name = competition_data.get("canonical_name")
        display_name = competition_data.get("display_name") or canonical_name
        if not canonical_name or not display_name:
            logger.warning("Skipping competition %s:%s because names are missing", source, source_tournament_id)
            return None

        competition = (
            session.query(Competition)
            .filter(
                Competition.source == source,
                Competition.source_tournament_id == source_tournament_id,
            )
            .first()
        )

        if competition:
            CompetitionRepository._apply_updates(competition, competition_data)
            return competition

        competition = Competition(
            source=source,
            source_tournament_id=source_tournament_id,
            source_unique_tournament_id=competition_data.get("source_unique_tournament_id"),
            canonical_name=canonical_name,
            display_name=display_name,
            slug=competition_data.get("slug"),
            unique_slug=competition_data.get("unique_slug"),
            category_id=competition_data.get("category_id"),
            category_name=competition_data.get("category_name"),
        )

        try:
            with session.begin_nested():
                session.add(competition)
                session.flush()
            return competition
        except IntegrityError:
            competition = (
                session.query(Competition)
                .filter(
                    Competition.source == source,
                    Competition.source_tournament_id == source_tournament_id,
                )
                .first()
            )
            if competition:
                CompetitionRepository._apply_updates(competition, competition_data)
            return competition

    @staticmethod
    def _apply_updates(competition: Competition, competition_data: Dict) -> None:
        for attr in (
            "source_unique_tournament_id",
            "canonical_name",
            "display_name",
            "slug",
            "unique_slug",
            "category_id",
            "category_name",
        ):
            value = competition_data.get(attr)
            if value is not None:
                setattr(competition, attr, value)
        competition.updated_at = get_local_now()
