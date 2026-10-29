import logging
from typing import Collection, Dict, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from infrastructure.persistence.models import Competition
from shared.temporal import utc_now

logger = logging.getLogger(__name__)


class CompetitionRepository:
    """Repository for normalized event competitions."""

    @staticmethod
    def get_source_competition_id_pairs(
        session: Session,
        *,
        source: str,
        competition_ids: Collection[int],
    ) -> set[tuple[int, int | None]]:
        """Get provider tournament ID pairs for canonical competitions."""
        ids = tuple(int(value) for value in competition_ids)
        if not ids:
            return set()
        values = (
            session.query(
                Competition.source_tournament_id,
                Competition.source_unique_tournament_id,
            )
            .filter(
                Competition.source == source,
                Competition.competition_id.in_(ids),
            )
            .all()
        )
        return {
            (int(tournament_id), int(unique_tournament_id) if unique_tournament_id is not None else None)
            for tournament_id, unique_tournament_id in values
            if tournament_id is not None
        }

    @staticmethod
    def upsert_competition(session: Session, competition_data: Dict) -> Optional[Competition]:
        if not competition_data:
            return None

        source = competition_data.get("source") or "sofascore"
        source_tournament_id = competition_data.get("source_tournament_id")
        if source_tournament_id is None:
            logger.warning("Skipping competition upsert because source_tournament_id is missing")
            return None

        source_unique_tournament_id = competition_data.get("source_unique_tournament_id")
        if source_unique_tournament_id is None:
            logger.warning("Skipping competition upsert because source_unique_tournament_id is missing")
            return None

        raw_canonical = competition_data.get("canonical_name")
        raw_display = competition_data.get("display_name")

        canonical_val = (raw_canonical or raw_display or "Unknown").strip()
        display_val = (raw_display or raw_canonical or "Unknown").strip()

        canonical_name = canonical_val
        display_name = display_val

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
            number_of_teams=competition_data.get("number_of_teams"),
            total_regular_season_games=competition_data.get("total_regular_season_games"),
            standings_grouping=competition_data.get("standings_grouping"),
            competition_config_source=competition_data.get("competition_config_source"),
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
            "number_of_teams",
            "total_regular_season_games",
            "standings_grouping",
            "competition_config_source",
        ):
            value = competition_data.get(attr)
            if value is not None:
                setattr(competition, attr, value)
        competition.updated_at = utc_now()



    @staticmethod
    def update_has_standings_source_endpoint(
        session: Session,
        competition_id: int,
        has_standings_source_endpoint: bool,
    ) -> bool:
        if competition_id is None:
            return False

        competition = (
            session.query(Competition)
            .filter(Competition.competition_id == competition_id)
            .first()
        )
        if competition is None:
            return False

        existing = competition.has_standings_source_endpoint
        if existing == has_standings_source_endpoint:
            return False

        competition.has_standings_source_endpoint = has_standings_source_endpoint
        competition.updated_at = utc_now()
        return True

    @staticmethod
    def update_has_standings_source_endpoints(
        session: Session,
        competition_ids: set[int],
        has_standings_source_endpoint: bool,
    ) -> int:
        if not competition_ids:
            return 0

        updated_count = 0
        for competition_id in sorted(int(competition_id) for competition_id in competition_ids if competition_id is not None):
            if CompetitionRepository.update_has_standings_source_endpoint(
                session=session,
                competition_id=competition_id,
                has_standings_source_endpoint=has_standings_source_endpoint,
            ):
                updated_count += 1
        return updated_count
