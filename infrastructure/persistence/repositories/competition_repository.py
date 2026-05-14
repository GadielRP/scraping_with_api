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
            number_of_teams=competition_data.get("number_of_teams"),
            total_regular_season_games=competition_data.get("total_regular_season_games"),
            standings_grouping=competition_data.get("standings_grouping"),
            league_config_source=competition_data.get("league_config_source"),
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
            "league_config_source",
        ):
            value = competition_data.get(attr)
            if value is not None:
                setattr(competition, attr, value)
        competition.updated_at = get_local_now()

    @staticmethod
    def update_number_of_teams_if_missing(
        session: Session,
        competition_id: int,
        number_of_teams: Optional[int],
    ) -> bool:
        if competition_id is None or number_of_teams is None or number_of_teams <= 1:
            return False

        competition = (
            session.query(Competition)
            .filter(Competition.competition_id == competition_id)
            .first()
        )
        if competition is None:
            return False
        if competition.number_of_teams is not None:
            return False

        competition.number_of_teams = number_of_teams
        competition.updated_at = get_local_now()
        return True

    @staticmethod
    def update_competition_metadata_if_better(
        session: Session,
        competition_id: int,
        number_of_teams: Optional[int] = None,
        total_regular_season_games: Optional[int] = None,
        standings_grouping: Optional[str] = None,
        league_config_source: Optional[str] = None,
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

        from infrastructure.settings import Config

        changed = False
        source = league_config_source or "missing"

        if number_of_teams is not None:
            try:
                number_of_teams = int(number_of_teams)
            except (TypeError, ValueError):
                number_of_teams = None

        if number_of_teams is not None and number_of_teams > 1:
            existing = competition.number_of_teams
            if existing is None:
                competition.number_of_teams = number_of_teams
                changed = True
            elif number_of_teams > existing:
                logger.warning(
                    "Competition metadata conflict for number_of_teams: competition_id=%s source_unique_tournament_id=%s db_value=%s new_value=%s source=%s; using greater value",
                    competition_id,
                    competition.source_unique_tournament_id,
                    existing,
                    number_of_teams,
                    source,
                )
                competition.number_of_teams = number_of_teams
                changed = True
            elif number_of_teams < existing:
                logger.warning(
                    "Competition metadata conflict for number_of_teams: competition_id=%s source_unique_tournament_id=%s db_value=%s new_value=%s source=%s; keeping db value",
                    competition_id,
                    competition.source_unique_tournament_id,
                    existing,
                    number_of_teams,
                    source,
                )

        if total_regular_season_games is not None:
            try:
                total_regular_season_games = int(total_regular_season_games)
            except (TypeError, ValueError):
                total_regular_season_games = None

        if total_regular_season_games is not None and total_regular_season_games > 0:
            existing = competition.total_regular_season_games
            if existing is None:
                competition.total_regular_season_games = total_regular_season_games
                changed = True
            elif source == "manual_config" and existing != total_regular_season_games:
                logger.warning(
                    "Competition metadata conflict for total_regular_season_games: competition_id=%s source_unique_tournament_id=%s db_value=%s new_value=%s source=%s; using manual_config",
                    competition_id,
                    competition.source_unique_tournament_id,
                    existing,
                    total_regular_season_games,
                    source,
                )
                competition.total_regular_season_games = total_regular_season_games
                changed = True

        if standings_grouping is not None:
            valid_groupings = {"single_table", "split_tables", "unknown"}
            if standings_grouping not in valid_groupings:
                standings_grouping = None

        if standings_grouping is not None:
            existing = competition.standings_grouping
            if existing is None:
                competition.standings_grouping = standings_grouping
                changed = True
            elif existing != standings_grouping:
                logger.warning(
                    "Competition metadata conflict for standings_grouping: competition_id=%s source_unique_tournament_id=%s db_value=%s new_value=%s source=%s",
                    competition_id,
                    competition.source_unique_tournament_id,
                    existing,
                    standings_grouping,
                    source,
                )
                if source == "standings_response" and Config.FORCE_STANDINGS_COMPETITION_METADATA_REFRESH:
                    competition.standings_grouping = standings_grouping
                    changed = True

        if changed:
            competition.league_config_source = source
            competition.updated_at = get_local_now()

        return changed

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
        competition.updated_at = get_local_now()
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
