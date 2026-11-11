"""Read regular-season football totals without loading individual fixtures."""

from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import case, func, literal, select, tuple_, union_all
from sqlalchemy.orm import Session

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Competition, Event, Participant, Result, Season
from modules.competition.season_aggregate import SeasonAggregateSource
from .disciplinary_incident_repository import DisciplinaryIncidentRepository


def _season_matches(competitions, season_year, kickoff_cutoff):
    identities = [
        (competition.source, competition.source_unique_tournament_id, competition.source_tournament_id)
        for competition in competitions
    ]
    return (
        select(
            Event.id.label("event_id"),
            Event.starts_at,
            Event.competition_id,
            Event.home_participant_id,
            Event.away_participant_id,
            Result.home_score,
            Result.away_score,
        )
        .select_from(Event)
        .join(Result, Result.event_id == Event.id)
        .join(Competition, Competition.competition_id == Event.competition_id)
        .join(Season, Season.id == Event.season_id)
        .where(
            tuple_(Competition.source, Competition.source_unique_tournament_id,
                   Competition.source_tournament_id).in_(identities),
            Season.year == season_year,
            Event.round == "regular_season",
            Event.starts_at < kickoff_cutoff,
            Event.home_participant_id != Event.away_participant_id,
            Result.home_score >= 0,
            Result.away_score >= 0,
        )
        .cte("season_matches")
    )


def read_season_card_records(session: Session, competitions, *, season_year, kickoff_cutoff):
    """Return dated team-side card counts for the same fixtures as the annual table."""
    matches = _season_matches(competitions, season_year, kickoff_cutoff)
    sides = union_all(
        select(matches.c.event_id, matches.c.starts_at, matches.c.home_participant_id.label("participant_id"),
               literal("home").label("side")),
        select(matches.c.event_id, matches.c.starts_at, matches.c.away_participant_id,
               literal("away")),
    ).subquery()
    rows = session.execute(
        select(sides.c.event_id, sides.c.starts_at, sides.c.side, Participant.source, Participant.source_participant_id)
        .join(Participant, Participant.participant_id == sides.c.participant_id)
        .order_by(sides.c.starts_at)
    ).all()
    totals = DisciplinaryIncidentRepository(db_manager).get_event_card_totals(
        "sofascore", {row.event_id for row in rows}, include_second_yellow=True, session=session,
    )
    return [
        (row.starts_at, (row.source, row.source_participant_id),
         totals.get((row.event_id, row.side, "yellow_card")),
         totals.get((row.event_id, row.side, "red_card")),
         totals.get((row.event_id, row.side, "red_card_second_yellow")))
        for row in rows
    ]


def read_season_aggregate_team_totals(
    session: Session,
    sources: Sequence[SeasonAggregateSource],
    *,
    season_year: int,
    kickoff_cutoff: datetime,
):
    """Yield one row of regular-season totals per participant and source tournament."""
    matches = _season_matches((source.competition for source in sources), season_year, kickoff_cutoff)
    teams = union_all(
        select(
            matches.c.competition_id,
            matches.c.home_participant_id.label("participant_id"),
            matches.c.home_score.label("goals_for"),
            matches.c.away_score.label("goals_against"),
        ),
        select(
            matches.c.competition_id,
            matches.c.away_participant_id,
            matches.c.away_score,
            matches.c.home_score,
        ),
    ).subquery()
    totals = (
        select(
            teams.c.competition_id,
            teams.c.participant_id,
            func.count().label("games_played"),
            func.sum(case((teams.c.goals_for > teams.c.goals_against, 1), else_=0)).label("wins"),
            func.sum(case((teams.c.goals_for == teams.c.goals_against, 1), else_=0)).label("draws"),
            func.sum(teams.c.goals_for).label("goals_for"),
            func.sum(teams.c.goals_against).label("goals_against"),
        )
        .group_by(teams.c.competition_id, teams.c.participant_id)
        .subquery()
    )
    query = (
        select(
            Competition.source,
            Competition.source_unique_tournament_id,
            Competition.source_tournament_id,
            Participant.source.label("participant_source"),
            Participant.source_participant_id,
            Participant.name,
            totals.c.games_played,
            totals.c.wins,
            totals.c.draws,
            totals.c.goals_for,
            totals.c.goals_against,
        )
        .select_from(totals)
        .join(Competition, Competition.competition_id == totals.c.competition_id)
        .join(Participant, Participant.participant_id == totals.c.participant_id)
    )
    return session.execute(query)
