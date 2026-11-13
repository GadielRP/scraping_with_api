"""Combine regular-season totals and apply the annual ranking policy."""

from collections import Counter, defaultdict
from datetime import datetime
from fractions import Fraction

from sqlalchemy.orm import Session

from infrastructure.persistence.repositories.season_aggregate_totals_repository import (
    read_season_aggregate_team_totals, read_season_card_records,
)
from modules.competition.competitive_rules import SeasonAggregateProfile
from modules.competition.final_outcome import ParticipantIdentity
from modules.competition.season_aggregate import (
    SeasonAggregateStandings, SeasonStanding,
)
from shared.temporal import as_utc
from modules.discipline.policies import accumulate_football_fair_play
from .standings_rules import rank_standings


def calculate_season_aggregate_standings(
    session: Session,
    profile: SeasonAggregateProfile,
    *,
    season_year: int,
    kickoff_cutoff: datetime,
) -> SeasonAggregateStandings:
    """Build an annual table from stored results of fixtures before the cutoff."""
    cutoff = as_utc(kickoff_cutoff)
    totals_by_team = defaultdict(Counter)
    participants = {}
    for row in read_season_aggregate_team_totals(
        session, profile.sources, season_year=season_year, kickoff_cutoff=cutoff,
    ):
        participant = ParticipantIdentity(
            row.participant_source, row.source_participant_id, row.name
        )
        team = f"{participant.source}:{participant.source_participant_id}"
        participants[team] = (participant, row.name)
        totals_by_team[team].update(
            games_played=row.games_played,
            wins=row.wins,
            draws=row.draws,
            losses=row.games_played - row.wins - row.draws,
            goals_for=row.goals_for,
            goals_against=row.goals_against,
        )

    # Use plain dicts for ranking so missing tie-break data cannot read as zero.
    stats = {}
    for team, totals in totals_by_team.items():
        stats[team] = dict(
            totals,
            goal_diff=totals["goals_for"] - totals["goals_against"],
            points=profile.scoring.points(totals),
        )
    if profile.standings.fair_play_weights:
        for _, (source, participant_id), yellow, red, second_yellow in read_season_card_records(
            session, (source.competition for source in profile.sources),
            season_year=season_year, kickoff_cutoff=cutoff,
        ):
            accumulate_football_fair_play(stats[f"{source}:{participant_id}"], yellow, red, second_yellow)
        for team_stats in stats.values():
            team_stats["fair_play_points"] = profile.standings.fair_play_points(team_stats)
    ordered, positions = rank_standings(stats.items(), profile, None)
    return SeasonAggregateStandings(
        season_year=season_year,
        kickoff_cutoff=cutoff,
        sources=profile.sources,
        standings=tuple(
            SeasonStanding(
                participant=participants[team][0],
                name=participants[team][1],
                rank=positions[team]["position"],
                ranking_reason=positions[team]["ranking_reason"],
                **team_stats,
            )
            for team, team_stats in ordered
        ),
    )

def calculate_multiseason_averages(
    session: Session,
    profile: SeasonAggregateProfile,
    annual: SeasonAggregateStandings,
    *,
    seasons: int,
) -> dict[ParticipantIdentity, Fraction] | None:
    """Points per game over the window: the table so far plus the completed campaigns before it."""
    points = {row.participant: Fraction(str(row.points)) for row in annual.standings}
    played = {row.participant: row.games_played for row in annual.standings}
    expected_games = {
        (source.competition.source, source.competition.source_unique_tournament_id,
         source.competition.source_tournament_id): source.games_per_team
        for source in profile.sources
    }
    for year in range(annual.season_year - seasons + 1, annual.season_year):
        games_by_tournament = defaultdict(dict)
        for row in read_season_aggregate_team_totals(
            session, profile.sources, season_year=year, kickoff_cutoff=annual.kickoff_cutoff,
        ):
            team = ParticipantIdentity(row.participant_source, row.source_participant_id)
            identity = (row.source, row.source_unique_tournament_id, row.source_tournament_id)
            games_by_tournament[identity][team] = row.games_played
            if team in points:
                points[team] += Fraction(str(profile.scoring.points({
                    "wins": row.wins, "draws": row.draws, "losses": row.games_played - row.wins - row.draws,
                })))
                played[team] += row.games_played
        if any(
            len(games_by_tournament[identity]) != profile.number_of_teams
            or any(games != expected for games in games_by_tournament[identity].values())
            for identity, expected in expected_games.items()
        ):
            return None
        cohorts = list(games_by_tournament.values())
        if any(cohort.keys() != cohorts[0].keys() for cohort in cohorts[1:]):
            return None
    return {team: points[team] / games for team, games in played.items()}
