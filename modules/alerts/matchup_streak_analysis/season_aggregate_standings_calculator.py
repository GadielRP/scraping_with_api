"""Combine regular-season totals and apply the annual ranking policy."""

from collections import Counter, defaultdict
from datetime import datetime

from sqlalchemy.orm import Session

from infrastructure.persistence.repositories.season_aggregate_totals_repository import (
    read_season_aggregate_team_totals, read_season_card_records,
)
from modules.competition.competition_identity import CompetitionIdentity
from modules.competition.final_outcome import ParticipantIdentity
from modules.competition.season_aggregate import (
    SeasonAggregateProfile, SeasonAggregateStandings, SeasonStanding,
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
    games_by_competition = defaultdict(dict)
    for row in read_season_aggregate_team_totals(
        session, profile.sources, season_year=season_year, kickoff_cutoff=cutoff,
    ):
        participant = ParticipantIdentity(row.participant_source, row.source_participant_id)
        team = f"{participant.source}:{participant.source_participant_id}"
        participants[team] = (participant, row.name)
        competition = CompetitionIdentity(
            row.source_unique_tournament_id, row.source_tournament_id, row.source,
        )
        games_by_competition[competition][team] = row.games_played
        totals_by_team[team].update(
            games_played=row.games_played,
            wins=row.wins,
            draws=row.draws,
            losses=row.games_played - row.wins - row.draws,
            goals_for=row.goals_for,
            goals_against=row.goals_against,
        )

    complete = len(participants) == profile.number_of_teams and all(
        source.games_per_team is not None
        and games_by_competition[source.competition].get(team) == source.games_per_team
        for source in profile.sources
        for team in participants
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
    ordered, positions = rank_standings(stats.items(), profile, None, season_complete=complete)
    return SeasonAggregateStandings(
        season_year=season_year,
        kickoff_cutoff=cutoff,
        sources=profile.sources,
        regular_season_complete=complete,
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
