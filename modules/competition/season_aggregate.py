"""Configuration and results for a table shared by multiple tournaments."""

from dataclasses import dataclass
from datetime import datetime

from .competition_identity import CompetitionIdentity
from .competitive_rules import ScoringRules, StandingsRules
from .final_outcome import ParticipantIdentity


@dataclass(frozen=True, slots=True)
class SeasonAggregateSource:
    competition: CompetitionIdentity
    games_per_team: int | None


@dataclass(frozen=True, slots=True)
class SeasonAggregateProfile:
    number_of_teams: int | None
    sources: tuple[SeasonAggregateSource, ...]
    scoring: ScoringRules
    standings: StandingsRules


@dataclass(frozen=True, slots=True)
class SeasonStanding:
    participant: ParticipantIdentity
    name: str
    rank: int | None
    points: float
    games_played: int
    wins: int
    draws: int
    losses: int
    goals_for: int
    goals_against: int
    goal_diff: int
    ranking_reason: str | None
    fair_play_points: int | None = None
    fair_play_games_counted: int = 0
    standalone_yellow_cards: int | None = None
    second_yellow_dismissals: int | None = None
    direct_red_cards: int | None = None


@dataclass(frozen=True, slots=True)
class SeasonAggregateStandings:
    season_year: int
    kickoff_cutoff: datetime
    sources: tuple[SeasonAggregateSource, ...]
    regular_season_complete: bool
    standings: tuple[SeasonStanding, ...]
