"""Results of a standings table shared by multiple tournaments."""

from dataclasses import dataclass
from datetime import datetime

from .competitive_rules import SeasonAggregateSource
from .final_outcome import ParticipantIdentity


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
    # Order data from the ranker: ``ranking_order`` is the first place of the row's tie group and
    # sorts rows even when ``rank`` is None; the policy fields appear when a closing criterion
    # handed the group to the in-season order.
    ranking_status: str = "RESOLVED"
    ranking_order: int | None = None
    ranking_policy: str | None = None
    ranking_policy_reason: str | None = None


@dataclass(frozen=True, slots=True)
class SeasonAggregateStandings:
    season_year: int
    kickoff_cutoff: datetime
    sources: tuple[SeasonAggregateSource, ...]
    standings: tuple[SeasonStanding, ...]
    season_completion_ratio: float | None = None
    standings_policy: str = "in_season"
