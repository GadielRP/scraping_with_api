"""Sporting facts, independent of tracking and pressure models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ObjectiveType = Literal[
    "COMPETITION_QUALIFICATION", "STAGE_QUALIFICATION", "DIVISION_RETENTION"
]


@dataclass(frozen=True, slots=True)
class ObjectiveRule:
    """A configured effective zone or a pending allocation, within its current competition profile.

    CONFIGURED confirms the sporting boundary, not M5's ability to calculate it.
    An omitted objective is absent; NO_APPLICABLE_OBJECTIVE is a team outcome.
    Sources and the reference season are inherited from CompetitionRules.
    The reference season documents the current facts; it does not select events.
    """

    objective_type: ObjectiveType
    target: str
    qualification_path: str
    ranking_scope: Literal["GLOBAL", "CONFERENCE", "DIVISION", "GROUP", "LEAGUE"] = "GLOBAL"
    aggregation_scope: Literal["TOURNAMENT", "SEASON_AGGREGATE", "MULTISEASON_RECORD"] = "TOURNAMENT"
    status: Literal["CONFIGURED", "PENDING"] = "CONFIGURED"
    rank_start: int | None = None
    rank_end: int | None = None
    reference_rank: int | None = None
    selection_method: str = "RANK_RANGE"
    places: int | None = None
    group_size: int | None = None
    target_season: str | None = None
    note: str = ""
    ranking_group: str | None = None

    def __post_init__(self) -> None:
        if (self.rank_start is None) != (self.rank_end is None):
            raise ValueError("rank_start and rank_end must be specified together")
        if self.rank_start is not None and not (1 <= self.rank_start <= self.rank_end):
            raise ValueError("invalid objective rank range")
        if self.reference_rank is not None and self.reference_rank < 1:
            raise ValueError("reference_rank must be positive")


@dataclass(frozen=True, slots=True)
class ScoringRules:
    """Points awarded for each outcome, shared by tables and competitive cost."""

    regulation_win: float
    overtime_win: float
    draw: float = 0.0
    overtime_loss: float = 0.0
    loss: float = 0.0

    @property
    def maximum_per_game(self) -> float:
        return max(self.regulation_win, self.overtime_win, self.draw, self.overtime_loss, self.loss)

    def points(self, stats) -> float:
        overtime_wins = stats.get("ot_so_wins", 0)
        overtime_losses = stats.get("ot_losses", 0)
        return (
            (stats.get("wins", 0) - overtime_wins) * self.regulation_win
            + overtime_wins * self.overtime_win
            + stats.get("draws", 0) * self.draw
            + overtime_losses * self.overtime_loss
            + (stats.get("losses", 0) - overtime_losses) * self.loss
        )


FOOTBALL_SCORING = ScoringRules(3, 3, draw=1)
WINS_SCORING = ScoringRules(1, 1)
NFL_SCORING = ScoringRules(1, 1, draw=0.5)
NHL_SCORING = ScoringRules(2, 2, overtime_loss=1)
HOCKEY_SCORING = ScoringRules(3, 2, overtime_loss=1)

@dataclass(frozen=True, slots=True)
class RankingCriterion:
    """One ordered comparison; a mini-table may contain several metrics.

    Missing required data stops the comparison. It never authorizes using a
    later criterion. ``meetings`` applies per pair, not to the total match count.
    """

    metrics: tuple[str, ...]
    meetings: int = 0
    only_two: bool = False
    only_multiple: bool = False
    reapply: bool = False
    unavailable: str | None = None
    final_only: bool = False
    share_if_incomplete: bool = False


@dataclass(frozen=True, slots=True)
class StandingsRules:
    """Competition-specific ordering, separate from points awarded per game."""

    name: str
    primary_metric: Literal["points", "wins", "pct"]
    criteria: tuple[RankingCriterion, ...]
    source_urls: tuple[str, ...]
    in_season_criteria: tuple[RankingCriterion, ...] | None = None
    record_percentage: bool = False
    regulation_scores_only: bool = False
    verified: bool = True
    note: str = ""


@dataclass(frozen=True, slots=True)
class CompetitionRules:
    """The single sporting configuration for a canonical competition.

    Missing size/duration/objectives mean those facts have not been configured.
    Reference season and provenance are descriptive, never lookup keys.
    """

    # Reference season under which these sporting rules, boundaries, and sizes were documented.
    # NOTE: This is descriptive audit metadata (provenance). It is NEVER used as a lookup filter
    # or query key, and does not cause events from other seasons to be rejected.
    reference_season: str
    number_of_teams: int | None
    total_regular_season_games: int | None
    scoring: ScoringRules
    standings: StandingsRules
    objectives: tuple[ObjectiveRule, ...] | None
    source_urls: tuple[str, ...]
    grouping_method: str = "league_wide"
    note: str = ""

    @property
    def season_key(self) -> str:
        return self.reference_season

    @property
    def ranking_metric(self) -> str:
        if self.standings.primary_metric == "pct":
            return "WIN_PERCENTAGE"
        return "WINS" if self.standings.primary_metric == "wins" else "POINTS"

    @property
    def standings_grouping(self) -> str:
        return "single_table" if self.grouping_method == "league_wide" else "split_tables"

    def __post_init__(self) -> None:
        if self.number_of_teams is not None and self.number_of_teams <= 1:
            raise ValueError("competition size must exceed one")
        if self.total_regular_season_games is not None and self.total_regular_season_games <= 0:
            raise ValueError("competition duration must be positive")
        if not self.source_urls:
            raise ValueError("rule provenance is required")
