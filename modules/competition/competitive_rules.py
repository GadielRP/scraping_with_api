"""Sporting facts, independent of tracking and pressure models."""

from __future__ import annotations

from dataclasses import dataclass

from .competition_identity import CompetitionIdentity
from .rule_terms import (
    AggregationScope,
    DestinationPhase,
    DestinationRound,
    GroupingMethod,
    ObjectiveType,
    QualificationPath,
    RelegationPath,
    RankingScope,
    RuleStatus,
    SelectionMethod,
    StandingsMetric,
)


@dataclass(frozen=True, slots=True)
class ObjectiveRule:
    """A configured effective zone or a pending allocation, within its current competition profile.

    CONFIGURED confirms the sporting boundary, not M5's ability to calculate it.
    An omitted objective is absent; NO_APPLICABLE_OBJECTIVE is a team outcome.
    The reference season is inherited from CompetitionRules.
    The reference season documents the current facts; it does not select events.
    """

    objective_type: ObjectiveType
    qualification_path: QualificationPath | None = None
    relegation_path: RelegationPath | None = None
    destination_competition_identity: CompetitionIdentity | None = None
    destination_competition_name: str | None = None
    destination_phase: DestinationPhase | None = None
    destination_round: DestinationRound | None = None
    ranking_scope: RankingScope = RankingScope.GLOBAL
    aggregation_scope: AggregationScope = AggregationScope.TOURNAMENT
    status: RuleStatus = RuleStatus.CONFIGURED
    rank_start: int | None = None
    rank_end: int | None = None
    reference_rank: int | None = None
    selection_method: SelectionMethod = SelectionMethod.RANK_RANGE
    places: int | None = None
    group_size: int | None = None
    note: str = ""
    ranking_group: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "objective_type", ObjectiveType(self.objective_type))
        if isinstance(self.destination_competition_identity, dict):
            object.__setattr__(self, "destination_competition_identity", CompetitionIdentity(**self.destination_competition_identity))
        if self.destination_competition_identity is not None and not isinstance(self.destination_competition_identity, CompetitionIdentity):
            raise ValueError("destination_competition_identity requires a provider identity")
        if self.destination_phase is not None:
            object.__setattr__(self, "destination_phase", DestinationPhase(self.destination_phase))
        if self.destination_round is not None:
            object.__setattr__(self, "destination_round", DestinationRound(self.destination_round))
            if self.destination_phase is None:
                raise ValueError("destination_round requires destination_phase")
        object.__setattr__(self, "ranking_scope", RankingScope(self.ranking_scope))
        object.__setattr__(self, "aggregation_scope", AggregationScope(self.aggregation_scope))
        object.__setattr__(self, "status", RuleStatus(self.status))
        object.__setattr__(self, "selection_method", SelectionMethod(self.selection_method))
        if self.qualification_path is not None:
            object.__setattr__(self, "qualification_path", QualificationPath(self.qualification_path))
        elif (self.objective_type != ObjectiveType.DIVISION_RETENTION
              and self.status != RuleStatus.PENDING):
            raise ValueError("only pending objectives may have an unknown qualification path")
        if self.objective_type == ObjectiveType.COMPETITION_QUALIFICATION:
            if self.destination_competition_identity is None:
                if self.status != RuleStatus.PENDING:
                    raise ValueError("configured competition qualification requires destination_competition_identity")
            if self.destination_phase is None and self.status != RuleStatus.PENDING:
                raise ValueError("configured competition qualification requires destination_phase")
            if self.relegation_path is not None:
                raise ValueError("competition qualification cannot have a relegation path")
        elif self.objective_type == ObjectiveType.PHASE_QUALIFICATION:
            if self.destination_phase is None:
                raise ValueError("phase qualification requires destination_phase")
            if (self.qualification_path is not None
                    and self.qualification_path != QualificationPath.DIRECT):
                raise ValueError("phase qualification must use the direct path to its destination")
            if self.relegation_path is not None:
                raise ValueError("phase qualification cannot have a relegation path")
        else:
            if (self.destination_competition_identity is not None or self.destination_phase is not None
                    or self.destination_round is not None):
                raise ValueError("division retention has no qualification destination")
            if self.qualification_path is not None:
                raise ValueError("division retention cannot have a qualification path")
        if self.objective_type == ObjectiveType.DIVISION_RETENTION:
            if self.relegation_path is not None:
                object.__setattr__(self, "relegation_path", RelegationPath(self.relegation_path))
            if self.relegation_path is None and self.status != RuleStatus.PENDING:
                raise ValueError("configured division retention requires relegation_path")
        elif self.relegation_path is not None:
            raise ValueError("qualification cannot have a relegation path")
        if (self.rank_start is None) != (self.rank_end is None):
            raise ValueError("rank_start and rank_end must be specified together")
        if self.rank_start is not None and not (1 <= self.rank_start <= self.rank_end):
            raise ValueError("invalid objective rank range")
        if self.reference_rank is not None and self.reference_rank < 1:
            raise ValueError("reference_rank must be positive")
        if (self.status == RuleStatus.CONFIGURED and self.selection_method == SelectionMethod.RANK_RANGE
                and self.rank_start is None):
            raise ValueError("configured rank-range selection requires ranks")


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
    primary_metric: StandingsMetric
    criteria: tuple[RankingCriterion, ...]
    in_season_criteria: tuple[RankingCriterion, ...] | None = None
    record_percentage: bool = False
    regulation_scores_only: bool = False
    verified: bool = True
    note: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "primary_metric", StandingsMetric(self.primary_metric))


@dataclass(frozen=True, slots=True)
class DisciplineRules:
    personal_foul_limit: int

    def __post_init__(self):
        if self.personal_foul_limit < 1:
            raise ValueError("personal_foul_limit must be positive")


@dataclass(frozen=True, slots=True)
class CompetitionRules:
    """The single sporting configuration for a canonical competition.

    Missing size/duration/objectives mean those facts have not been configured.
    Reference season is descriptive, never a lookup key.
    """

    # Reference season under which these sporting rules, boundaries, and sizes were documented.
    # NOTE: This is descriptive metadata. It is NEVER used as a lookup filter
    # or query key, and does not cause events from other seasons to be rejected.
    reference_season: str
    number_of_teams: int | None
    total_regular_season_games: int | None
    scoring: ScoringRules
    standings: StandingsRules
    objectives: tuple[ObjectiveRule, ...] | None
    grouping_method: GroupingMethod = GroupingMethod.LEAGUE_WIDE
    note: str = ""
    discipline: DisciplineRules | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "grouping_method", GroupingMethod(self.grouping_method))
        if self.number_of_teams is not None and self.number_of_teams <= 1:
            raise ValueError("competition size must exceed one")
        if self.total_regular_season_games is not None and self.total_regular_season_games <= 0:
            raise ValueError("competition duration must be positive")
