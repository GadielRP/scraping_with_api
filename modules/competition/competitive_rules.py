"""Sporting facts, independent of tracking and pressure models."""

from __future__ import annotations

from dataclasses import dataclass

from .competition_identity import CompetitionIdentity
from .qualification_destination import QualificationDestination
from .qualification_reallocation import CupReallocationPolicy
from .rule_terms import (
    AggregationScope,
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
class RankSelection:
    """How a standings table selects teams for an objective."""

    rank_start: int | None = None
    rank_end: int | None = None
    reference_rank: int | None = None
    selection_method: SelectionMethod = SelectionMethod.RANK_RANGE
    ranking_scope: RankingScope = RankingScope.GLOBAL
    aggregation_scope: AggregationScope = AggregationScope.TOURNAMENT
    places: int | None = None
    group_size: int | None = None
    ranking_group: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "selection_method", SelectionMethod(self.selection_method))
        object.__setattr__(self, "ranking_scope", RankingScope(self.ranking_scope))
        object.__setattr__(self, "aggregation_scope", AggregationScope(self.aggregation_scope))
        if (self.rank_start is None) != (self.rank_end is None):
            raise ValueError("rank_start and rank_end must be specified together")
        if self.rank_start is not None and not (1 <= self.rank_start <= self.rank_end):
            raise ValueError("invalid objective rank range")
        if self.reference_rank is not None and self.reference_rank < 1:
            raise ValueError("reference_rank must be positive")
        if self.places is not None and self.places < 1:
            raise ValueError("places must be positive")
        if self.group_size is not None and self.group_size < 1:
            raise ValueError("group_size must be positive")


@dataclass(frozen=True, slots=True)
class ObjectiveRule:
    """A league objective with independent table selection and destination."""

    objective_type: ObjectiveType
    rank_selection: RankSelection = RankSelection()
    qualification_destination: QualificationDestination | None = None
    relegation_path: RelegationPath | None = None
    status: RuleStatus = RuleStatus.CONFIGURED
    note: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "objective_type", ObjectiveType(self.objective_type))
        object.__setattr__(self, "status", RuleStatus(self.status))
        if isinstance(self.rank_selection, dict):
            object.__setattr__(self, "rank_selection", RankSelection(**self.rank_selection))
        if not isinstance(self.rank_selection, RankSelection):
            raise ValueError("rank_selection requires RankSelection")
        if isinstance(self.qualification_destination, dict):
            object.__setattr__(self, "qualification_destination", QualificationDestination(**self.qualification_destination))
        destination = self.qualification_destination
        if destination is not None and not isinstance(destination, QualificationDestination):
            raise ValueError("qualification_destination requires QualificationDestination")
        path = destination.qualification_path if destination is not None else None
        if path is None and self.objective_type != ObjectiveType.DIVISION_RETENTION and self.status != RuleStatus.PENDING:
            raise ValueError("only pending objectives may have an unknown qualification path")
        if self.objective_type == ObjectiveType.COMPETITION_QUALIFICATION:
            if destination is None:
                raise ValueError("competition qualification requires qualification_destination")
            if destination.destination_competition_identity is None and self.status != RuleStatus.PENDING:
                raise ValueError("configured competition qualification requires destination_competition_identity")
            if destination.destination_competition_name is None and self.status != RuleStatus.PENDING:
                raise ValueError("configured competition qualification requires destination_competition_name")
            if destination.destination_phase is None and self.status != RuleStatus.PENDING:
                raise ValueError("configured competition qualification requires destination_phase")
            if self.relegation_path is not None:
                raise ValueError("competition qualification cannot have a relegation path")
        elif self.objective_type == ObjectiveType.PHASE_QUALIFICATION:
            if destination is None or destination.destination_phase is None:
                raise ValueError("phase qualification requires destination_phase")
            if path is not None and path != QualificationPath.DIRECT:
                raise ValueError("phase qualification must use the direct path to its destination")
            if self.relegation_path is not None:
                raise ValueError("phase qualification cannot have a relegation path")
        else:
            if destination is not None:
                raise ValueError("division retention has no qualification destination")
            if path is not None:
                raise ValueError("division retention cannot have a qualification path")
        if self.objective_type == ObjectiveType.DIVISION_RETENTION:
            if self.relegation_path is not None:
                object.__setattr__(self, "relegation_path", RelegationPath(self.relegation_path))
            if self.relegation_path is None and self.status != RuleStatus.PENDING:
                raise ValueError("configured division retention requires relegation_path")
        elif self.relegation_path is not None:
            raise ValueError("qualification cannot have a relegation path")
        if (self.status == RuleStatus.CONFIGURED
                and self.rank_selection.selection_method == SelectionMethod.RANK_RANGE
                and self.rank_selection.rank_start is None):
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
    fair_play_weights: tuple[tuple[str, int], ...] = ()
    require_complete_fair_play: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "primary_metric", StandingsMetric(self.primary_metric))

    def fair_play_points(self, stats) -> int | None:
        """Disciplinary penalty configured by the profile; fewer points is better."""
        if not self.fair_play_weights:
            return None
        if self.require_complete_fair_play and stats.get("fair_play_games_counted", 0) != stats.get(
            "fair_play_games_expected", stats.get("games_played"),
        ):
            return None
        points = 0
        for stat, weight in self.fair_play_weights:
            count = stats.get(stat)
            if count is None:
                return None
            points += count * weight
        return points


@dataclass(frozen=True, slots=True)
class DisciplineRules:
    personal_foul_limit: int

    def __post_init__(self):
        if self.personal_foul_limit < 1:
            raise ValueError("personal_foul_limit must be positive")


@dataclass(frozen=True, slots=True)
class CupQualificationRule:
    """A cup-result rule with its own selection source and shared destination."""

    competition_identity: CompetitionIdentity
    competition_name: str
    final_round: str
    qualification_destination: QualificationDestination
    reallocation_policy: CupReallocationPolicy = CupReallocationPolicy()
    destination_season_offset: int | None = None
    sources: tuple[str, ...] = ()
    status: RuleStatus = RuleStatus.CONFIGURED
    runner_up_qualification_destination: QualificationDestination | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", RuleStatus(self.status))
        if isinstance(self.qualification_destination, dict):
            object.__setattr__(self, "qualification_destination", QualificationDestination(**self.qualification_destination))
        if not isinstance(self.qualification_destination, QualificationDestination):
            raise ValueError("cup qualification requires qualification_destination")
        if isinstance(self.reallocation_policy, dict):
            object.__setattr__(self, "reallocation_policy", CupReallocationPolicy(**self.reallocation_policy))
        if not isinstance(self.reallocation_policy, CupReallocationPolicy):
            raise ValueError("reallocation_policy requires CupReallocationPolicy")
        if (self.qualification_destination.destination_competition_identity is None
                or self.qualification_destination.destination_competition_name is None
                or self.qualification_destination.destination_phase is None
                or self.qualification_destination.qualification_path is None):
            raise ValueError("cup qualification requires a complete external destination")
        runner_up_destination = self.runner_up_qualification_destination
        if isinstance(runner_up_destination, dict):
            runner_up_destination = QualificationDestination(**runner_up_destination)
            object.__setattr__(self, "runner_up_qualification_destination", runner_up_destination)
        if runner_up_destination is not None:
            if (not isinstance(runner_up_destination, QualificationDestination)
                    or runner_up_destination.destination_competition_identity is None
                    or runner_up_destination.destination_competition_name is None
                    or runner_up_destination.destination_phase is None
                    or runner_up_destination.qualification_path is None):
                raise ValueError("runner-up qualification requires a complete external destination")
            if self.status != RuleStatus.PENDING:
                raise ValueError("the cup resolver only supports champion berths")
        if (not isinstance(self.competition_name, str) or not self.competition_name.strip()
                or not isinstance(self.final_round, str) or not self.final_round.strip()):
            raise ValueError("cup competition_name and final_round are required")


@dataclass(frozen=True, slots=True)
class CompetitionRules:
    """The single sporting configuration for a canonical competition.

    Missing size/duration/objectives mean those facts have not been configured.
    Reference season is descriptive, never a lookup key. Cup qualification is
    configured alongside the league objectives that it can change.
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
    # Ordered for deterministic handling when configured cup winners overlap.
    cup_qualifications: tuple[CupQualificationRule, ...] = ()
    fair_play_carryover: CompetitionIdentity | None = None

    @property
    def configured_cup_qualifications(self) -> tuple[CupQualificationRule, ...]:
        return tuple(rule for rule in self.cup_qualifications if rule.status == RuleStatus.CONFIGURED)

    def __post_init__(self) -> None:
        object.__setattr__(self, "grouping_method", GroupingMethod(self.grouping_method))
        object.__setattr__(self, "cup_qualifications", tuple(self.cup_qualifications))
        if self.number_of_teams is not None and self.number_of_teams <= 1:
            raise ValueError("competition size must exceed one")
        if self.total_regular_season_games is not None and self.total_regular_season_games <= 0:
            raise ValueError("competition duration must be positive")
