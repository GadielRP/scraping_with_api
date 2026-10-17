"""Season-scoped sporting facts, independent of tracking and pressure models."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal


ObjectiveType = Literal[
    "COMPETITION_QUALIFICATION", "STAGE_QUALIFICATION", "DIVISION_RETENTION"
]


@dataclass(frozen=True, slots=True)
class ObjectiveRule:
    """A configured effective zone or a pending allocation, within its profile's season.

    CONFIGURED confirms the sporting boundary, not M5's ability to calculate it.
    An omitted objective is absent; NO_APPLICABLE_OBJECTIVE is a team outcome.
    Sources and the domestic season are inherited from CompetitionRules.
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

    def __post_init__(self) -> None:
        if (self.rank_start is None) != (self.rank_end is None):
            raise ValueError("rank_start and rank_end must be specified together")
        if self.rank_start is not None and not (1 <= self.rank_start <= self.rank_end):
            raise ValueError("invalid objective rank range")
        if self.reference_rank is not None and self.reference_rank < 1:
            raise ValueError("reference_rank must be positive")


@dataclass(frozen=True, slots=True)
class CompetitionRules:
    """Immutable season facts shared by P1 modules; contains no standings/history.

    points_per_win is the maximum match increment in the table's existing unit.
    It does not convert a percentage or average into an additive scoring unit.
    """

    season_key: str
    number_of_teams: int
    total_regular_season_games: int
    points_per_win: float
    ranking_metric: Literal["POINTS", "WINS", "WIN_PERCENTAGE"]
    objectives: tuple[ObjectiveRule, ...]
    source_urls: tuple[str, ...]
    standings_grouping: str = "single_table"
    points_per_draw: float | None = None
    note: str = ""

    def __post_init__(self) -> None:
        if self.number_of_teams <= 1 or self.total_regular_season_games <= 0:
            raise ValueError("competition size and duration must be positive")
        if self.points_per_win <= 0 or not self.source_urls:
            raise ValueError("scoring unit and rule provenance are required")


def season_key_from_name(season_name: str | None) -> str | None:
    """Match explicit season labels; never infer a season from today's date/year."""
    matches = re.findall(r"(?<!\d)((?:19|20)\d{2})(?:\s*[/–—-]\s*(\d{2,4}))?(?!\d)", season_name or "")
    if len(matches) != 1:
        return None
    start, end = matches[0]
    if not end:
        return start
    end_year = int(end) if len(end) == 4 else (int(start) // 100) * 100 + int(end)
    if end_year != int(start) + 1:
        return None
    return f"{start}/{end_year % 100:02d}"
