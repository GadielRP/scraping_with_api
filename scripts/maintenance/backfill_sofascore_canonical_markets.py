"""Canonicalize legacy SofaScore market metadata against the current catalog.

This script intentionally updates only ``markets.market_name``,
``markets.market_group`` and ``markets.market_period``.  Choice rows, odds,
snapshots, source lineage and canonical mapping tables are read-only here.

LEGACY_PHASE8_REMOVE_OR_PORT: duplicate-market conflict resolution still
depends on snapshot.choice_id and the old MarketChoice price mirror despite
the normal metadata-only path above. Phase 6/7 schema preflights block that
branch. In Phase 8 port the merge to quote lineage or remove the command and
its tests; do not restore the legacy columns.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from sqlalchemy import inspect, text, bindparam

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from infrastructure.persistence.catalogs.canonical_market_types import (  # noqa: E402
    CANONICAL_MARKET_TYPE_SEEDS,
)
from infrastructure.persistence.database import db_manager  # noqa: E402


logger = logging.getLogger(__name__)


@contextmanager
def progress_step(label: str):
    """Print start/end messages around indivisible DB operations."""
    started_at = time.monotonic()
    print(f"[START] {label}", flush=True)
    try:
        yield
    except BaseException:
        elapsed = time.monotonic() - started_at
        print(f"[FAIL ] {label} ({elapsed:.1f}s elapsed)", flush=True)
        raise
    else:
        elapsed = time.monotonic() - started_at
        print(f"[ DONE] {label} ({elapsed:.1f}s elapsed)", flush=True)


@dataclass(frozen=True)
class CanonicalizationRule:
    rule_id: str
    source_market_name: str
    source_market_group: str
    source_market_period: str
    target_market_name: str
    target_market_group: str
    target_market_period: str
    canonical_market_key: str
    market_family: str
    requires_choice_group: bool
    expected_choice_pattern: str
    expected_choice_names: tuple[str, ...]
    safe: bool = True
    trajectory_required: bool = True

    @property
    def source_shape(self) -> str:
        return " / ".join(
            (self.source_market_name, self.source_market_group, self.source_market_period)
        )

    @property
    def target_shape(self) -> str:
        return " / ".join(
            (self.target_market_name, self.target_market_group, self.target_market_period)
        )


SIDE_2_WAY_PATTERN = r"^(1|2)$"
SIDE_3_WAY_PATTERN = r"^(1|X|2)$"
TOTAL_PATTERN = r"^(Over|Under)$"
HANDICAP_PATTERN = r"^(1|2|\([+-]?[0-9]+(\.[0-9]+)?\)[[:space:]]+.+)$"
DECISION_PATTERN = r"^(yes|no|Yes|No|YES|NO)$"
GOAL_TEAM_PATTERN = r"^(1|2|no_goal|no goal|No goal|No Goal)$"
DOUBLE_CHANCE_PATTERN = r"^(1X|12|X2)$"


def _rule(
    rule_id: str,
    source_market_name: str,
    source_market_group: str,
    source_market_period: str,
    *,
    canonical_market_key: str,
    expected_choice_pattern: str,
    expected_choice_names: tuple[str, ...],
    safe: bool = True,
) -> CanonicalizationRule:
    """Build a rule whose target triple always comes from the catalog seed."""
    seed = CANONICAL_MARKET_TYPE_SEEDS.get(canonical_market_key)
    if seed is None:
        raise KeyError(
            f"Rule {rule_id!r} references unknown canonical_market_key="
            f"{canonical_market_key!r}"
        )
    return CanonicalizationRule(
        rule_id=rule_id,
        source_market_name=source_market_name,
        source_market_group=source_market_group,
        source_market_period=source_market_period,
        target_market_name=seed["canonical_market_name"],
        target_market_group=seed["canonical_market_group"],
        target_market_period=seed["canonical_market_period"],
        canonical_market_key=canonical_market_key,
        market_family=seed["market_family"],
        requires_choice_group=bool(seed["requires_choice_group"]),
        expected_choice_pattern=expected_choice_pattern,
        expected_choice_names=expected_choice_names,
        safe=safe,
        trajectory_required=bool(seed["enabled_for_trajectory"]),
    )


# Source shapes are exact DB matches (case/spacing sensitive).
# Target key/name/group/period/family always come from
# CANONICAL_MARKET_TYPE_SEEDS via canonical_market_key.
RULES: tuple[CanonicalizationRule, ...] = (
    # --- 1X2 Full Time ---
    _rule("full_time_1x2_full_time", "Full time", "1X2", "Full-time",
          canonical_market_key="1x2_full_time",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    _rule("full_time_1x2_full_time_spaced", "Full time", "1X2", "Full Time",
          canonical_market_key="1x2_full_time",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    _rule("full_time_title_1x2_full_time", "Full Time", "1X2", "Full Time",
          canonical_market_key="1x2_full_time",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    # --- 1X2 1st Half ---
    _rule("first_half_1x2", "1st half", "1X2", "1st half",
          canonical_market_key="1x2_1st_half",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    _rule("first_half_1x2_canonical_period", "1st half", "1X2", "1st Half",
          canonical_market_key="1x2_1st_half",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    _rule("first_half_title_1x2", "1st Half", "1X2", "1st Half",
          canonical_market_key="1x2_1st_half",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    # --- 1X2 1st Quarter ---
    _rule("first_quarter_1x2", "1st quarter winner", "1X2", "1st quarter",
          canonical_market_key="1x2_1st_quarter",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    _rule("first_quarter_title_1x2", "1st Quarter", "1X2", "1st Quarter",
          canonical_market_key="1x2_1st_quarter",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    # --- Home/Away Full Time ---
    _rule("full_time_home_away_match", "Full time", "Home/Away", "Match",
          canonical_market_key="home_away_full_time",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    _rule("full_time_home_away_full_time", "Full time", "Home/Away", "Full Time",
          canonical_market_key="home_away_full_time",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    _rule("full_time_home_away_hyphen", "Full time", "Home/Away", "Full-time",
          canonical_market_key="home_away_full_time",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    _rule("full_time_title_home_away", "Full Time", "Home/Away", "Full Time",
          canonical_market_key="home_away_full_time",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    # --- Home/Away 1st Half ---
    _rule("first_half_home_away", "1st half", "Home/Away", "1st half",
          canonical_market_key="home_away_1st_half",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    _rule("first_half_home_away_canonical_period", "1st half", "Home/Away", "1st Half",
          canonical_market_key="home_away_1st_half",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    _rule("first_half_title_home_away", "1st Half", "Home/Away", "1st Half",
          canonical_market_key="home_away_1st_half",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    # --- Home/Away 1st Quarter ---
    _rule("home_away_1st_quarter", "1st quarter winner", "Home/Away", "1st quarter",
          canonical_market_key="home_away_1st_quarter",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    _rule("home_away_1st_quarter_title", "1st Quarter", "Home/Away", "1st Quarter",
          canonical_market_key="home_away_1st_quarter",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    # --- Home/Away Full Time Including Overtime ---
    _rule("full_time_including_overtime",
          "Full time (including overtime)", "Full time (including overtime)", "Full-time",
          canonical_market_key="home_away_full_time_including_overtime",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    _rule("full_time_including_overtime_full_time_period",
          "Full time (including overtime)", "Full time (including overtime)", "Full Time",
          canonical_market_key="home_away_full_time_including_overtime",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    # --- First / Current Set Winner ---
    _rule("first_set_winner_1st_set", "First set winner", "Home/Away", "1st set",
          canonical_market_key="first_set_winner_1st_set",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    _rule("current_set_winner_current_set", "Current set winner", "Current set winner", "Current set",
          canonical_market_key="current_set_winner_current_set",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    # --- Over/Under Full Time ---
    _rule("game_total_match", "Game total", "Over/Under", "Match",
          canonical_market_key="over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("total_points_match", "Total points", "Over/Under", "Match",
          canonical_market_key="over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("match_goals_match", "Match goals", "Match goals", "Match",
          canonical_market_key="over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("match_goals_full_time", "Match goals", "Match goals", "Full-time",
          canonical_market_key="over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("over_under_bare_full_time", "Over/Under", "Over/Under", "Full Time",
          canonical_market_key="over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("over_under_bare_full_time_hyphen", "Over/Under", "Over/Under", "Full-time",
          canonical_market_key="over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("over_under_bare_match", "Over/Under", "Over/Under", "Match",
          canonical_market_key="over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    # --- Sets Over/Under Full Time ---
    _rule("total_sets_over_under_full_time", "Total sets", "Over/Under", "Full-time",
          canonical_market_key="sets_over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("total_sets_over_under_full_time_spaced", "Total sets", "Over/Under", "Full Time",
          canonical_market_key="sets_over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("match_sets_over_under_full_time", "Match sets", "Over/Under", "Full-time",
          canonical_market_key="sets_over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("sets_total_over_under_full_time", "Sets total", "Total sets", "Full Time",
          canonical_market_key="sets_over_under_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    # --- Over/Under Full Time Including Overtime ---
    _rule("over_under_including_overtime_period", "Over/Under", "Over/Under", "Full Time Including Overtime",
          canonical_market_key="over_under_full_time_including_overtime",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("over_under_incl_overtime_name", "Over under (incl. overtime)", "Over/Under", "Full-time",
          canonical_market_key="over_under_full_time_including_overtime",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("total_incl_overtime_name", "Total (incl. overtime)", "Over/Under", "Full Time",
          canonical_market_key="over_under_full_time_including_overtime",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    # --- Over/Under 1st Half ---
    _rule("first_half_over_under_compound", "1st half Over/Under", "Over/Under", "1st Half",
          canonical_market_key="over_under_1st_half",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("over_under_1st_half_lower", "Over/Under 1st half", "Over/Under", "1st Half",
          canonical_market_key="over_under_1st_half",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("over_under_1st_half_trailing_space", "Over/Under 1st half ", "Over/Under", "1st Half",
          canonical_market_key="over_under_1st_half",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("over_under_bare_1st_half", "Over/Under", "Over/Under", "1st Half",
          canonical_market_key="over_under_1st_half",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("first_half_name_over_under", "1st half", "Over/Under", "1st Half",
          canonical_market_key="over_under_1st_half",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    # --- Over/Under 1st Quarter ---
    _rule("over_under_bare_1st_quarter", "Over/Under", "Over/Under", "1st Quarter",
          canonical_market_key="over_under_1st_quarter",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("first_quarter_over_under", "1st quarter", "Over/Under", "1st Quarter",
          canonical_market_key="over_under_1st_quarter",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    # --- Over/Under 1st Period ---
    _rule("over_under_1st_period", "1st period goals", "Over/Under", "1st period",
          canonical_market_key="over_under_1st_period",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    # --- Total Cards / Corners / Sets-Games ---
    _rule("total_cards_full_time", "Cards in match", "Total Cards", "Full-time",
          canonical_market_key="total_cards_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("total_corners_full_time_hyphen", "Corners 2-Way", "Corners 2-Way", "Full-time",
          canonical_market_key="total_corners_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("total_corners_full_time_spaced", "Corners 2-Way", "Corners 2-Way", "Full Time",
          canonical_market_key="total_corners_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("total_corners_full_time_compound_name", "Corners 2-Way Full Time", "Corners 2-Way", "Full Time",
          canonical_market_key="total_corners_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("total_sets_games_extra_time", "Total games won", "Total sets/games", "Extra time",
          canonical_market_key="total_sets_games_extra_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    # --- Team totals ---
    _rule("team_total_home_full_time", "Over under team 1", "Over/Under Team 1", "Full-time",
          canonical_market_key="team_total_home_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("team_total_home_full_time_spaced", "Over under team 1", "Over/Under Team 1", "Full Time",
          canonical_market_key="team_total_home_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("team_total_home_bare_group", "Over/Under Team 1", "Over/Under Team 1", "Full Time",
          canonical_market_key="team_total_home_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("team_total_away_full_time", "Over under team 2", "Over/Under Team 2", "Full-time",
          canonical_market_key="team_total_away_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("team_total_away_full_time_spaced", "Over under team 2", "Over/Under Team 2", "Full Time",
          canonical_market_key="team_total_away_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("team_total_away_bare_group", "Over/Under Team 2", "Over/Under Team 2", "Full Time",
          canonical_market_key="team_total_away_full_time",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("team_total_home_including_overtime",
          "Over under team 1 (incl. overtime)", "Over/Under Team 1", "Full-time",
          canonical_market_key="team_total_home_full_time_including_overtime",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("team_total_home_including_overtime_period",
          "Over/Under Team 1", "Over/Under Team 1", "Full Time Including Overtime",
          canonical_market_key="team_total_home_full_time_including_overtime",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("team_total_away_including_overtime",
          "Over under team 2 (incl. overtime)", "Over/Under Team 2", "Full-time",
          canonical_market_key="team_total_away_full_time_including_overtime",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    _rule("team_total_away_including_overtime_period",
          "Over/Under Team 2", "Over/Under Team 2", "Full Time Including Overtime",
          canonical_market_key="team_total_away_full_time_including_overtime",
          expected_choice_pattern=TOTAL_PATTERN, expected_choice_names=("Over", "Under")),
    # --- Asian Handicap Full Time ---
    _rule("point_spread_match", "Point spread", "Point spread", "Match",
          canonical_market_key="asian_handicap_full_time",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    _rule("asian_handicap_group_case", "Asian handicap", "Asian Handicap", "Full-time",
          canonical_market_key="asian_handicap_full_time",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    _rule("asian_handicap_title_full_time", "Asian Handicap", "Asian Handicap", "Full Time",
          canonical_market_key="asian_handicap_full_time",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    _rule("asian_handicap_title_full_time_hyphen", "Asian Handicap", "Asian Handicap", "Full-time",
          canonical_market_key="asian_handicap_full_time",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    _rule("asian_handicap_lower_full_time_spaced", "Asian handicap", "Asian Handicap", "Full Time",
          canonical_market_key="asian_handicap_full_time",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    # --- Asian Handicap 1st Half ---
    _rule("asian_handicap_1st_half_compound", "Asian Handicap 1st half", "Asian Handicap", "1st Half",
          canonical_market_key="asian_handicap_1st_half",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    _rule("first_half_asian_handicap_compound", "1st half Asian Handicap", "Asian Handicap", "1st Half",
          canonical_market_key="asian_handicap_1st_half",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    _rule("asian_handicap_bare_1st_half", "Asian Handicap", "Asian Handicap", "1st Half",
          canonical_market_key="asian_handicap_1st_half",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    _rule("asian_handicap_lower_1st_half", "Asian handicap", "Asian Handicap", "1st Half",
          canonical_market_key="asian_handicap_1st_half",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    # --- Asian Handicap Full Time Including Overtime ---
    _rule("asian_handicap_including_overtime_period",
          "Asian Handicap", "Asian Handicap", "Full Time Including Overtime",
          canonical_market_key="asian_handicap_full_time_including_overtime",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    _rule("asian_handicap_incl_overtime_name",
          "Asian handicap (incl. overtime)", "Asian Handicap", "Full-time",
          canonical_market_key="asian_handicap_full_time_including_overtime",
          expected_choice_pattern=HANDICAP_PATTERN, expected_choice_names=("1", "2")),
    # --- European Handicap Full Time ---
    _rule("european_handicap_full_time", "European handicap", "European Handicap", "Full-time",
          canonical_market_key="european_handicap_full_time",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    _rule("european_handicap_title_full_time", "European Handicap", "European Handicap", "Full Time",
          canonical_market_key="european_handicap_full_time",
          expected_choice_pattern=SIDE_3_WAY_PATTERN, expected_choice_names=("1", "X", "2")),
    # --- Draw No Bet / Double Chance / BTTS ---
    _rule("draw_no_bet_full_time", "Draw no bet", "Draw no bet", "Full-time",
          canonical_market_key="draw_no_bet_full_time",
          expected_choice_pattern=SIDE_2_WAY_PATTERN, expected_choice_names=("1", "2")),
    _rule("double_chance_full_time", "Double chance", "Double chance", "Full-time",
          canonical_market_key="double_chance_full_time",
          expected_choice_pattern=DOUBLE_CHANCE_PATTERN, expected_choice_names=("12", "1X", "X2")),
    _rule("both_teams_to_score_full_time", "Both teams to score", "Both teams to score", "Full-time",
          canonical_market_key="both_teams_to_score_full_time",
          expected_choice_pattern=DECISION_PATTERN, expected_choice_names=("yes", "no")),
    _rule("both_teams_to_score_including_overtime",
          "Both teams to score (incl. overtime and penalties)", "Both teams to score", "Full-time",
          canonical_market_key="both_teams_to_score_full_time_including_overtime",
          expected_choice_pattern=DECISION_PATTERN, expected_choice_names=("yes", "no")),
    _rule("both_teams_to_score_including_overtime_full_time_period",
          "Both teams to score (incl. overtime and penalties)", "Both Teams To Score", "Full Time",
          canonical_market_key="both_teams_to_score_full_time_including_overtime",
          expected_choice_pattern=DECISION_PATTERN, expected_choice_names=("yes", "no")),
    # --- Goal-team markets ---
    _rule("first_goal_full_time", "First goal", "First goal", "Full-time",
          canonical_market_key="first_goal_full_time",
          expected_choice_pattern=GOAL_TEAM_PATTERN, expected_choice_names=("1", "2", "no_goal")),
    _rule("last_goal_full_time", "Last goal", "Last goal", "Full-time",
          canonical_market_key="last_goal_full_time",
          expected_choice_pattern=GOAL_TEAM_PATTERN, expected_choice_names=("1", "2", "no_goal")),
    _rule("first_team_to_score_full_time", "First team to score", "First team to score", "Full-time",
          canonical_market_key="first_team_to_score_full_time",
          expected_choice_pattern=GOAL_TEAM_PATTERN, expected_choice_names=("1", "2", "no_goal")),
    _rule("next_goal_full_time", "Next goal", "Next goal", "Full-time",
          canonical_market_key="next_goal_full_time",
          expected_choice_pattern=GOAL_TEAM_PATTERN, expected_choice_names=("1", "2", "no_goal")),
    _rule("tie_break_in_match_extra_time", "Tie break in match", "Tie break in match", "Extra time",
          canonical_market_key="tie_break_in_match_extra_time",
          expected_choice_pattern=DECISION_PATTERN, expected_choice_names=("yes", "no")),
)


def validate_rule_definitions(
    rules: Sequence[CanonicalizationRule],
) -> tuple[CanonicalizationRule, ...]:
    """Validate and normalize an explicit rule collection without touching the DB."""
    normalized = tuple(rules)
    if not normalized:
        raise ValueError("At least one canonicalization rule is required")

    ids: set[str] = set()
    sources: set[tuple[str, str, str]] = set()
    target_keys: set[str] = set()
    for rule in normalized:
        values = asdict(rule)
        blank_fields = [
            key for key, value in values.items()
            if isinstance(value, str) and not value.strip()
        ]
        if blank_fields:
            raise ValueError(f"Rule {rule.rule_id!r} has blank fields: {blank_fields}")
        if rule.rule_id in ids:
            raise ValueError(f"Duplicate rule_id: {rule.rule_id}")
        source = (
            rule.source_market_name,
            rule.source_market_group,
            rule.source_market_period,
        )
        if source in sources:
            raise ValueError(f"Duplicate source market shape: {rule.source_shape}")
        if not rule.expected_choice_names:
            raise ValueError(f"Rule {rule.rule_id!r} is missing expected_choice_names")

        seed = CANONICAL_MARKET_TYPE_SEEDS.get(rule.canonical_market_key)
        if seed is None:
            raise ValueError(
                f"Rule {rule.rule_id!r} has unknown canonical_market_key="
                f"{rule.canonical_market_key!r}"
            )
        expected_target = (
            seed["canonical_market_name"],
            seed["canonical_market_group"],
            seed["canonical_market_period"],
        )
        actual_target = (
            rule.target_market_name,
            rule.target_market_group,
            rule.target_market_period,
        )
        if actual_target != expected_target:
            raise ValueError(
                f"Rule {rule.rule_id!r} target {actual_target} does not match "
                f"CANONICAL_MARKET_TYPE_SEEDS[{rule.canonical_market_key!r}]="
                f"{expected_target}"
            )
        if bool(rule.requires_choice_group) != bool(seed["requires_choice_group"]):
            raise ValueError(
                f"Rule {rule.rule_id!r} requires_choice_group="
                f"{rule.requires_choice_group} does not match catalog "
                f"{seed['requires_choice_group']}"
            )
        if rule.market_family != seed["market_family"]:
            raise ValueError(
                f"Rule {rule.rule_id!r} market_family={rule.market_family!r} "
                f"does not match catalog {seed['market_family']!r}"
            )
        if bool(rule.trajectory_required) != bool(seed["enabled_for_trajectory"]):
            raise ValueError(
                f"Rule {rule.rule_id!r} trajectory_required="
                f"{rule.trajectory_required} does not match catalog "
                f"enabled_for_trajectory={seed['enabled_for_trajectory']}"
            )

        ids.add(rule.rule_id)
        sources.add(source)
        target_keys.add(rule.canonical_market_key)

    missing_keys = sorted(set(CANONICAL_MARKET_TYPE_SEEDS) - target_keys)
    if missing_keys:
        raise ValueError(
            "RULES are missing target coverage for canonical keys: "
            + ", ".join(missing_keys)
        )
    return normalized


def detect_projected_conflicts(rows: Iterable[Mapping]) -> list[dict]:
    """Pure equivalent of the DB conflict grouping, useful for focused tests.

    The projection mirrors the SQL conflict check by treating ``NULL`` choice
    groups like an empty identity value.
    """
    grouped: dict[tuple, list[Mapping]] = defaultdict(list)
    for row in rows:
        key = (
            row["event_id"], row["bookie_id"], row["market_name"],
            row["market_period"], row.get("choice_group") or "", row["is_live"],
        )
        grouped[key].append(row)

    conflicts = []
    for key, grouped_rows in grouped.items():
        if len(grouped_rows) < 2:
            continue
        conflicts.append(
            {
                "event_id": key[0],
                "bookie_id": key[1],
                "projected_market_name": key[2],
                "projected_market_period": key[3],
                "choice_group": key[4],
                "is_live": key[5],
                "market_ids": sorted(row["market_id"] for row in grouped_rows),
            }
        )
    return sorted(
        conflicts,
        key=lambda item: (item["event_id"], item["projected_market_name"], item["choice_group"]),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Canonicalize historical SofaScore market metadata (dry-run by default)."
    )
    bookie = parser.add_mutually_exclusive_group()
    bookie.add_argument(
        "--bookie-id",
        type=int,
        default=None,
        help="Only markets for this bookie_id (default: 1)",
    )
    bookie.add_argument(
        "--exclude-bookie-id",
        type=int,
        default=None,
        help="All markets except this bookie_id (e.g. --exclude-bookie-id 1 for non-SofaScore)",
    )
    parser.add_argument("--sport", help="Limit candidates to an exact events.sport value")
    parser.add_argument("--event-id", type=int, help="Limit candidates to one event")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Canonical event_id range processed per batch (default: 1000)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Maximum number of active event_id batches to process per run (default: all)",
    )
    parser.add_argument(
        "--run-migrations",
        action="store_true",
        help="Run db_manager.check_and_migrate_schema() before the lightweight preflight",
    )
    parser.add_argument(
        "--include-risky",
        action="store_true",
        help="Include line markets whose choice_group is NULL",
    )
    parser.add_argument("--markets-analysis-csv", type=Path)
    parser.add_argument("--canonical-targets-csv", type=Path)
    parser.add_argument(
        "--output-json",
        action="store_true",
        help="Print a machine-readable JSON summary after the readable report",
    )
    parser.add_argument(
        "--conflict-limit",
        type=int,
        default=100,
        help="Maximum projected conflicts to display per batch (default: 100)",
    )
    parser.add_argument(
        "--print-index-recommendations",
        action="store_true",
        help="Print a suggested supporting index after the report",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Analyze without updates (default)")
    mode.add_argument("--commit", action="store_true", help="Apply all eligible updates atomically")
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    args = build_parser().parse_args(argv)
    if args.exclude_bookie_id is not None:
        args.bookie_id = args.exclude_bookie_id
        args.exclude_bookie = True
    else:
        if args.bookie_id is None:
            args.bookie_id = 1
        args.exclude_bookie = False
    return args


def build_rules_values_sql(rules: Sequence[CanonicalizationRule]) -> tuple[str, dict]:
    rows = []
    params: dict[str, object] = {}
    for index, rule in enumerate(rules):
        columns = (
            "rule_id", "source_market_name", "source_market_group", "source_market_period",
            "target_market_name", "target_market_group", "target_market_period",
            "canonical_market_key", "requires_choice_group", "expected_choice_pattern", "safe",
        )
        placeholders = []
        values = (
            rule.rule_id, rule.source_market_name, rule.source_market_group,
            rule.source_market_period, rule.target_market_name, rule.target_market_group,
            rule.target_market_period, rule.canonical_market_key,
            rule.requires_choice_group, rule.expected_choice_pattern, rule.safe,
        )
        for column, value in zip(columns, values):
            name = f"r{index}_{column}"
            placeholders.append(f":{name}")
            params[name] = value
        rows.append("(" + ", ".join(placeholders) + ")")
    columns_sql = ", ".join(
        (
            "rule_id", "source_market_name", "source_market_group", "source_market_period",
            "target_market_name", "target_market_group", "target_market_period",
            "canonical_market_key", "requires_choice_group", "expected_choice_pattern", "safe",
        )
    )
    return f"rules ({columns_sql}) AS (VALUES\n        " + ",\n        ".join(rows) + ")", params


_rule_values_sql = build_rules_values_sql


def _scope_sql() -> str:
    return """
        (
            (
                NOT CAST(:exclude_bookie AS BOOLEAN)
                AND m.bookie_id = CAST(:bookie_id AS BIGINT)
            )
            OR (
                CAST(:exclude_bookie AS BOOLEAN)
                AND m.bookie_id <> CAST(:bookie_id AS BIGINT)
            )
        )
        AND (CAST(:sport AS TEXT) IS NULL OR e.sport = CAST(:sport AS TEXT))
        AND (CAST(:event_id AS BIGINT) IS NULL OR m.event_id = CAST(:event_id AS BIGINT))
        AND (CAST(:batch_start AS BIGINT) IS NULL OR m.event_id >= CAST(:batch_start AS BIGINT))
        AND (CAST(:batch_end AS BIGINT) IS NULL OR m.event_id <= CAST(:batch_end AS BIGINT))
    """


def build_candidate_ctes(rules: Sequence[CanonicalizationRule]) -> tuple[str, dict]:
    """Return the reusable explicit-rules/candidates/eligibility SQL fragment."""
    rules_sql, params = _rule_values_sql(rules)
    ctes = f"""
    WITH {rules_sql},
    candidates AS (
        SELECT
            r.*,
            m.market_id,
            m.event_id,
            m.bookie_id,
            m.market_name,
            m.market_group,
            m.market_period,
            m.choice_group,
            m.is_live,
            e.sport,
            (
                m.market_name = r.target_market_name
                AND m.market_group = r.target_market_group
                AND m.market_period = r.target_market_period
            ) AS already_in_target
        FROM markets m
        JOIN events e ON e.id = m.event_id
        JOIN rules r
          ON m.market_name = r.source_market_name
         AND m.market_group = r.source_market_group
         AND m.market_period = r.source_market_period
        WHERE {_scope_sql()}
    ),
    target_markets AS (
        SELECT r.rule_id, m.market_id
        FROM markets m
        JOIN events e ON e.id = m.event_id
        JOIN rules r
          ON m.market_name = r.target_market_name
         AND m.market_group = r.target_market_group
         AND m.market_period = r.target_market_period
        WHERE {_scope_sql()}
    ),
    candidate_updates AS (
        SELECT *
        FROM candidates
        WHERE NOT already_in_target
          AND safe
          AND (:include_risky OR NOT requires_choice_group OR choice_group IS NOT NULL)
    )
    """
    return ctes, params


def _query_params(args: argparse.Namespace, rule_params: dict) -> dict:
    return {
        **rule_params,
        "bookie_id": args.bookie_id,
        "exclude_bookie": bool(args.exclude_bookie),
        "sport": args.sport,
        "event_id": args.event_id,
        "include_risky": args.include_risky,
        "conflict_limit": args.conflict_limit,
        "batch_start": None,
        "batch_end": None,
    }


def _bookie_scope_label(args: argparse.Namespace) -> str:
    if args.exclude_bookie:
        return f"!={args.bookie_id}"
    return str(args.bookie_id)


def _load_csv_summary(path: Path | None, label: str) -> dict | None:
    if path is None:
        return None
    resolved = path.expanduser().resolve()
    with resolved.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        columns = reader.fieldnames or []
    sports = sorted({row.get("sport", "").strip() for row in rows if row.get("sport", "").strip()})
    market_shapes = set()
    for row in rows:
        market_name = row.get("market_name") or row.get("canonical_market_name")
        market_group = row.get("market_group") or row.get("canonical_market_group")
        market_period = row.get("market_period") or row.get("canonical_market_period")
        if market_name:
            market_shapes.add((market_name, market_group, market_period))
    markets_count_sum = sum(
        int(row.get("markets_count") or 0)
        for row in rows
        if str(row.get("markets_count") or "").strip()
    )
    top_market_shapes = sorted(
        (
            (
                int(row.get("markets_count") or 0),
                row.get("sport", "").strip(),
                row.get("market_name") or row.get("canonical_market_name") or "",
                row.get("market_group") or row.get("canonical_market_group") or "",
                row.get("market_period") or row.get("canonical_market_period") or "",
            )
            for row in rows
            if str(row.get("markets_count") or "").strip()
        ),
        reverse=True,
    )[:10]
    canonical_keys = {
        row["canonical_market_key"].strip()
        for row in rows
        if row.get("canonical_market_key", "").strip()
    }
    return {
        "label": label,
        "path": str(resolved),
        "rows": len(rows),
        "columns": columns,
        "sports": sports,
        "distinct_market_shapes": len(market_shapes),
        "distinct_canonical_market_keys": len(canonical_keys),
        "markets_count_sum": markets_count_sum,
        "top_market_shapes": top_market_shapes,
    }


def validate_required_schema(connection) -> dict:
    """Validate only the tables and columns this script needs."""
    inspector = inspect(connection)
    required_columns = {
        "markets": {
            "market_id",
            "event_id",
            "bookie_id",
            "market_name",
            "market_group",
            "market_period",
            "choice_group",
            "is_live",
        },
        "events": {
            "id",
            "sport",
        },
        "canonical_market_types": {
            "canonical_market_key",
            "canonical_market_name",
            "canonical_market_group",
            "canonical_market_period",
            "requires_choice_group",
            "enabled_for_trajectory",
        },
        # LEGACY_PHASE6_BLOCKER: duplicate-choice merging still reassigns
        # snapshots by choice_id. The Phase 6 slim schema intentionally blocks
        # this historical maintenance command until its merge logic is ported
        # to quote lineage or removed in Phase 8.
        "market_choice_snapshots": {"snapshot_id", "choice_id"},
    }

    errors: list[str] = []
    for table_name, column_names in required_columns.items():
        if not inspector.has_table(table_name):
            errors.append(f"missing table {table_name}")
            continue
        existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
        missing_columns = sorted(column_names - existing_columns)
        if missing_columns:
            errors.append(
                f"{table_name} missing columns: {', '.join(missing_columns)}"
            )

    if errors:
        raise RuntimeError("Required schema validation failed:\n- " + "\n- ".join(errors))

    return {
        "tables": sorted(required_columns),
        "columns": {table: sorted(columns) for table, columns in required_columns.items()},
    }


def _preflight(connection, rules: Sequence[CanonicalizationRule]) -> list[dict]:
    keys = sorted({rule.canonical_market_key for rule in rules})
    placeholders = ", ".join(f":key_{index}" for index, _ in enumerate(keys))
    rows = connection.execute(
        text(
            "SELECT canonical_market_key, canonical_market_name, canonical_market_group, "
            "canonical_market_period, requires_choice_group, enabled_for_trajectory "
            f"FROM canonical_market_types WHERE canonical_market_key IN ({placeholders})"
        ),
        {f"key_{index}": key for index, key in enumerate(keys)},
    ).mappings().all()
    by_key = {row["canonical_market_key"]: row for row in rows}
    errors = []
    for rule in rules:
        target = by_key.get(rule.canonical_market_key)
        if target is None:
            errors.append(f"missing canonical_market_key={rule.canonical_market_key}")
            continue
        expected_shape = (
            rule.target_market_name, rule.target_market_group, rule.target_market_period,
        )
        actual_shape = (
            target["canonical_market_name"], target["canonical_market_group"],
            target["canonical_market_period"],
        )
        if actual_shape != expected_shape:
            errors.append(
                f"{rule.rule_id}: target shape mismatch; expected={expected_shape!r} "
                f"database={actual_shape!r}"
            )
        if bool(target["requires_choice_group"]) != rule.requires_choice_group:
            errors.append(
                f"{rule.rule_id}: requires_choice_group expected="
                f"{rule.requires_choice_group} database={target['requires_choice_group']}"
            )
        if rule.trajectory_required and not bool(target["enabled_for_trajectory"]):
            errors.append(
                f"{rule.rule_id}: canonical target is not enabled_for_trajectory"
            )
    if errors:
        raise RuntimeError("Canonical target preflight failed:\n- " + "\n- ".join(errors))
    return [dict(row) for row in rows]


def _analyze(connection, rules: Sequence[CanonicalizationRule], params: dict) -> dict:
    ctes, _ = build_candidate_ctes(rules)
    breakdown_rows = connection.execute(
        text(
            ctes
            + """
            SELECT
                r.rule_id,
                r.canonical_market_key,
                r.requires_choice_group,
                COUNT(c.market_id) AS candidate_markets,
                COUNT(DISTINCT c.event_id) AS events,
                COUNT(DISTINCT c.sport) AS sports,
                ARRAY_AGG(DISTINCT c.sport ORDER BY c.sport)
                    FILTER (WHERE c.sport IS NOT NULL) AS sport_names,
                0 AS already_in_target,
                COUNT(c.market_id) FILTER (WHERE NOT c.already_in_target) AS requires_update,
                COUNT(c.market_id) FILTER (WHERE c.choice_group IS NULL) AS choice_group_null,
                COUNT(cu.market_id) AS eligible_markets,
                COUNT(c.market_id) FILTER (
                    WHERE NOT c.already_in_target
                      AND c.requires_choice_group
                      AND c.choice_group IS NULL
                ) AS risky_markets,
                COUNT(c.market_id) FILTER (
                    WHERE NOT c.already_in_target
                      AND c.requires_choice_group
                      AND c.choice_group IS NULL
                      AND NOT :include_risky
                ) AS skipped_missing_choice_group,
                0 AS markets_with_unexpected_choice_names
            FROM rules r
            LEFT JOIN candidates c ON c.rule_id = r.rule_id
            LEFT JOIN candidate_updates cu ON cu.market_id = c.market_id
            GROUP BY r.rule_id, r.canonical_market_key, r.requires_choice_group
            ORDER BY r.rule_id
            """
        ),
        params,
    ).mappings().all()

    unexpected_rows = connection.execute(
        text(
            ctes
            + """
            , unexpected_pairs AS (
                SELECT DISTINCT c.rule_id, c.market_id, mc.choice_name
                FROM candidates c
                JOIN market_choices mc ON mc.market_id = c.market_id
                WHERE NOT (mc.choice_name ~ c.expected_choice_pattern)
            ),
            unexpected_counts AS (
                SELECT rule_id, choice_name, COUNT(*) AS markets
                FROM unexpected_pairs
                GROUP BY rule_id, choice_name
            ),
            unexpected_totals AS (
                SELECT rule_id, COUNT(DISTINCT market_id) AS markets_with_unexpected
                FROM unexpected_pairs
                GROUP BY rule_id
            ),
            ranked AS (
                SELECT
                    uc.*,
                    ut.markets_with_unexpected,
                    ROW_NUMBER() OVER (
                        PARTITION BY uc.rule_id ORDER BY uc.markets DESC, uc.choice_name
                    ) AS sample_rank
                FROM unexpected_counts uc
                JOIN unexpected_totals ut ON ut.rule_id = uc.rule_id
            )
            SELECT rule_id, choice_name, markets, markets_with_unexpected
            FROM ranked
            WHERE sample_rank <= 10
            ORDER BY rule_id, sample_rank
            """
        ),
        params,
    ).mappings().all()

    sport_rows = connection.execute(
        text(
            ctes
            + """
            SELECT
                c.sport,
                COUNT(*) AS candidate_markets,
                COUNT(DISTINCT c.event_id) AS events,
                COUNT(cu.market_id) AS eligible_markets,
                COUNT(*) FILTER (
                    WHERE NOT c.already_in_target
                      AND c.requires_choice_group
                      AND c.choice_group IS NULL
                      AND NOT :include_risky
                ) AS skipped_missing_choice_group
            FROM candidates c
            LEFT JOIN candidate_updates cu ON cu.market_id = c.market_id
            GROUP BY c.sport
            ORDER BY c.sport
            """
        ),
        params,
    ).mappings().all()

    unexpected_by_rule: dict[str, list[dict]] = defaultdict(list)
    unexpected_totals: dict[str, int] = {}
    for row in unexpected_rows:
        unexpected_by_rule[row["rule_id"]].append(
            {"choice_name": row["choice_name"], "markets": row["markets"]}
        )
        unexpected_totals[row["rule_id"]] = row["markets_with_unexpected"]
    breakdown = []
    for row in breakdown_rows:
        item = dict(row)
        item["markets_with_unexpected_choice_names"] = unexpected_totals.get(
            item["rule_id"], 0
        )
        item["sample_unexpected_choice_names"] = unexpected_by_rule.get(item["rule_id"], [])[:10]
        item["markets_missing_choice_group_for_required_line"] = (
            item["choice_group_null"] if item["requires_choice_group"] else 0
        )
        breakdown.append(item)
    return {
        "rules": breakdown,
        "by_sport": [dict(row) for row in sport_rows],
    }


def _fetch_active_event_ids(
    connection, rules: Sequence[CanonicalizationRule], params: dict
) -> list[int]:
    """Fetch distinct event_ids for candidate markets that actually require metadata updates."""
    ctes, _ = build_candidate_ctes(rules)
    rows = connection.execute(
        text(
            ctes
            + """
            SELECT DISTINCT event_id
            FROM candidate_updates
            ORDER BY event_id
            """
        ),
        params,
    ).scalars().all()
    return [int(row) for row in rows]


def _target_counts(connection, rules, params: dict) -> dict[str, int]:
    """Count already-canonical rows once instead of repeating it in every batch."""
    ctes, _ = build_candidate_ctes(rules)
    rows = connection.execute(
        text(
            ctes
            + """
            SELECT rule_id, COUNT(*) AS markets
            FROM target_markets
            GROUP BY rule_id
            """
        ),
        params,
    ).mappings().all()
    return {row["rule_id"]: row["markets"] for row in rows}


def _create_analysis_accumulator() -> dict:
    return {
        "rules_by_id": {},
        "sport_names_by_rule": defaultdict(set),
        "unexpected_by_rule": defaultdict(lambda: defaultdict(int)),
        "sports": {},
    }


def _accumulate_batch_analysis(state: dict, batch_analysis: dict) -> None:
    rules_by_id = state["rules_by_id"]
    sport_names_by_rule = state["sport_names_by_rule"]
    unexpected_by_rule = state["unexpected_by_rule"]
    sports = state["sports"]
    additive_rule_fields = (
        "candidate_markets",
        "events",
        "requires_update",
        "choice_group_null",
        "eligible_markets",
        "risky_markets",
        "skipped_missing_choice_group",
        "markets_with_unexpected_choice_names",
    )

    for row in batch_analysis["rules"]:
        rule_id = row["rule_id"]
        if rule_id not in rules_by_id:
            rules_by_id[rule_id] = {
                **row,
                **{field: 0 for field in additive_rule_fields},
                "already_in_target": 0,
                "sample_unexpected_choice_names": [],
            }
        merged = rules_by_id[rule_id]
        for field in additive_rule_fields:
            merged[field] += row[field]
        sport_names_by_rule[rule_id].update(row.get("sport_names") or [])
        for sample in row.get("sample_unexpected_choice_names") or []:
            unexpected_by_rule[rule_id][sample["choice_name"]] += sample["markets"]

    for row in batch_analysis["by_sport"]:
        sport = row["sport"]
        merged_sport = sports.setdefault(
            sport,
            {
                "sport": sport,
                "candidate_markets": 0,
                "events": 0,
                "eligible_markets": 0,
                "skipped_missing_choice_group": 0,
            },
        )
        for field in (
            "candidate_markets", "events", "eligible_markets",
            "skipped_missing_choice_group",
        ):
            merged_sport[field] += row[field]


def _finalize_accumulated_analysis(state: dict, target_counts: dict[str, int]) -> dict:
    rules_by_id = state["rules_by_id"]
    sport_names_by_rule = state["sport_names_by_rule"]
    unexpected_by_rule = state["unexpected_by_rule"]
    sports = state["sports"]

    for rule_id, row in rules_by_id.items():
        row["already_in_target"] = target_counts.get(rule_id, 0)
        row["sport_names"] = sorted(sport_names_by_rule[rule_id])
        row["sports"] = len(sport_names_by_rule[rule_id])
        row["markets_missing_choice_group_for_required_line"] = (
            row["choice_group_null"] if row["requires_choice_group"] else 0
        )
        ranked_samples = sorted(
            unexpected_by_rule[rule_id].items(), key=lambda item: (-item[1], item[0])
        )[:10]
        row["sample_unexpected_choice_names"] = [
            {"choice_name": choice_name, "markets": markets}
            for choice_name, markets in ranked_samples
        ]

    return {
        "rules": [rules_by_id[rule_id] for rule_id in sorted(rules_by_id)],
        "by_sport": [
            sports[sport] for sport in sorted(sports, key=lambda value: value or "")
        ],
    }


def _merge_batch_analyses(batch_analyses: Sequence[dict]) -> dict:
    """Merge disjoint event-range analyses without losing distinct sport counts."""
    state = _create_analysis_accumulator()
    for batch_analysis in batch_analyses:
        _accumulate_batch_analysis(state, batch_analysis)
    return _finalize_accumulated_analysis(state, target_counts={})


def _analyze_in_event_batches(
    connection,
    rules: Sequence[CanonicalizationRule],
    params: dict,
    batch_size: int,
    max_batches: int | None = None,
) -> tuple[dict, dict, list[tuple[int, int]], dict]:
    """Analyze and conflict-check disjoint active event-id ranges."""
    bounds_params = {**params, "batch_start": None, "batch_end": None}
    active_event_ids = _fetch_active_event_ids(connection, rules, bounds_params)
    target_counts = _target_counts(connection, rules, bounds_params)

    if not active_event_ids:
        empty_params = {**params, "batch_start": 1, "batch_end": 0}
        state = _create_analysis_accumulator()
        _accumulate_batch_analysis(state, _analyze(connection, rules, empty_params))
        merged = _finalize_accumulated_analysis(state, target_counts)
        bounds = {
            "min_event_id": None,
            "max_event_id": None,
            "candidate_events": 0,
            "active_events_needing_update": 0,
        }
        print(
            "Candidate scan: active_events=0 batch_size="
            f"{batch_size}",
            flush=True,
        )
        return merged, {"total_conflicts": 0, "rows": []}, [], bounds

    min_event_id = active_event_ids[0]
    max_event_id = active_event_ids[-1]
    total_active_events = len(active_event_ids)

    chunks = [
        active_event_ids[i : i + batch_size]
        for i in range(0, total_active_events, batch_size)
    ]
    if max_batches is not None and max_batches > 0:
        chunks = chunks[:max_batches]

    ranges = [(chunk[0], chunk[-1]) for chunk in chunks]

    print(
        "Candidate scan: "
        f"active_events_needing_update={total_active_events} "
        f"active_event_id_range={min_event_id}..{max_event_id} "
        f"batch_size={batch_size} batches_to_run={len(ranges)}",
        flush=True,
    )

    acc_state = _create_analysis_accumulator()
    conflicts = []
    total_conflicts = 0
    processed_events = 0
    processed_markets = 0
    conflict_limit = params.get("conflict_limit", 100)

    for batch_number, (batch_start, batch_end) in enumerate(ranges, start=1):
        batch_params = {**params, "batch_start": batch_start, "batch_end": batch_end}
        logger.info(
            "Processing canonical market batch %s/%s: event_id_range=%s..%s",
            batch_number,
            len(ranges),
            batch_start,
            batch_end,
        )
        batch_analysis = _analyze(connection, rules, batch_params)
        batch_conflicts = _find_conflicts(connection, rules, batch_params)

        _accumulate_batch_analysis(acc_state, batch_analysis)

        if len(conflicts) < conflict_limit:
            remaining_slots = conflict_limit - len(conflicts)
            conflicts.extend(batch_conflicts["rows"][:remaining_slots])
        total_conflicts += batch_conflicts["total_conflicts"]

        # Every event has one sport, so summing distinct events by sport stays exact.
        batch_event_count = sum(row["events"] for row in batch_analysis["by_sport"])
        batch_market_count = sum(row["candidate_markets"] for row in batch_analysis["rules"])
        processed_events += batch_event_count
        processed_markets += batch_market_count
        logger.info(
            "Completed canonical market batch %s/%s: events=%s markets=%s "
            "total_events_processed=%s remaining_events=%s total_markets_processed=%s",
            batch_number,
            len(ranges),
            batch_event_count,
            batch_market_count,
            processed_events,
            max(total_active_events - processed_events, 0),
            processed_markets,
        )

    merged = _finalize_accumulated_analysis(acc_state, target_counts)
    bounds = {
        "min_event_id": min_event_id,
        "max_event_id": max_event_id,
        "candidate_events": total_active_events,
        "active_events_needing_update": total_active_events,
    }
    return merged, {"total_conflicts": total_conflicts, "rows": conflicts}, ranges, bounds


def _find_conflicts(connection, rules: Sequence[CanonicalizationRule], params: dict) -> dict:
    ctes, _ = build_candidate_ctes(rules)
    rows = connection.execute(
        text(
            ctes
            + f"""
            , scoped_markets AS (
                SELECT m.*
                FROM markets m
                JOIN events e ON e.id = m.event_id
                WHERE {_scope_sql()}
            ),
            projected AS (
                SELECT
                    m.market_id,
                    m.event_id,
                    m.bookie_id,
                    COALESCE(cu.target_market_name, m.market_name) AS market_name,
                    COALESCE(cu.target_market_period, m.market_period) AS market_period,
                    COALESCE(m.choice_group, '') AS choice_group,
                    m.is_live
                FROM scoped_markets m
                LEFT JOIN candidate_updates cu ON cu.market_id = m.market_id
            )
            SELECT
                event_id,
                bookie_id,
                market_name AS projected_market_name,
                market_period AS projected_market_period,
                choice_group,
                is_live,
                ARRAY_AGG(market_id ORDER BY market_id) AS market_ids,
                COUNT(*) OVER () AS total_conflicts
            FROM projected
            GROUP BY event_id, bookie_id, market_name, market_period, choice_group, is_live
            HAVING COUNT(*) > 1
            ORDER BY event_id, market_name, market_period, choice_group
            LIMIT :conflict_limit
            """
        ),
        params,
    ).mappings().all()
    total_conflicts = int(rows[0]["total_conflicts"]) if rows else 0
    return {
        "total_conflicts": total_conflicts,
        "rows": [dict(row) for row in rows],
    }


def _merge_duplicate_markets(connection, keeper_id: int, duplicate_id: int) -> None:
    # 1. Find all choices for duplicate market
    dup_choices = connection.execute(
        text("SELECT choice_id, choice_name, initial_odds, current_odds, change FROM market_choices WHERE market_id = :dup_id"),
        {"dup_id": duplicate_id}
    ).mappings().all()
    
    for dup_choice in dup_choices:
        # Check if choice_name exists in keeper
        keeper_choice = connection.execute(
            text("SELECT choice_id FROM market_choices WHERE market_id = :keeper_id AND LOWER(choice_name) = LOWER(:name)"),
            {"keeper_id": keeper_id, "name": dup_choice["choice_name"]}
        ).mappings().one_or_none()
        
        if not keeper_choice:
            # Reassign choice to keeper
            connection.execute(
                text("UPDATE market_choices SET market_id = :keeper_id WHERE choice_id = :choice_id"),
                {"keeper_id": keeper_id, "choice_id": dup_choice["choice_id"]}
            )
        else:
            keeper_choice_id = keeper_choice["choice_id"]
            # Reassign snapshots to keeper choice
            connection.execute(
                text("UPDATE market_choice_snapshots SET choice_id = :keeper_choice_id WHERE choice_id = :choice_id"),
                {"keeper_choice_id": keeper_choice_id, "choice_id": dup_choice["choice_id"]}
            )
            # Update keeper choice odds if they are NULL
            connection.execute(
                text("""
                    UPDATE market_choices
                    SET initial_odds = COALESCE(initial_odds, :init_odds),
                        current_odds = COALESCE(:curr_odds, current_odds),
                        change = COALESCE(:change, change)
                    WHERE choice_id = :keeper_choice_id
                """),
                {
                    "init_odds": dup_choice["initial_odds"],
                    "curr_odds": dup_choice["current_odds"],
                    "change": dup_choice["change"],
                    "keeper_choice_id": keeper_choice_id
                }
            )
            # Delete duplicate choice
            connection.execute(
                text("DELETE FROM market_choices WHERE choice_id = :choice_id"),
                {"choice_id": dup_choice["choice_id"]}
            )
            
    # 2. Update keeper market collected_at to latest of both
    connection.execute(
        text("""
            UPDATE markets keeper
            SET collected_at = GREATEST(keeper.collected_at, duplicate.collected_at)
            FROM markets duplicate
            WHERE keeper.market_id = :keeper_id AND duplicate.market_id = :dup_id
        """),
        {"keeper_id": keeper_id, "dup_id": duplicate_id}
    )
    
    # 3. Delete duplicate market
    connection.execute(
        text("DELETE FROM markets WHERE market_id = :dup_id"),
        {"dup_id": duplicate_id}
    )


def _resolve_and_merge_conflicts(connection, conflicts: list[dict], rules: Sequence[CanonicalizationRule]) -> int:
    merged_count = 0
    for conflict in conflicts:
        market_ids = conflict["market_ids"]
        # Fetch details to determine keeper
        rows = connection.execute(
            text("SELECT market_id, market_name, market_period, collected_at FROM markets WHERE market_id IN :ids").bindparams(
                bindparam("ids", expanding=True)
            ),
            {"ids": list(market_ids)}
        ).mappings().all()
        
        # Sort rows: is_canonical first, then collected_at, then market_id
        sorted_rows = sorted(
            rows,
            key=lambda r: (
                1 if any(r["market_name"] == rule.target_market_name and r["market_period"] == rule.target_market_period for rule in rules) else 0,
                r["collected_at"].timestamp() if r["collected_at"] else 0,
                r["market_id"]
            ),
            reverse=True
        )
        keeper_id = sorted_rows[0]["market_id"]
        duplicate_ids = [r["market_id"] for r in sorted_rows[1:]]
        
        for dup_id in duplicate_ids:
            _merge_duplicate_markets(connection, keeper_id, dup_id)
            merged_count += 1
            
    return merged_count


def _apply_updates(connection, rules: Sequence[CanonicalizationRule], params: dict) -> dict[str, int]:
    ctes, _ = build_candidate_ctes(rules)
    rows = connection.execute(
        text(
            ctes
            + """
            UPDATE markets AS m
            SET market_name = cu.target_market_name,
                market_group = cu.target_market_group,
                market_period = cu.target_market_period
            FROM candidate_updates cu
            WHERE m.market_id = cu.market_id
            RETURNING cu.rule_id, m.market_id
            """
        ),
        params,
    ).mappings().all()
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["rule_id"]] += 1
    return dict(counts)


def _post_update_validation(connection, params: dict) -> dict:
    match_row = connection.execute(
        text(
            f"""
            SELECT
                COUNT(DISTINCT m.market_id) AS scoped_markets,
                COUNT(DISTINCT m.market_id) FILTER (
                    WHERE cmt.canonical_market_key IS NOT NULL
                ) AS canonical_matches,
                COUNT(DISTINCT m.market_id) FILTER (
                    WHERE cmt.canonical_market_key IS NOT NULL
                      AND cmt.enabled_for_trajectory
                ) AS enabled_for_trajectory
            FROM markets m
            JOIN events e ON e.id = m.event_id
            LEFT JOIN canonical_market_types cmt
              ON m.market_name = cmt.canonical_market_name
             AND m.market_group = cmt.canonical_market_group
             AND m.market_period = cmt.canonical_market_period
            WHERE {_scope_sql()}
            """
        ),
        params,
    ).mappings().one()
    unmatched = connection.execute(
        text(
            f"""
            SELECT m.market_name, m.market_group, m.market_period, COUNT(*) AS markets
            FROM markets m
            JOIN events e ON e.id = m.event_id
            LEFT JOIN canonical_market_types cmt
              ON m.market_name = cmt.canonical_market_name
             AND m.market_group = cmt.canonical_market_group
             AND m.market_period = cmt.canonical_market_period
            WHERE {_scope_sql()}
              AND cmt.canonical_market_key IS NULL
            GROUP BY m.market_name, m.market_group, m.market_period
            ORDER BY markets DESC, m.market_name, m.market_group, m.market_period
            """
        ),
        params,
    ).mappings().all()
    return {**dict(match_row), "unmatched_shapes": [dict(row) for row in unmatched]}


def _print_rules(rules: Sequence[CanonicalizationRule]) -> None:
    print("Loaded canonicalization rules:")
    for rule in rules:
        risk = "requires choice_group" if rule.requires_choice_group else "no line required"
        expected_choices = ", ".join(rule.expected_choice_names) if rule.expected_choice_names else "n/a"
        print(
            f"  - {rule.rule_id}: {rule.source_shape} -> {rule.target_shape} "
            f"[{rule.canonical_market_key}; family={rule.market_family}; "
            f"{risk}; expected={expected_choices}]"
        )


def _print_summary(summary: dict) -> None:
    print("\nSofaScore canonical market backfill")
    print(f"  mode: {summary['mode']}")
    print(f"  bookie_id: {summary['bookie_scope']}")
    print(f"  sport: {summary['sport'] or '(all)'}")
    print(f"  event_id: {summary['event_id'] or '(all)'}")
    print(f"  batch_size: {summary['batch_size']} event IDs")
    if summary.get("max_batches") is not None:
        print(f"  max_batches: {summary['max_batches']}")
    print(f"  run_migrations: {summary['run_migrations']}")
    print(f"  include_risky: {summary['include_risky']}")
    print(f"  conflict_limit: {summary['conflict_limit']}")
    print(f"  total candidate markets: {summary['total_candidate_markets']}")
    print(f"  total eligible markets: {summary['total_eligible_markets']}")
    print(
        "  total skipped because missing choice_group: "
        f"{summary['total_skipped_missing_choice_group']}"
    )
    print(f"  total risky markets included: {summary['total_risky_markets_included']}")
    print(
        f"  total conflicts: {summary['total_conflicts']} "
        f"(shown: {len(summary['conflicts'])})"
    )
    print(f"  total rows updated: {summary['total_rows_updated']}")

    print("\nBreakdown by rule:")
    for item in summary["breakdown_by_rule"]:
        print(
            f"  - {item['rule_id']}: candidates={item['candidate_markets']} "
            f"events={item['events']} sports={item['sports']} "
            f"already_target={item['already_in_target']} "
            f"needs_update={item['requires_update']} eligible={item['eligible_markets']} "
            f"risky={item['risky_markets']} "
            f"skipped_missing_line={item['skipped_missing_choice_group']} "
            f"unexpected_choices={item['markets_with_unexpected_choice_names']} "
            f"updated={item['rows_updated']}"
        )
        if item["sample_unexpected_choice_names"]:
            samples = ", ".join(
                f"{entry['choice_name']!r} ({entry['markets']})"
                for entry in item["sample_unexpected_choice_names"]
            )
            print(f"      unexpected samples: {samples}")

    print("\nBreakdown by sport:")
    if not summary["breakdown_by_sport"]:
        print("  (none)")
    for item in summary["breakdown_by_sport"]:
        print(
            f"  - {item['sport']}: candidates={item['candidate_markets']} "
            f"events={item['events']} "
            f"eligible={item['eligible_markets']} "
            f"skipped_missing_line={item['skipped_missing_choice_group']}"
        )

    if summary["conflicts"]:
        print("\nProjected unique-constraint conflicts:")
        for item in summary["conflicts"]:
            print(
                "  - event_id={event_id} bookie_id={bookie_id} "
                "market={projected_market_name!r} period={projected_market_period!r} "
                "choice_group={choice_group!r} is_live={is_live} market_ids={market_ids}".format(
                    **item
                )
            )

    for csv_summary in summary["csv_references"]:
        print(
            f"\nCSV reference {csv_summary['label']}: rows={csv_summary['rows']} "
            f"sports={len(csv_summary['sports'])} "
            f"market_shapes={csv_summary['distinct_market_shapes']} "
            f"canonical_keys={csv_summary['distinct_canonical_market_keys']} "
            f"path={csv_summary['path']}"
        )
        if csv_summary["markets_count_sum"]:
            print(f"  markets_count_sum={csv_summary['markets_count_sum']}")
        if csv_summary["top_market_shapes"]:
            print("  top market shapes:")
            for count, sport, market_name, market_group, market_period in csv_summary["top_market_shapes"]:
                print(
                    f"    - {count}: {sport or '(all)'} / {market_name} / "
                    f"{market_group} / {market_period}"
                )

    if summary.get("post_update_validation"):
        validation = summary["post_update_validation"]
        print("\nPost-update validation:")
        print(f"  scoped markets: {validation['scoped_markets']}")
        print(f"  canonical matches: {validation['canonical_matches']}")
        print(f"  enabled_for_trajectory: {validation['enabled_for_trajectory']}")
        print(f"  unmatched shapes: {len(validation['unmatched_shapes'])}")
        for row in validation["unmatched_shapes"]:
            print(
                f"    - {row['market_name']} / {row['market_group']} / "
                f"{row['market_period']}: {row['markets']}"
            )

    print("\nSuggested verification query:")
    print("SELECT *")
    print("FROM public.v_pre_start_odds_trajectory")
    print(f"WHERE event_id = {summary['event_id'] if summary['event_id'] is not None else '<event_id>'}")
    print("ORDER BY market_display_order, bookie_name, choice_group, choice_name, collected_at;")

    if summary.get("print_index_recommendations"):
        print("\nIndex recommendation:")
        print("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_markets_bookie_market_identity")
        print("ON markets (bookie_id, is_live, market_group, market_period, market_name, choice_group);")


def run(args: argparse.Namespace) -> tuple[int, dict | None]:
    rules = validate_rule_definitions(RULES)
    _print_rules(rules)

    if args.batch_size <= 0:
        print("ERROR: --batch-size must be greater than zero", file=sys.stderr)
        return 2, None

    csv_references = []
    try:
        for path, label in (
            (args.markets_analysis_csv, "markets_analysis"),
            (args.canonical_targets_csv, "canonical_targets"),
        ):
            csv_summary = _load_csv_summary(path, label)
            if csv_summary:
                csv_references.append(csv_summary)
    except (OSError, csv.Error) as exc:
        print(f"ERROR: could not read CSV reference: {exc}", file=sys.stderr)
        return 1, None

    ctes, rule_params = build_candidate_ctes(rules)
    del ctes
    params = _query_params(args, rule_params)
    mode = "commit" if args.commit else "dry-run"
    updated_by_rule: dict[str, int] = {}
    post_update_validation = None
    canonical_targets = []
    conflicts_report = {"total_conflicts": 0, "rows": []}

    try:
        if args.run_migrations:
            with progress_step("Database schema migrations (check_and_migrate_schema)"):
                schema_ready = db_manager.check_and_migrate_schema()
            if not schema_ready:
                print("ERROR: db_manager.check_and_migrate_schema() failed", file=sys.stderr)
                return 1, None

        if args.commit:
            with progress_step("Opening database transaction"):
                with db_manager.engine.begin() as connection:
                    if db_manager.engine.dialect.name == "postgresql":
                        logger.info(
                            "Commit transaction opened for bookie_id=%s event_id=%s sport=%s",
                            _bookie_scope_label(args),
                            args.event_id,
                            args.sport or "(all)",
                        )
                    if db_manager.engine.dialect.name == "postgresql":
                        with progress_step("Acquiring the PostgreSQL markets maintenance lock"):
                            connection.execute(text("LOCK TABLE markets IN SHARE ROW EXCLUSIVE MODE"))

                    with progress_step("Validating required schema"):
                        validate_required_schema(connection)

                    with progress_step("Validating canonical targets"):
                        canonical_targets = _preflight(connection, rules)
                    with progress_step(
                        f"Analyzing markets in canonical event_id batches of {args.batch_size}"
                    ):
                        analysis, conflicts_report, batch_ranges, candidate_bounds = (
                            _analyze_in_event_batches(
                                connection,
                                rules,
                                params,
                                batch_size=args.batch_size,
                                max_batches=args.max_batches,
                            )
                        )

                    if conflicts_report["total_conflicts"]:
                        with progress_step(
                            f"Merging {conflicts_report['total_conflicts']} projected unique-constraint conflicts"
                        ):
                            merged_count = _resolve_and_merge_conflicts(
                                connection, conflicts_report["rows"], rules
                            )
                            logger.info("Successfully merged %s duplicate markets", merged_count)

                    with progress_step("Updating eligible market metadata by event batch"):
                        total_update_batches = len(batch_ranges)
                        updated_total = 0
                        eligible_total = sum(
                            row["eligible_markets"] for row in analysis["rules"]
                        )
                        for batch_number, (batch_start, batch_end) in enumerate(
                            batch_ranges, start=1
                        ):
                            batch_params = {
                                **params,
                                "batch_start": batch_start,
                                "batch_end": batch_end,
                            }
                            logger.info(
                                "Updating canonical market batch %s/%s: event_id_range=%s..%s",
                                batch_number,
                                total_update_batches,
                                batch_start,
                                batch_end,
                            )
                            batch_updates = _apply_updates(connection, rules, batch_params)
                            batch_updated = sum(batch_updates.values())
                            updated_total += batch_updated
                            for rule_id, count in batch_updates.items():
                                updated_by_rule[rule_id] = (
                                    updated_by_rule.get(rule_id, 0) + count
                                )
                            logger.info(
                                "Completed update batch %s/%s: updated=%s "
                                "total_updated=%s remaining_eligible=%s",
                                batch_number,
                                total_update_batches,
                                batch_updated,
                                updated_total,
                                max(
                                    eligible_total - updated_total,
                                    0,
                                ),
                            )
                    with progress_step("Running post-update validation"):
                        post_update_validation = _post_update_validation(connection, params)
        else:
            with progress_step("Opening database connection"):
                with db_manager.engine.connect() as connection:
                    with progress_step("Validating required schema"):
                        validate_required_schema(connection)

                    with progress_step("Validating canonical targets"):
                        canonical_targets = _preflight(connection, rules)
                    with progress_step(
                        f"Analyzing markets in canonical event_id batches of {args.batch_size}"
                    ):
                        analysis, conflicts_report, batch_ranges, candidate_bounds = (
                            _analyze_in_event_batches(
                                connection,
                                rules,
                                params,
                                batch_size=args.batch_size,
                                max_batches=args.max_batches,
                            )
                        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1, None

    for item in analysis["rules"]:
        item["rows_updated"] = updated_by_rule.get(item["rule_id"], 0)
    summary = {
        "mode": mode,
        "bookie_id": args.bookie_id,
        "exclude_bookie": args.exclude_bookie,
        "bookie_scope": _bookie_scope_label(args),
        "sport": args.sport,
        "event_id": args.event_id,
        "batch_size": args.batch_size,
        "max_batches": args.max_batches,
        "run_migrations": args.run_migrations,
        "candidate_event_bounds": candidate_bounds,
        "include_risky": args.include_risky,
        "conflict_limit": args.conflict_limit,
        "print_index_recommendations": args.print_index_recommendations,
        "total_candidate_markets": sum(row["candidate_markets"] for row in analysis["rules"]),
        "total_eligible_markets": sum(row["eligible_markets"] for row in analysis["rules"]),
        "total_skipped_missing_choice_group": sum(
            row["skipped_missing_choice_group"] for row in analysis["rules"]
        ),
        "total_risky_markets_included": (
            sum(row["risky_markets"] for row in analysis["rules"])
            if args.include_risky
            else 0
        ),
        "total_conflicts": conflicts_report["total_conflicts"],
        "total_rows_updated": sum(updated_by_rule.values()),
        "breakdown_by_rule": analysis["rules"],
        "breakdown_by_sport": analysis["by_sport"],
        "conflicts": conflicts_report["rows"],
        "canonical_targets": canonical_targets,
        "csv_references": csv_references,
        "post_update_validation": post_update_validation,
    }
    _print_summary(summary)
    if args.output_json:
        print("\nJSON summary:")
        print(json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True, default=str))
    return 0, summary


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )
    args = parse_args(argv)
    return run(args)[0]


if __name__ == "__main__":
    raise SystemExit(main())
