"""Pure standings rules for matchup streak analysis.

This module contains only deterministic helpers. It has no database access,
no caching, and no side effects.
"""

from typing import Dict, List, Optional, Tuple


def normalize_result_subtype(raw_subtype: Optional[str], winner: Optional[str]) -> str:
    """Normalize raw result subtype values to REG/OT/SO/DRAW."""
    if winner == "X":
        return "DRAW"
    if raw_subtype is None:
        return "REG"

    value = str(raw_subtype).strip().upper()
    if not value:
        return "REG"
    if value in {"DRAW", "X", "TIE"}:
        return "DRAW"
    if "SO" in value or "SHOOT" in value:
        return "SO"
    if value in {"OT", "OVERTIME"} or "OT" in value or "EXTRA" in value:
        return "OT"
    if value in {"REG", "REGULATION"} or "REG" in value:
        return "REG"
    return "REG"


def _normalize_team_name_for_sort(team_name: str) -> str:
    """Normalize team name for deterministic sorting."""
    return team_name.lower().strip()


def build_sort_key(stats: Dict, standings_method: str) -> Tuple:
    """Build method-specific sort key for deterministic simplified ranking."""
    if standings_method == "football_3_1_0":
        return (
            stats.get("points", 0),
            stats.get("goal_diff", 0),
            stats.get("wins", 0),
        )

    if standings_method in {"win_pct", "win_pct_half_tie"}:
        return (
            stats.get("pct", 0.0) or 0.0,
            stats.get("wins", 0),
            stats.get("goal_diff", 0),
        )

    if standings_method == "nhl_2_1_0_otl":
        if stats.get("regulation_wins") is not None:
            return (
                stats.get("points", 0),
                stats.get("pct", 0.0) or 0.0,
                stats.get("regulation_wins", 0),
                stats.get("wins", 0),
                stats.get("goal_diff", 0),
            )
        return (
            stats.get("points", 0),
            stats.get("pct", 0.0) or 0.0,
            stats.get("wins", 0),
            stats.get("goal_diff", 0),
        )

    if standings_method == "hockey_3_2_1_0":
        return (
            stats.get("points", 0),
            stats.get("regulation_wins", 0),
            stats.get("wins", 0),
            stats.get("goal_diff", 0),
        )

    return (
        stats.get("points", 0),
        stats.get("goal_diff", 0),
        stats.get("wins", 0),
    )


def build_primary_rank_key(stats: Dict, standings_method: str) -> Tuple:
    """Build the primary metric key for rank grouping and tie detection."""
    if standings_method in {"win_pct", "win_pct_half_tie"}:
        return (stats.get("pct", 0.0) or 0.0,)

    return build_sort_key(stats, standings_method)


def build_display_sort_key(team_name: str, stats: Dict, standings_method: str) -> Tuple:
    """Build deterministic sort key for stable ordering inside a tie cluster."""
    inverted_name = tuple(-ord(c) for c in _normalize_team_name_for_sort(team_name))

    if standings_method in {"win_pct", "win_pct_half_tie"}:
        return (
            stats.get("pct", 0.0) or 0.0,
            inverted_name,
        )

    base_key = build_sort_key(stats, standings_method)
    return base_key + (inverted_name,)


def assign_positions_with_ties(sorted_items: List[Tuple[str, Dict]], standings_method: str) -> Dict[str, Dict]:
    """Assign competition-style positions while preserving tie clusters."""
    result = {}
    current_pos = 1
    tied_count = 0
    prev_rank_key = None

    for i, (team_name, stats) in enumerate(sorted_items):
        rank_key = build_primary_rank_key(stats, standings_method)

        if i == 0:
            result[team_name] = {"position": 1, "is_primary_tie": False, "primary_rank_key": rank_key}
            prev_rank_key = rank_key
            continue

        if rank_key == prev_rank_key:
            tied_count += 1
            if tied_count == 1:
                prev_team_name = sorted_items[i - 1][0]
                result[prev_team_name]["is_primary_tie"] = True

            result[team_name] = {"position": current_pos, "is_primary_tie": True, "primary_rank_key": rank_key}
        else:
            current_pos += 1 + tied_count
            tied_count = 0
            result[team_name] = {"position": current_pos, "is_primary_tie": False, "primary_rank_key": rank_key}
            prev_rank_key = rank_key

    return result


__all__ = [
    "assign_positions_with_ties",
    "build_display_sort_key",
    "build_primary_rank_key",
    "build_sort_key",
    "normalize_result_subtype",
]
