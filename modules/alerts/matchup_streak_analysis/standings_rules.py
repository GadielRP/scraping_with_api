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
    if standings_method in {"football_3_1_0", "football_3_1_0_h2h"}:
        # Simplified football ranking: points first, then goal differential,
        # then goals for, with wins as a final generic fallback.
        return (
            stats.get("points", 0),
            stats.get("goal_diff", 0),
            stats.get("goals_for", 0),
            stats.get("wins", 0),
        )

    if standings_method in {"win_pct", "win_pct_half_tie"}:
        return (
            stats.get("pct", 0.0) or 0.0,
            stats.get("wins", 0),
            stats.get("goal_diff", 0),
        )

    if standings_method == "nhl_2_1_0_otl":
        # TODO: For full NHL exactness, separate ROW/OT/SO wins and other
        # official tie-break details when the source data supports them.
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


def assign_positions_with_ties(sorted_items: List[Tuple[str, Dict]], standings_method: str, rank_key_by_team=None) -> Dict[str, Dict]:
    """Assign competition-style positions while preserving tie clusters."""
    result = {}
    current_pos = 1
    tied_count = 0
    prev_rank_key = None

    for i, (team_name, stats) in enumerate(sorted_items):
        if rank_key_by_team is not None and team_name in rank_key_by_team:
            rank_key = rank_key_by_team[team_name]
        else:
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


def build_football_h2h_stats(team_names, match_records) -> Dict[str, Dict[str, int]]:
    team_set = set(team_names)
    h2h_stats = {}
    for name in team_names:
        h2h_stats[name] = {
            "points": 0,
            "goal_diff": 0,
            "goals_for": 0,
            "matches": 0,
        }

    for match in match_records:
        home = match["home_team"]
        away = match["away_team"]
        if home in team_set and away in team_set:
            home_score = match["home_score"]
            away_score = match["away_score"]
            
            h2h_stats[home]["goals_for"] += home_score
            h2h_stats[home]["goal_diff"] += (home_score - away_score)
            h2h_stats[home]["matches"] += 1
            
            h2h_stats[away]["goals_for"] += away_score
            h2h_stats[away]["goal_diff"] += (away_score - home_score)
            h2h_stats[away]["matches"] += 1
            
            if home_score > away_score:
                h2h_stats[home]["points"] += 3
            elif away_score > home_score:
                h2h_stats[away]["points"] += 3
            else:
                h2h_stats[home]["points"] += 1
                h2h_stats[away]["points"] += 1

    return h2h_stats


def is_football_h2h_cluster_complete(team_names, h2h_stats) -> bool:
    n = len(team_names)
    if n <= 1:
        return False
    expected_matches = n * (n - 1)  # double round-robin
    actual_matches = sum(h2h_stats[name]["matches"] for name in team_names) // 2
    return actual_matches >= expected_matches


def sort_football_h2h_items(items, match_records):
    by_points = {}
    for team_name, stats in items:
        pts = stats.get("points", 0)
        by_points.setdefault(pts, []).append((team_name, stats))

    sorted_items = []
    rank_key_by_team = {}

    for pts in sorted(by_points.keys(), reverse=True):
        cluster = by_points[pts]
        if len(cluster) == 1:
            name, stats = cluster[0]
            sorted_items.append((name, stats))
            rank_key_by_team[name] = (
                pts,
                "H2H_COMPLETE",
                0,
                0,
                stats.get("goal_diff", 0),
                stats.get("goals_for", 0),
                stats.get("wins", 0),
            )
        else:
            cluster_team_names = [name for name, _ in cluster]
            h2h_stats = build_football_h2h_stats(cluster_team_names, match_records)

            if is_football_h2h_cluster_complete(cluster_team_names, h2h_stats):
                def cluster_sort_key(item):
                    name, stats = item
                    h2h = h2h_stats[name]
                    inverted_name = tuple(-ord(c) for c in _normalize_team_name_for_sort(name))
                    return (
                        h2h["points"],
                        h2h["goal_diff"],
                        stats.get("goal_diff", 0),
                        stats.get("goals_for", 0),
                        stats.get("wins", 0),
                        inverted_name,
                    )

                sorted_cluster = sorted(cluster, key=cluster_sort_key, reverse=True)
                for name, stats in sorted_cluster:
                    sorted_items.append((name, stats))
                    h2h = h2h_stats[name]
                    rank_key_by_team[name] = (
                        pts,
                        "H2H_COMPLETE",
                        h2h["points"],
                        h2h["goal_diff"],
                        stats.get("goal_diff", 0),
                        stats.get("goals_for", 0),
                        stats.get("wins", 0),
                    )
            else:
                def fallback_sort_key(item):
                    name, stats = item
                    inverted_name = tuple(-ord(c) for c in _normalize_team_name_for_sort(name))
                    return (
                        stats.get("goal_diff", 0),
                        stats.get("goals_for", 0),
                        stats.get("wins", 0),
                        inverted_name,
                    )

                sorted_cluster = sorted(cluster, key=fallback_sort_key, reverse=True)
                for name, stats in sorted_cluster:
                    sorted_items.append((name, stats))
                    rank_key_by_team[name] = (
                        pts,
                        "H2H_INCOMPLETE",
                        stats.get("goal_diff", 0),
                        stats.get("goals_for", 0),
                        stats.get("wins", 0),
                    )

    return sorted_items, rank_key_by_team


__all__ = [
    "assign_positions_with_ties",
    "build_display_sort_key",
    "build_primary_rank_key",
    "build_sort_key",
    "normalize_result_subtype",
    "build_football_h2h_stats",
    "sort_football_h2h_items",
    "is_football_h2h_cluster_complete",
]
