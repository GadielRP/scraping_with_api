"""M1 — Base Strength module.

Measures the base competitive strength of each team during the season using
four components: RESULT_EDGE, GD_EDGE, CONSISTENCY_EDGE, and
VOL_DIRECTION_EDGE.

This module is **pure**: it receives a pre-built ``MatchupStreakContext`` and
returns a structured ``ModuleResult``.  It never calls external APIs, sends
messages, or writes to the database.

Data source:
    All W/L/D, GP, and GD are computed from the form results
    (``home_team_results`` / ``away_team_results``), NOT from the SofaScore
    standings API endpoint.

Convention:  + = HOME, − = AWAY, 0 = NEUTRAL.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Component weights
# ---------------------------------------------------------------------------

_WEIGHT_RESULT_EDGE = 0.35
_WEIGHT_GD_EDGE = 0.35
_WEIGHT_CONSISTENCY_EDGE = 0.15
_WEIGHT_VOL_DIRECTION_EDGE = 0.15

_BATCH_SIZE = 5
_CONSISTENCY_WINDOW_STEP = 5  # L5, L10, L15 …


# ---------------------------------------------------------------------------
# Internal helpers — form data extraction
# ---------------------------------------------------------------------------

def _count_wins(results: List[Dict]) -> int:
    """Count wins from form results (``team_result_code == '1'``)."""
    return sum(1 for r in results if r.get("team_result_code") == "1")


def _get_games_played(standing: Optional[Dict]) -> Optional[int]:
    """Extract games-played from a standing snapshot.

    Supports keys ``matches``, ``gp``, and ``games_played`` in that
    priority order.  Returns ``None`` when the value is missing or
    not a positive integer.
    """
    if not standing:
        return None
    for key in ("matches", "gp", "games_played"):
        val = standing.get(key)
        if val is not None:
            try:
                val = int(val)
                if val > 0:
                    return val
            except (ValueError, TypeError):
                continue
    return None


def _extract_game_gd_series(results: List[Dict]) -> List[float]:
    """Build a list of per-game goal-differentials from *results*.

    Tries ``team_score − opponent_score`` first, then
    ``score_for − score_against``, then ``net_score``.
    Games with no valid score are silently skipped.  The returned list
    preserves the order of *results* (most recent first when that is how
    ``streak_analysis`` provides them).
    """
    series: List[float] = []
    for game in results:
        gd = _try_extract_gd(game)
        if gd is not None:
            series.append(float(gd))
    return series


def _try_extract_gd(game: Dict) -> Optional[float]:
    """Return a single game-GD from *game*, or ``None``."""
    # Preferred: team_score / opponent_score
    ts = game.get("team_score")
    os_ = game.get("opponent_score")
    if ts is not None and os_ is not None:
        try:
            return float(ts) - float(os_)
        except (ValueError, TypeError):
            pass

    # Fallback 1: score_for / score_against
    sf = game.get("score_for")
    sa = game.get("score_against")
    if sf is not None and sa is not None:
        try:
            return float(sf) - float(sa)
        except (ValueError, TypeError):
            pass

    # Fallback 2: net_score
    ns = game.get("net_score")
    if ns is not None:
        try:
            return float(ns)
        except (ValueError, TypeError):
            pass

    return None


# ---------------------------------------------------------------------------
# GD dynamic scale
# ---------------------------------------------------------------------------

def _calculate_p75_dynamic_scale(standings_list: List[Dict]) -> Optional[float]:
    """Compute P75 of ``|GD_PER_GAME|`` from a list of standing snapshots.

    Uses the **nearest-rank** method:
        rank = ceil(0.75 * N),  index = rank − 1
    """
    gd_per_game_abs: List[float] = []
    for s in standings_list:
        gd = s.get("goal_diff") or s.get("diff")
        gp = _get_games_played(s)
        if gd is None or gp is None or gp <= 0:
            continue
        try:
            gd_per_game_abs.append(abs(float(gd) / float(gp)))
        except (ValueError, TypeError, ZeroDivisionError):
            continue

    if not gd_per_game_abs:
        return None

    gd_per_game_abs.sort()
    n = len(gd_per_game_abs)
    rank = math.ceil(0.75 * n)
    idx = rank - 1
    return gd_per_game_abs[idx]


def _gather_full_league_standings_from_results(
    home_results: List[Dict],
    away_results: List[Dict],
) -> List[Dict]:
    """Collect the full league standings from per-game standing snapshots.

    Each result dict has ``team_standing`` and ``opponent_standing``.
    The team identity is inferred from the result-level ``team_name`` and
    ``opponent_name`` keys (the standing snapshot itself does not carry a
    team name).

    For each unique team name, keeps the snapshot with the highest GP
    (most games played = most recent point in time).
    """
    best: Dict[str, Dict] = {}

    def _process(team_key: Optional[str], snapshot: Optional[Dict]) -> None:
        if not team_key or not snapshot or not isinstance(snapshot, dict):
            return
        gp = _get_games_played(snapshot)
        if gp is None or gp <= 0:
            return
        existing_gp = _get_games_played(best.get(team_key)) or 0
        if gp > existing_gp:
            best[team_key] = snapshot

    for game in home_results:
        _process(game.get("team_name"), game.get("team_standing"))
        _process(game.get("opponent_name"), game.get("opponent_standing"))
    for game in away_results:
        _process(game.get("team_name"), game.get("team_standing"))
        _process(game.get("opponent_name"), game.get("opponent_standing"))

    return list(best.values())


def _resolve_gd_dynamic_scale(
    home_results: List[Dict],
    away_results: List[Dict],
    home_gd_per_game: float,
    away_gd_per_game: float,
) -> Tuple[Optional[float], str, int]:
    """Determine the GD dynamic scale using the fallback chain.

    Returns ``(scale, source_label, sample_size)``.
    """
    # Primary: full league standings from per-game computed snapshots
    league_standings = _gather_full_league_standings_from_results(home_results, away_results)
    if league_standings:
        scale = _calculate_p75_dynamic_scale(league_standings)
        if scale is not None and scale > 0:
            return scale, "computed_league_standings", len(league_standings)

    # Fallback: pair difference
    pair_diff = abs(home_gd_per_game - away_gd_per_game)
    if pair_diff > 0:
        return pair_diff, "pair_fallback", 2

    # No scale available
    return None, "missing", 0


# ---------------------------------------------------------------------------
# Population standard deviation helper
# ---------------------------------------------------------------------------

def _pstdev(values: List[float]) -> float:
    """Population standard deviation (not sample)."""
    if not values:
        return 0.0
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return math.sqrt(variance)


# ---------------------------------------------------------------------------
# Component calculators
# ---------------------------------------------------------------------------

def _calculate_result_edge(
    home_results: List[Dict],
    away_results: List[Dict],
) -> Tuple[float, Dict[str, Any]]:
    """Component 1 — RESULT_EDGE (win-rate differential).

    Wins and GP are computed directly from the form results, not from
    the SofaScore standings API.
    """
    home_gp = len(home_results)
    away_gp = len(away_results)
    home_wins = _count_wins(home_results)
    away_wins = _count_wins(away_results)

    raw: Dict[str, Any] = {
        "home_wins": home_wins,
        "away_wins": away_wins,
        "home_gp": home_gp,
        "away_gp": away_gp,
    }

    if home_gp == 0 or away_gp == 0:
        raw["reason"] = "no_results"
        return 0.0, raw

    home_win_rate = float(home_wins) / float(home_gp)
    away_win_rate = float(away_wins) / float(away_gp)

    edge = clamp(home_win_rate - away_win_rate)
    raw["home_win_rate"] = home_win_rate
    raw["away_win_rate"] = away_win_rate
    return edge, raw


def _calculate_gd_edge(
    home_gd_series: List[float],
    away_gd_series: List[float],
    home_results: List[Dict],
    away_results: List[Dict],
) -> Tuple[float, Dict[str, Any]]:
    """Component 2 — GD_EDGE (goal-difference per game, dynamically scaled).

    Total GD and GP are computed from the form results (game-GD series),
    not from the SofaScore standings API.
    """
    home_gp = len(home_gd_series)
    away_gp = len(away_gd_series)
    home_total_gd = sum(home_gd_series)
    away_total_gd = sum(away_gd_series)

    raw: Dict[str, Any] = {
        "home_total_gd": home_total_gd,
        "away_total_gd": away_total_gd,
        "home_gp": home_gp,
        "away_gp": away_gp,
    }

    if home_gp == 0 or away_gp == 0:
        raw["reason"] = "no_results"
        return 0.0, raw

    home_gd_per_game = home_total_gd / float(home_gp)
    away_gd_per_game = away_total_gd / float(away_gp)

    raw["home_gd_per_game"] = home_gd_per_game
    raw["away_gd_per_game"] = away_gd_per_game

    # Resolve dynamic scale from computed standings in results
    scale, source, sample_size = _resolve_gd_dynamic_scale(
        home_results, away_results,
        home_gd_per_game, away_gd_per_game,
    )

    raw["dynamic_scale"] = scale
    raw["scale_source"] = source
    raw["league_sample_size"] = sample_size

    if scale is None or scale <= 0:
        raw["reason"] = "missing_dynamic_scale"
        return 0.0, raw

    edge_raw = (home_gd_per_game - away_gd_per_game) / scale
    raw["edge_raw"] = edge_raw
    return clamp(edge_raw), raw


def _calculate_consistency_edge(
    home_series: List[float],
    away_series: List[float],
) -> Tuple[float, Dict[str, Any]]:
    """Component 3 — CONSISTENCY_EDGE (STD of game-GD across rolling windows)."""
    n = min(len(home_series), len(away_series))

    raw: Dict[str, Any] = {
        "home_game_gd": home_series,
        "away_game_gd": away_series,
    }

    if n < _CONSISTENCY_WINDOW_STEP:
        raw["reason"] = "insufficient_games"
        raw["available_games"] = n
        return 0.0, raw

    # Build windows: L5, L10, L15, …
    windows: List[int] = list(range(_CONSISTENCY_WINDOW_STEP, n + 1, _CONSISTENCY_WINDOW_STEP))
    if not windows:
        raw["reason"] = "no_valid_windows"
        return 0.0, raw

    home_window_stds: Dict[str, float] = {}
    away_window_stds: Dict[str, float] = {}

    for w in windows:
        label = f"L{w}"
        home_window_stds[label] = _pstdev(home_series[:w])
        away_window_stds[label] = _pstdev(away_series[:w])

    avg_std_home = sum(home_window_stds.values()) / len(home_window_stds) if home_window_stds else 0.0
    avg_std_away = sum(away_window_stds.values()) / len(away_window_stds) if away_window_stds else 0.0

    raw["windows"] = windows
    raw["home_window_stds"] = home_window_stds
    raw["away_window_stds"] = away_window_stds
    raw["avg_std_home"] = avg_std_home
    raw["avg_std_away"] = avg_std_away

    # Lower STD is better → positive when home is more consistent
    consistency_raw = avg_std_away - avg_std_home
    scale = max(avg_std_home, avg_std_away, 1.0)
    raw["scale"] = scale

    edge = clamp(consistency_raw / scale)
    return edge, raw


def _calculate_vol_direction_edge(
    home_series: List[float],
    away_series: List[float],
) -> Tuple[float, Dict[str, Any]]:
    """Component 4 — VOL_DIRECTION_EDGE (recent-batch trend vs baseline)."""
    n = min(len(home_series), len(away_series))

    raw: Dict[str, Any] = {}

    # Need at least 2 batches (10 games) per team
    if n < _BATCH_SIZE * 2:
        raw["reason"] = "insufficient_batches"
        raw["available_games"] = n
        return 0.0, raw

    def _batch_averages(series: List[float], length: int) -> List[float]:
        """Split *series[:length]* into batches and return per-batch averages."""
        avgs: List[float] = []
        for i in range(0, length, _BATCH_SIZE):
            batch = series[i : i + _BATCH_SIZE]
            if batch:
                avgs.append(sum(batch) / len(batch))
        return avgs

    home_batch_avgs = _batch_averages(home_series, n)
    away_batch_avgs = _batch_averages(away_series, n)

    raw["home_batch_avg_gd"] = home_batch_avgs
    raw["away_batch_avg_gd"] = away_batch_avgs

    if len(home_batch_avgs) < 2 or len(away_batch_avgs) < 2:
        raw["reason"] = "insufficient_batches"
        return 0.0, raw

    # Direction = recent_batch − mean(baseline_batches)
    home_recent = home_batch_avgs[0]
    home_baseline = sum(home_batch_avgs[1:]) / len(home_batch_avgs[1:])
    home_direction = home_recent - home_baseline

    away_recent = away_batch_avgs[0]
    away_baseline = sum(away_batch_avgs[1:]) / len(away_batch_avgs[1:])
    away_direction = away_recent - away_baseline

    raw["home_direction"] = home_direction
    raw["away_direction"] = away_direction

    vol_raw = home_direction - away_direction
    scale = max(abs(home_direction), abs(away_direction), 1.0)
    raw["scale"] = scale

    edge = clamp(vol_raw / scale)
    return edge, raw


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calculate_base_strength(streak_analysis: Any) -> ModuleResult:
    """Calculate M1 — Base Strength for an event.

    Args:
        streak_analysis: A ``MatchupStreakContext`` (or compatible object)
            with fields such as ``home_team_results``,
            ``away_team_results``, etc.

    Returns:
        A fully-auditable ``ModuleResult``.
    """
    home_results: List[Dict] = getattr(streak_analysis, "home_team_results", None) or []
    away_results: List[Dict] = getattr(streak_analysis, "away_team_results", None) or []
    event_id: int = getattr(streak_analysis, "event_id", 0)
    participants: str = getattr(streak_analysis, "participants", "")

    # Pre-compute GD series (shared by GD_EDGE, consistency & vol_direction)
    home_gd_series = _extract_game_gd_series(home_results)
    away_gd_series = _extract_game_gd_series(away_results)

    # --- Component 1: RESULT_EDGE ---
    result_edge, result_raw = _calculate_result_edge(home_results, away_results)

    # --- Component 2: GD_EDGE ---
    gd_edge, gd_raw = _calculate_gd_edge(
        home_gd_series, away_gd_series, home_results, away_results,
    )

    # --- Component 3: CONSISTENCY_EDGE ---
    consistency_edge, consistency_raw = _calculate_consistency_edge(
        home_gd_series, away_gd_series,
    )

    # --- Component 4: VOL_DIRECTION_EDGE ---
    vol_direction_edge, vol_raw = _calculate_vol_direction_edge(
        home_gd_series, away_gd_series,
    )

    # --- Assemble components ---
    def _component(name: str, edge: float, weight: float, raw: Dict) -> ModuleComponentResult:
        return ModuleComponentResult(
            name=name,
            edge=edge,
            bias=calculate_bias(edge),
            strength=classify_strength(edge),
            weight=weight,
            weighted_edge=edge * weight,
            raw=raw,
        )

    components = [
        _component("RESULT_EDGE", result_edge, _WEIGHT_RESULT_EDGE, result_raw),
        _component("GD_EDGE", gd_edge, _WEIGHT_GD_EDGE, gd_raw),
        _component("CONSISTENCY_EDGE", consistency_edge, _WEIGHT_CONSISTENCY_EDGE, consistency_raw),
        _component("VOL_DIRECTION_EDGE", vol_direction_edge, _WEIGHT_VOL_DIRECTION_EDGE, vol_raw),
    ]

    base_value = sum(c.weighted_edge for c in components)
    final_value = clamp(base_value)

    raw_audit: Dict[str, Any] = {
        "home_team": getattr(streak_analysis, "home_team_name", None),
        "away_team": getattr(streak_analysis, "away_team_name", None),
        "result_edge": result_raw,
        "gd_edge": gd_raw,
        "consistency_edge": consistency_raw,
        "vol_direction_edge": vol_raw,
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M1",
        module_name="Base Strength",
        event_id=event_id,
        participants=participants,
        value=final_value,
        bias=calculate_bias(final_value),
        strength=classify_strength(final_value),
        components=components,
        raw=raw_audit,
    )
