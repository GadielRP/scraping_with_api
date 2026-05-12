"""M1 - Base Strength module.

Measures the base competitive strength of each team during the season using
four components: RESULT_EDGE, GD_EDGE, CONSISTENCY_EDGE, and
VOL_DIRECTION_EDGE.

This module is pure: it receives a pre-built ``MatchupStreakContext`` and
returns a structured ``ModuleResult``. It never calls external APIs, sends
messages, or writes to the database.

Data source:
    All W/L/D, GP, and GD are computed from the form results
    (``home_team_results`` / ``away_team_results``), not from the SofaScore
    standings API endpoint.

Convention: + = HOME, - = AWAY, 0 = NEUTRAL.
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
_CONSISTENCY_WINDOW_STEP = 5  # L5, L10, L15 ...
_MIN_GD_SCALE_SAMPLE_SIZE = 20


# ---------------------------------------------------------------------------
# Internal helpers - form data extraction
# ---------------------------------------------------------------------------

def _count_wins(results: List[Dict]) -> int:
    """Count wins from form results (team_result_code == '1')."""
    return sum(1 for r in results if r.get("team_result_code") == "1")


def _get_games_played(standing: Optional[Dict]) -> Optional[int]:
    """Extract games-played from a standing snapshot."""
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


def _try_extract_gd(game: Dict) -> Optional[float]:
    """Return a single game GD from *game*, or ``None``.

    The explicit goal_diff -> diff ordering keeps 0 as a valid value.
    """
    gd = game.get("goal_diff")
    if gd is not None:
        try:
            return float(gd)
        except (ValueError, TypeError):
            pass

    gd = game.get("diff")
    if gd is not None:
        try:
            return float(gd)
        except (ValueError, TypeError):
            pass

    ts = game.get("team_score")
    os_ = game.get("opponent_score")
    if ts is not None and os_ is not None:
        try:
            return float(ts) - float(os_)
        except (ValueError, TypeError):
            pass

    sf = game.get("score_for")
    sa = game.get("score_against")
    if sf is not None and sa is not None:
        try:
            return float(sf) - float(sa)
        except (ValueError, TypeError):
            pass

    ns = game.get("net_score")
    if ns is not None:
        try:
            return float(ns)
        except (ValueError, TypeError):
            pass

    return None


def _extract_game_gd_series(results: List[Dict]) -> List[float]:
    """Build a list of per-game GD values from *results*."""
    series: List[float] = []
    for game in results:
        gd = _try_extract_gd(game)
        if gd is not None:
            series.append(float(gd))
    return series


# ---------------------------------------------------------------------------
# GD dynamic scale
# ---------------------------------------------------------------------------

def _extract_gd_scale_samples(standings_list: List[Dict]) -> List[float]:
    """Return absolute GD-per-game samples used for the P75 dynamic scale."""
    gd_per_game_abs: List[float] = []
    for standing in standings_list:
        gd = standing.get("goal_diff")
        if gd is None:
            gd = standing.get("diff")
        gp = _get_games_played(standing)
        if gd is None or gp is None or gp <= 0:
            continue
        try:
            gd_per_game_abs.append(abs(float(gd) / float(gp)))
        except (ValueError, TypeError, ZeroDivisionError):
            continue
    return gd_per_game_abs


def _calculate_p75_dynamic_scale(standings_list: List[Dict]) -> Optional[float]:
    """Compute P75 of ``|GD_PER_GAME|`` from a list of standing snapshots."""
    gd_per_game_abs = _extract_gd_scale_samples(standings_list)

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
    """Collect standing snapshots from the result payloads.

    Each result dict may carry ``team_standing`` and ``opponent_standing``.
    The team identity is inferred from the result-level ``team_name`` and
    ``opponent_name`` keys.
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
    """Determine the GD dynamic scale using the official v4 chain."""
    del home_gd_per_game, away_gd_per_game

    league_standings = _gather_full_league_standings_from_results(home_results, away_results)
    gd_scale_samples = _extract_gd_scale_samples(league_standings)
    sample_size = len(gd_scale_samples)

    if sample_size == 0:
        return None, "missing", 0

    if sample_size < _MIN_GD_SCALE_SAMPLE_SIZE:
        return None, "incomplete_league_standings", sample_size

    scale = _calculate_p75_dynamic_scale(league_standings)
    if scale is not None and scale > 0:
        return scale, "computed_league_standings", sample_size

    return None, "incomplete_league_standings", sample_size


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
    """Component 1 - RESULT_EDGE (win-rate differential)."""
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
        raw["final_edge_clamped"] = 0.0
        return 0.0, raw

    home_win_rate = float(home_wins) / float(home_gp)
    away_win_rate = float(away_wins) / float(away_gp)

    edge_raw = home_win_rate - away_win_rate
    edge = clamp(edge_raw)
    raw["home_win_rate"] = home_win_rate
    raw["away_win_rate"] = away_win_rate
    raw["edge_raw"] = edge_raw
    raw["final_edge_clamped"] = edge
    return edge, raw


def _calculate_gd_edge(
    home_gd_series: List[float],
    away_gd_series: List[float],
    home_results: List[Dict],
    away_results: List[Dict],
) -> Tuple[float, Dict[str, Any]]:
    """Component 2 - GD_EDGE (goal-difference per game, dynamically scaled)."""
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
        raw["dynamic_scale"] = None
        raw["m1_gd_dynamic_scale"] = None
        raw["final_edge_clamped"] = 0.0
        return 0.0, raw

    home_gd_per_game = home_total_gd / float(home_gp)
    away_gd_per_game = away_total_gd / float(away_gp)

    raw["home_gd_per_game"] = home_gd_per_game
    raw["away_gd_per_game"] = away_gd_per_game

    scale, source, sample_size = _resolve_gd_dynamic_scale(
        home_results,
        away_results,
        home_gd_per_game,
        away_gd_per_game,
    )

    raw["dynamic_scale"] = scale
    raw["m1_gd_dynamic_scale"] = scale
    raw["scale_source"] = source
    raw["league_sample_size"] = sample_size

    if scale is None or scale <= 0:
        raw["reason"] = "missing_dynamic_scale"
        raw["edge_raw"] = 0.0
        raw["final_edge_clamped"] = 0.0
        return 0.0, raw

    edge_raw = (home_gd_per_game - away_gd_per_game) / scale
    edge = clamp(edge_raw)
    raw["edge_raw"] = edge_raw
    raw["final_edge_clamped"] = edge
    return edge, raw


def _calculate_consistency_edge(
    home_series: List[float],
    away_series: List[float],
) -> Tuple[float, Dict[str, Any]]:
    """Component 3 - CONSISTENCY_EDGE (STD of game-GD across rolling windows)."""
    n = min(len(home_series), len(away_series))

    raw: Dict[str, Any] = {
        "home_game_gd": home_series,
        "away_game_gd": away_series,
    }

    if n < _CONSISTENCY_WINDOW_STEP:
        raw["reason"] = "insufficient_games"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        return 0.0, raw

    windows: List[int] = list(range(_CONSISTENCY_WINDOW_STEP, n + 1, _CONSISTENCY_WINDOW_STEP))
    if not windows:
        raw["reason"] = "no_valid_windows"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        return 0.0, raw

    home_window_stds: Dict[str, float] = {}
    away_window_stds: Dict[str, float] = {}
    consistency_edges_by_window: Dict[str, float] = {}

    for w in windows:
        label = f"L{w}"
        home_std = _pstdev(home_series[:w])
        away_std = _pstdev(away_series[:w])
        home_window_stds[label] = home_std
        away_window_stds[label] = away_std
        consistency_edges_by_window[label] = away_std - home_std

    consistency_edge_raw_average = (
        sum(consistency_edges_by_window.values()) / len(consistency_edges_by_window)
        if consistency_edges_by_window
        else 0.0
    )
    final_edge_clamped = clamp(consistency_edge_raw_average)

    raw["windows"] = windows
    raw["home_window_stds"] = home_window_stds
    raw["away_window_stds"] = away_window_stds
    raw["consistency_edges_by_window"] = consistency_edges_by_window
    raw["consistency_edge_raw_average"] = consistency_edge_raw_average
    raw["final_edge_clamped"] = final_edge_clamped

    return final_edge_clamped, raw


def _calculate_vol_direction_edge(
    home_series: List[float],
    away_series: List[float],
) -> Tuple[float, Dict[str, Any]]:
    """Component 4 - VOL_DIRECTION_EDGE (destroy/collapse power by window)."""
    n = min(len(home_series), len(away_series))

    raw: Dict[str, Any] = {}

    if n < _BATCH_SIZE:
        raw["reason"] = "insufficient_games"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        return 0.0, raw

    windows: List[int] = list(range(_BATCH_SIZE, n + 1, _BATCH_SIZE))
    if not windows:
        raw["reason"] = "no_valid_windows"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        return 0.0, raw

    def _powers(series: List[float]) -> Tuple[float, float, float]:
        positive_values = [value for value in series if value > 0]
        negative_values = [abs(value) for value in series if value < 0]
        destroy_power = (
            sum(positive_values) / len(positive_values)
            if positive_values
            else 0.0
        )
        collapse_power = (
            sum(negative_values) / len(negative_values)
            if negative_values
            else 0.0
        )
        net_vol = destroy_power - collapse_power
        return destroy_power, collapse_power, net_vol

    home_destroy_power_by_window: Dict[str, float] = {}
    home_collapse_power_by_window: Dict[str, float] = {}
    home_net_vol_by_window: Dict[str, float] = {}
    away_destroy_power_by_window: Dict[str, float] = {}
    away_collapse_power_by_window: Dict[str, float] = {}
    away_net_vol_by_window: Dict[str, float] = {}
    vol_direction_edges_by_window: Dict[str, float] = {}

    for w in windows:
        label = f"L{w}"
        home_destroy, home_collapse, home_net = _powers(home_series[:w])
        away_destroy, away_collapse, away_net = _powers(away_series[:w])

        home_destroy_power_by_window[label] = home_destroy
        home_collapse_power_by_window[label] = home_collapse
        home_net_vol_by_window[label] = home_net
        away_destroy_power_by_window[label] = away_destroy
        away_collapse_power_by_window[label] = away_collapse
        away_net_vol_by_window[label] = away_net
        vol_direction_edges_by_window[label] = home_net - away_net

    vol_direction_edge_raw_average = (
        sum(vol_direction_edges_by_window.values()) / len(vol_direction_edges_by_window)
        if vol_direction_edges_by_window
        else 0.0
    )
    final_edge_clamped = clamp(vol_direction_edge_raw_average)

    raw["windows"] = windows
    raw["home_destroy_power_by_window"] = home_destroy_power_by_window
    raw["home_collapse_power_by_window"] = home_collapse_power_by_window
    raw["home_net_vol_by_window"] = home_net_vol_by_window
    raw["away_destroy_power_by_window"] = away_destroy_power_by_window
    raw["away_collapse_power_by_window"] = away_collapse_power_by_window
    raw["away_net_vol_by_window"] = away_net_vol_by_window
    raw["vol_direction_edges_by_window"] = vol_direction_edges_by_window
    raw["vol_direction_edge_raw_average"] = vol_direction_edge_raw_average
    raw["final_edge_clamped"] = final_edge_clamped

    return final_edge_clamped, raw


def _determine_m1_status(
    result_raw: Dict[str, Any],
    gd_raw: Dict[str, Any],
    consistency_raw: Dict[str, Any],
    vol_raw: Dict[str, Any],
) -> Tuple[str, str]:
    """Derive a v4 status label and reason for the module output."""
    if (
        result_raw.get("reason") == "no_results"
        or gd_raw.get("reason") == "no_results"
        or consistency_raw.get("reason") == "insufficient_games"
        or vol_raw.get("reason") == "insufficient_games"
    ):
        return "INSUFFICIENT_DATA", "insufficient_results_or_series"

    scale_source = gd_raw.get("scale_source")
    if scale_source == "computed_league_standings":
        return "ACTIVE", "official_league_scale"
    if scale_source == "missing":
        return "INVALID_GD_SCALE", "missing_league_standings"
    if scale_source == "incomplete_league_standings":
        return "DEGRADED", "incomplete_league_standings"

    return "DEGRADED", "non_official_gd_scale"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calculate_base_strength(streak_analysis: Any) -> ModuleResult:
    """Calculate M1 - Base Strength for an event."""
    home_results: List[Dict] = getattr(streak_analysis, "home_team_results", None) or []
    away_results: List[Dict] = getattr(streak_analysis, "away_team_results", None) or []
    event_id: int = getattr(streak_analysis, "event_id", 0)
    participants: str = getattr(streak_analysis, "participants", "")

    home_gd_series = _extract_game_gd_series(home_results)
    away_gd_series = _extract_game_gd_series(away_results)

    result_edge, result_raw = _calculate_result_edge(home_results, away_results)
    gd_edge, gd_raw = _calculate_gd_edge(
        home_gd_series,
        away_gd_series,
        home_results,
        away_results,
    )
    consistency_edge, consistency_raw = _calculate_consistency_edge(
        home_gd_series,
        away_gd_series,
    )
    vol_direction_edge, vol_raw = _calculate_vol_direction_edge(
        home_gd_series,
        away_gd_series,
    )

    m1_status, m1_status_reason = _determine_m1_status(
        result_raw,
        gd_raw,
        consistency_raw,
        vol_raw,
    )

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
        "m1_edge": final_value,
        "m1_abs_edge": abs(final_value),
        "m1_bias": calculate_bias(final_value),
        "m1_strength": classify_strength(final_value),
        "m1_status": m1_status,
        "m1_status_reason": m1_status_reason,
        "m1_gd_dynamic_scale": gd_raw.get("dynamic_scale"),
        "result_edge": result_edge,
        "gd_edge": gd_edge,
        "consistency_edge": consistency_edge,
        "vol_direction_edge": vol_direction_edge,
        "result_edge_raw": result_raw,
        "gd_edge_raw": gd_raw,
        "consistency_edge_raw": consistency_raw,
        "vol_direction_edge_raw": vol_raw,
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
