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
from modules.pillars.context import EventContext

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


def _build_cumulative_windows(n: int, step: int = 5) -> List[int]:
    """Build cumulative window sizes in ``step`` increments plus the final window."""
    if n <= 0:
        return []

    windows = list(range(step, n + 1, step))
    if n >= step and (not windows or windows[-1] != n):
        windows.append(n)
    return windows


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
    expected_league_size: Optional[int] = None,
) -> Tuple[Optional[float], str, int]:
    """Determine the GD dynamic scale using the official v4 chain."""
    del home_gd_per_game, away_gd_per_game

    league_standings = _gather_full_league_standings_from_results(home_results, away_results)
    gd_scale_samples = _extract_gd_scale_samples(league_standings)
    sample_size = len(gd_scale_samples)

    if sample_size == 0:
        return None, "missing", 0

    if expected_league_size is not None and sample_size < expected_league_size:
        return None, "incomplete_league_standings", sample_size

    scale = _calculate_p75_dynamic_scale(league_standings)
    if scale is not None and scale > 0:
        if expected_league_size is None:
            return scale, "missing_expected_league_size", sample_size
        return scale, "computed_league_standings", sample_size

    if expected_league_size is None:
        return None, "missing_expected_league_size", sample_size
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
    debug_mode: bool = False,
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

    if debug_mode:
        logger.info(f"  [RESULT_EDGE] home_wins={home_wins}/{home_gp}  away_wins={away_wins}/{away_gp}")

    if home_gp == 0 or away_gp == 0:
        raw["reason"] = "no_results"
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            logger.info("  [RESULT_EDGE] => SKIP (no_results) -> edge=0.0")
        return 0.0, raw

    home_win_rate = float(home_wins) / float(home_gp)
    away_win_rate = float(away_wins) / float(away_gp)

    edge_raw = home_win_rate - away_win_rate
    edge = clamp(edge_raw)
    raw["home_win_rate"] = home_win_rate
    raw["away_win_rate"] = away_win_rate
    raw["edge_raw"] = edge_raw
    raw["final_edge_clamped"] = edge

    if debug_mode:
        logger.info(
            f"  [RESULT_EDGE] home_wr={home_win_rate:.4f}  away_wr={away_win_rate:.4f}  "
            f"edge_raw={edge_raw:.4f}  edge_clamped={edge:.4f}"
        )

    return edge, raw


def _calculate_gd_edge(
    home_gd_series: List[float],
    away_gd_series: List[float],
    home_results: List[Dict],
    away_results: List[Dict],
    expected_league_size: Optional[int] = None,
    debug_mode: bool = False,
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

    if debug_mode:
        logger.info(
            f"  [GD_EDGE] home_total_gd={home_total_gd:.2f}/{home_gp}gp  "
            f"away_total_gd={away_total_gd:.2f}/{away_gp}gp  "
            f"expected_league_size={expected_league_size}"
        )

    if home_gp == 0 or away_gp == 0:
        raw["reason"] = "no_results"
        raw["dynamic_scale"] = None
        raw["m1_gd_dynamic_scale"] = None
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            logger.info("  [GD_EDGE] => SKIP (no_results) -> edge=0.0")
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
        expected_league_size=expected_league_size,
    )

    raw["dynamic_scale"] = scale
    raw["m1_gd_dynamic_scale"] = scale
    raw["scale_source"] = source
    raw["league_sample_size"] = sample_size
    raw["expected_league_size"] = expected_league_size

    if debug_mode:
        logger.info(
            f"  [GD_EDGE] home_gd/g={home_gd_per_game:.4f}  away_gd/g={away_gd_per_game:.4f}  "
            f"scale={scale}  scale_source={source}  league_samples={sample_size}"
        )

    if scale is None or scale <= 0:
        raw["reason"] = "missing_dynamic_scale"
        raw["edge_raw"] = 0.0
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            logger.info("  [GD_EDGE] => SKIP (missing_dynamic_scale) -> edge=0.0")
        return 0.0, raw

    edge_raw = (home_gd_per_game - away_gd_per_game) / scale
    edge = clamp(edge_raw)
    raw["edge_raw"] = edge_raw
    raw["final_edge_clamped"] = edge

    if debug_mode:
        logger.info(f"  [GD_EDGE] edge_raw={edge_raw:.4f}  edge_clamped={edge:.4f}")

    return edge, raw


def _calculate_consistency_edge(
    home_series: List[float],
    away_series: List[float],
    debug_mode: bool = False,
) -> Tuple[float, Dict[str, Any]]:
    """Component 3 - CONSISTENCY_EDGE (STD of game-GD across rolling windows)."""
    n = min(len(home_series), len(away_series))

    raw: Dict[str, Any] = {
        "home_game_gd": home_series,
        "away_game_gd": away_series,
    }

    if debug_mode:
        logger.info(f"  [CONSISTENCY_EDGE] n_comparable_games={n}  windows_step={_CONSISTENCY_WINDOW_STEP}")

    if n < _CONSISTENCY_WINDOW_STEP:
        raw["reason"] = "insufficient_games"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            logger.info(f"  [CONSISTENCY_EDGE] => SKIP (insufficient_games, need >={_CONSISTENCY_WINDOW_STEP}) -> edge=0.0")
        return 0.0, raw

    windows: List[int] = _build_cumulative_windows(n, _CONSISTENCY_WINDOW_STEP)
    if not windows:
        raw["reason"] = "no_valid_windows"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            logger.info("  [CONSISTENCY_EDGE] => SKIP (no_valid_windows) -> edge=0.0")
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
        window_edge = away_std - home_std
        consistency_edges_by_window[label] = window_edge
        if debug_mode:
            logger.info(
                f"  [CONSISTENCY_EDGE] {label}: home_std={home_std:.4f}  "
                f"away_std={away_std:.4f}  edge={window_edge:.4f}"
            )

    consistency_edge_raw_average = (
        sum(consistency_edges_by_window.values()) / len(consistency_edges_by_window)
        if consistency_edges_by_window
        else 0.0
    )
    final_edge_clamped = clamp(consistency_edge_raw_average)

    if debug_mode:
        logger.info(
            f"  [CONSISTENCY_EDGE] avg_raw={consistency_edge_raw_average:.4f}  "
            f"edge_clamped={final_edge_clamped:.4f}"
        )

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
    debug_mode: bool = False,
) -> Tuple[float, Dict[str, Any]]:
    """Component 4 - VOL_DIRECTION_EDGE (destroy/collapse power by window)."""
    n = min(len(home_series), len(away_series))

    raw: Dict[str, Any] = {}

    if debug_mode:
        logger.info(f"  [VOL_DIRECTION_EDGE] n_comparable_games={n}  batch_size={_BATCH_SIZE}")

    if n < _BATCH_SIZE:
        raw["reason"] = "insufficient_games"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            logger.info(f"  [VOL_DIRECTION_EDGE] => SKIP (insufficient_games, need >={_BATCH_SIZE}) -> edge=0.0")
        return 0.0, raw

    windows: List[int] = _build_cumulative_windows(n, _BATCH_SIZE)
    if not windows:
        raw["reason"] = "no_valid_windows"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            logger.info("  [VOL_DIRECTION_EDGE] => SKIP (no_valid_windows) -> edge=0.0")
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
        window_edge = home_net - away_net

        home_destroy_power_by_window[label] = home_destroy
        home_collapse_power_by_window[label] = home_collapse
        home_net_vol_by_window[label] = home_net
        away_destroy_power_by_window[label] = away_destroy
        away_collapse_power_by_window[label] = away_collapse
        away_net_vol_by_window[label] = away_net
        vol_direction_edges_by_window[label] = window_edge

        if debug_mode:
            logger.info(
                f"  [VOL_DIRECTION_EDGE] {label}: "
                f"home(destroy={home_destroy:.4f}, collapse={home_collapse:.4f}, net={home_net:.4f})  "
                f"away(destroy={away_destroy:.4f}, collapse={away_collapse:.4f}, net={away_net:.4f})  "
                f"edge={window_edge:.4f}"
            )

    vol_direction_edge_raw_average = (
        sum(vol_direction_edges_by_window.values()) / len(vol_direction_edges_by_window)
        if vol_direction_edges_by_window
        else 0.0
    )
    final_edge_clamped = clamp(vol_direction_edge_raw_average)

    if debug_mode:
        logger.info(
            f"  [VOL_DIRECTION_EDGE] avg_raw={vol_direction_edge_raw_average:.4f}  "
            f"edge_clamped={final_edge_clamped:.4f}"
        )

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
    expected_league_size = gd_raw.get("expected_league_size")
    league_sample_size = gd_raw.get("league_sample_size")

    if scale_source == "computed_league_standings" and expected_league_size and league_sample_size is not None:
        if league_sample_size >= expected_league_size:
            return "ACTIVE", "official_league_scale"
        return "DEGRADED", "insufficient_league_sample_size"
    if scale_source == "missing":
        return "INVALID_GD_SCALE", "missing_league_standings"
    if scale_source == "missing_expected_league_size":
        if gd_raw.get("dynamic_scale") is None:
            return "INVALID_GD_SCALE", "missing_expected_league_size"
        return "DEGRADED", "missing_expected_league_size"
    if scale_source == "incomplete_league_standings":
        return "DEGRADED", "incomplete_league_standings"

    return "DEGRADED", "non_official_gd_scale"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calculate_base_strength(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    """Calculate M1 - Base Strength for an event."""
    home_results: List[Dict] = getattr(streak_analysis, "home_team_results", None) or []
    away_results: List[Dict] = getattr(streak_analysis, "away_team_results", None) or []
    event_id: int = getattr(streak_analysis, "event_id", 0)
    participants: str = getattr(streak_analysis, "participants", "")
    competition_id = event_context.competition.competition_id
    competition_display_name = event_context.competition.display_name or event_context.competition.canonical_name
    competition_number_of_teams = event_context.competition.number_of_teams
    competition_number_of_teams_source = event_context.competition.number_of_teams_source
    total_regular_season_games = getattr(event_context.competition, "total_regular_season_games", None)
    standings_grouping = getattr(event_context.competition, "standings_grouping", None)
    league_config_source = getattr(event_context.competition, "league_config_source", None)
    expected_league_size = competition_number_of_teams

    home_gd_series = _extract_game_gd_series(home_results)
    away_gd_series = _extract_game_gd_series(away_results)

    if debug_mode:
        logger.info(
            f"--- M1 Base Strength Debug: Event {event_id} ({participants}) ---"
        )
        logger.info(
            f"  competition={competition_display_name} (id={competition_id})  "
            f"expected_league_size={expected_league_size}  "
            f"home_results={len(home_results)}  away_results={len(away_results)}"
        )
        logger.info(
            f"  home_gd_series ({len(home_gd_series)} games): {home_gd_series}"
        )
        logger.info(
            f"  away_gd_series ({len(away_gd_series)} games): {away_gd_series}"
        )

    result_edge, result_raw = _calculate_result_edge(home_results, away_results, debug_mode=debug_mode)
    gd_edge, gd_raw = _calculate_gd_edge(
        home_gd_series,
        away_gd_series,
        home_results,
        away_results,
        expected_league_size=expected_league_size,
        debug_mode=debug_mode,
    )
    consistency_edge, consistency_raw = _calculate_consistency_edge(
        home_gd_series,
        away_gd_series,
        debug_mode=debug_mode,
    )
    vol_direction_edge, vol_raw = _calculate_vol_direction_edge(
        home_gd_series,
        away_gd_series,
        debug_mode=debug_mode,
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

    if debug_mode:
        logger.info("  --- Component Summary ---")
        for c in components:
            logger.info(
                f"  {c.name}: edge={c.edge:.4f}  weight={c.weight}  weighted={c.weighted_edge:.4f}  "
                f"bias={c.bias}  strength={c.strength}"
            )
        logger.info(
            f"  M1 Final: base_value={base_value:.4f}  final_clamped={final_value:.4f}  "
            f"bias={calculate_bias(final_value)}  strength={classify_strength(final_value)}  "
            f"status={m1_status} ({m1_status_reason})"
        )
        logger.info("-" * 60)

    raw_audit: Dict[str, Any] = {
        "home_team": getattr(streak_analysis, "home_team_name", None),
        "away_team": getattr(streak_analysis, "away_team_name", None),
        "event_context_present": True,
        "context_status": "normalized",
        "competition_id": competition_id,
        "competition_display_name": competition_display_name,
        "competition_number_of_teams": competition_number_of_teams,
        "competition_number_of_teams_source": competition_number_of_teams_source,
        "total_regular_season_games": total_regular_season_games,
        "standings_grouping": standings_grouping,
        "league_config_source": league_config_source,
        "expected_league_size": expected_league_size,
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
