"""M1 - Base Strength module.

Measures the base competitive strength of each team during the season using
four components: RESULT_EDGE, GD_EDGE, CONSISTENCY_EDGE, and
VOL_DIRECTION_EDGE.

This module is pure: it receives a pre-built ``MatchupStreakContext`` and
returns a structured ``ModuleResult``. It never calls external APIs, sends
messages, or writes to the database.

v4.0 separates the sources by layer:
    - season records / standings drive RESULT_EDGE and GD_EDGE
    - game GD series drive CONSISTENCY_EDGE and VOL_DIRECTION_EDGE

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
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== M1_BASE_STRENGTH DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("M1_BASE_STRENGTH DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("M1_BASE_STRENGTH DEBUG | %s", name)
    logger.info("M1_BASE_STRENGTH DEBUG |   Formula: %s", formula)
    logger.info("M1_BASE_STRENGTH DEBUG |   Sustitución: %s", substitution)
    logger.info("M1_BASE_STRENGTH DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("M1_BASE_STRENGTH DEBUG |   Lectura: %s", meaning)


def _fmt(value: Any, decimals: int = 6) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isfinite(value):
            return f"{value:.{decimals}f}"
        return str(value)
    if isinstance(value, dict):
        items = list(value.items())
        preview = ", ".join(f"{k}: {_fmt(v, decimals)}" for k, v in items[:4])
        if len(items) > 4:
            preview += ", ..."
        return f"{{{preview}}} (n={len(items)})"
    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        preview = ", ".join(_fmt(item, decimals) for item in sequence[:5])
        if len(sequence) > 5:
            preview += ", ..."
        return f"[{preview}] (n={len(sequence)})"
    return str(value)


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
# Internal helpers - generic extraction
# ---------------------------------------------------------------------------

def _normalize_team_name(value: Optional[str]) -> str:
    """Normalize team names for tolerant, case-insensitive matching."""
    if value is None:
        return ""
    return " ".join(str(value).strip().split()).casefold()


def _extract_int_field(data: Optional[Dict], keys: Tuple[str, ...]) -> Optional[int]:
    """Extract an integer field without breaking valid zero values."""
    if not isinstance(data, dict):
        return None

    for key in keys:
        if key not in data:
            continue
        value = data.get(key)
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            continue
        if isinstance(value, str):
            text = value.strip()
            if not text:
                continue
            try:
                parsed = float(text)
            except ValueError:
                continue
            if parsed.is_integer():
                return int(parsed)
    return None


def _extract_float_field(data: Optional[Dict], keys: Tuple[str, ...]) -> Optional[float]:
    """Extract a float field without breaking valid zero values."""
    if not isinstance(data, dict):
        return None

    for key in keys:
        if key not in data:
            continue
        value = data.get(key)
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                continue
            try:
                return float(text)
            except ValueError:
                continue
    return None


def _count_wins(results: List[Dict]) -> int:
    """Count wins from form results (team_result_code == '1')."""
    return sum(1 for r in results if r.get("team_result_code") == "1")


def _try_extract_gd(game: Dict) -> Optional[float]:
    """Return a single game GD from *game*, or ``None``."""
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


def _pstdev(values: List[float]) -> float:
    """Population standard deviation (not sample)."""
    if not values:
        return 0.0
    n = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / n
    return math.sqrt(variance)


def _format_record_summary(record: Dict[str, Any]) -> str:
    return (
        f"source={record.get('source')} "
        f"W/GP/GD={record.get('wins')}/{record.get('gp')}/{record.get('goal_diff')}"
    )


# ---------------------------------------------------------------------------
# Internal helpers - standings / season records
# ---------------------------------------------------------------------------

def _extract_record_name(record: Any) -> Optional[str]:
    if not isinstance(record, dict):
        return None

    for key in ("team_name", "teamName", "name", "display_name", "short_name", "team"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            nested_name = value.get("name") or value.get("display_name") or value.get("short_name")
            if isinstance(nested_name, str) and nested_name.strip():
                return nested_name.strip()
    return None


def _standings_items(current_standings: Any) -> List[Tuple[Any, Any]]:
    """Return a flat list of ``(key, standing)`` pairs for dict/list payloads."""
    if isinstance(current_standings, dict):
        if "standings" in current_standings:
            return _standings_items(current_standings.get("standings"))

        items: List[Tuple[Any, Any]] = []
        for key, value in current_standings.items():
            if isinstance(value, dict):
                items.append((key, value))
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    if isinstance(item, dict):
                        items.append((f"{key}[{index}]", item))
        return items

    if isinstance(current_standings, list):
        items: List[Tuple[Any, Any]] = []
        for index, item in enumerate(current_standings):
            if not isinstance(item, dict):
                continue
            rows = item.get("rows")
            if isinstance(rows, list):
                for row_index, row in enumerate(rows):
                    if isinstance(row, dict):
                        items.append((f"{index}:{row_index}", row))
                continue
            items.append((index, item))
        return items

    return []


def _find_team_standing_in_current_standings(
    current_standings: Any,
    team_name: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Find a team standing by name across dict/list standings payloads."""
    normalized_team_name = _normalize_team_name(team_name)
    if not normalized_team_name:
        return None

    if isinstance(current_standings, dict):
        direct_record = current_standings.get(team_name)
        if isinstance(direct_record, dict):
            return direct_record

    for key, record in _standings_items(current_standings):
        if not isinstance(record, dict):
            continue

        if _normalize_team_name(str(key)) == normalized_team_name:
            return record

        record_name = _extract_record_name(record)
        if _normalize_team_name(record_name) == normalized_team_name:
            return record

    return None


def _collect_unique_standings(payload: Any) -> List[Dict[str, Any]]:
    """Collect unique standing records by team name while preserving the best row."""
    best_by_team: Dict[str, Tuple[Tuple[int, int, int, int], Dict[str, Any]]] = {}

    for key, record in _standings_items(payload):
        if not isinstance(record, dict):
            continue

        key_name = str(key).strip() if key is not None else ""
        record_name = _extract_record_name(record)
        normalized_name = _normalize_team_name(key_name or record_name)
        if not normalized_name:
            continue

        wins = _extract_int_field(record, ("wins", "w"))
        gp = _extract_int_field(record, ("gp", "games_played", "matches", "played"))
        goal_diff = _extract_float_field(record, ("goal_diff", "diff"))
        score = (
            1 if wins is not None else 0,
            1 if gp is not None else 0,
            1 if goal_diff is not None else 0,
            gp if gp is not None else -1,
        )

        existing = best_by_team.get(normalized_name)
        if existing is None or score > existing[0]:
            best_by_team[normalized_name] = (score, record)

    return [record for _, record in best_by_team.values()]


def _extract_gd_scale_samples(standings_list: List[Dict]) -> List[float]:
    """Return absolute GD-per-game samples used for the P75 dynamic scale."""
    gd_per_game_abs: List[float] = []
    for standing in standings_list:
        gd = _extract_float_field(standing, ("goal_diff", "diff"))
        gp = _extract_int_field(standing, ("gp", "games_played", "matches", "played"))
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


def _build_fallback_season_record_from_results(results: List[Dict]) -> Dict[str, Any]:
    """Build a season record from game results when standings are unavailable."""
    if not results:
        return {
            "wins": None,
            "gp": None,
            "goal_diff": None,
            "source": "missing",
            "raw_standing": None,
        }

    goal_diff_values = [_try_extract_gd(game) for game in results]
    goal_diff = sum(value for value in goal_diff_values if value is not None)
    return {
        "wins": _count_wins(results),
        "gp": len(results),
        "goal_diff": float(goal_diff),
        "source": "fallback_results",
        "raw_standing": None,
    }


def _select_best_result_standing(results: List[Dict]) -> Optional[Dict[str, Any]]:
    """Return the best team standing snapshot found inside game results."""
    best_standing: Optional[Dict[str, Any]] = None
    best_score: Tuple[int, int, int, int] = (-1, -1, -1, -1)

    for game in results:
        if not isinstance(game, dict):
            continue
        for key in ("team_standing", "standing", "current_standing"):
            standing = game.get(key)
            if not isinstance(standing, dict):
                continue

            wins = _extract_int_field(standing, ("wins", "w"))
            gp = _extract_int_field(standing, ("gp", "games_played", "matches", "played"))
            goal_diff = _extract_float_field(standing, ("goal_diff", "diff"))
            score = (
                1 if wins is not None else 0,
                1 if gp is not None else 0,
                1 if goal_diff is not None else 0,
                gp if gp is not None else -1,
            )
            if score > best_score:
                best_score = score
                best_standing = standing

    return best_standing


def _extract_team_name_from_results(results: List[Dict]) -> Optional[str]:
    for game in results:
        if not isinstance(game, dict):
            continue
        for key in ("team_name", "team", "name", "display_name"):
            value = game.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        standing = game.get("team_standing")
        if isinstance(standing, dict):
            standing_name = _extract_record_name(standing)
            if standing_name:
                return standing_name
    return None


def _normalize_season_record(raw_standing: Optional[Dict[str, Any]], source: str) -> Dict[str, Any]:
    """Normalize a standing snapshot into the season record schema."""
    wins = _extract_int_field(raw_standing, ("wins", "w"))
    gp = _extract_int_field(raw_standing, ("gp", "games_played", "matches", "played"))
    goal_diff = _extract_float_field(raw_standing, ("goal_diff", "diff"))

    return {
        "wins": wins,
        "gp": gp,
        "goal_diff": goal_diff,
        "source": source,
        "raw_standing": raw_standing,
    }


def _season_record_has_required_result_data(record: Dict[str, Any]) -> bool:
    wins = record.get("wins")
    gp = record.get("gp")
    return wins is not None and gp is not None and gp > 0


def _season_record_has_required_gd_data(record: Dict[str, Any]) -> bool:
    gp = record.get("gp")
    goal_diff = record.get("goal_diff")
    return gp is not None and gp > 0 and goal_diff is not None


def _extract_team_season_record(
    streak_analysis: Any,
    side: str,
    fallback_results: List[Dict],
) -> Dict[str, Any]:
    """Extract a season record from the best available source for one side."""
    direct_attr = f"{side}_team_current_standing"
    team_name_attr = f"{side}_team_name"

    direct_current_standing = getattr(streak_analysis, direct_attr, None)
    team_name = getattr(streak_analysis, team_name_attr, None) or _extract_team_name_from_results(fallback_results)
    current_standings = getattr(streak_analysis, "current_standings", None)

    candidates: List[Tuple[str, Optional[Dict[str, Any]]]] = [
        ("direct_current_standing", direct_current_standing if isinstance(direct_current_standing, dict) else None),
        (
            "current_standings",
            _find_team_standing_in_current_standings(current_standings, team_name),
        ),
        ("results_team_standing", _select_best_result_standing(fallback_results)),
    ]

    for source, raw_standing in candidates:
        if not isinstance(raw_standing, dict):
            continue
        record = _normalize_season_record(raw_standing, source)
        if (
            record["gp"] is not None
            and record["gp"] > 0
            and (
                record["wins"] is not None
                or record["goal_diff"] is not None
            )
        ):
            return record

    return _build_fallback_season_record_from_results(fallback_results)


# ---------------------------------------------------------------------------
# GD dynamic scale
# ---------------------------------------------------------------------------

def _resolve_gd_dynamic_scale(
    streak_analysis: Any,
    home_results: List[Dict],
    away_results: List[Dict],
    expected_league_size: Optional[int] = None,
) -> Tuple[Optional[float], str, int]:
    """Determine the GD dynamic scale using the official v4 chain."""

    def _resolve_from_payload(payload: Any, source_name: str) -> Tuple[Optional[float], str, int]:
        standings_list = _collect_unique_standings(payload)
        gd_scale_samples = _extract_gd_scale_samples(standings_list)
        sample_size = len(gd_scale_samples)
        if sample_size == 0:
            return None, "missing", 0

        scale = _calculate_p75_dynamic_scale(standings_list)
        if scale is None or scale <= 0:
            return None, "missing", sample_size

        if expected_league_size is not None and sample_size < expected_league_size:
            return scale, "incomplete_league_standings", sample_size
        return scale, source_name, sample_size

    current_standings = getattr(streak_analysis, "current_standings", None)
    scale, source, sample_size = _resolve_from_payload(current_standings, "current_standings")
    if scale is not None:
        return scale, source, sample_size

    standings_response = getattr(streak_analysis, "standings_response", None)
    scale, source, sample_size = _resolve_from_payload(standings_response, "standings_response")
    if scale is not None:
        return scale, source, sample_size

    fallback_payload = _gather_full_league_standings_from_results(home_results, away_results)
    scale, source, sample_size = _resolve_from_payload(fallback_payload, "results_snapshots")
    if scale is not None:
        return scale, source, sample_size

    return None, "missing", 0


def _gather_full_league_standings_from_results(
    home_results: List[Dict],
    away_results: List[Dict],
) -> List[Dict]:
    """Collect standing snapshots from the result payloads."""
    best_by_team: Dict[str, Tuple[Tuple[int, int, int, int], Dict[str, Any]]] = {}

    def _process(team_key: Optional[str], snapshot: Optional[Dict]) -> None:
        if not team_key or not snapshot or not isinstance(snapshot, dict):
            return

        normalized_team = _normalize_team_name(team_key)
        if not normalized_team:
            return

        wins = _extract_int_field(snapshot, ("wins", "w"))
        gp = _extract_int_field(snapshot, ("gp", "games_played", "matches", "played"))
        goal_diff = _extract_float_field(snapshot, ("goal_diff", "diff"))
        score = (
            1 if wins is not None else 0,
            1 if gp is not None else 0,
            1 if goal_diff is not None else 0,
            gp if gp is not None else -1,
        )

        existing = best_by_team.get(normalized_team)
        if existing is None or score > existing[0]:
            best_by_team[normalized_team] = (score, snapshot)

    for game in home_results:
        _process(game.get("team_name"), game.get("team_standing"))
        _process(game.get("opponent_name"), game.get("opponent_standing"))
    for game in away_results:
        _process(game.get("team_name"), game.get("team_standing"))
        _process(game.get("opponent_name"), game.get("opponent_standing"))

    return [record for _, record in best_by_team.values()]


# ---------------------------------------------------------------------------
# Component calculators
# ---------------------------------------------------------------------------

def _calculate_result_edge(
    home_record: Dict[str, Any],
    away_record: Dict[str, Any],
    debug_mode: bool = False,
) -> Tuple[float, Dict[str, Any]]:
    """Component 1 - RESULT_EDGE (win-rate differential)."""
    home_wins = home_record.get("wins")
    away_wins = away_record.get("wins")
    home_gp = home_record.get("gp")
    away_gp = away_record.get("gp")

    raw: Dict[str, Any] = {
        "home_wins": home_wins,
        "away_wins": away_wins,
        "home_gp": home_gp,
        "away_gp": away_gp,
        "home_record_source": home_record.get("source"),
        "away_record_source": away_record.get("source"),
    }

    if debug_mode:
        _debug_section("RESULT_EDGE")
        _debug_line("home_record: %s", _format_record_summary(home_record))
        _debug_line("away_record: %s", _format_record_summary(away_record))

    if (
        home_wins is None
        or away_wins is None
        or home_gp is None
        or away_gp is None
        or home_gp <= 0
        or away_gp <= 0
    ):
        raw["reason"] = "missing_season_record"
        raw["home_win_rate"] = None
        raw["away_win_rate"] = None
        raw["edge_raw"] = 0.0
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            _debug_line("=> SKIP (missing_season_record) -> edge=0.0")
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
        _debug_formula(
            "WIN_RATE_HOME",
            "WIN_RATE_HOME = W_HOME / GP_HOME",
            f"{home_wins} / {home_gp}",
            _fmt(home_win_rate),
            "Frecuencia con la que HOME transforma partidos en victorias."
        )
        _debug_formula(
            "WIN_RATE_AWAY",
            "WIN_RATE_AWAY = W_AWAY / GP_AWAY",
            f"{away_wins} / {away_gp}",
            _fmt(away_win_rate),
            "Frecuencia con la que AWAY transforma partidos en victorias."
        )
        _debug_formula(
            "RESULT_EDGE_RAW",
            "RESULT_EDGE = WIN_RATE_HOME - WIN_RATE_AWAY",
            f"{_fmt(home_win_rate)} - {_fmt(away_win_rate)}",
            f"{edge_raw:+.12f}",
            "Diferencial crudo de tasa de victorias."
        )
        _debug_formula(
            "RESULT_EDGE_CLAMPED",
            "RESULT_EDGE = clamp(RESULT_EDGE_RAW)",
            f"clamp({_fmt(edge_raw)})",
            f"{edge:+.12f}",
            f"RESULT_BIAS = {calculate_bias(edge)} | RESULT_STRENGTH = {classify_strength(edge)}"
        )

    return edge, raw


def _calculate_gd_edge(
    home_record: Dict[str, Any],
    away_record: Dict[str, Any],
    streak_analysis: Any,
    home_results: List[Dict],
    away_results: List[Dict],
    expected_league_size: Optional[int] = None,
    debug_mode: bool = False,
) -> Tuple[float, Dict[str, Any]]:
    """Component 2 - GD_EDGE (goal-difference per game, dynamically scaled)."""
    home_total_gd = home_record.get("goal_diff")
    away_total_gd = away_record.get("goal_diff")
    home_gp = home_record.get("gp")
    away_gp = away_record.get("gp")

    raw: Dict[str, Any] = {
        "home_total_gd": home_total_gd,
        "away_total_gd": away_total_gd,
        "home_gp": home_gp,
        "away_gp": away_gp,
        "home_record_source": home_record.get("source"),
        "away_record_source": away_record.get("source"),
        "expected_league_size": expected_league_size,
    }

    scale, source, sample_size = _resolve_gd_dynamic_scale(
        streak_analysis,
        home_results,
        away_results,
        expected_league_size=expected_league_size,
    )

    raw["dynamic_scale"] = scale
    raw["m1_gd_dynamic_scale"] = scale
    raw["scale_source"] = source
    raw["league_sample_size"] = sample_size

    if debug_mode:
        _debug_section("GD_EDGE")
        _debug_line("home_record: %s", _format_record_summary(home_record))
        _debug_line("away_record: %s", _format_record_summary(away_record))
        _debug_line("Resolving GD Dynamic Scale. Source: %s. Sample size: %s. Expected size: %s", source, sample_size, expected_league_size)

        current_standings = getattr(streak_analysis, "current_standings", None)
        if current_standings is None:
            current_standings = getattr(streak_analysis, "standings_response", None)
        if current_standings is not None:
            standings_list = _collect_unique_standings(current_standings)
        else:
            standings_list = _gather_full_league_standings_from_results(home_results, away_results)
        
        gd_scale_samples = _extract_gd_scale_samples(standings_list)
        gd_scale_samples.sort()
        _debug_line("Ordered absolute GD per game samples (n=%s): %s", len(gd_scale_samples), _fmt(gd_scale_samples))
        if gd_scale_samples:
            pos = math.ceil(0.75 * len(gd_scale_samples))
            idx = pos - 1
            _debug_line("P75 Position: %s (index %s)", pos, idx)

    if (
        home_total_gd is None
        or away_total_gd is None
        or home_gp is None
        or away_gp is None
        or home_gp <= 0
        or away_gp <= 0
    ):
        raw["reason"] = "missing_season_record"
        raw["home_gd_per_game"] = None
        raw["away_gd_per_game"] = None
        raw["edge_raw"] = 0.0
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            _debug_line("=> SKIP (missing_season_record) -> edge=0.0")
        return 0.0, raw

    if scale is None or scale <= 0:
        raw["reason"] = "missing_dynamic_scale"
        raw["home_gd_per_game"] = float(home_total_gd) / float(home_gp)
        raw["away_gd_per_game"] = float(away_total_gd) / float(away_gp)
        raw["edge_raw"] = 0.0
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            _debug_line("=> SKIP (missing_dynamic_scale) -> edge=0.0")
        return 0.0, raw

    home_gd_per_game = float(home_total_gd) / float(home_gp)
    away_gd_per_game = float(away_total_gd) / float(away_gp)
    edge_raw = (home_gd_per_game - away_gd_per_game) / scale
    edge = clamp(edge_raw)

    raw["home_gd_per_game"] = home_gd_per_game
    raw["away_gd_per_game"] = away_gd_per_game
    raw["edge_raw"] = edge_raw
    raw["final_edge_clamped"] = edge

    if debug_mode:
        _debug_formula(
            "GD_PER_GAME_HOME",
            "GD_PER_GAME_HOME = GD_HOME / GP_HOME",
            f"{home_total_gd} / {home_gp}",
            _fmt(home_gd_per_game),
            "Diferencial de goles promedio de HOME por partido."
        )
        _debug_formula(
            "GD_PER_GAME_AWAY",
            "GD_PER_GAME_AWAY = GD_AWAY / GP_AWAY",
            f"{away_total_gd} / {away_gp}",
            _fmt(away_gd_per_game),
            "Diferencial de goles promedio de AWAY por partido."
        )
        _debug_formula(
            "GD_EDGE_RAW",
            "GD_EDGE_RAW = (GD_PER_GAME_HOME - GD_PER_GAME_AWAY) / M1_GD_DYNAMIC_SCALE",
            f"({_fmt(home_gd_per_game)} - ({_fmt(away_gd_per_game)})) / {_fmt(scale)}",
            f"{edge_raw:+.12f}",
            "Margen competitivo relativo normalizado."
        )
        _debug_formula(
            "GD_EDGE_CLAMPED",
            "GD_EDGE = clamp(GD_EDGE_RAW)",
            f"clamp({_fmt(edge_raw)})",
            f"{edge:+.12f}",
            f"GD_BIAS = {calculate_bias(edge)} | GD_STRENGTH = {classify_strength(edge)}"
        )

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
        _debug_section("CONSISTENCY_EDGE")
        _debug_line("n_comparable_games = min(len(home_series), len(away_series))")
        _debug_line("  n_comparable_games = min(%s, %s) = %s", len(home_series), len(away_series), n)
        _debug_line("  window_step = %s", _CONSISTENCY_WINDOW_STEP)

    if n < _CONSISTENCY_WINDOW_STEP:
        raw["reason"] = "insufficient_games"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            _debug_line("=> SKIP (insufficient_games, need >=%s) -> edge=0.0", _CONSISTENCY_WINDOW_STEP)
        return 0.0, raw

    windows: List[int] = _build_cumulative_windows(n, _CONSISTENCY_WINDOW_STEP)
    if not windows:
        raw["reason"] = "no_valid_windows"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            _debug_line("=> SKIP (no_valid_windows) -> edge=0.0")
        return 0.0, raw

    home_window_stds: Dict[str, float] = {}
    away_window_stds: Dict[str, float] = {}
    consistency_edges_by_window: Dict[str, float] = {}

    if debug_mode:
        _debug_line("Suma y promedio por ventana:")
        _debug_line("  Ventana\tSTD HOME\tSTD AWAY\tEdge (AWAY - HOME)")

    for window_size in windows:
        label = f"L{window_size}"
        home_std = _pstdev(home_series[:window_size])
        away_std = _pstdev(away_series[:window_size])
        home_window_stds[label] = home_std
        away_window_stds[label] = away_std
        window_edge = away_std - home_std
        consistency_edges_by_window[label] = window_edge
        if debug_mode:
            _debug_line("  %s\t%f\t%f\t%+f", label, home_std, away_std, window_edge)

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

    if debug_mode:
        _debug_formula(
            "CONSISTENCY_EDGE_RAW_AVERAGE",
            "CONSISTENCY_EDGE_RAW_AVERAGE = sum(window_edges) / count",
            f"{sum(consistency_edges_by_window.values()):.12f} / {len(consistency_edges_by_window)}",
            _fmt(consistency_edge_raw_average),
            "Promedio de la dispersión de diferencias de goles."
        )
        _debug_formula(
            "CONSISTENCY_EDGE_CLAMPED",
            "CONSISTENCY_EDGE = clamp(CONSISTENCY_EDGE_RAW_AVERAGE)",
            f"clamp({_fmt(consistency_edge_raw_average)})",
            f"{final_edge_clamped:+.12f}",
            f"CONSISTENCY_BIAS = {calculate_bias(final_edge_clamped)} | CONSISTENCY_STRENGTH = {classify_strength(final_edge_clamped)}"
        )

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
        _debug_section("VOL_DIRECTION_EDGE")
        _debug_line("n_comparable_games = min(len(home_series), len(away_series))")
        _debug_line("  n_comparable_games = min(%s, %s) = %s", len(home_series), len(away_series), n)
        _debug_line("  batch_size = %s", _BATCH_SIZE)

    if n < _BATCH_SIZE:
        raw["reason"] = "insufficient_games"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            _debug_line("=> SKIP (insufficient_games, need >=%s) -> edge=0.0", _BATCH_SIZE)
        return 0.0, raw

    windows: List[int] = _build_cumulative_windows(n, _BATCH_SIZE)
    if not windows:
        raw["reason"] = "no_valid_windows"
        raw["available_games"] = n
        raw["final_edge_clamped"] = 0.0
        if debug_mode:
            _debug_line("=> SKIP (no_valid_windows) -> edge=0.0")
        return 0.0, raw

    def _powers(series: List[float]) -> Tuple[float, float, float]:
        positive_values = [value for value in series if value > 0]
        negative_values = [abs(value) for value in series if value < 0]
        destroy_power = sum(positive_values) / len(positive_values) if positive_values else 0.0
        collapse_power = sum(negative_values) / len(negative_values) if negative_values else 0.0
        net_vol = destroy_power - collapse_power
        return destroy_power, collapse_power, net_vol

    home_destroy_power_by_window: Dict[str, float] = {}
    home_collapse_power_by_window: Dict[str, float] = {}
    home_net_vol_by_window: Dict[str, float] = {}
    away_destroy_power_by_window: Dict[str, float] = {}
    away_collapse_power_by_window: Dict[str, float] = {}
    away_net_vol_by_window: Dict[str, float] = {}
    vol_direction_edges_by_window: Dict[str, float] = {}

    if debug_mode:
        _debug_line("Suma y promedio por ventana:")
        _debug_line("  Ventana\tNET HOME (D - C)\tNET AWAY (D - C)\tEdge (HOME - AWAY)")

    for window_size in windows:
        label = f"L{window_size}"
        home_destroy, home_collapse, home_net = _powers(home_series[:window_size])
        away_destroy, away_collapse, away_net = _powers(away_series[:window_size])
        window_edge = home_net - away_net

        home_destroy_power_by_window[label] = home_destroy
        home_collapse_power_by_window[label] = home_collapse
        home_net_vol_by_window[label] = home_net
        away_destroy_power_by_window[label] = away_destroy
        away_collapse_power_by_window[label] = away_collapse
        away_net_vol_by_window[label] = away_net
        vol_direction_edges_by_window[label] = window_edge

        if debug_mode:
            _debug_line(
                "  %s\t%f (%f - %f)\t%f (%f - %f)\t%+f",
                label,
                home_net,
                home_destroy,
                home_collapse,
                away_net,
                away_destroy,
                away_collapse,
                window_edge,
            )

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

    if debug_mode:
        _debug_formula(
            "VOL_DIRECTION_EDGE_RAW_AVERAGE",
            "VOL_DIRECTION_EDGE_RAW_AVERAGE = sum(window_edges) / count",
            f"{sum(vol_direction_edges_by_window.values()):.12f} / {len(vol_direction_edges_by_window)}",
            _fmt(vol_direction_edge_raw_average),
            "Promedio de la dirección de volatilidad de goles."
        )
        _debug_formula(
            "VOL_DIRECTION_EDGE_CLAMPED",
            "VOL_DIRECTION_EDGE = clamp(VOL_DIRECTION_EDGE_RAW_AVERAGE)",
            f"clamp({_fmt(vol_direction_edge_raw_average)})",
            f"{final_edge_clamped:+.12f}",
            f"VOL_DIRECTION_BIAS = {calculate_bias(final_edge_clamped)} | VOL_DIRECTION_STRENGTH = {classify_strength(final_edge_clamped)}"
        )

    return final_edge_clamped, raw


def _determine_m1_status(
    home_record: Dict[str, Any],
    away_record: Dict[str, Any],
    result_raw: Dict[str, Any],
    gd_raw: Dict[str, Any],
    consistency_raw: Dict[str, Any],
    vol_raw: Dict[str, Any],
) -> Tuple[str, str]:
    """Derive a v4 status label and reason for the module output."""
    if (
        not _season_record_has_required_result_data(home_record)
        or not _season_record_has_required_result_data(away_record)
        or not _season_record_has_required_gd_data(home_record)
        or not _season_record_has_required_gd_data(away_record)
        or result_raw.get("reason") == "missing_season_record"
        or gd_raw.get("reason") == "missing_season_record"
        or consistency_raw.get("reason") == "insufficient_games"
        or vol_raw.get("reason") == "insufficient_games"
    ):
        return "INSUFFICIENT_DATA", "missing_season_record_or_short_series"

    scale_source = gd_raw.get("scale_source")
    expected_league_size = gd_raw.get("expected_league_size")
    league_sample_size = gd_raw.get("league_sample_size")

    if scale_source in (None, "missing"):
        return "INVALID_GD_SCALE", "missing_league_standings"

    if scale_source == "current_standings":
        if expected_league_size is None:
            return "DEGRADED", "missing_expected_league_size"
        if league_sample_size is None or league_sample_size < expected_league_size:
            return "DEGRADED", "incomplete_league_standings"
        return "ACTIVE", "official_league_scale"

    if scale_source == "incomplete_league_standings":
        return "DEGRADED", "incomplete_league_standings"

    if scale_source in ("standings_response", "results_snapshots"):
        return "DEGRADED", "fallback_league_scale"

    if gd_raw.get("dynamic_scale") is None:
        return "INVALID_GD_SCALE", "missing_league_standings"

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

    home_team_name = getattr(streak_analysis, "home_team_name", None)
    away_team_name = getattr(streak_analysis, "away_team_name", None)

    home_record = _extract_team_season_record(streak_analysis, "home", home_results)
    away_record = _extract_team_season_record(streak_analysis, "away", away_results)

    home_gd_series = _extract_game_gd_series(home_results)
    away_gd_series = _extract_game_gd_series(away_results)

    if debug_mode:
        _debug_section("INICIO")
        _debug_line("Event ID: %s", _fmt(event_id))
        _debug_line("Participantes: %s", participants or "N/A")
        _debug_line("Home team: %s", home_team_name or "N/A")
        _debug_line("Away team: %s", away_team_name or "N/A")
        _debug_line("Competition: %s (ID=%s)", competition_display_name, competition_id)
        _debug_line("Expected League Size: %s", expected_league_size)

        _debug_section("INPUTS BASE")
        _debug_line("HOME season record extracted: wins=%s, gp=%s, goal_diff=%s, source=%s",
                    _fmt(home_record.get("wins")), _fmt(home_record.get("gp")), _fmt(home_record.get("goal_diff")), home_record.get("source"))
        _debug_line("AWAY season record extracted: wins=%s, gp=%s, goal_diff=%s, source=%s",
                    _fmt(away_record.get("wins")), _fmt(away_record.get("gp")), _fmt(away_record.get("goal_diff")), away_record.get("source"))
        _debug_line("HOME game GD series: count=%s, values=%s", len(home_gd_series), _fmt(home_gd_series))
        _debug_line("AWAY game GD series: count=%s, values=%s", len(away_gd_series), _fmt(away_gd_series))

    result_edge, result_raw = _calculate_result_edge(home_record, away_record, debug_mode=debug_mode)
    gd_edge, gd_raw = _calculate_gd_edge(
        home_record,
        away_record,
        streak_analysis,
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
        home_record,
        away_record,
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
        _debug_section("FORMULA FINAL M1")
        _debug_formula(
            "M1_EDGE_RAW",
            "M1_EDGE_RAW = 0.35(RESULT_EDGE) + 0.35(GD_EDGE) + 0.15(CONSISTENCY_EDGE) + 0.15(VOL_DIRECTION_EDGE)",
            f"0.35 * ({_fmt(result_edge)}) + 0.35 * ({_fmt(gd_edge)}) + 0.15 * ({_fmt(consistency_edge)}) + 0.15 * ({_fmt(vol_direction_edge)})",
            _fmt(base_value),
            "Suma ponderada de las ventajas de los 4 componentes."
        )
        _debug_formula(
            "M1_EDGE_FINAL",
            "M1_EDGE = clamp(M1_EDGE_RAW)",
            f"clamp({_fmt(base_value)})",
            f"{final_value:+.12f}",
            f"M1_BIAS = {calculate_bias(final_value)} | M1_STRENGTH = {classify_strength(final_value)} | M1_STATUS = {m1_status}"
        )
        
        _debug_section("STRENGTH CLASSIFICATION")
        _debug_line("Nivel de magnitud:")
        _debug_line("  <0.05       -> IGNORE")
        _debug_line("  0.05 - 0.15 -> LOW")
        _debug_line("  0.15 - 0.30 -> MEDIUM")
        _debug_line("  0.30 - 0.60 -> HIGH")
        _debug_line("  >0.60       -> EXTREME")
        _debug_line("Aplicación: ABS_EDGE = %s -> M1_STRENGTH = %s", _fmt(abs(final_value)), classify_strength(final_value))

        _debug_section("OUTPUT FINAL")
        _debug_line("M1_EDGE = %+f", final_value)
        _debug_line("M1_ABS_EDGE = %f", abs(final_value))
        _debug_line("M1_BIAS = %s", calculate_bias(final_value))
        _debug_line("M1_STRENGTH = %s", classify_strength(final_value))
        _debug_line("M1_STATUS = %s (Reason: %s)", m1_status, m1_status_reason)
        _debug_line("")
        _debug_line("Submódulos:")
        _debug_line("  RESULT_EDGE = %+f", result_edge)
        _debug_line("  GD_EDGE = %+f", gd_edge)
        _debug_line("  CONSISTENCY_EDGE = %+f", consistency_edge)
        _debug_line("  VOL_DIRECTION_EDGE = %+f", vol_direction_edge)
        _debug_line("")
        _debug_line("Escala dinámica:")
        _debug_line("  M1_GD_DYNAMIC_SCALE = %s", _fmt(gd_raw.get("dynamic_scale")))
        
        _debug_section("LECTURA CORTA PARA NOTES")
        bias_team = home_team_name if final_value > 0 else (away_team_name if final_value < 0 else "Ninguno")
        _debug_line("M1 favorece claramente a %s.", bias_team)
        _debug_line("La ventaja no viene solo de ganar más partidos.")
        _debug_line("Viene sobre todo de:")
        _debug_line("  1) mejor diferencial de goles contra la escala real de la liga")
        _debug_line("  2) menor dispersión en resultados")
        _debug_line("  3) mejor relación entre picos positivos y caídas")
        
        _debug_section("ESTADO FINAL")

    raw_audit: Dict[str, Any] = {
        "home_team": home_team_name,
        "away_team": away_team_name,
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
        "home_season_record": home_record,
        "away_season_record": away_record,
        "m1_edge": final_value,
        "m1_abs_edge": abs(final_value),
        "m1_bias": calculate_bias(final_value),
        "m1_strength": classify_strength(final_value),
        "m1_status": m1_status,
        "m1_status_reason": m1_status_reason,
        "m1_gd_dynamic_scale": gd_raw.get("dynamic_scale"),
        "m1_gd_dynamic_scale_source": gd_raw.get("scale_source"),
        "result_edge": result_edge,
        "gd_edge": gd_edge,
        "consistency_edge": consistency_edge,
        "vol_direction_edge": vol_direction_edge,
        "m1_component_contributions": {
            "result": result_edge * _WEIGHT_RESULT_EDGE,
            "gd": gd_edge * _WEIGHT_GD_EDGE,
            "consistency": consistency_edge * _WEIGHT_CONSISTENCY_EDGE,
            "vol_direction": vol_direction_edge * _WEIGHT_VOL_DIRECTION_EDGE,
        },
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
