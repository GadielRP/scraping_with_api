"""P1 Totals directional engine with separated variance handling.

Pure totals profile engine for Pillar 1. It consumes matchup streak context
and normalized event context, then returns a structured OVER/UNDER profile.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

from modules.pillars.common import clamp
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)

_ENGINE_VERSION = "p1_totals_directional_vol_separated_v2_2"

TOTALS_TEMPORAL_WINDOW_NAMES = ("SHORT", "RECENT", "MID", "FULL")

SHORT_RATIO = 0.15
RECENT_RATIO = 0.35
MID_RATIO = 0.60
FULL_RATIO = 1.00

WINDOW_BASE_WEIGHT_SHORT = 0.30
WINDOW_BASE_WEIGHT_RECENT = 0.35
WINDOW_BASE_WEIGHT_MID = 0.20
WINDOW_BASE_WEIGHT_FULL = 0.15

W_STRUCTURAL = 0.45
W_TEMPORAL = 0.30
W_TREND = 0.25

IGNORE_THRESHOLD = 0.05

TREND_SHORT_WEIGHT_SHORT = 0.60
TREND_SHORT_WEIGHT_RECENT = 0.40
TREND_LONG_WEIGHT_MID = 0.60
TREND_LONG_WEIGHT_FULL = 0.40

TOTALS_TEMPORAL_WINDOW_CONFIG = {
    "SHORT": {"ratio": SHORT_RATIO, "base_weight": WINDOW_BASE_WEIGHT_SHORT},
    "RECENT": {"ratio": RECENT_RATIO, "base_weight": WINDOW_BASE_WEIGHT_RECENT},
    "MID": {"ratio": MID_RATIO, "base_weight": WINDOW_BASE_WEIGHT_MID},
    "FULL": {"ratio": FULL_RATIO, "base_weight": WINDOW_BASE_WEIGHT_FULL},
}


@dataclass(frozen=True)
class P1TotalsLayerOutput:
    layer: str
    status: str
    raw_signal: float
    final_signal: float
    weight: float
    weighted_signal: float
    ignored_reason: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class P1TotalsOutput:
    pillar_id: str
    module_id: str
    module_name: str
    engine_version: str

    event_id: int
    participants: str
    status: str
    status_reason: Optional[str]

    P1_TOTALS_DIRECTIONAL_SCORE: float
    P1_TOTALS_DIRECTION: str
    P1_TOTALS_STRENGTH: str
    P1_TOTALS_VARIANCE_STATE: str
    P1_TOTALS_INTERNAL_STATE: Dict[str, Any]

    P1_TOTALS_STRUCTURAL_SCORE: float
    STRUCTURAL_PROFILE_SCORE: float
    STRUCTURAL_ANCHOR: float

    VOL_EDGE: Optional[float]

    TEMPORAL_PROFILE_SCORE: float
    TEMPORAL_FINAL: float

    MATCHUP_TREND_DELTA: float
    TREND_BASELINE: float
    TREND_DYNAMIC_SCALE: float
    TREND_SIGNAL: float
    TREND_FINAL: float

    EXPECTED_TOTAL_STRUCTURAL: float
    MATCHUP_VOLATILITY: Optional[float]
    MATCHUP_TEMPORAL_TOTAL: float

    LEAGUE_MATCH_TOTAL_BASELINE: float
    TOTAL_DYNAMIC_SCALE: float
    VOL_BASELINE: Optional[float]
    VOL_DYNAMIC_SCALE: Optional[float]
    VOL_EDGE_P50: Optional[float]
    VOL_EDGE_P75: Optional[float]
    VOL_EDGE_P90: Optional[float]

    ACTIVE_WEIGHT_SUM: float

    ALIGNMENT_SCORE: float
    OVER_COUNT: int
    UNDER_COUNT: int
    IGNORE_COUNT: int
    ACTIVE_LAYER_COUNT: int

    STRUCTURAL_STATUS: str
    TEMPORAL_STATUS: str
    TREND_STATUS: str

    WINDOWS_USED: Dict[str, int]
    WINDOW_COMPLETENESS_BY_WINDOW: Dict[str, float]

    active_layers: List[P1TotalsLayerOutput] = field(default_factory=list)
    ignored_layers: List[P1TotalsLayerOutput] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)


def _clamp_01(value: float) -> float:
    return clamp(value, 0.0, 1.0)


def _sign(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _is_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(numeric)


def _to_float(value: Any) -> Optional[float]:
    if not _is_number(value):
        return None
    return float(value)


def _to_int(value: Any) -> Optional[int]:
    if not _is_number(value):
        return None
    return int(float(value))


def _round_half_up(value: float) -> int:
    return int(Decimal(str(value)).to_integral_value(rounding=ROUND_HALF_UP))


def _resolve_window_size_from_ratio(total_season_games: int, ratio: float) -> int:
    return _round_half_up(total_season_games * ratio)


def _percentile_cont(values: List[float], percentile: float) -> Optional[float]:
    numeric_values = [float(value) for value in values if _is_number(value)]
    if not numeric_values:
        return None

    ordered = sorted(numeric_values)
    n_values = len(ordered)
    if n_values == 1:
        return ordered[0]

    bounded_percentile = clamp(float(percentile), 0.0, 1.0)
    position = (n_values - 1) * bounded_percentile
    lower_idx = math.floor(position)
    upper_idx = math.ceil(position)
    fraction = position - lower_idx
    lower = ordered[lower_idx]
    upper = ordered[upper_idx]
    return lower + ((upper - lower) * fraction)


def _pstdev(values: List[float]) -> Optional[float]:
    numeric_values = [float(value) for value in values if _is_number(value)]
    if not numeric_values:
        return None

    mean = sum(numeric_values) / len(numeric_values)
    variance = sum((value - mean) ** 2 for value in numeric_values) / len(numeric_values)
    return math.sqrt(variance)


def _normalize_team_name(value: Optional[str]) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split()).casefold()


def _extract_team_game_result(game: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(game, dict):
        return None

    gf = _to_float(game.get("team_score"))
    ga = _to_float(game.get("opponent_score"))
    if gf is None or ga is None:
        return None

    game_total = _to_float(game.get("game_total"))
    if game_total is None:
        game_total = gf + ga

    return {
        "gf": gf,
        "ga": ga,
        "game_total": game_total,
        "startTimestamp": _to_float(game.get("startTimestamp")),
        "raw": game,
    }


def _sort_results_recent_first(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    indexed_results = list(enumerate(results))
    if not any(_to_float(result.get("startTimestamp")) is not None for _, result in indexed_results):
        return results

    sorted_indexed = sorted(
        indexed_results,
        key=lambda item: (
            _to_float(item[1].get("startTimestamp")) is not None,
            _to_float(item[1].get("startTimestamp")) or float("-inf"),
            -item[0],
        ),
        reverse=True,
    )
    return [result for _, result in sorted_indexed]


def _extract_team_results(results: Any) -> List[Dict[str, Any]]:
    if not isinstance(results, list):
        return []

    extracted: List[Dict[str, Any]] = []
    for game in results:
        if not isinstance(game, dict):
            continue
        parsed = _extract_team_game_result(game)
        if parsed is not None:
            extracted.append(parsed)

    return _sort_results_recent_first(extracted)


def _extract_match_game_total(match: Dict[str, Any]) -> Optional[float]:
    game_total = _to_float(match.get("game_total"))
    if game_total is not None:
        return game_total

    home_score = _to_float(match.get("home_score"))
    away_score = _to_float(match.get("away_score"))
    if home_score is None or away_score is None:
        return None
    return home_score + away_score


def _extract_league_match_totals(league_totals_context: Any) -> List[float]:
    if not isinstance(league_totals_context, dict):
        return []

    matches = league_totals_context.get("matches")
    if not isinstance(matches, list):
        return []

    totals: List[float] = []
    for match in matches:
        if not isinstance(match, dict):
            continue
        game_total = _extract_match_game_total(match)
        if game_total is not None:
            totals.append(game_total)
    return totals


def _extract_totals_from_team_payload(team_payload: Any) -> List[float]:
    if not isinstance(team_payload, dict):
        return []

    raw_game_totals = team_payload.get("game_totals")
    if isinstance(raw_game_totals, list):
        game_totals = [
            numeric
            for numeric in (_to_float(value) for value in raw_game_totals)
            if numeric is not None
        ]
        if game_totals:
            return game_totals

    results = _extract_team_results(team_payload.get("results"))
    return [result["game_total"] for result in results]


def _extract_league_team_game_totals_by_team(league_totals_context: Any) -> Dict[str, List[float]]:
    if not isinstance(league_totals_context, dict):
        return {}

    teams = league_totals_context.get("teams")
    if not isinstance(teams, dict):
        return {}

    totals_by_team: Dict[str, List[float]] = {}
    for team_key, team_payload in teams.items():
        if not isinstance(team_payload, dict):
            continue
        team_name = team_payload.get("team_name", team_key)
        normalized_name = _normalize_team_name(team_name)
        if not normalized_name:
            continue
        totals_by_team[normalized_name] = _extract_totals_from_team_payload(team_payload)

    return totals_by_team


def _extract_league_team_results_by_team(league_totals_context: Any) -> Dict[str, List[Dict[str, Any]]]:
    if not isinstance(league_totals_context, dict):
        return {}

    teams = league_totals_context.get("teams")
    if not isinstance(teams, dict):
        return {}

    results_by_team: Dict[str, List[Dict[str, Any]]] = {}
    for team_key, team_payload in teams.items():
        if not isinstance(team_payload, dict):
            continue
        team_name = team_payload.get("team_name", team_key)
        normalized_name = _normalize_team_name(team_name)
        if not normalized_name:
            continue
        results_by_team[normalized_name] = _extract_team_results(team_payload.get("results"))

    return results_by_team




def _resolve_totals_temporal_window_config(total_season_games: int, n_avail: int) -> Dict[str, Any]:
    windows: Dict[str, int] = {}
    games_used: Dict[str, int] = {}
    completeness: Dict[str, float] = {}
    effective_weights: Dict[str, float] = {}
    base_weights: Dict[str, float] = {}
    ratios: Dict[str, float] = {}

    for window_name in TOTALS_TEMPORAL_WINDOW_NAMES:
        config = TOTALS_TEMPORAL_WINDOW_CONFIG[window_name]
        ratio = float(config["ratio"])
        base_weight = float(config["base_weight"])
        window_size = _resolve_window_size_from_ratio(total_season_games, ratio)
        if window_size == 0:
            raise ValueError("ABORT_WINDOW_SIZE_ZERO")

        window_games_used = min(n_avail, window_size)
        if window_games_used == 0:
            raise ValueError("ABORT_WINDOW_GAMES_USED_ZERO")

        window_completeness = _clamp_01(window_games_used / window_size)

        windows[window_name] = window_size
        games_used[window_name] = window_games_used
        completeness[window_name] = window_completeness
        effective_weights[window_name] = base_weight * window_completeness
        base_weights[window_name] = base_weight
        ratios[window_name] = ratio

    return {
        "windows": windows,
        "games_used": games_used,
        "completeness": completeness,
        "effective_weights": effective_weights,
        "base_weights": base_weights,
        "ratios": ratios,
    }


def _team_total_for_window(results: List[Dict[str, Any]], games_used: int) -> Optional[float]:
    if games_used <= 0 or len(results) < games_used:
        return None

    window_results = results[:games_used]
    gf_total = sum(result["gf"] for result in window_results)
    ga_total = sum(result["ga"] for result in window_results)
    return (gf_total / games_used) + (ga_total / games_used)


def _weighted_team_temporal_total(
    team_window_totals: Dict[str, Optional[float]],
    effective_weights: Dict[str, float],
) -> Optional[float]:
    weighted_total = 0.0
    active_weight_sum = 0.0

    for window_name in TOTALS_TEMPORAL_WINDOW_NAMES:
        window_total = team_window_totals.get(window_name)
        effective_weight = effective_weights.get(window_name, 0.0)
        if window_total is None or effective_weight <= 0:
            continue
        weighted_total += effective_weight * window_total
        active_weight_sum += effective_weight

    if active_weight_sum == 0:
        return None
    return weighted_total / active_weight_sum


def _team_window_totals_from_config(
    results: List[Dict[str, Any]],
    games_used_by_window: Dict[str, int],
) -> Dict[str, Optional[float]]:
    return {
        window_name: _team_total_for_window(results, games_used_by_window[window_name])
        for window_name in TOTALS_TEMPORAL_WINDOW_NAMES
    }


def _calculate_team_trend_delta(team_window_totals: Dict[str, Optional[float]]) -> Optional[float]:
    required_totals = [team_window_totals.get(window_name) for window_name in TOTALS_TEMPORAL_WINDOW_NAMES]
    if any(value is None for value in required_totals):
        return None

    short_term_profile = (
        TREND_SHORT_WEIGHT_SHORT * float(team_window_totals["SHORT"])
        + TREND_SHORT_WEIGHT_RECENT * float(team_window_totals["RECENT"])
    )
    long_term_profile = (
        TREND_LONG_WEIGHT_MID * float(team_window_totals["MID"])
        + TREND_LONG_WEIGHT_FULL * float(team_window_totals["FULL"])
    )
    return short_term_profile - long_term_profile


def _calculate_league_trend_deltas(
    league_team_results_by_team: Dict[str, List[Dict[str, Any]]],
    target_windows: Dict[str, int],
) -> Dict[str, float]:
    league_trend_deltas: Dict[str, float] = {}

    for team_name, team_results in league_team_results_by_team.items():
        team_n_avail = len(team_results)
        if team_n_avail <= 0:
            continue

        games_used = {
            window_name: min(team_n_avail, target_windows[window_name])
            for window_name in TOTALS_TEMPORAL_WINDOW_NAMES
        }
        if any(value == 0 for value in games_used.values()):
            continue

        team_window_totals = _team_window_totals_from_config(team_results, games_used)
        team_trend_delta = _calculate_team_trend_delta(team_window_totals)
        if team_trend_delta is not None:
            league_trend_deltas[team_name] = team_trend_delta

    return league_trend_deltas


def _classify_totals_strength(score: float) -> str:
    abs_score = abs(score)
    if abs_score < 0.05:
        return "NONE"
    if abs_score < 0.15:
        return "WEAK"
    if abs_score < 0.30:
        return "MODERATE"
    if abs_score <= 0.60:
        return "STRONG"
    return "VERY_STRONG"


def _totals_direction(score: float) -> str:
    if score > 0:
        return "OVER_PROFILE"
    if score < 0:
        return "UNDER_PROFILE"
    return "NEUTRAL_PROFILE"


def _build_layer(
    layer: str,
    raw_signal: float,
    final_signal: float,
    weight: float,
    raw: Optional[Dict[str, Any]] = None,
) -> P1TotalsLayerOutput:
    status = "ACTIVE" if abs(final_signal) >= IGNORE_THRESHOLD else "IGNORE"
    ignored_reason = None if status == "ACTIVE" else "ABS_SIGNAL_BELOW_0_05"
    weighted_signal = weight * final_signal if status == "ACTIVE" else 0.0
    return P1TotalsLayerOutput(
        layer=layer,
        status=status,
        raw_signal=raw_signal,
        final_signal=final_signal,
        weight=weight,
        weighted_signal=weighted_signal,
        ignored_reason=ignored_reason,
        raw=raw or {},
    )


def _serialize_layer_summary(layer: P1TotalsLayerOutput) -> Dict[str, Any]:
    return {
        "layer": layer.layer,
        "status": layer.status,
        "raw_signal": layer.raw_signal,
        "final_signal": layer.final_signal,
        "weight": layer.weight,
        "weighted_signal": layer.weighted_signal,
        "ignored_reason": layer.ignored_reason,
    }


def _debug_section(title: str) -> None:
    logger.info("")
    logger.info("========== P1_TOTALS DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("P1_TOTALS DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("P1_TOTALS DEBUG | %s", name)
    logger.info("P1_TOTALS DEBUG |   Formula: %s", formula)
    logger.info("P1_TOTALS DEBUG |   Sustitución: %s", substitution)
    logger.info("P1_TOTALS DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("P1_TOTALS DEBUG |   Lectura: %s", meaning)


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


def _percentile_cont_debug_details(values: List[float], percentile: float, result: Any) -> Dict[str, Any]:
    ordered = sorted(float(value) for value in values if _is_number(value))
    if not ordered:
        return {
            "N": 0,
            "percentile": percentile,
            "position": None,
            "lower_idx": None,
            "upper_idx": None,
            "lower_value": None,
            "upper_value": None,
            "fraction": None,
            "result": result,
        }

    position = (len(ordered) - 1) * percentile
    lower_idx = math.floor(position)
    upper_idx = math.ceil(position)
    fraction = position - lower_idx
    return {
        "N": len(ordered),
        "percentile": percentile,
        "position": position,
        "lower_idx": lower_idx,
        "upper_idx": upper_idx,
        "lower_value": ordered[lower_idx],
        "upper_value": ordered[upper_idx],
        "fraction": fraction,
        "result": result,
    }


def _debug_percentile_cont(
    name: str,
    values: List[float],
    percentile: float,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    details = _percentile_cont_debug_details(values, percentile, result)
    logger.info("P1_TOTALS DEBUG | %s", name)
    logger.info("P1_TOTALS DEBUG |   percentile_cont formula:")
    logger.info(
        "P1_TOTALS DEBUG |     position = (N - 1) × percentile"
    )
    logger.info("P1_TOTALS DEBUG |     lower_idx = floor(position)")
    logger.info("P1_TOTALS DEBUG |     upper_idx = ceil(position)")
    logger.info("P1_TOTALS DEBUG |     fraction = position - lower_idx")
    logger.info(
        "P1_TOTALS DEBUG |     result = lower_value + ((upper_value - lower_value) × fraction)"
    )
    logger.info(
        "P1_TOTALS DEBUG |   N=%s percentile=%s position=%s lower_idx=%s upper_idx=%s lower_value=%s upper_value=%s fraction=%s result=%s",
        _fmt(details["N"]),
        _fmt(details["percentile"]),
        _fmt(details["position"]),
        _fmt(details["lower_idx"]),
        _fmt(details["upper_idx"]),
        _fmt(details["lower_value"]),
        _fmt(details["upper_value"]),
        _fmt(details["fraction"]),
        _fmt(details["result"]),
    )
    if meaning:
        logger.info("P1_TOTALS DEBUG |   Lectura: %s", meaning)


def _summarize_numeric_series(values: List[float], sample_size: int = 10) -> Dict[str, Any]:
    numeric_values = [float(value) for value in values if _is_number(value)]
    if not numeric_values:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "avg": None,
            "sample": [],
        }
    return {
        "count": len(numeric_values),
        "min": min(numeric_values),
        "max": max(numeric_values),
        "avg": sum(numeric_values) / len(numeric_values),
        "sample": numeric_values[:sample_size],
    }


def _window_components(results: List[Dict[str, Any]], games_used: int) -> Optional[Dict[str, float]]:
    if games_used <= 0 or len(results) < games_used:
        return None

    window_results = results[:games_used]
    gf_total = sum(result["gf"] for result in window_results)
    ga_total = sum(result["ga"] for result in window_results)
    gfpg = gf_total / games_used
    gapg = ga_total / games_used
    return {
        "games_used": games_used,
        "gf_total": gf_total,
        "ga_total": ga_total,
        "gfpg": gfpg,
        "gapg": gapg,
        "team_total": gfpg + gapg,
    }


def _explain_abort_reason(reason: str) -> str:
    mapping = {
        "ABORT_TOTAL_SEASON_GAMES_ZERO": (
            "No existe TOTAL_SEASON_GAMES válido. Sin ese dato no se pueden calcular "
            "ventanas SHORT/RECENT/MID/FULL."
        ),
        "ABORT_N_AVAIL_ZERO": (
            "No hay partidos suficientes para HOME y AWAY. El motor necesita al menos "
            "un partido válido por equipo."
        ),
        "ABORT_WINDOW_SIZE_ZERO": (
            "Alguna ventana calculada dio tamaño 0. Esto impide calcular perfiles temporales."
        ),
        "ABORT_WINDOW_GAMES_USED_ZERO": (
            "Alguna ventana no tiene partidos utilizables. No se puede dividir entre cero."
        ),
        "ABORT_TOTAL_DYNAMIC_SCALE_ZERO": (
            "La escala dinámica estructural de la liga es 0 o no existe. No se puede "
            "normalizar STRUCTURAL_PROFILE_SCORE."
        ),
        "ABORT_VOL_DYNAMIC_SCALE_ZERO": (
            "La escala dinámica de volatilidad no está disponible. La varianza quedará como UNKNOWN_VARIANCE."
        ),
        "ABORT_TREND_DYNAMIC_SCALE_ZERO": (
            "La escala dinámica de tendencia es 0 o no existe. No se puede normalizar TREND_SIGNAL."
        ),
        "ABORT_ACTIVE_WEIGHT_SUM_ZERO": (
            "Todas las capas quedaron sin peso activo. No se puede calcular P1_TOTALS_DIRECTIONAL_SCORE."
        ),
    }
    return mapping.get(reason, "Motivo no documentado en el blueprint.")


def _split_layers_by_status(
    layers: List[P1TotalsLayerOutput],
) -> Tuple[List[P1TotalsLayerOutput], List[P1TotalsLayerOutput]]:
    active_layers = [layer for layer in layers if layer.status == "ACTIVE"]
    ignored_layers = [layer for layer in layers if layer.status == "IGNORE"]
    return active_layers, ignored_layers


def _abort(
    reason: str,
    event_id: Any,
    participants: str,
    debug_context: Optional[Dict[str, Any]] = None,
    debug_mode: bool = False,
) -> None:
    logger.warning(
        "P1_TOTALS aborted for event_id=%s participants=%s reason=%s context=%s",
        event_id,
        participants,
        reason,
        debug_context or {},
    )
    if debug_mode:
        _debug_section("ABORT")
        _debug_line("Evento: %s", _fmt(event_id))
        _debug_line("Participantes: %s", participants or "N/A")
        _debug_line("Motivo: %s", reason)
        _debug_line("Explicación humana: %s", _explain_abort_reason(reason))
        _debug_line("Contexto disponible: %s", _fmt(debug_context or {}))


def _resolve_event_id(streak_analysis: Any, event_context: Optional[EventContext]) -> int:
    event_id = getattr(streak_analysis, "event_id", None)
    if event_id is None and event_context is not None:
        event_id = getattr(event_context, "event_id", 0)
    resolved = _to_int(event_id)
    return resolved if resolved is not None else 0


def _resolve_participants(streak_analysis: Any, event_context: Optional[EventContext]) -> str:
    participants = getattr(streak_analysis, "participants", None)
    if participants is None and event_context is not None:
        participants = getattr(event_context, "participants_label", None)
    return str(participants or "")


def _resolve_total_season_games(event_context: Optional[EventContext]) -> Optional[int]:
    competition = getattr(event_context, "competition", None)
    total_season_games = _to_int(getattr(competition, "total_regular_season_games", None))
    if total_season_games is None or total_season_games <= 0:
        return None
    return total_season_games




def calculate_p1_totals(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> Optional[P1TotalsOutput]:
    """Calculate P1_TOTALS v2.2 directional profile with separated variance handling."""
    event_id = _resolve_event_id(streak_analysis, event_context)
    participants = _resolve_participants(streak_analysis, event_context)
    home_team_name = getattr(streak_analysis, "home_team_name", None)
    away_team_name = getattr(streak_analysis, "away_team_name", None)

    competition = getattr(event_context, "competition", None)
    sport = getattr(event_context, "sport", None)
    competition_name = getattr(competition, "display_name", None)
    season_name = getattr(event_context, "season_name", None)
    season_year = getattr(event_context, "season_year", None)
    season_label = f"{season_name} ({season_year})" if season_name or season_year else "N/A"

    if debug_mode:
        _debug_section("INICIO")
        _debug_line("Engine version: %s", _ENGINE_VERSION)
        _debug_line("Event ID: %s", _fmt(event_id))
        _debug_line("Participantes: %s", participants or "N/A")
        _debug_line("Home team: %s", home_team_name or "N/A")
        _debug_line("Away team: %s", away_team_name or "N/A")
        _debug_line("Sport: %s", _fmt(sport))
        _debug_line("Competition: %s", _fmt(competition_name))
        _debug_line("Season: %s", season_label)
        _debug_line("Context status: %s", _fmt(getattr(event_context, "context_status", None)))
        _debug_line(
            "Objetivo del módulo: Evaluar si el ecosistema estructural y temporal de goles del matchup apunta a OVER_PROFILE, UNDER_PROFILE o NEUTRAL_PROFILE."
        )
        _debug_line("P1_TOTALS NO usa odds, línea actual, drift, books, cluster ni mercado.")

    total_season_games = _resolve_total_season_games(event_context)
    if total_season_games is None:
        _abort(
            "ABORT_TOTAL_SEASON_GAMES_ZERO",
            event_id,
            participants,
            {
                "total_regular_season_games": getattr(
                    competition,
                    "total_regular_season_games",
                    None,
                )
            },
            debug_mode=debug_mode,
        )
        return None

    if debug_mode:
        _debug_section("INPUTS BASE")
        _debug_line(
            "Fuente TOTAL_SEASON_GAMES: event_context.competition.total_regular_season_games"
        )
        _debug_formula(
            "TOTAL_SEASON_GAMES",
            "valor leído desde event_context.competition.total_regular_season_games",
            f"total_regular_season_games = {_fmt(total_season_games)}",
            _fmt(total_season_games),
            "Cantidad esperada de partidos por equipo en la temporada. Este valor define el tamaño objetivo de las ventanas.",
        )

    raw_home_results = getattr(streak_analysis, "home_team_results", None) or []
    raw_away_results = getattr(streak_analysis, "away_team_results", None) or []
    match_results_home = _extract_team_results(getattr(streak_analysis, "home_team_results", None))
    match_results_away = _extract_team_results(getattr(streak_analysis, "away_team_results", None))

    if debug_mode:
        _debug_line("home_team_results crudo: %s", len(raw_home_results) if isinstance(raw_home_results, list) else 0)
        _debug_line("away_team_results crudo: %s", len(raw_away_results) if isinstance(raw_away_results, list) else 0)
        _debug_line("HOME válidos extraídos: %s", _fmt(len(match_results_home)))
        _debug_line("AWAY válidos extraídos: %s", _fmt(len(match_results_away)))
        _debug_line("HOME sample extraído: %s", _fmt(match_results_home[:2]))
        _debug_line("AWAY sample extraído: %s", _fmt(match_results_away[:2]))

    n_avail = min(len(match_results_home), len(match_results_away))
    if n_avail <= 0:
        _abort(
            "ABORT_N_AVAIL_ZERO",
            event_id,
            participants,
            {"home_results": len(match_results_home), "away_results": len(match_results_away)},
            debug_mode=debug_mode,
        )
        return None

    match_results_home_used = match_results_home[:n_avail]
    match_results_away_used = match_results_away[:n_avail]

    if debug_mode:
        _debug_formula(
            "N_AVAIL",
            "N_AVAIL = min(GP_HOME_AVAILABLE, GP_AWAY_AVAILABLE)",
            f"N_AVAIL = min({len(match_results_home)}, {len(match_results_away)})",
            _fmt(n_avail),
            "Se usa el mismo número de partidos para ambos equipos para evitar comparar muestras desbalanceadas.",
        )

    try:
        window_config = _resolve_totals_temporal_window_config(total_season_games, n_avail)
    except ValueError as exc:
        reason = str(exc)
        if reason not in {"ABORT_WINDOW_SIZE_ZERO", "ABORT_WINDOW_GAMES_USED_ZERO"}:
            raise
        _abort(
            reason,
            event_id,
            participants,
            {"total_season_games": total_season_games, "n_avail": n_avail},
            debug_mode=debug_mode,
        )
        return None

    if debug_mode:
        _debug_section("VENTANAS")
        for window_name in TOTALS_TEMPORAL_WINDOW_NAMES:
            config = TOTALS_TEMPORAL_WINDOW_CONFIG[window_name]
            window_size = window_config["windows"][window_name]
            games_used = window_config["games_used"][window_name]
            completeness = window_config["completeness"][window_name]
            effective_weight = window_config["effective_weights"][window_name]
            base_weight = window_config["base_weights"][window_name]
            ratio = config["ratio"]
            _debug_formula(
                f"{window_name}_WINDOW",
                f"{window_name}_WINDOW = round_half_up(TOTAL_SEASON_GAMES × {window_name}_RATIO)",
                f"round_half_up({_fmt(total_season_games)} × {_fmt(ratio)})",
                _fmt(window_size),
                "Tamaño objetivo de la ventana temporal para esa franja de temporada.",
            )
            _debug_formula(
                f"WINDOW_GAMES_USED_{window_name}",
                f"WINDOW_GAMES_USED_{window_name} = min(N_AVAIL, {window_name}_WINDOW)",
                f"min({_fmt(n_avail)}, {_fmt(window_size)})",
                _fmt(games_used),
                "Número de partidos realmente utilizables dentro de esa ventana.",
            )
            _debug_formula(
                f"WINDOW_COMPLETENESS_{window_name}",
                f"WINDOW_COMPLETENESS_{window_name} = WINDOW_GAMES_USED_{window_name} / {window_name}_WINDOW",
                f"{_fmt(games_used)} / {_fmt(window_size)}",
                _fmt(completeness),
                "Qué tan completa quedó la ventana respecto al tamaño objetivo.",
            )
            _debug_formula(
                f"WINDOW_EFFECTIVE_WEIGHT_{window_name}",
                f"WINDOW_EFFECTIVE_WEIGHT_{window_name} = WINDOW_BASE_WEIGHT_{window_name} × WINDOW_COMPLETENESS_{window_name}",
                f"{_fmt(base_weight, 2)} × {_fmt(completeness)}",
                _fmt(effective_weight),
                "Peso real que aporta la ventana al promedio temporal.",
            )
        for window_name in TOTALS_TEMPORAL_WINDOW_NAMES:
            _debug_line(
                "%s | target=%s | used=%s | completeness=%s | base_weight=%s | effective_weight=%s",
                window_name,
                _fmt(window_config["windows"][window_name]),
                _fmt(window_config["games_used"][window_name]),
                _fmt(window_config["completeness"][window_name]),
                f"{window_config['base_weights'][window_name]:.2f}",
                _fmt(window_config["effective_weights"][window_name]),
            )

    gf_home = sum(result["gf"] for result in match_results_home_used)
    ga_home = sum(result["ga"] for result in match_results_home_used)
    gf_away = sum(result["gf"] for result in match_results_away_used)
    ga_away = sum(result["ga"] for result in match_results_away_used)

    gfpg_home = gf_home / n_avail
    gapg_home = ga_home / n_avail
    gfpg_away = gf_away / n_avail
    gapg_away = ga_away / n_avail

    home_attack_environment = (gfpg_home + gapg_away) / 2
    away_attack_environment = (gfpg_away + gapg_home) / 2
    expected_total_structural = home_attack_environment + away_attack_environment
    league_totals_context = getattr(streak_analysis, "league_totals_context", None)
    game_totals_league = _extract_league_match_totals(league_totals_context)
    league_match_total_baseline = _percentile_cont(game_totals_league, 0.50)
    total_dynamic_scale = None
    if league_match_total_baseline is not None:
        goal_environment_extremeness_samples = [
            abs(game_total - league_match_total_baseline)
            for game_total in game_totals_league
        ]
        total_dynamic_scale = _percentile_cont(goal_environment_extremeness_samples, 0.75)

    if (
        league_match_total_baseline is None
        or total_dynamic_scale is None
        or total_dynamic_scale == 0
    ):
        _abort(
            "ABORT_TOTAL_DYNAMIC_SCALE_ZERO",
            event_id,
            participants,
            {
                "league_match_total_count": len(game_totals_league),
                "league_match_total_baseline": league_match_total_baseline,
                "total_dynamic_scale": total_dynamic_scale,
            },
            debug_mode=debug_mode,
        )
        return None

    structural_profile_score = clamp(
        (expected_total_structural - league_match_total_baseline) / total_dynamic_scale
    )
    structural_anchor = clamp(0.50 + (abs(structural_profile_score) * 0.50), 0.50, 1.00)

    if debug_mode:
        _debug_section("STRUCTURAL CORE")
        _debug_line("HOME: GF_HOME=%s GA_HOME=%s GP_HOME=%s", _fmt(gf_home), _fmt(ga_home), _fmt(n_avail))
        _debug_line("AWAY: GF_AWAY=%s GA_AWAY=%s GP_AWAY=%s", _fmt(gf_away), _fmt(ga_away), _fmt(n_avail))
        _debug_formula(
            "GFPG_HOME",
            "GFPG_HOME = GF_HOME / GP_HOME",
            f"{_fmt(gf_home)} / {_fmt(n_avail)}",
            _fmt(gfpg_home),
            "Promedio de goles anotados por HOME por partido.",
        )
        _debug_formula(
            "GAPG_HOME",
            "GAPG_HOME = GA_HOME / GP_HOME",
            f"{_fmt(ga_home)} / {_fmt(n_avail)}",
            _fmt(gapg_home),
            "Promedio de goles recibidos por HOME por partido.",
        )
        _debug_formula(
            "GFPG_AWAY",
            "GFPG_AWAY = GF_AWAY / GP_AWAY",
            f"{_fmt(gf_away)} / {_fmt(n_avail)}",
            _fmt(gfpg_away),
            "Promedio de goles anotados por AWAY por partido.",
        )
        _debug_formula(
            "GAPG_AWAY",
            "GAPG_AWAY = GA_AWAY / GP_AWAY",
            f"{_fmt(ga_away)} / {_fmt(n_avail)}",
            _fmt(gapg_away),
            "Promedio de goles recibidos por AWAY por partido.",
        )
        _debug_formula(
            "HOME_ATTACK_ENVIRONMENT",
            "HOME_ATTACK_ENVIRONMENT = (GFPG_HOME + GAPG_AWAY) / 2",
            f"({_fmt(gfpg_home)} + {_fmt(gapg_away)}) / 2",
            _fmt(home_attack_environment),
            "Potencial ofensivo esperado del HOME contra la defensa del AWAY.",
        )
        _debug_formula(
            "AWAY_ATTACK_ENVIRONMENT",
            "AWAY_ATTACK_ENVIRONMENT = (GFPG_AWAY + GAPG_HOME) / 2",
            f"({_fmt(gfpg_away)} + {_fmt(gapg_home)}) / 2",
            _fmt(away_attack_environment),
            "Potencial ofensivo esperado del AWAY contra la defensa del HOME.",
        )
        _debug_formula(
            "EXPECTED_TOTAL_STRUCTURAL",
            "EXPECTED_TOTAL_STRUCTURAL = HOME_ATTACK_ENVIRONMENT + AWAY_ATTACK_ENVIRONMENT",
            f"{_fmt(home_attack_environment)} + {_fmt(away_attack_environment)}",
            _fmt(expected_total_structural),
            "Estimación estructural base del volumen de goles del matchup.",
        )


    if debug_mode:
        _debug_section("BASELINE ESTRUCTURAL DE LIGA")
        _debug_line(
            "Fuente: league_totals_context | equipos=%s | partidos=%s",
            _fmt(league_totals_context.get("team_count") if isinstance(league_totals_context, dict) else None),
            _fmt(league_totals_context.get("match_count") if isinstance(league_totals_context, dict) else None),
        )
        league_summary = _summarize_numeric_series(game_totals_league)
        _debug_line(
            "  Formula GAME_TOTALS_LEAGUE = [match.game_total if match.game_total is not None else (match.home_score + match.away_score) for match in league_totals_context.matches]"
        )
        _debug_line(
            "  Formula min = min(GAME_TOTALS_LEAGUE) | max = max(GAME_TOTALS_LEAGUE) | avg = sum(GAME_TOTALS_LEAGUE) / count"
        )
        _debug_line(
            "GAME_TOTALS_LEAGUE: count=%s min=%s max=%s avg=%s sample=%s",
            _fmt(league_summary["count"]),
            _fmt(league_summary["min"]),
            _fmt(league_summary["max"]),
            _fmt(league_summary["avg"]),
            _fmt(league_summary["sample"]),
        )
        
        # Detalle de cada partido de liga
        league_matches_info = []
        matches = league_totals_context.get("matches") if isinstance(league_totals_context, dict) else None
        if isinstance(matches, list):
            for idx, match in enumerate(matches, 1):
                if not isinstance(match, dict):
                    continue
                home_s = match.get("home_score")
                away_s = match.get("away_score")
                gt = match.get("game_total")
                if gt is not None:
                    src_str = f"gt={_fmt(gt)}"
                elif home_s is not None and away_s is not None:
                    src_str = f"home={_fmt(home_s)}+away={_fmt(away_s)}={_fmt(home_s+away_s)}"
                else:
                    src_str = "N/A"
                league_matches_info.append(f"P{idx}: {src_str}")
        _debug_line("Detalle de GAME_TOTAL_i por partido de liga: %s", ", ".join(league_matches_info))
        _debug_line("percentile_cont matemática:")
        _debug_line("  position = (N - 1) × percentile")
        _debug_line("  lower_idx = floor(position)")
        _debug_line("  upper_idx = ceil(position)")
        _debug_line("  fraction = position - lower_idx")
        _debug_line("  result = lower_value + ((upper_value - lower_value) × fraction)")
        _debug_percentile_cont(
            "LEAGUE_MATCH_TOTAL_BASELINE",
            game_totals_league,
            0.50,
            league_match_total_baseline,
            "Total típico real de goles por partido en esta liga.",
        )

        _debug_formula(
            "LEAGUE_MATCH_TOTAL_BASELINE",
            "LEAGUE_MATCH_TOTAL_BASELINE = P50(GAME_TOTALS_LEAGUE)",
            f"P50 sobre {_fmt(len(game_totals_league))} partidos de liga",
            _fmt(league_match_total_baseline),
            "Total típico real de goles por partido en esta liga.",
        )
        goal_environment_extremeness_summary = _summarize_numeric_series(goal_environment_extremeness_samples)
        _debug_line(
            "GOAL_ENVIRONMENT_EXTREMENESS_SAMPLES: count=%s min=%s max=%s avg=%s sample=%s",
            _fmt(goal_environment_extremeness_summary["count"]),
            _fmt(goal_environment_extremeness_summary["min"]),
            _fmt(goal_environment_extremeness_summary["max"]),
            _fmt(goal_environment_extremeness_summary["avg"]),
            _fmt(goal_environment_extremeness_summary["sample"]),
        )
        _debug_percentile_cont(
            "TOTAL_DYNAMIC_SCALE",
            goal_environment_extremeness_samples,
            0.75,
            total_dynamic_scale,
            "Escala usada para saber qué tan fuerte es una desviación del entorno normal de goles.",
        )
        _debug_formula(
            "GOAL_ENVIRONMENT_EXTREMENESS_SAMPLES",
            "GOAL_ENVIRONMENT_EXTREMENESS_i = abs(GAME_TOTAL_i - LEAGUE_MATCH_TOTAL_BASELINE)",
            f"abs(GAME_TOTAL_0 - LEAGUE_MATCH_TOTAL_BASELINE) = abs({_fmt(game_totals_league[0])} - {_fmt(league_match_total_baseline)}) = {_fmt(goal_environment_extremeness_samples[0])}" if game_totals_league else "N/A",
            _fmt(goal_environment_extremeness_summary["count"]),
            f"Se generaron {_fmt(goal_environment_extremeness_summary['count'])} distancias extremas del entorno de goles.",
        )
        _debug_formula(
            "TOTAL_DYNAMIC_SCALE",
            "TOTAL_DYNAMIC_SCALE = P75(GOAL_ENVIRONMENT_EXTREMENESS_SAMPLES)",
            f"P75 sobre {_fmt(goal_environment_extremeness_summary['count'])} distancias extremas",
            _fmt(total_dynamic_scale),
            "Escala usada para saber qué tan fuerte es una desviación del entorno normal de goles.",
        )
        _debug_formula(
            "STRUCTURAL_PROFILE_SCORE",
            "STRUCTURAL_PROFILE_SCORE = (EXPECTED_TOTAL_STRUCTURAL - LEAGUE_MATCH_TOTAL_BASELINE) / TOTAL_DYNAMIC_SCALE",
            f"({_fmt(expected_total_structural)} - {_fmt(league_match_total_baseline)}) / {_fmt(total_dynamic_scale)}",
            _fmt(structural_profile_score),
            "Positivo apunta a entorno estructural alto de goles. Negativo apunta a entorno bajo.",
        )
        _debug_formula(
            "STRUCTURAL_ANCHOR",
            "STRUCTURAL_ANCHOR = 0.50 + (abs(STRUCTURAL_PROFILE_SCORE) × 0.50)",
            f"0.50 + (abs({_fmt(structural_profile_score)}) × 0.50)",
            _fmt(structural_anchor),
            "Define cuánto pueden influir las capas secundarias. Si la estructura es débil, las secundarias pesan menos.",
        )

    game_totals_home = [result["game_total"] for result in match_results_home_used]
    game_totals_away = [result["game_total"] for result in match_results_away_used]
    std_dev_totals_home = _pstdev(game_totals_home)
    std_dev_totals_away = _pstdev(game_totals_away)
    matchup_volatility = None
    if std_dev_totals_home is not None and std_dev_totals_away is not None:
        matchup_volatility = (float(std_dev_totals_home) + float(std_dev_totals_away)) / 2

    league_team_game_totals_by_team = _extract_league_team_game_totals_by_team(league_totals_context)
    league_team_results_by_team = _extract_league_team_results_by_team(league_totals_context)
    team_volatilities: List[float] = []
    team_volatility_by_name: Dict[str, float] = {}
    for team_name, game_totals in league_team_game_totals_by_team.items():
        if not game_totals:
            continue
        team_std_dev = _pstdev(game_totals)
        if team_std_dev is None:
            continue
        team_volatilities.append(team_std_dev)
        team_volatility_by_name[team_name] = team_std_dev
    vol_baseline = _percentile_cont(team_volatilities, 0.50)
    vol_dynamic_scale = None
    league_vol_edge_samples: List[float] = []
    if vol_baseline is not None:
        vol_deviations = [
            abs(team_volatility - vol_baseline)
            for team_volatility in team_volatilities
        ]
        vol_dynamic_scale = _percentile_cont(vol_deviations, 0.75)
    vol_edge: Optional[float] = None
    vol_edge_p50: Optional[float] = None
    vol_edge_p75: Optional[float] = None
    vol_edge_p90: Optional[float] = None
    p1_totals_variance_state = "UNKNOWN_VARIANCE"
    vol_available = (
        matchup_volatility is not None
        and vol_baseline is not None
        and vol_dynamic_scale is not None
        and vol_dynamic_scale != 0
    )
    if vol_available:
        vol_edge = clamp((matchup_volatility - vol_baseline) / vol_dynamic_scale)
        league_vol_edge_samples = [
            abs(clamp((team_volatility - vol_baseline) / vol_dynamic_scale))
            for team_volatility in team_volatilities
        ]
        vol_edge_p50 = _percentile_cont(league_vol_edge_samples, 0.50)
        vol_edge_p75 = _percentile_cont(league_vol_edge_samples, 0.75)
        vol_edge_p90 = _percentile_cont(league_vol_edge_samples, 0.90)
        abs_vol_edge = abs(vol_edge)
        if (
            vol_edge_p50 is None
            or vol_edge_p75 is None
            or vol_edge_p90 is None
        ):
            p1_totals_variance_state = "UNKNOWN_VARIANCE"
        elif abs_vol_edge < vol_edge_p50:
            p1_totals_variance_state = "NORMAL_VARIANCE"
        elif abs_vol_edge < vol_edge_p75:
            p1_totals_variance_state = "ELEVATED_VARIANCE"
        elif abs_vol_edge < vol_edge_p90:
            p1_totals_variance_state = "HIGH_VARIANCE"
        else:
            p1_totals_variance_state = "EXTREME_VARIANCE"
    else:
        logger.warning(
            "P1_TOTALS variance unavailable; directional score will continue. context=%s",
            {
                "matchup_volatility": matchup_volatility,
                "vol_baseline": vol_baseline,
                "vol_dynamic_scale": vol_dynamic_scale,
                "team_volatility_count": len(team_volatilities),
            },
        )

    if debug_mode:
        _debug_section("VOLATILITY LAYER")
        _debug_line("VOL_EDGE is variance-only and is not used as OVER/UNDER direction.")
        _debug_line(
            "  Formula GAME_TOTALS_HOME = [r['game_total'] for r in match_results_home_used]"
        )
        _debug_line(
            "  Formula GAME_TOTALS_AWAY = [r['game_total'] for r in match_results_away_used]"
        )
        _debug_line(
            "  Formula min = min(GAME_TOTALS) | max = max(GAME_TOTALS) | avg = sum(GAME_TOTALS) / N | std = population_std_dev(GAME_TOTALS)"
        )
        home_vol_summary = _summarize_numeric_series(game_totals_home)
        away_vol_summary = _summarize_numeric_series(game_totals_away)
        _debug_line(
            "GAME_TOTALS_HOME: N=%s min=%s max=%s avg=%s std=%s sample=%s",
            _fmt(home_vol_summary["count"]),
            _fmt(home_vol_summary["min"]),
            _fmt(home_vol_summary["max"]),
            _fmt(home_vol_summary["avg"]),
            _fmt(std_dev_totals_home),
            _fmt(home_vol_summary["sample"]),
        )
        _debug_line(
            "GAME_TOTALS_AWAY: N=%s min=%s max=%s avg=%s std=%s sample=%s",
            _fmt(away_vol_summary["count"]),
            _fmt(away_vol_summary["min"]),
            _fmt(away_vol_summary["max"]),
            _fmt(away_vol_summary["avg"]),
            _fmt(std_dev_totals_away),
            _fmt(away_vol_summary["sample"]),
        )
        mean_home = sum(game_totals_home) / len(game_totals_home) if game_totals_home else 0.0
        _debug_formula(
            "STD_DEV_TOTALS_HOME",
            "STD_DEV_TOTALS_HOME = sqrt( sum( (GAME_TOTAL_i - mean)^2 ) / N )",
            f"mean = {_fmt(mean_home)} | N = {len(game_totals_home)} | samples = {_fmt(game_totals_home[:10])}",
            _fmt(std_dev_totals_home),
            "Qué tan variable es el volumen de goles en partidos de HOME.",
        )
        mean_away = sum(game_totals_away) / len(game_totals_away) if game_totals_away else 0.0
        _debug_formula(
            "STD_DEV_TOTALS_AWAY",
            "STD_DEV_TOTALS_AWAY = sqrt( sum( (GAME_TOTAL_i - mean)^2 ) / N )",
            f"mean = {_fmt(mean_away)} | N = {len(game_totals_away)} | samples = {_fmt(game_totals_away[:10])}",
            _fmt(std_dev_totals_away),
            "Qué tan variable es el volumen de goles en partidos de AWAY.",
        )
        _debug_formula(
            "MATCHUP_VOLATILITY",
            "MATCHUP_VOLATILITY = (STD_DEV_TOTALS_HOME + STD_DEV_TOTALS_AWAY) / 2",
            f"({_fmt(std_dev_totals_home)} + {_fmt(std_dev_totals_away)}) / 2",
            _fmt(matchup_volatility),
            "Volatilidad media del matchup combinando HOME y AWAY.",
        )
        _debug_line("team_volatilities: count=%s min=%s max=%s avg=%s sample=%s",
            _fmt(len(team_volatilities)),
            _fmt(min(team_volatilities) if team_volatilities else None),
            _fmt(max(team_volatilities) if team_volatilities else None),
            _fmt(sum(team_volatilities) / len(team_volatilities) if team_volatilities else None),
            _fmt(team_volatilities[:10]),
        )
        _debug_percentile_cont(
            "VOL_BASELINE",
            team_volatilities,
            0.50,
            vol_baseline,
            "Volatilidad típica de la liga.",
        )
        team_volatility_summary = _summarize_numeric_series(team_volatilities)
        _debug_line(
            "team_volatilities: count=%s min=%s max=%s avg=%s sample=%s",
            _fmt(team_volatility_summary["count"]),
            _fmt(team_volatility_summary["min"]),
            _fmt(team_volatility_summary["max"]),
            _fmt(team_volatility_summary["avg"]),
            _fmt(team_volatility_summary["sample"]),
        )
        _debug_percentile_cont(
            "VOL_BASELINE",
            team_volatilities,
            0.50,
            vol_baseline,
            "Volatilidad típica de la liga.",
        )
        _debug_line("--- Liga: STD_DEV_TOTALS por equipo (VOLATILITY) ---")
        for t_name in sorted(team_volatility_by_name):
            t_std = team_volatility_by_name[t_name]
            t_gp = len(league_team_game_totals_by_team.get(t_name, []))
            t_dev = abs(t_std - vol_baseline)
            
            # Extract matches details (gf, ga, tot) for each team match-by-match
            results = league_team_results_by_team.get(t_name, [])
            match_details = []
            if results:
                for idx, r in enumerate(results, 1):
                    match_details.append(f"P{idx}: gf={_fmt(r['gf'])} ga={_fmt(r['ga'])} tot={_fmt(r['game_total'])}")
            else:
                g_tots = league_team_game_totals_by_team.get(t_name, [])
                for idx, gt in enumerate(g_tots, 1):
                    match_details.append(f"P{idx}: tot={_fmt(gt)}")
            
            g_tots_nums = league_team_game_totals_by_team.get(t_name, [])
            mean_val = sum(g_tots_nums) / len(g_tots_nums) if g_tots_nums else 0.0
            sq_diffs_str = ", ".join(f"({_fmt(x)}-{_fmt(mean_val)})^2" for x in g_tots_nums[:3])
            if len(g_tots_nums) > 3:
                sq_diffs_str += ", ..."
                
            _debug_line("  Equipo: %s | GP=%s", t_name, t_gp)
            _debug_line("    Partidos: %s", ", ".join(match_details))
            _debug_line("    Formula STD_DEV_TOTALS = sqrt( sum( (GAME_TOTAL_i - mean)^2 ) / GP )")
            _debug_line(
                "    Sustitución: mean = %s | sum(dev^2) = sum([%s]) = %s",
                _fmt(mean_val),
                sq_diffs_str,
                _fmt(sum((x - mean_val)**2 for x in g_tots_nums)),
            )
            _debug_line("    Resultado: STD_DEV_TOTALS = %s", _fmt(t_std))
            _debug_line("    Formula VOL_DEVIATION = abs(STD_DEV_TOTALS - VOL_BASELINE)")
            _debug_line("    Sustitución: abs(%s - %s)", _fmt(t_std), _fmt(vol_baseline))
            _debug_line("    Resultado: VOL_DEVIATION = %s", _fmt(t_dev))

        if vol_available:
            team_volatility_summary = _summarize_numeric_series(team_volatilities)
            _debug_line(
                "VOL baseline team summary: count=%s min=%s max=%s avg=%s sample=%s",
                _fmt(team_volatility_summary["count"]),
                _fmt(team_volatility_summary["min"]),
                _fmt(team_volatility_summary["max"]),
                _fmt(team_volatility_summary["avg"]),
                _fmt(team_volatility_summary["sample"]),
            )
            _debug_formula(
                "VOL_BASELINE",
                "VOL_BASELINE = P50(STD_DEV_TOTALS_TEAM de todos los equipos de la liga)",
                f"P50 sobre {_fmt(team_volatility_summary['count'])} equipos con totals válidos",
                _fmt(vol_baseline),
                "Volatilidad típica de la liga.",
            )
            vol_deviations = [abs(team_volatility - vol_baseline) for team_volatility in team_volatilities]
            vol_deviation_summary = _summarize_numeric_series(vol_deviations)
            _debug_line(
                "VOL_DEVIATIONS: count=%s min=%s max=%s avg=%s sample=%s",
                _fmt(vol_deviation_summary["count"]),
                _fmt(vol_deviation_summary["min"]),
                _fmt(vol_deviation_summary["max"]),
                _fmt(vol_deviation_summary["avg"]),
                _fmt(vol_deviation_summary["sample"]),
            )
            _debug_percentile_cont(
                "VOL_DYNAMIC_SCALE",
                vol_deviations,
                0.75,
                vol_dynamic_scale,
                "Escala para saber qué tan inusual es la volatilidad del matchup frente a la liga.",
            )
            _debug_formula(
                "VOL_DYNAMIC_SCALE",
                "VOL_DYNAMIC_SCALE = P75(abs(STD_DEV_TOTALS_TEAM_i - VOL_BASELINE))",
                f"P75 sobre {_fmt(vol_deviation_summary['count'])} desviaciones",
                _fmt(vol_dynamic_scale),
                "Escala para saber qué tan inusual es la volatilidad del matchup frente a la liga.",
            )
            _debug_formula(
                "VOL_EDGE",
                "VOL_EDGE = (MATCHUP_VOLATILITY - VOL_BASELINE) / VOL_DYNAMIC_SCALE",
                f"({_fmt(matchup_volatility)} - {_fmt(vol_baseline)}) / {_fmt(vol_dynamic_scale)}",
                _fmt(vol_edge),
                "Positivo indica matchup más variable que la liga. Negativo indica matchup más estable.",
            )
            _debug_formula(
                "VOL_EDGE_P50",
                "VOL_EDGE_P50 = P50(abs(clamp((STD_DEV_TOTALS_TEAM_i - VOL_BASELINE) / VOL_DYNAMIC_SCALE)))",
                f"P50 sobre {_fmt(len(league_vol_edge_samples))} muestras",
                _fmt(vol_edge_p50),
                "Percentil 50 del edge absoluto de varianza en la liga.",
            )
            _debug_formula(
                "VOL_EDGE_P75",
                "VOL_EDGE_P75 = P75(abs(clamp((STD_DEV_TOTALS_TEAM_i - VOL_BASELINE) / VOL_DYNAMIC_SCALE)))",
                f"P75 sobre {_fmt(len(league_vol_edge_samples))} muestras",
                _fmt(vol_edge_p75),
                "Percentil 75 del edge absoluto de varianza en la liga.",
            )
            _debug_formula(
                "VOL_EDGE_P90",
                "VOL_EDGE_P90 = P90(abs(clamp((STD_DEV_TOTALS_TEAM_i - VOL_BASELINE) / VOL_DYNAMIC_SCALE)))",
                f"P90 sobre {_fmt(len(league_vol_edge_samples))} muestras",
                _fmt(vol_edge_p90),
                "Percentil 90 del edge absoluto de varianza en la liga.",
            )
            _debug_formula(
                "P1_TOTALS_VARIANCE_STATE",
                "if abs(VOL_EDGE) < P50 => NORMAL_VARIANCE; < P75 => ELEVATED_VARIANCE; < P90 => HIGH_VARIANCE; else EXTREME_VARIANCE",
                f"abs(VOL_EDGE) = {_fmt(abs(vol_edge) if vol_edge is not None else None)}",
                p1_totals_variance_state,
                "Clasificación dinámica de incertidumbre del matchup.",
            )
        else:
            _debug_line("VOL variance unavailable; skipping detailed variance percentiles and classification.")
        _debug_line("VOL_AVAILABLE: %s", _fmt(vol_available))
        _debug_line("VOL_EDGE_SAMPLE_COUNT: %s", _fmt(len(league_vol_edge_samples)))

    games_used_by_window = window_config["games_used"]
    effective_weights = window_config["effective_weights"]
    home_window_totals = _team_window_totals_from_config(match_results_home_used, games_used_by_window)
    away_window_totals = _team_window_totals_from_config(match_results_away_used, games_used_by_window)
    home_window_components = {
        window_name: _window_components(match_results_home_used, games_used_by_window[window_name])
        for window_name in TOTALS_TEMPORAL_WINDOW_NAMES
    }
    away_window_components = {
        window_name: _window_components(match_results_away_used, games_used_by_window[window_name])
        for window_name in TOTALS_TEMPORAL_WINDOW_NAMES
    }

    team_total_weighted_home = _weighted_team_temporal_total(home_window_totals, effective_weights)
    team_total_weighted_away = _weighted_team_temporal_total(away_window_totals, effective_weights)
    if team_total_weighted_home is None or team_total_weighted_away is None:
        _abort(
            "ABORT_ACTIVE_WEIGHT_SUM_ZERO",
            event_id,
            participants,
            {
                "home_window_totals": home_window_totals,
                "away_window_totals": away_window_totals,
                "effective_weights": effective_weights,
            },
            debug_mode=debug_mode,
        )
        return None

    matchup_temporal_total = (team_total_weighted_home + team_total_weighted_away) / 2
    temporal_profile_score = clamp(
        (matchup_temporal_total - league_match_total_baseline) / total_dynamic_scale
    )
    temporal_final = temporal_profile_score

    if debug_mode:
        _debug_section("TEMPORAL PROFILE LAYER")
        for window_name in TOTALS_TEMPORAL_WINDOW_NAMES:
            home_components = home_window_components[window_name]
            away_components = away_window_components[window_name]
            _debug_formula(
                f"TEAM_TOTAL_{window_name}_HOME",
                f"TEAM_TOTAL_{window_name}_HOME = GFPG_{window_name}_HOME + GAPG_{window_name}_HOME",
                f"({_fmt(home_components['gf_total'])} / {_fmt(home_components['games_used'])}) + ({_fmt(home_components['ga_total'])} / {_fmt(home_components['games_used'])})",
                _fmt(home_window_totals[window_name]),
                "Total de goles esperado para HOME en esa ventana temporal.",
            )
            _debug_formula(
                f"TEAM_TOTAL_{window_name}_AWAY",
                f"TEAM_TOTAL_{window_name}_AWAY = GFPG_{window_name}_AWAY + GAPG_{window_name}_AWAY",
                f"({_fmt(away_components['gf_total'])} / {_fmt(away_components['games_used'])}) + ({_fmt(away_components['ga_total'])} / {_fmt(away_components['games_used'])})",
                _fmt(away_window_totals[window_name]),
                "Total de goles esperado para AWAY en esa ventana temporal.",
            )
        sum_weights_home = sum(
            effective_weights[w] for w in TOTALS_TEMPORAL_WINDOW_NAMES
            if home_window_totals.get(w) is not None and effective_weights.get(w, 0.0) > 0
        )
        sum_weights_away = sum(
            effective_weights[w] for w in TOTALS_TEMPORAL_WINDOW_NAMES
            if away_window_totals.get(w) is not None and effective_weights.get(w, 0.0) > 0
        )
        _debug_formula(
            "TEAM_TOTAL_WEIGHTED_HOME",
            "TEAM_TOTAL_WEIGHTED_HOME = Σ(WINDOW_EFFECTIVE_WEIGHT_i × TEAM_TOTAL_WINDOW_i) / Σ(WINDOW_EFFECTIVE_WEIGHT_i activos)",
            f"({_fmt(effective_weights['SHORT'])}×{_fmt(home_window_totals['SHORT'])} + {_fmt(effective_weights['RECENT'])}×{_fmt(home_window_totals['RECENT'])} + {_fmt(effective_weights['MID'])}×{_fmt(home_window_totals['MID'])} + {_fmt(effective_weights['FULL'])}×{_fmt(home_window_totals['FULL'])}) / {_fmt(sum_weights_home)}",
            _fmt(team_total_weighted_home),
            "Perfil temporal ponderado del volumen de goles de HOME.",
        )
        _debug_formula(
            "TEAM_TOTAL_WEIGHTED_AWAY",
            "TEAM_TOTAL_WEIGHTED_AWAY = Σ(WINDOW_EFFECTIVE_WEIGHT_i × TEAM_TOTAL_WINDOW_i) / Σ(WINDOW_EFFECTIVE_WEIGHT_i activos)",
            f"({_fmt(effective_weights['SHORT'])}×{_fmt(away_window_totals['SHORT'])} + {_fmt(effective_weights['RECENT'])}×{_fmt(away_window_totals['RECENT'])} + {_fmt(effective_weights['MID'])}×{_fmt(away_window_totals['MID'])} + {_fmt(effective_weights['FULL'])}×{_fmt(away_window_totals['FULL'])}) / {_fmt(sum_weights_away)}",
            _fmt(team_total_weighted_away),
            "Perfil temporal ponderado del volumen de goles de AWAY.",
        )
        _debug_formula(
            "MATCHUP_TEMPORAL_TOTAL",
            "MATCHUP_TEMPORAL_TOTAL = (TEAM_TOTAL_WEIGHTED_HOME + TEAM_TOTAL_WEIGHTED_AWAY) / 2",
            f"({_fmt(team_total_weighted_home)} + {_fmt(team_total_weighted_away)}) / 2",
            _fmt(matchup_temporal_total),
            "Promedio temporal del matchup.",
        )
        _debug_formula(
            "TEMPORAL_PROFILE_SCORE",
            "TEMPORAL_PROFILE_SCORE = (MATCHUP_TEMPORAL_TOTAL - LEAGUE_MATCH_TOTAL_BASELINE) / TOTAL_DYNAMIC_SCALE",
            f"({_fmt(matchup_temporal_total)} - {_fmt(league_match_total_baseline)}) / {_fmt(total_dynamic_scale)}",
            _fmt(temporal_profile_score),
            "Indica si el entorno reciente del matchup está por encima o por debajo del entorno típico de liga.",
        )
        _debug_formula(
            "TEMPORAL_FINAL",
            "TEMPORAL_FINAL = TEMPORAL_PROFILE_SCORE",
            _fmt(temporal_profile_score),
            _fmt(temporal_final),
            "Temporalidad final usada como señal direccional pura.",
        )

    team_trend_delta_home = _calculate_team_trend_delta(home_window_totals)
    team_trend_delta_away = _calculate_team_trend_delta(away_window_totals)
    if team_trend_delta_home is None or team_trend_delta_away is None:
        _abort(
            "ABORT_TREND_DYNAMIC_SCALE_ZERO",
            event_id,
            participants,
            {
                "home_window_totals": home_window_totals,
                "away_window_totals": away_window_totals,
            },
            debug_mode=debug_mode,
        )
        return None

    matchup_trend_delta = (team_trend_delta_home + team_trend_delta_away) / 2


    league_trend_deltas_by_team = _calculate_league_trend_deltas(
        league_team_results_by_team,
        window_config["windows"],
    )
    league_trend_deltas = list(league_trend_deltas_by_team.values())
    trend_baseline = _percentile_cont(league_trend_deltas, 0.50)
    trend_dynamic_scale = None
    if trend_baseline is not None:
        trend_deviations = [
            abs(team_trend_delta - trend_baseline)
            for team_trend_delta in league_trend_deltas
        ]
        trend_dynamic_scale = _percentile_cont(trend_deviations, 0.75)

    if trend_baseline is None or trend_dynamic_scale is None or trend_dynamic_scale == 0:
        _abort(
            "ABORT_TREND_DYNAMIC_SCALE_ZERO",
            event_id,
            participants,
            {
                "league_trend_delta_count": len(league_trend_deltas),
                "trend_baseline": trend_baseline,
                "trend_dynamic_scale": trend_dynamic_scale,
            },
            debug_mode=debug_mode,
        )
        return None

    trend_signal = clamp((matchup_trend_delta - trend_baseline) / trend_dynamic_scale)
    trend_final = trend_signal

    if debug_mode:
        _debug_section("TREND ENGINE")
        short_term_profile_home = (
            TREND_SHORT_WEIGHT_SHORT * float(home_window_totals["SHORT"])
            + TREND_SHORT_WEIGHT_RECENT * float(home_window_totals["RECENT"])
        )
        long_term_profile_home = (
            TREND_LONG_WEIGHT_MID * float(home_window_totals["MID"])
            + TREND_LONG_WEIGHT_FULL * float(home_window_totals["FULL"])
        )
        short_term_profile_away = (
            TREND_SHORT_WEIGHT_SHORT * float(away_window_totals["SHORT"])
            + TREND_SHORT_WEIGHT_RECENT * float(away_window_totals["RECENT"])
        )
        long_term_profile_away = (
            TREND_LONG_WEIGHT_MID * float(away_window_totals["MID"])
            + TREND_LONG_WEIGHT_FULL * float(away_window_totals["FULL"])
        )
        _debug_formula(
            "SHORT_TERM_PROFILE_HOME",
            "SHORT_TERM_PROFILE_HOME = (0.60 × TEAM_TOTAL_SHORT_HOME) + (0.40 × TEAM_TOTAL_RECENT_HOME)",
            f"(0.60 × {_fmt(home_window_totals['SHORT'])}) + (0.40 × {_fmt(home_window_totals['RECENT'])})",
            _fmt(short_term_profile_home),
            "Lectura corta del HOME para detectar cambios recientes de volumen.",
        )
        _debug_formula(
            "LONG_TERM_PROFILE_HOME",
            "LONG_TERM_PROFILE_HOME = (0.60 × TEAM_TOTAL_MID_HOME) + (0.40 × TEAM_TOTAL_FULL_HOME)",
            f"(0.60 × {_fmt(home_window_totals['MID'])}) + (0.40 × {_fmt(home_window_totals['FULL'])})",
            _fmt(long_term_profile_home),
            "Lectura larga del HOME para comparar contra la tendencia reciente.",
        )
        _debug_formula(
            "TEAM_TREND_DELTA_HOME",
            "TEAM_TREND_DELTA_HOME = SHORT_TERM_PROFILE_HOME - LONG_TERM_PROFILE_HOME",
            f"{_fmt(short_term_profile_home)} - {_fmt(long_term_profile_home)}",
            _fmt(team_trend_delta_home),
            "Positivo significa que HOME se está calentando en volumen de goles; negativo significa enfriamiento.",
        )
        _debug_formula(
            "SHORT_TERM_PROFILE_AWAY",
            "SHORT_TERM_PROFILE_AWAY = (0.60 × TEAM_TOTAL_SHORT_AWAY) + (0.40 × TEAM_TOTAL_RECENT_AWAY)",
            f"(0.60 × {_fmt(away_window_totals['SHORT'])}) + (0.40 × {_fmt(away_window_totals['RECENT'])})",
            _fmt(short_term_profile_away),
            "Lectura corta del AWAY para detectar cambios recientes de volumen.",
        )
        _debug_formula(
            "LONG_TERM_PROFILE_AWAY",
            "LONG_TERM_PROFILE_AWAY = (0.60 × TEAM_TOTAL_MID_AWAY) + (0.40 × TEAM_TOTAL_FULL_AWAY)",
            f"(0.60 × {_fmt(away_window_totals['MID'])}) + (0.40 × {_fmt(away_window_totals['FULL'])})",
            _fmt(long_term_profile_away),
            "Lectura larga del AWAY para comparar contra la tendencia reciente.",
        )
        _debug_formula(
            "TEAM_TREND_DELTA_AWAY",
            "TEAM_TREND_DELTA_AWAY = SHORT_TERM_PROFILE_AWAY - LONG_TERM_PROFILE_AWAY",
            f"{_fmt(short_term_profile_away)} - {_fmt(long_term_profile_away)}",
            _fmt(team_trend_delta_away),
            "Positivo significa que AWAY se está calentando en volumen de goles; negativo significa enfriamiento.",
        )
        _debug_formula(
            "MATCHUP_TREND_DELTA",
            "MATCHUP_TREND_DELTA = (TEAM_TREND_DELTA_HOME + TEAM_TREND_DELTA_AWAY) / 2",
            f"({_fmt(team_trend_delta_home)} + {_fmt(team_trend_delta_away)}) / 2",
            _fmt(matchup_trend_delta),
            "Tendencia agregada del matchup.",
        )
        league_trend_summary = _summarize_numeric_series(league_trend_deltas)
        _debug_line(
            "league_trend_deltas: count=%s min=%s max=%s avg=%s sample=%s",
            _fmt(league_trend_summary["count"]),
            _fmt(league_trend_summary["min"]),
            _fmt(league_trend_summary["max"]),
            _fmt(league_trend_summary["avg"]),
            _fmt(league_trend_summary["sample"]),
        )
        _debug_percentile_cont(
            "TREND_BASELINE",
            league_trend_deltas,
            0.50,
            trend_baseline,
            "Tendencia típica de la liga.",
        )
        _debug_line("--- Liga: TEAM_TREND_DELTA por equipo (TREND) ---")
        for t_name in sorted(league_trend_deltas_by_team):
            t_delta = league_trend_deltas_by_team[t_name]
            t_results = league_team_results_by_team.get(t_name, [])
            t_gp = len(t_results)
            t_dev = abs(t_delta - trend_baseline)
            
            t_games_used = {
                window_name: min(t_gp, window_config["windows"][window_name])
                for window_name in TOTALS_TEMPORAL_WINDOW_NAMES
            }
            t_window_totals = _team_window_totals_from_config(t_results, t_games_used)
            
            t_short = t_window_totals.get("SHORT")
            t_recent = t_window_totals.get("RECENT")
            t_mid = t_window_totals.get("MID")
            t_full = t_window_totals.get("FULL")
            
            t_short_profile = (
                TREND_SHORT_WEIGHT_SHORT * float(t_short)
                + TREND_SHORT_WEIGHT_RECENT * float(t_recent)
            ) if t_short is not None and t_recent is not None else 0.0
            
            t_long_profile = (
                TREND_LONG_WEIGHT_MID * float(t_mid)
                + TREND_LONG_WEIGHT_FULL * float(t_full)
            ) if t_mid is not None and t_full is not None else 0.0
            
            _debug_line("  Equipo: %s | GP=%s", t_name, t_gp)
            _debug_line(
                "    Juegos Usados Ventanas: SHORT=%s, RECENT=%s, MID=%s, FULL=%s",
                t_games_used.get("SHORT"), t_games_used.get("RECENT"), t_games_used.get("MID"), t_games_used.get("FULL")
            )
            _debug_line(
                "    Promedios Ventanas: SHORT=%s, RECENT=%s, MID=%s, FULL=%s",
                _fmt(t_short), _fmt(t_recent), _fmt(t_mid), _fmt(t_full)
            )
            _debug_line("    Formula SHORT_TERM_PROFILE = 0.60 * SHORT + 0.40 * RECENT")
            _debug_line("    Sustitución: 0.60 * %s + 0.40 * %s = %s", _fmt(t_short), _fmt(t_recent), _fmt(t_short_profile))
            _debug_line("    Formula LONG_TERM_PROFILE = 0.60 * MID + 0.40 * FULL")
            _debug_line("    Sustitución: 0.60 * %s + 0.40 * %s = %s", _fmt(t_mid), _fmt(t_full), _fmt(t_long_profile))
            _debug_line("    Formula TREND_DELTA = SHORT_TERM_PROFILE - LONG_TERM_PROFILE")
            _debug_line("    Sustitución: %s - %s = %s", _fmt(t_short_profile), _fmt(t_long_profile), _fmt(t_delta))
            _debug_line("    Formula TREND_DEVIATION = abs(TREND_DELTA - TREND_BASELINE)")
            _debug_line("    Sustitución: abs(%s - %s) = %s", _fmt(t_delta), _fmt(trend_baseline), _fmt(t_dev))
        trend_deviations = [abs(team_trend_delta - trend_baseline) for team_trend_delta in league_trend_deltas]
        trend_deviation_summary = _summarize_numeric_series(trend_deviations)
        _debug_line(
            "TREND_DEVIATIONS: count=%s min=%s max=%s avg=%s sample=%s",
            _fmt(trend_deviation_summary["count"]),
            _fmt(trend_deviation_summary["min"]),
            _fmt(trend_deviation_summary["max"]),
            _fmt(trend_deviation_summary["avg"]),
            _fmt(trend_deviation_summary["sample"]),
        )
        _debug_percentile_cont(
            "TREND_DYNAMIC_SCALE",
            trend_deviations,
            0.75,
            trend_dynamic_scale,
            "Escala para medir cuánto se aparta el matchup de la tendencia normal de la liga.",
        )
        _debug_formula(
            "TREND_DYNAMIC_SCALE",
            "TREND_DYNAMIC_SCALE = P75(abs(TEAM_TREND_DELTA_i - TREND_BASELINE))",
            f"P75 sobre {_fmt(trend_deviation_summary['count'])} desviaciones",
            _fmt(trend_dynamic_scale),
            "Escala para medir cuánto se aparta el matchup de la tendencia normal de la liga.",
        )
        _debug_formula(
            "TREND_SIGNAL",
            "TREND_SIGNAL = (MATCHUP_TREND_DELTA - TREND_BASELINE) / TREND_DYNAMIC_SCALE",
            f"({_fmt(matchup_trend_delta)} - {_fmt(trend_baseline)}) / {_fmt(trend_dynamic_scale)}",
            _fmt(trend_signal),
            "Positivo indica heating relativo a la liga; negativo indica cooling relativo a la liga.",
        )
        _debug_formula(
            "TREND_FINAL",
            "TREND_FINAL = TREND_SIGNAL",
            _fmt(trend_signal),
            _fmt(trend_final),
            "Tendencia final usada como señal direccional pura.",
        )

    layers = [
        _build_layer(
            "STRUCTURAL",
            structural_profile_score,
            structural_profile_score,
            W_STRUCTURAL,
            {"structural_anchor": structural_anchor},
        ),
        _build_layer(
            "TEMPORAL",
            temporal_profile_score,
            temporal_final,
            W_TEMPORAL,
            {"matchup_temporal_total": matchup_temporal_total},
        ),
        _build_layer(
            "TREND",
            trend_signal,
            trend_final,
            W_TREND,
            {"matchup_trend_delta": matchup_trend_delta},
        ),
    ]

    active_layers, ignored_layers = _split_layers_by_status(layers)
    active_weight_sum = sum(layer.weight for layer in active_layers)
    if active_weight_sum == 0:
        _abort(
            "ABORT_ACTIVE_WEIGHT_SUM_ZERO",
            event_id,
            participants,
            {"layers": [_serialize_layer_summary(layer) for layer in layers]},
            debug_mode=debug_mode,
        )
        return None

    if debug_mode:
        _debug_section("POLICY IGNORE")
        _debug_line(
            "Regla: una capa queda ACTIVE si abs(signal) >= 0.05. Queda IGNORE si abs(signal) < 0.05."
        )
        for layer in layers:
            evaluated_signal = layer.final_signal
            _debug_line(
                "%s: raw_signal=%s | final_signal=%s | evaluated_signal_for_ignore=%s | abs(final_signal)=%s | status=%s",
                layer.layer,
                _fmt(layer.raw_signal),
                _fmt(layer.final_signal),
                _fmt(evaluated_signal),
                _fmt(abs(layer.final_signal)),
                layer.status,
            )
            _debug_line(
                "  status = ACTIVE si abs(final_signal) >= 0.05, si no IGNORE -> %s",
                layer.status,
            )
        _debug_line("active_layers:")
        for layer in active_layers:
            weighted_signal = layer.weight * layer.final_signal
            _debug_line(
                "  - %s | final_signal=%s | weight=%s | weighted_signal=%s",
                layer.layer,
                _fmt(layer.final_signal),
                _fmt(layer.weight),
                _fmt(weighted_signal),
            )
            _debug_line(
                "    %s weighted_signal = %s × %s = %s",
                layer.layer,
                _fmt(layer.weight, 2),
                _fmt(layer.final_signal),
                _fmt(weighted_signal),
            )
        _debug_line("ignored_layers:")
        for layer in ignored_layers:
            _debug_line(
                "  - %s | final_signal=%s | ignored_reason=%s",
                layer.layer,
                _fmt(layer.final_signal),
                layer.ignored_reason or "N/A",
            )

    p1_totals_directional_numerator = sum(layer.weight * layer.final_signal for layer in active_layers)
    p1_totals_directional_score = clamp(
        p1_totals_directional_numerator / active_weight_sum
    )
    p1_totals_direction = _totals_direction(p1_totals_directional_score)
    p1_totals_strength = _classify_totals_strength(p1_totals_directional_score)

    if debug_mode:
        _debug_section("SCORE FINAL")
        _debug_formula(
            "ACTIVE_DIRECTIONAL_WEIGHT_SUM",
            "ACTIVE_DIRECTIONAL_WEIGHT_SUM = suma de pesos de capas direccionales ACTIVE",
            f"{' + '.join(_fmt(layer.weight) for layer in active_layers)}",
            _fmt(active_weight_sum),
            "Peso total de las capas direccionales que sí participaron en el score final.",
        )
        _debug_formula(
            "DIRECTIONAL_NUMERATOR",
            "DIRECTIONAL_NUMERATOR = Σ(weight_i × final_signal_i) solo para capas direccionales ACTIVE",
            " + ".join(
                f"({_fmt(layer.weight)}×{_fmt(layer.final_signal)})" for layer in active_layers
            ),
            _fmt(p1_totals_directional_numerator),
            "Suma ponderada de las señales direccionales activas antes de normalizar por peso activo.",
        )
        _debug_formula(
            "P1_TOTALS_DIRECTIONAL_SCORE",
            "P1_TOTALS_DIRECTIONAL_SCORE = DIRECTIONAL_NUMERATOR / ACTIVE_DIRECTIONAL_WEIGHT_SUM",
            f"{_fmt(p1_totals_directional_numerator)} / {_fmt(active_weight_sum)}",
            _fmt(p1_totals_directional_score),
            "Score direccional del ecosistema de goles. Positivo favorece OVER_PROFILE; negativo favorece UNDER_PROFILE.",
        )
        _debug_formula(
            "P1_TOTALS_DIRECTION",
            "si directional_score > 0 => OVER_PROFILE; si directional_score < 0 => UNDER_PROFILE; si directional_score = 0 => NEUTRAL_PROFILE",
            f"directional_score = {_fmt(p1_totals_directional_score)}",
            p1_totals_direction,
            "La dirección final resume hacia dónde empuja el conjunto de capas activas.",
        )
        _debug_formula(
            "P1_TOTALS_STRENGTH",
            "se clasifica por abs(P1_TOTALS_DIRECTIONAL_SCORE)",
            f"abs({_fmt(p1_totals_directional_score)}) = {_fmt(abs(p1_totals_directional_score))}",
            p1_totals_strength,
            "La intensidad crece a medida que la magnitud del score se aleja de 0.",
        )
        _debug_line("<0.05 = NONE")
        _debug_line("0.05–0.15 = WEAK")
        _debug_line("0.15–0.30 = MODERATE")
        _debug_line("0.30–0.60 = STRONG")
        _debug_line(">0.60 = VERY_STRONG")

    over_count = sum(1 for layer in active_layers if layer.final_signal > 0)
    under_count = sum(1 for layer in active_layers if layer.final_signal < 0)
    ignore_count = len(ignored_layers)
    active_layer_count = over_count + under_count
    alignment_score = (over_count - under_count) / active_layer_count

    layer_by_name = {layer.layer: layer for layer in layers}
    structural_status = layer_by_name["STRUCTURAL"].status
    temporal_status = layer_by_name["TEMPORAL"].status
    trend_status = layer_by_name["TREND"].status

    active_secondary_layers = [
        layer
        for layer in (layer_by_name["TEMPORAL"], layer_by_name["TREND"])
        if layer.status == "ACTIVE"
    ]
    secondary_signs = [_sign(layer.final_signal) for layer in active_secondary_layers]
    secondary_nonzero_signs = [value for value in secondary_signs if value != 0]

    p1_totals_internal_state = {
        "P1_TOTALS_VARIANCE_STATE": p1_totals_variance_state,
        "CONSENSUS_OVER": (
            active_layer_count >= 2
            and all(layer.final_signal > 0 for layer in active_layers)
            and not any(layer.final_signal < 0 for layer in active_layers)
        ),
        "CONSENSUS_UNDER": (
            active_layer_count >= 2
            and all(layer.final_signal < 0 for layer in active_layers)
            and not any(layer.final_signal > 0 for layer in active_layers)
        ),
        "HEATING_CONFLICT": (
            structural_status == "ACTIVE"
            and trend_status == "ACTIVE"
            and structural_profile_score < 0
            and trend_final > 0
        ),
        "COOLING_CONFLICT": (
            structural_status == "ACTIVE"
            and trend_status == "ACTIVE"
            and structural_profile_score > 0
            and trend_final < 0
        ),
        "CHAOTIC_CONFLICT": (
            p1_totals_variance_state in {"HIGH_VARIANCE", "EXTREME_VARIANCE"}
            and structural_status == "ACTIVE"
            and temporal_status == "ACTIVE"
            and _sign(structural_profile_score) != _sign(temporal_profile_score)
        ),
        "STRUCTURAL_NEUTRAL_SECONDARY_PUSH": (
            structural_status == "IGNORE"
            and len(active_secondary_layers) >= 2
            and len(secondary_nonzero_signs) == len(active_secondary_layers)
            and len(set(secondary_nonzero_signs)) == 1
        ),
        "ALIGNMENT_SCORE": alignment_score,
        "OVER_COUNT": over_count,
        "UNDER_COUNT": under_count,
        "IGNORE_COUNT": ignore_count,
        "ACTIVE_LAYER_COUNT": active_layer_count,
    }

    if debug_mode:
        _debug_section("INTERNAL STATE")
        over_layers = [layer.layer for layer in active_layers if layer.final_signal > 0]
        under_layers = [layer.layer for layer in active_layers if layer.final_signal < 0]
        ignored_layer_names = [layer.layer for layer in ignored_layers]
        _debug_formula(
            "OVER_COUNT",
            "OVER_COUNT = count(active directional layers final_signal > 0)",
            f"{_fmt(over_layers)}",
            _fmt(over_count),
            "Capas activas que empujan hacia OVER.",
        )
        _debug_formula(
            "UNDER_COUNT",
            "UNDER_COUNT = count(active directional layers final_signal < 0)",
            f"{_fmt(under_layers)}",
            _fmt(under_count),
            "Capas activas que empujan hacia UNDER.",
        )
        _debug_formula(
            "IGNORE_COUNT",
            "IGNORE_COUNT = count(ignored_layers)",
            f"{_fmt(ignored_layer_names)}",
            _fmt(ignore_count),
            "Capas que quedaron por debajo del umbral de ignorado.",
        )
        _debug_line("ACTIVE_LAYER_COUNT: OVER_COUNT + UNDER_COUNT = %s", _fmt(active_layer_count))
        _debug_formula(
            "ALIGNMENT_SCORE",
            "ALIGNMENT_SCORE = (OVER_COUNT - UNDER_COUNT) / ACTIVE_LAYER_COUNT",
            f"({_fmt(over_count)} - {_fmt(under_count)}) / {_fmt(active_layer_count)}",
            _fmt(alignment_score),
            "Mide qué tan alineadas están las capas activas entre sí.",
        )
        _debug_formula(
            "HEATING_CONFLICT",
            "STRUCTURAL_STATUS == ACTIVE AND TREND_STATUS == ACTIVE AND STRUCTURAL_PROFILE_SCORE < 0 AND TREND_FINAL > 0",
            f"STRUCTURAL_STATUS={structural_status} | TREND_STATUS={trend_status} | STRUCTURAL_PROFILE_SCORE={_fmt(structural_profile_score)} | TREND_FINAL={_fmt(trend_final)}",
            _fmt(p1_totals_internal_state["HEATING_CONFLICT"]),
            "STRUCTURAL apunta a UNDER mientras TREND apunta a OVER.",
        )
        _debug_formula(
            "COOLING_CONFLICT",
            "STRUCTURAL_STATUS == ACTIVE AND TREND_STATUS == ACTIVE AND STRUCTURAL_PROFILE_SCORE > 0 AND TREND_FINAL < 0",
            f"STRUCTURAL_STATUS={structural_status} | TREND_STATUS={trend_status} | STRUCTURAL_PROFILE_SCORE={_fmt(structural_profile_score)} | TREND_FINAL={_fmt(trend_final)}",
            _fmt(p1_totals_internal_state["COOLING_CONFLICT"]),
            "STRUCTURAL apunta a OVER mientras TREND apunta a UNDER.",
        )
        _debug_formula(
            "CHAOTIC_CONFLICT",
            "P1_TOTALS_VARIANCE_STATE in {'HIGH_VARIANCE', 'EXTREME_VARIANCE'} AND STRUCTURAL_STATUS == ACTIVE AND TEMPORAL_STATUS == ACTIVE AND sign(STRUCTURAL_PROFILE_SCORE) != sign(TEMPORAL_PROFILE_SCORE)",
            f"VARIANCE_STATE={p1_totals_variance_state} | STRUCTURAL_STATUS={structural_status} | TEMPORAL_STATUS={temporal_status} | sign(STRUCTURAL_PROFILE_SCORE)={_sign(structural_profile_score)} | sign(TEMPORAL_PROFILE_SCORE)={_sign(temporal_profile_score)}",
            _fmt(p1_totals_internal_state["CHAOTIC_CONFLICT"]),
            "La varianza es alta y estructura/temporalidad están en signos opuestos.",
        )
        _debug_line(
            "CONSENSUS_OVER: all ACTIVE directional layers > 0 and ACTIVE_LAYER_COUNT >= 2 -> %s",
            _fmt(p1_totals_internal_state["CONSENSUS_OVER"]),
        )
        _debug_line(
            "CONSENSUS_UNDER: all ACTIVE directional layers < 0 and ACTIVE_LAYER_COUNT >= 2 -> %s",
            _fmt(p1_totals_internal_state["CONSENSUS_UNDER"]),
        )
        _debug_line(
            "STRUCTURAL_NEUTRAL_SECONDARY_PUSH: STRUCTURAL IGNORE y al menos 2 secundarias direccionales ACTIVE con mismo signo -> %s",
            _fmt(p1_totals_internal_state["STRUCTURAL_NEUTRAL_SECONDARY_PUSH"]),
        )
        if p1_totals_internal_state["CONSENSUS_OVER"]:
            global_reading = "OVER"
        elif p1_totals_internal_state["CONSENSUS_UNDER"]:
            global_reading = "UNDER"
        else:
            global_reading = "DIVIDED"
        _debug_line("Lectura global: las capas activas están alineadas hacia %s.", global_reading)

        _debug_section("AUDIT SNAPSHOT")
        _debug_line("WINDOW_GAMES_USED: %s", _fmt(games_used_by_window))
        _debug_line("WINDOW_EFFECTIVE_WEIGHTS: %s", _fmt(effective_weights))
        _debug_line("HOME_WINDOW_TOTALS: %s", _fmt(home_window_totals))
        _debug_line("AWAY_WINDOW_TOTALS: %s", _fmt(away_window_totals))
        _debug_line("EXPECTED_TOTAL_STRUCTURAL: %s", _fmt(expected_total_structural))
        _debug_line("LEAGUE_MATCH_TOTAL_BASELINE: %s", _fmt(league_match_total_baseline))
        _debug_line("TOTAL_DYNAMIC_SCALE: %s", _fmt(total_dynamic_scale))
        _debug_line("STRUCTURAL_PROFILE_SCORE: %s", _fmt(structural_profile_score))
        _debug_line("STRUCTURAL_ANCHOR: %s", _fmt(structural_anchor))
        _debug_line("P1_TOTALS_STRUCTURAL_SCORE: %s", _fmt(structural_profile_score))
        _debug_line("TEAM_TOTAL_WEIGHTED_HOME: %s", _fmt(team_total_weighted_home))
        _debug_line("TEAM_TOTAL_WEIGHTED_AWAY: %s", _fmt(team_total_weighted_away))
        _debug_line("MATCHUP_TEMPORAL_TOTAL: %s", _fmt(matchup_temporal_total))
        _debug_line("MATCHUP_TREND_DELTA: %s", _fmt(matchup_trend_delta))
        _debug_line("TREND_BASELINE: %s", _fmt(trend_baseline))
        _debug_line("TREND_DYNAMIC_SCALE: %s", _fmt(trend_dynamic_scale))
        _debug_line("TREND_SIGNAL: %s", _fmt(trend_signal))
        _debug_line("TREND_FINAL: %s", _fmt(trend_final))
        _debug_line("MATCHUP_VOLATILITY: %s", _fmt(matchup_volatility))
        _debug_line("VOL_BASELINE: %s", _fmt(vol_baseline))
        _debug_line("VOL_DYNAMIC_SCALE: %s", _fmt(vol_dynamic_scale))
        _debug_line("VOL_EDGE: %s", _fmt(vol_edge))
        _debug_line("VOL_EDGE_P50: %s", _fmt(vol_edge_p50))
        _debug_line("VOL_EDGE_P75: %s", _fmt(vol_edge_p75))
        _debug_line("VOL_EDGE_P90: %s", _fmt(vol_edge_p90))
        _debug_line("P1_TOTALS_VARIANCE_STATE: %s", p1_totals_variance_state)
        _debug_line("P1_TOTALS_DIRECTIONAL_SCORE: %s", _fmt(p1_totals_directional_score))
        _debug_line("P1_TOTALS_DIRECTION: %s", p1_totals_direction)
        _debug_line("P1_TOTALS_STRENGTH: %s", p1_totals_strength)
        _debug_line("ACTIVE_WEIGHT_SUM: %s", _fmt(active_weight_sum))
        _debug_line("ALIGNMENT_SCORE: %s", _fmt(alignment_score))
        _debug_line("OVER_COUNT: %s", _fmt(over_count))
        _debug_line("UNDER_COUNT: %s", _fmt(under_count))
        _debug_line("IGNORE_COUNT: %s", _fmt(ignore_count))
        _debug_line("ACTIVE_LAYER_COUNT: %s", _fmt(active_layer_count))
        _debug_line("LAYERS_ACTIVE: %s", _fmt([layer.layer for layer in active_layers]))
        _debug_line("LAYERS_IGNORED: %s", _fmt([layer.layer for layer in ignored_layers]))
        _debug_line("INTERNAL_STATE: %s", _fmt(p1_totals_internal_state))

    raw = {
        "engine_version": _ENGINE_VERSION,
        "event_context_present": event_context is not None,
        "context_status": getattr(event_context, "context_status", None),
        "total_season_games": total_season_games,
        "n_avail": n_avail,
        "source_fields": {
            "home_team_results": "streak_analysis.home_team_results",
            "away_team_results": "streak_analysis.away_team_results",
            "league_totals_context": "streak_analysis.league_totals_context",
            "total_regular_season_games": "event_context.competition.total_regular_season_games",
        },
        "windows": window_config,
        "structural_core": {
            "gp_home_available": n_avail,
            "gp_away_available": n_avail,
            "gf_home": gf_home,
            "ga_home": ga_home,
            "gf_away": gf_away,
            "ga_away": ga_away,
            "gfpg_home": gfpg_home,
            "gapg_home": gapg_home,
            "gfpg_away": gfpg_away,
            "gapg_away": gapg_away,
            "home_attack_environment": home_attack_environment,
            "away_attack_environment": away_attack_environment,
            "expected_total_structural": expected_total_structural,
            "structural_profile_score": structural_profile_score,
            "structural_anchor": structural_anchor,
        },
        "league_baselines": {
            "league_match_total_count": len(game_totals_league),
            "league_match_total_baseline": league_match_total_baseline,
            "total_dynamic_scale": total_dynamic_scale,
        },
        "directional_components": {
            "STRUCTURAL_PROFILE_SCORE": structural_profile_score,
            "TEMPORAL_PROFILE_SCORE": temporal_profile_score,
            "TREND_SIGNAL": trend_signal,
            "P1_TOTALS_DIRECTIONAL_SCORE": p1_totals_directional_score,
        },
        "variance_components": {
            "std_dev_totals_home": std_dev_totals_home,
            "std_dev_totals_away": std_dev_totals_away,
            "matchup_volatility": matchup_volatility,
            "team_volatility_count": len(team_volatilities),
            "vol_baseline": vol_baseline,
            "vol_dynamic_scale": vol_dynamic_scale,
            "vol_edge": vol_edge,
            "vol_edge_p50": vol_edge_p50,
            "vol_edge_p75": vol_edge_p75,
            "vol_edge_p90": vol_edge_p90,
            "P1_TOTALS_VARIANCE_STATE": p1_totals_variance_state,
        },
        "temporal": {
            "home_window_totals": home_window_totals,
            "away_window_totals": away_window_totals,
            "team_total_weighted_home": team_total_weighted_home,
            "team_total_weighted_away": team_total_weighted_away,
            "matchup_temporal_total": matchup_temporal_total,
            "temporal_profile_score": temporal_profile_score,
            "temporal_final": temporal_final,
        },
        "trend": {
            "team_trend_delta_home": team_trend_delta_home,
            "team_trend_delta_away": team_trend_delta_away,
            "matchup_trend_delta": matchup_trend_delta,
            "league_trend_delta_count": len(league_trend_deltas),
            "trend_baseline": trend_baseline,
            "trend_dynamic_scale": trend_dynamic_scale,
            "trend_signal": trend_signal,
            "trend_final": trend_final,
        },
        "policy_ignore": {
            "active_layers": [_serialize_layer_summary(layer) for layer in active_layers],
            "ignored_layers": [_serialize_layer_summary(layer) for layer in ignored_layers],
        },
        "internal_state": p1_totals_internal_state,
        "debug_logging_mode": "step_by_step_human_readable" if debug_mode else "off",
    }

    if debug_mode:
        _debug_section("OUTPUT FINAL")
        _debug_line("P1_TOTALS_DIRECTIONAL_SCORE: %s", _fmt(p1_totals_directional_score))
        _debug_line("P1_TOTALS_DIRECTION: %s", p1_totals_direction)
        _debug_line("P1_TOTALS_STRENGTH: %s", p1_totals_strength)
        _debug_line("P1_TOTALS_VARIANCE_STATE: %s", p1_totals_variance_state)
        _debug_line("P1_TOTALS_STRUCTURAL_SCORE: %s", _fmt(structural_profile_score))
        _debug_line("STRUCTURAL_STATUS: %s", structural_status)
        _debug_line("TEMPORAL_STATUS: %s", temporal_status)
        _debug_line("TREND_STATUS: %s", trend_status)
        _debug_line("ACTIVE_WEIGHT_SUM: %s", _fmt(active_weight_sum))
        _debug_line("ALIGNMENT_SCORE: %s", _fmt(alignment_score))
        direction_reading = {
            "OVER_PROFILE": "alto",
            "UNDER_PROFILE": "bajo",
            "NEUTRAL_PROFILE": "neutral",
        }.get(p1_totals_direction, "neutral")
        _debug_line(
            "Resultado final: el matchup presenta un ecosistema %s de goles con intensidad %s.",
            direction_reading,
            p1_totals_strength,
        )

    return P1TotalsOutput(
        pillar_id="pillar_1_team_structure",
        module_id="P1_TOTALS",
        module_name="P1 Totals",
        engine_version=_ENGINE_VERSION,
        event_id=event_id,
        participants=participants,
        status="OK",
        status_reason=None,
        P1_TOTALS_DIRECTIONAL_SCORE=p1_totals_directional_score,
        P1_TOTALS_DIRECTION=p1_totals_direction,
        P1_TOTALS_STRENGTH=p1_totals_strength,
        P1_TOTALS_VARIANCE_STATE=p1_totals_variance_state,
        P1_TOTALS_INTERNAL_STATE=p1_totals_internal_state,
        P1_TOTALS_STRUCTURAL_SCORE=structural_profile_score,
        STRUCTURAL_PROFILE_SCORE=structural_profile_score,
        STRUCTURAL_ANCHOR=structural_anchor,
        VOL_EDGE=vol_edge,
        TEMPORAL_PROFILE_SCORE=temporal_profile_score,
        TEMPORAL_FINAL=temporal_final,
        MATCHUP_TREND_DELTA=matchup_trend_delta,
        TREND_BASELINE=trend_baseline,
        TREND_DYNAMIC_SCALE=trend_dynamic_scale,
        TREND_SIGNAL=trend_signal,
        TREND_FINAL=trend_final,
        EXPECTED_TOTAL_STRUCTURAL=expected_total_structural,
        MATCHUP_VOLATILITY=matchup_volatility,
        MATCHUP_TEMPORAL_TOTAL=matchup_temporal_total,
        LEAGUE_MATCH_TOTAL_BASELINE=league_match_total_baseline,
        TOTAL_DYNAMIC_SCALE=total_dynamic_scale,
        VOL_BASELINE=vol_baseline,
        VOL_DYNAMIC_SCALE=vol_dynamic_scale,
        VOL_EDGE_P50=vol_edge_p50,
        VOL_EDGE_P75=vol_edge_p75,
        VOL_EDGE_P90=vol_edge_p90,
        ACTIVE_WEIGHT_SUM=active_weight_sum,
        ALIGNMENT_SCORE=alignment_score,
        OVER_COUNT=over_count,
        UNDER_COUNT=under_count,
        IGNORE_COUNT=ignore_count,
        ACTIVE_LAYER_COUNT=active_layer_count,
        STRUCTURAL_STATUS=structural_status,
        TEMPORAL_STATUS=temporal_status,
        TREND_STATUS=trend_status,
        WINDOWS_USED=window_config["windows"],
        WINDOW_COMPLETENESS_BY_WINDOW=window_config["completeness"],
        active_layers=active_layers,
        ignored_layers=ignored_layers,
        raw=raw,
    )
