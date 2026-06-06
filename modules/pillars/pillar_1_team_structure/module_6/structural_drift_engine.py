"""M6 - Structural Drift Engine."""

from __future__ import annotations

from datetime import datetime, timezone
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
from modules.pillars.score_series import extract_score_for_against

logger = logging.getLogger(__name__)

_BLOCK_COUNT = 5
_WEIGHT_TREND_GLOBAL = 0.65
_WEIGHT_STABILITY = 0.35


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== M6_STRUCTURAL_DRIFT_ENGINE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("M6_STRUCTURAL_DRIFT_ENGINE DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("M6_STRUCTURAL_DRIFT_ENGINE DEBUG | %s", name)
    logger.info("M6_STRUCTURAL_DRIFT_ENGINE DEBUG |   Formula: %s", formula)
    logger.info("M6_STRUCTURAL_DRIFT_ENGINE DEBUG |   Sustitución: %s", substitution)
    logger.info("M6_STRUCTURAL_DRIFT_ENGINE DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("M6_STRUCTURAL_DRIFT_ENGINE DEBUG |   Lectura: %s", meaning)


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
        preview = ", ".join(f"{k}: {_fmt(v, decimals)}" for k, v in items)
        return f"{{{preview}}} (n={len(items)})"
    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        preview = ", ".join(_fmt(item, decimals) for item in sequence)
        return f"[{preview}] (n={len(sequence)})"
    return str(value)


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_goal_diff(result: Dict[str, Any]) -> Optional[float]:
    net_score = _coerce_float(result.get("net_score"))
    if net_score is not None:
        return net_score

    score = extract_score_for_against(result)
    if score is None:
        return None
    team_score, opponent_score = score
    return float(team_score) - float(opponent_score)


def _ordered_goal_diffs(
    results: List[Dict[str, Any]],
    team_label: str = "",
    debug: bool = False,
) -> Tuple[List[float], str]:
    if not results:
        if debug:
            _debug_line("  [%s] No hay resultados históricos disponibles.", team_label)
        return [], "reversed_fallback"

    timestamps = [_coerce_float(result.get("startTimestamp")) for result in results]
    if all(timestamp is not None for timestamp in timestamps):
        ordered_results = [result for _, result in sorted(zip(timestamps, results), key=lambda item: item[0])]
        ordering = "startTimestamp_ascending"
    else:
        ordered_results = list(reversed(results))
        ordering = "reversed_fallback"

    if debug:
        _debug_line("  [%s] Procesamiento de encuentros históricos (orden: %s):", team_label, ordering)

    goal_diffs: List[float] = []
    for idx, result in enumerate(ordered_results):
        ts = result.get("startTimestamp")
        net_score = result.get("net_score")
        score = extract_score_for_against(result)
        goal_diff = _extract_goal_diff(result)
        
        if debug:
            date_str = "N/A"
            if ts is not None:
                try:
                    date_str = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")
                except Exception:
                    pass
            ts_str = str(ts) if ts is not None else "N/A"
            net_str = str(net_score) if net_score is not None else "N/A"
            score_str = f"{score[0]} - {score[1]}" if score is not None else "N/A"
            gd_str = str(goal_diff) if goal_diff is not None else "N/A"
            opp_name = result.get("opponent_name") or result.get("opponent_team_name") or result.get("opponentName") or result.get("opponent") or "Rival Desconocido"
            if isinstance(opp_name, dict):
                opp_name = opp_name.get("name") or opp_name.get("display_name") or opp_name.get("shortName") or opp_name.get("short_name") or "Rival Desconocido"
            opp_name = str(opp_name).strip()
            _debug_line("    [%s] Encuentro %d: fecha=%s, timestamp=%s, rival=%s, net_score=%s, score_for_against=%s -> goal_difference=%s",
                        team_label, idx + 1, date_str, ts_str, opp_name, net_str, score_str, gd_str)

        if goal_diff is not None:
            goal_diffs.append(goal_diff)
    return goal_diffs, ordering


def _build_structural_vector(
    goal_diffs: List[float],
    block_count: int = _BLOCK_COUNT,
    team_label: str = "",
    debug: bool = False,
) -> List[float]:
    if block_count <= 0:
        return []
    if not goal_diffs:
        return [0.0] * block_count

    n = len(goal_diffs)
    vector: List[float] = []
    for i in range(block_count):
        start = round(i * n / block_count)
        end = round((i + 1) * n / block_count)
        block = goal_diffs[start:end]
        avg = sum(block) / float(len(block)) if block else 0.0
        vector.append(avg)
        if debug:
            _debug_line(
                "  [Vector Build - %s] Bloque %d (índices %d a %d): partidos=%s -> promedio=%s",
                team_label, i + 1, start, end - 1 if end > start else start, _fmt(block), _fmt(avg)
            )
    return vector


def _linear_trend(vector: List[float], team_label: str = "", debug: bool = False) -> float:
    n = len(vector)
    if n <= 1:
        return 0.0

    mean_x = (n + 1) / 2.0
    mean_y = sum(vector) / float(n)
    numerator = 0.0
    numerator_terms = []
    for index, value in enumerate(vector):
        term = (index + 1 - mean_x) * (value - mean_y)
        numerator += term
        numerator_terms.append(f"((x={index+1} - {mean_x:.1f}) * (y={_fmt(value)} - {_fmt(mean_y)})) = {_fmt(term)}")
    denominator = sum((index + 1 - mean_x) ** 2 for index in range(n))
    slope = numerator / denominator if denominator != 0 else 0.0
    
    if debug:
        _debug_line("  [Linear Trend - %s] vector=%s", team_label, _fmt(vector))
        _debug_line("    mean_x=%s, mean_y=%s", _fmt(mean_x), _fmt(mean_y))
        for term_str in numerator_terms:
            _debug_line("    term: %s", term_str)
        _debug_formula(
            f"SLOPE_{team_label}",
            "numerator / denominator",
            f"{_fmt(numerator)} / {_fmt(denominator)}",
            _fmt(slope),
            f"Pendiente de la tendencia lineal para el equipo {team_label}"
        )
        
    return slope


def _population_std(vector: List[float], team_label: str = "", debug: bool = False) -> float:
    n = len(vector)
    if n == 0:
        return 0.0
    mean = sum(vector) / float(n)
    sum_sq_diff = 0.0
    devs_sq = []
    for value in vector:
        sq_diff = (value - mean) ** 2
        sum_sq_diff += sq_diff
        devs_sq.append(f"({_fmt(value)} - mean={_fmt(mean)})^2 = {_fmt(sq_diff)}")
    variance = sum_sq_diff / float(n)
    std = math.sqrt(variance)
    
    if debug:
        _debug_line("  [Stability - %s] vector=%s", team_label, _fmt(vector))
        _debug_line("    mean=%s", _fmt(mean))
        for dev_str in devs_sq:
            _debug_line("    sq_diff: %s", dev_str)
        _debug_formula(
            f"STD_DEV_{team_label}",
            "sqrt(sum((x - mean)^2) / n)",
            f"sqrt({_fmt(sum_sq_diff)} / {n})",
            _fmt(std),
            f"Desviación estándar de los bloques (volatilidad) para el equipo {team_label}"
        )
        
    return std


def _relative_edge(home_value: float, away_value: float) -> float:
    denominator = abs(home_value) + abs(away_value)
    if denominator == 0:
        return 0.0
    return (home_value - away_value) / denominator


def _component(name: str, edge: float, weight: float, raw: Dict[str, Any]) -> ModuleComponentResult:
    return ModuleComponentResult(
        name=name,
        edge=edge,
        bias=calculate_bias(edge),
        strength=classify_strength(edge),
        weight=weight,
        weighted_edge=edge * weight,
        raw=raw,
    )


def _determine_status(
    home_gd: List[float],
    away_gd: List[float],
    home_vector: List[float],
    away_vector: List[float],
) -> Tuple[str, str]:
    del home_vector, away_vector

    home_count = len(home_gd)
    away_count = len(away_gd)
    if home_count < 10 or away_count < 10:
        return "INSUFFICIENT_DATA", "insufficient_structural_sample"
    if home_count < 20 or away_count < 20:
        return "DEGRADED", "partial_structural_sample"
    return "ACTIVE", "active"


def calculate_structural_drift_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    del event_context

    home_results: List[Dict[str, Any]] = getattr(streak_analysis, "home_team_results", None) or []
    away_results: List[Dict[str, Any]] = getattr(streak_analysis, "away_team_results", None) or []
    home_team = getattr(streak_analysis, "home_team_name", None)
    away_team = getattr(streak_analysis, "away_team_name", None)
    event_id = getattr(streak_analysis, "event_id", 0)
    participants = getattr(streak_analysis, "participants", "") or ""

    if debug_mode:
        _debug_section("Propósito del módulo")
        _debug_line("M6 mide la trayectoria estructural a largo plazo y la deriva de estabilidad de cada equipo.")
        _debug_line("Se calcula dividiendo el historial de diferencia de puntos en 5 bloques secuenciales.")

        _debug_section("Parámetros Globales y Constantes")
        _debug_line("BLOCK_COUNT: %s", _fmt(_BLOCK_COUNT))
        _debug_line("WEIGHT_TREND_GLOBAL: %s", _fmt(_WEIGHT_TREND_GLOBAL))
        _debug_line("WEIGHT_STABILITY: %s", _fmt(_WEIGHT_STABILITY))

        _debug_section("Datos del Evento")
        _debug_line("Event ID: %s", _fmt(event_id))
        _debug_line("Participantes: %s", _fmt(participants))
        _debug_line("Equipo Local (Home): %s", _fmt(home_team))
        _debug_line("Equipo Visitante (Away): %s", _fmt(away_team))

        _debug_section("Muestras Históricas y Ordenamiento")

    home_goal_diffs, home_ordering = _ordered_goal_diffs(home_results, team_label="HOME", debug=debug_mode)
    away_goal_diffs, away_ordering = _ordered_goal_diffs(away_results, team_label="AWAY", debug=debug_mode)

    if debug_mode:
        _debug_section("Construcción del Vector Estructural (HOME)")

    home_structural_vector = _build_structural_vector(home_goal_diffs, _BLOCK_COUNT, team_label="HOME", debug=debug_mode)

    if debug_mode:
        _debug_section("Construcción del Vector Estructural (AWAY)")

    away_structural_vector = _build_structural_vector(away_goal_diffs, _BLOCK_COUNT, team_label="AWAY", debug=debug_mode)

    home_block_sizes = [
        round((i + 1) * len(home_goal_diffs) / _BLOCK_COUNT) - round(i * len(home_goal_diffs) / _BLOCK_COUNT)
        for i in range(_BLOCK_COUNT)
    ]
    away_block_sizes = [
        round((i + 1) * len(away_goal_diffs) / _BLOCK_COUNT) - round(i * len(away_goal_diffs) / _BLOCK_COUNT)
        for i in range(_BLOCK_COUNT)
    ]

    m6_status, m6_status_reason = _determine_status(
        home_goal_diffs,
        away_goal_diffs,
        home_structural_vector,
        away_structural_vector,
    )

    if debug_mode:
        _debug_section("Validación de Estado")
        _debug_line("Tamaños de bloques HOME: %s", _fmt(home_block_sizes))
        _debug_line("Tamaños de bloques AWAY: %s", _fmt(away_block_sizes))
        _debug_line("Vectores construidos:")
        _debug_line("  HOME_VECTOR: %s", _fmt(home_structural_vector))
        _debug_line("  AWAY_VECTOR: %s", _fmt(away_structural_vector))
        _debug_line("Estado del motor M6: %s (razón: %s)", _fmt(m6_status), _fmt(m6_status_reason))

        _debug_section("Cálculo de la Pendiente de Tendencia (Linear Trend)")

    home_trend = _linear_trend(home_structural_vector, team_label="HOME", debug=debug_mode)
    away_trend = _linear_trend(away_structural_vector, team_label="AWAY", debug=debug_mode)
    trend_global_edge = clamp(_relative_edge(home_trend, away_trend))

    if debug_mode:
        _debug_section("Cálculo de la Volatilidad/Desviación Estándar (Stability)")

    home_volatility = _population_std(home_structural_vector, team_label="HOME", debug=debug_mode)
    away_volatility = _population_std(away_structural_vector, team_label="AWAY", debug=debug_mode)
    stability_edge = clamp(_relative_edge(away_volatility, home_volatility))

    m6_edge_raw = (
        (_WEIGHT_TREND_GLOBAL * trend_global_edge)
        + (_WEIGHT_STABILITY * stability_edge)
    )
    m6_edge = clamp(m6_edge_raw)
    if m6_status == "INSUFFICIENT_DATA":
        m6_edge = 0.0

    if debug_mode:
        _debug_section("Cálculo de Relaciones de Ventaja (Edges)")
        _debug_formula(
            "TREND_GLOBAL_EDGE",
            "(home_trend - away_trend) / (abs(home_trend) + abs(away_trend))",
            f"({_fmt(home_trend)} - {_fmt(away_trend)}) / (abs({_fmt(home_trend)}) + abs({_fmt(away_trend)}))",
            _fmt(trend_global_edge),
            f"Ventaja de tendencia global (clamped), bias={_fmt(calculate_bias(trend_global_edge))}"
        )
        _debug_formula(
            "STABILITY_EDGE",
            "(away_volatility - home_volatility) / (away_volatility + home_volatility)",
            f"({_fmt(away_volatility)} - {_fmt(home_volatility)}) / ({_fmt(away_volatility)} + {_fmt(home_volatility)})",
            _fmt(stability_edge),
            f"Ventaja de estabilidad global (clamped), bias={_fmt(calculate_bias(stability_edge))}"
        )

        _debug_section("Agregación y Edge Final")
        _debug_formula(
            "M6_EDGE_RAW",
            "WEIGHT_TREND_GLOBAL * trend_global_edge + WEIGHT_STABILITY * stability_edge",
            f"{_fmt(_WEIGHT_TREND_GLOBAL)} * {_fmt(trend_global_edge)} + {_fmt(_WEIGHT_STABILITY)} * {_fmt(stability_edge)}",
            _fmt(m6_edge_raw),
            "Agregación ponderada de los componentes"
        )
        _debug_line("M6_EDGE (Clamped): %s", _fmt(m6_edge))

        _debug_section("Resumen del Output")
        _debug_line("M6_EDGE: %s", _fmt(m6_edge))
        _debug_line("M6_BIAS (Dirección): %s", _fmt(calculate_bias(m6_edge)))
        _debug_line("M6_STRENGTH (Intensidad): %s", _fmt(classify_strength(m6_edge)))
        _debug_line("M6_STATUS: %s (%s)", _fmt(m6_status), _fmt(m6_status_reason))
        _debug_line("Componentes:")
        _debug_line("  - TREND_GLOBAL_EDGE: edge=%s, weight=%s, weighted=%s, bias=%s, strength=%s",
                    _fmt(trend_global_edge), _fmt(_WEIGHT_TREND_GLOBAL), _fmt(trend_global_edge * _WEIGHT_TREND_GLOBAL),
                    _fmt(calculate_bias(trend_global_edge)), _fmt(classify_strength(trend_global_edge)))
        _debug_line("  - STABILITY_EDGE: edge=%s, weight=%s, weighted=%s, bias=%s, strength=%s",
                    _fmt(stability_edge), _fmt(_WEIGHT_STABILITY), _fmt(stability_edge * _WEIGHT_STABILITY),
                    _fmt(calculate_bias(stability_edge)), _fmt(classify_strength(stability_edge)))
        _debug_line("-" * 60)

    components = [
        _component(
            "TREND_GLOBAL_EDGE",
            trend_global_edge,
            _WEIGHT_TREND_GLOBAL,
            {
                "home_trend": home_trend,
                "away_trend": away_trend,
                "formula": "(TREND_HOME - TREND_AWAY) / (abs(TREND_HOME) + abs(TREND_AWAY))",
            },
        ),
        _component(
            "STABILITY_EDGE",
            stability_edge,
            _WEIGHT_STABILITY,
            {
                "home_volatility": home_volatility,
                "away_volatility": away_volatility,
                "formula": "(VOLATILITY_AWAY - VOLATILITY_HOME) / (VOLATILITY_AWAY + VOLATILITY_HOME)",
            },
        ),
    ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "block_count": _BLOCK_COUNT,
        "home_ordering": home_ordering,
        "away_ordering": away_ordering,
        "home_games_available": len(home_goal_diffs),
        "away_games_available": len(away_goal_diffs),
        "home_goal_diffs_ascending": home_goal_diffs,
        "away_goal_diffs_ascending": away_goal_diffs,
        "home_block_sizes": home_block_sizes,
        "away_block_sizes": away_block_sizes,
        "home_structural_vector": home_structural_vector,
        "away_structural_vector": away_structural_vector,
        "trend_global": {
            "home_trend": home_trend,
            "away_trend": away_trend,
            "edge": trend_global_edge,
        },
        "stability": {
            "home_volatility": home_volatility,
            "away_volatility": away_volatility,
            "edge": stability_edge,
        },
        "m6_edge_raw": m6_edge_raw,
        "m6_edge": m6_edge,
        "m6_abs_edge": abs(m6_edge),
        "m6_status": m6_status,
        "m6_status_reason": m6_status_reason,
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M6",
        module_name="Structural Drift Engine",
        event_id=event_id,
        participants=participants,
        value=m6_edge,
        bias=calculate_bias(m6_edge),
        strength=classify_strength(m6_edge),
        components=components,
        raw=raw,
    )
