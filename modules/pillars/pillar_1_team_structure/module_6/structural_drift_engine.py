"""M6 - Structural Drift Engine."""

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
from modules.pillars.score_series import extract_score_for_against

logger = logging.getLogger(__name__)

_BLOCK_COUNT = 5
_WEIGHT_TREND_GLOBAL = 0.65
_WEIGHT_STABILITY = 0.35


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


def _ordered_goal_diffs(results: List[Dict[str, Any]]) -> Tuple[List[float], str]:
    if not results:
        return [], "reversed_fallback"

    timestamps = [_coerce_float(result.get("startTimestamp")) for result in results]
    if all(timestamp is not None for timestamp in timestamps):
        ordered_results = [result for _, result in sorted(zip(timestamps, results), key=lambda item: item[0])]
        ordering = "startTimestamp_ascending"
    else:
        ordered_results = list(reversed(results))
        ordering = "reversed_fallback"

    goal_diffs: List[float] = []
    for result in ordered_results:
        goal_diff = _extract_goal_diff(result)
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
            logger.info(
                f"  [Vector Build - {team_label}] Block {i+1} (indices {start} to {end-1}): "
                f"games={block} -> avg={avg:.12f}"
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
        numerator_terms.append(f"((x={index+1} - {mean_x:.1f}) * (y={value:.12f} - {mean_y:.12f})) = {term:.12f}")
    denominator = sum((index + 1 - mean_x) ** 2 for index in range(n))
    slope = numerator / denominator if denominator != 0 else 0.0
    
    if debug:
        logger.info(f"  [Linear Trend - {team_label}] vector={vector}")
        logger.info(f"    mean_x={mean_x:.1f}, mean_y={mean_y:.12f}")
        for term_str in numerator_terms:
            logger.info(f"    term: {term_str}")
        logger.info(f"    numerator={numerator:.12f}, denominator={denominator:.12f} -> slope={slope:.12f}")
        
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
        devs_sq.append(f"({value:.12f} - mean={mean:.12f})^2 = {sq_diff:.12f}")
    variance = sum_sq_diff / float(n)
    std = math.sqrt(variance)
    
    if debug:
        logger.info(f"  [Stability - {team_label}] vector={vector}")
        logger.info(f"    mean={mean:.12f}")
        for dev_str in devs_sq:
            logger.info(f"    sq_diff: {dev_str}")
        logger.info(f"    variance={variance:.12f} -> std={std:.12f}")
        
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

    home_goal_diffs, home_ordering = _ordered_goal_diffs(home_results)
    away_goal_diffs, away_ordering = _ordered_goal_diffs(away_results)

    home_structural_vector = _build_structural_vector(home_goal_diffs, _BLOCK_COUNT, team_label="HOME", debug=debug_mode)
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

    home_trend = _linear_trend(home_structural_vector, team_label="HOME", debug=debug_mode)
    away_trend = _linear_trend(away_structural_vector, team_label="AWAY", debug=debug_mode)
    trend_global_edge = clamp(_relative_edge(home_trend, away_trend))

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
        logger.info(f"--- M6 Structural Drift Engine Debug: Event {event_id} ({participants}) ---")
        logger.info(f"  home_team={home_team} | away_team={away_team}")
        logger.info(
            f"  home_games_available={len(home_goal_diffs)} | away_games_available={len(away_goal_diffs)}"
        )
        logger.info(f"  home_ordering={home_ordering} | away_ordering={away_ordering}")
        logger.info(f"  home_goal_diffs_ascending={home_goal_diffs}")
        logger.info(f"  away_goal_diffs_ascending={away_goal_diffs}")
        logger.info(f"  home_block_sizes={home_block_sizes}")
        logger.info(f"  away_block_sizes={away_block_sizes}")
        logger.info(f"  home_structural_vector={home_structural_vector}")
        logger.info(f"  away_structural_vector={away_structural_vector}")
        logger.info(f"  activation_status={m6_status} ({m6_status_reason})")

        logger.info(
            f"  [TREND_GLOBAL_EDGE] home_trend={home_trend:.12f} away_trend={away_trend:.12f} "
            f"edge={trend_global_edge:.12f}"
        )
        logger.info(
            f"  [STABILITY_EDGE] home_volatility={home_volatility:.12f} away_volatility={away_volatility:.12f} "
            f"edge={stability_edge:.12f}"
        )
        logger.info(
            f"  [M6_EDGE_RAW] ({_WEIGHT_TREND_GLOBAL:.2f} * trend_global_edge({trend_global_edge:.12f})) + "
            f"({_WEIGHT_STABILITY:.2f} * stability_edge({stability_edge:.12f})) = "
            f"{m6_edge_raw:.12f}"
        )
        logger.info(f"  [M6_EDGE] clamped = {m6_edge:.12f}")
        logger.info("  --- Component Summary ---")
        logger.info(
            f"  TREND_GLOBAL_EDGE: edge={trend_global_edge:.12f}  weight={_WEIGHT_TREND_GLOBAL:.2f}  "
            f"weighted={trend_global_edge * _WEIGHT_TREND_GLOBAL:.12f}  "
            f"bias={calculate_bias(trend_global_edge)}  strength={classify_strength(trend_global_edge)}"
        )
        logger.info(
            f"  STABILITY_EDGE: edge={stability_edge:.12f}  weight={_WEIGHT_STABILITY:.2f}  "
            f"weighted={stability_edge * _WEIGHT_STABILITY:.12f}  "
            f"bias={calculate_bias(stability_edge)}  strength={classify_strength(stability_edge)}"
        )
        logger.info(
            f"  M6 Final: edge_raw={m6_edge_raw:.12f}  edge_clamped={m6_edge:.12f}  "
            f"bias={calculate_bias(m6_edge)}  strength={classify_strength(m6_edge)}  "
            f"status={m6_status} ({m6_status_reason})"
        )
        logger.info("-" * 60)

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

