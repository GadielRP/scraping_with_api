"""M6 - Structural Drift Engine.

Measures full-season structural drift using goal-differential blocks,
linear trend, and population volatility.
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

    goals_for, goals_against = score
    goals_for_value = _coerce_float(goals_for)
    goals_against_value = _coerce_float(goals_against)
    if goals_for_value is None or goals_against_value is None:
        return None
    return goals_for_value - goals_against_value


def _ordered_goal_diffs(results: List[Dict[str, Any]]) -> Tuple[List[float], str]:
    if not results:
        return [], "reversed_fallback"

    timestamps: List[Optional[float]] = [
        _coerce_float(result.get("startTimestamp")) for result in results
    ]
    if all(timestamp is not None for timestamp in timestamps):
        ordered_results = [
            result
            for _, result in sorted(
                zip(timestamps, results),
                key=lambda item: item[0],
            )
        ]
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
    del team_label, debug

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
        avg = sum(block) / len(block) if block else 0.0
        vector.append(avg)
    return vector


def _linear_trend(
    vector: List[float],
    team_label: str = "",
    debug: bool = False,
) -> float:
    del team_label, debug

    n = len(vector)
    if n <= 1:
        return 0.0

    mean_x = (n + 1) / 2.0
    mean_y = sum(vector) / float(n)

    numerator = 0.0
    denominator = 0.0
    for index, value in enumerate(vector):
        x_delta = (index + 1) - mean_x
        numerator += x_delta * (value - mean_y)
        denominator += x_delta ** 2

    if denominator == 0:
        return 0.0
    return numerator / denominator


def _relative_edge(home_value: float, away_value: float) -> float:
    denominator = abs(home_value) + abs(away_value)
    if denominator == 0:
        return 0.0
    return (home_value - away_value) / denominator


def _population_std(
    vector: List[float],
    team_label: str = "",
    debug: bool = False,
) -> float:
    del team_label, debug

    if not vector:
        return 0.0

    mean = sum(vector) / float(len(vector))
    variance = sum((value - mean) ** 2 for value in vector) / float(len(vector))
    return math.sqrt(variance)


def _determine_status(
    home_gd: List[float],
    away_gd: List[float],
) -> Tuple[str, str]:
    home_count = len(home_gd)
    away_count = len(away_gd)

    if home_count < 10 or away_count < 10:
        return "INSUFFICIENT_DATA", "insufficient_structural_sample"
    if home_count < 20 or away_count < 20:
        return "DEGRADED", "partial_structural_sample"
    return "ACTIVE", "active"


def _component(
    name: str,
    edge: float,
    weight: float,
    raw: Dict[str, Any],
) -> ModuleComponentResult:
    return ModuleComponentResult(
        name=name,
        edge=edge,
        bias=calculate_bias(edge),
        strength=classify_strength(edge),
        weight=weight,
        weighted_edge=edge * weight,
        raw=raw,
    )


def calculate_m6_structural_drift_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    event_id = getattr(streak_analysis, "event_id", 0)
    participants = getattr(streak_analysis, "participants", "") or ""
    home_team = getattr(streak_analysis, "home_team_name", None)
    away_team = getattr(streak_analysis, "away_team_name", None)
    home_results = getattr(streak_analysis, "home_team_results", None) or []
    away_results = getattr(streak_analysis, "away_team_results", None) or []

    del event_context

    home_goal_diffs, home_ordering = _ordered_goal_diffs(home_results)
    away_goal_diffs, away_ordering = _ordered_goal_diffs(away_results)

    home_structural_vector = _build_structural_vector(
        home_goal_diffs,
        block_count=_BLOCK_COUNT,
        team_label=str(home_team or ""),
        debug=debug_mode,
    )
    away_structural_vector = _build_structural_vector(
        away_goal_diffs,
        block_count=_BLOCK_COUNT,
        team_label=str(away_team or ""),
        debug=debug_mode,
    )

    home_trend = _linear_trend(
        home_structural_vector,
        team_label=str(home_team or ""),
        debug=debug_mode,
    )
    away_trend = _linear_trend(
        away_structural_vector,
        team_label=str(away_team or ""),
        debug=debug_mode,
    )
    trend_global_edge = clamp(_relative_edge(home_trend, away_trend))

    home_volatility = _population_std(
        home_structural_vector,
        team_label=str(home_team or ""),
        debug=debug_mode,
    )
    away_volatility = _population_std(
        away_structural_vector,
        team_label=str(away_team or ""),
        debug=debug_mode,
    )
    stability_denominator = home_volatility + away_volatility
    if stability_denominator == 0:
        stability_edge = 0.0
    else:
        stability_edge = (away_volatility - home_volatility) / stability_denominator
    stability_edge = clamp(stability_edge)

    m6_edge_raw = (
        (_WEIGHT_TREND_GLOBAL * trend_global_edge)
        + (_WEIGHT_STABILITY * stability_edge)
    )
    m6_edge = clamp(m6_edge_raw)

    m6_status, m6_status_reason = _determine_status(
        home_goal_diffs,
        away_goal_diffs,
    )
    if m6_status == "INSUFFICIENT_DATA":
        m6_edge = 0.0

    home_block_sizes = [
        round((i + 1) * len(home_goal_diffs) / _BLOCK_COUNT)
        - round(i * len(home_goal_diffs) / _BLOCK_COUNT)
        for i in range(_BLOCK_COUNT)
    ]
    away_block_sizes = [
        round((i + 1) * len(away_goal_diffs) / _BLOCK_COUNT)
        - round(i * len(away_goal_diffs) / _BLOCK_COUNT)
        for i in range(_BLOCK_COUNT)
    ]

    if debug_mode:
        logger.info(
            "--- M6 Structural Drift Engine Debug: Event %s (%s) ---",
            event_id,
            participants,
        )
        logger.info("home_team=%s | away_team=%s", home_team, away_team)
        logger.info(
            "home_games_available=%s away_games_available=%s",
            len(home_goal_diffs),
            len(away_goal_diffs),
        )
        logger.info("home_ordering=%s", home_ordering)
        logger.info("away_ordering=%s", away_ordering)
        logger.info("home_goal_diffs_ascending=%s", home_goal_diffs)
        logger.info("away_goal_diffs_ascending=%s", away_goal_diffs)
        logger.info("home_block_sizes=%s", home_block_sizes)
        logger.info("away_block_sizes=%s", away_block_sizes)
        logger.info("home_structural_vector=%s", home_structural_vector)
        logger.info("away_structural_vector=%s", away_structural_vector)
        logger.info("home_trend=%.12f", home_trend)
        logger.info("away_trend=%.12f", away_trend)
        logger.info("trend_global_edge=%.12f", trend_global_edge)
        logger.info("home_volatility=%.12f", home_volatility)
        logger.info("away_volatility=%.12f", away_volatility)
        logger.info("stability_edge=%.12f", stability_edge)
        logger.info(
            "m6_edge_raw = 0.65 * trend_global_edge + 0.35 * stability_edge = %.12f",
            m6_edge_raw,
        )
        logger.info(
            "bias=%s strength=%s status=%s status_reason=%s",
            calculate_bias(m6_edge),
            classify_strength(m6_edge),
            m6_status,
            m6_status_reason,
        )
        logger.info("-" * 60)

    components = [] if m6_status == "INSUFFICIENT_DATA" else [
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
            "weight": _WEIGHT_TREND_GLOBAL,
        },
        "stability": {
            "home_volatility": home_volatility,
            "away_volatility": away_volatility,
            "edge": stability_edge,
            "weight": _WEIGHT_STABILITY,
        },
        "m6_edge_raw": m6_edge_raw,
        "m6_edge": m6_edge,
        "m6_abs_edge": abs(m6_edge),
        "m6_status": m6_status,
        "m6_status_reason": m6_status_reason,
        "formula_profile": "m6_structural_drift_v1",
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


# Validation note:
# For the Celta vs Levante fixture, the following approximate outputs should
# reproduce within 1e-9 tolerance:
# home_trend=0.042857142857, away_trend=0.100000000000,
# trend_global_edge=-0.400000000000,
# home_volatility=0.606091526731, away_volatility=0.417963966809,
# stability_edge=-0.183708364545, m6_edge=-0.324297927591.
