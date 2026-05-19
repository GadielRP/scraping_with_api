"""M5 - Recent Inertia Engine module.

Measures how teams are arriving right now using only recent form and recent
goal-difference signals. This module is pure and does not touch standings,
H2H, odds, motivation, APIs, or persistence.
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

_L5_SIZE = 5
_WEIGHT_ACTIVE_STREAK = 0.25
_WEIGHT_RESULT_FORM = 0.45
_WEIGHT_PERFORMANCE_FORM = 0.30


def _encode_result(code: Any) -> int:
    if code in (1, "1", "+1", True):
        return 1
    if code in (0, "0", "X", "x", "D", "d"):
        return 0
    if code in (-1, "2", "-1", False):
        return -1
    raise ValueError(f"unsupported_result_code={code!r}")


def _extract_result_codes(results: List[Dict], limit: int = _L5_SIZE) -> List[int]:
    encoded: List[int] = []
    for result in results[:limit]:
        code = result.get("team_result_code")
        try:
            encoded.append(_encode_result(code))
        except ValueError:
            continue
    return encoded


def _extract_goal_diff(result: Dict[str, Any]) -> Optional[float]:
    net_score = result.get("net_score")
    if net_score is not None:
        try:
            return float(net_score)
        except (TypeError, ValueError):
            pass

    score = extract_score_for_against(result)
    if score is None:
        return None
    team_score, opponent_score = score
    return float(team_score) - float(opponent_score)


def _extract_gd_series(results: List[Dict], limit: int = _L5_SIZE) -> List[float]:
    series: List[float] = []
    for result in results[:limit]:
        gd = _extract_goal_diff(result)
        if gd is not None:
            series.append(gd)
    return series


def _active_streak_score(encoded_results: List[int]) -> Dict[str, Any]:
    if not encoded_results:
        return {"type": "NONE", "length": 0, "sign": 0, "score": 0.0}

    first = encoded_results[0]
    streak_type = "WIN" if first == 1 else "DRAW" if first == 0 else "LOSS"
    streak_length = 0
    for value in encoded_results:
        if value != first:
            break
        streak_length += 1

    sign = 1 if first == 1 else -1 if first == -1 else 0
    score = 0.0 if sign == 0 else sign * math.log(1.0 + float(streak_length))
    return {
        "type": streak_type,
        "length": streak_length,
        "sign": sign,
        "score": score,
    }


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
    home_results_l5: List[int],
    away_results_l5: List[int],
    home_gd_l5: List[float],
    away_gd_l5: List[float],
) -> Tuple[str, str]:
    del home_gd_l5, away_gd_l5

    home_count = len(home_results_l5)
    away_count = len(away_results_l5)
    if home_count < 3 or away_count < 3:
        return "INSUFFICIENT_DATA", "insufficient_recent_results"
    if home_count < _L5_SIZE or away_count < _L5_SIZE:
        return "DEGRADED", "partial_l5_sample"
    return "ACTIVE", "active"


def calculate_recent_inertia_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    """Calculate M5 - Recent Inertia Engine for an event."""
    del event_context

    home_results: List[Dict] = getattr(streak_analysis, "home_team_results", None) or []
    away_results: List[Dict] = getattr(streak_analysis, "away_team_results", None) or []
    home_team = getattr(streak_analysis, "home_team_name", None)
    away_team = getattr(streak_analysis, "away_team_name", None)
    event_id = getattr(streak_analysis, "event_id", 0)
    participants = getattr(streak_analysis, "participants", "") or ""

    home_result_l5_encoded = _extract_result_codes(home_results)
    away_result_l5_encoded = _extract_result_codes(away_results)
    home_gd_l5 = _extract_gd_series(home_results)
    away_gd_l5 = _extract_gd_series(away_results)

    m5_status, m5_status_reason = _determine_status(
        home_result_l5_encoded,
        away_result_l5_encoded,
        home_gd_l5,
        away_gd_l5,
    )

    home_active_streak = _active_streak_score(home_result_l5_encoded)
    away_active_streak = _active_streak_score(away_result_l5_encoded)
    active_streak_edge = clamp(
        _relative_edge(
            float(home_active_streak["score"]),
            float(away_active_streak["score"]),
        )
    )

    home_result_count = len(home_result_l5_encoded)
    away_result_count = len(away_result_l5_encoded)
    home_result_form_score = (
        sum(home_result_l5_encoded) / float(home_result_count)
        if home_result_count > 0
        else 0.0
    )
    away_result_form_score = (
        sum(away_result_l5_encoded) / float(away_result_count)
        if away_result_count > 0
        else 0.0
    )
    result_form_edge = clamp(home_result_form_score - away_result_form_score)

    home_gd_count = len(home_gd_l5)
    away_gd_count = len(away_gd_l5)
    performance_form_reason = "active"
    if home_gd_count < 3 or away_gd_count < 3:
        performance_form_reason = "insufficient_gd_sample"
        performance_form_edge = 0.0
        if m5_status == "ACTIVE":
            m5_status = "DEGRADED"
            m5_status_reason = "missing_or_partial_gd_sample"
    else:
        home_avg_gd_l5 = sum(home_gd_l5) / float(home_gd_count)
        away_avg_gd_l5 = sum(away_gd_l5) / float(away_gd_count)
        performance_form_edge = clamp(_relative_edge(home_avg_gd_l5, away_avg_gd_l5))

    m5_edge_raw = (
        (_WEIGHT_ACTIVE_STREAK * active_streak_edge)
        + (_WEIGHT_RESULT_FORM * result_form_edge)
        + (_WEIGHT_PERFORMANCE_FORM * performance_form_edge)
    )
    m5_edge = clamp(m5_edge_raw)
    if m5_status == "INSUFFICIENT_DATA":
        m5_edge = 0.0

    home_avg_gd_l5 = sum(home_gd_l5) / float(home_gd_count) if home_gd_count > 0 else 0.0
    away_avg_gd_l5 = sum(away_gd_l5) / float(away_gd_count) if away_gd_count > 0 else 0.0

    if debug_mode:
        logger.info(f"--- M5 Recent Inertia Engine Debug: Event {event_id} ({participants}) ---")
        logger.info(f"  home_result_l5_encoded={home_result_l5_encoded}")
        logger.info(f"  away_result_l5_encoded={away_result_l5_encoded}")
        logger.info(f"  home_gd_l5={home_gd_l5}")
        logger.info(f"  away_gd_l5={away_gd_l5}")
        logger.info(
            "  active_streak_home=%s active_streak_away=%s",
            home_active_streak,
            away_active_streak,
        )
        logger.info(
            "  [ACTIVE_STREAK_EDGE] home_score=%.12f away_score=%.12f edge=%.12f",
            float(home_active_streak["score"]),
            float(away_active_streak["score"]),
            active_streak_edge,
        )
        logger.info(
            "  [RESULT_FORM_EDGE] home_score=%.12f away_score=%.12f edge=%.12f",
            home_result_form_score,
            away_result_form_score,
            result_form_edge,
        )
        logger.info(
            "  [PERFORMANCE_FORM_EDGE] home_avg_gd_l5=%.12f away_avg_gd_l5=%.12f edge=%.12f reason=%s",
            home_avg_gd_l5,
            away_avg_gd_l5,
            performance_form_edge,
            performance_form_reason,
        )
        logger.info(
            "  [M5_EDGE_RAW] (0.25 * %.12f) + (0.45 * %.12f) + (0.30 * %.12f) = %.12f",
            active_streak_edge,
            result_form_edge,
            performance_form_edge,
            m5_edge_raw,
        )
        logger.info(
            "  [M5_EDGE] clamped=%.12f bias=%s strength=%s status=%s (%s)",
            m5_edge,
            calculate_bias(m5_edge),
            classify_strength(m5_edge),
            m5_status,
            m5_status_reason,
        )
        logger.info("-" * 60)

    components = [
        _component(
            "ACTIVE_STREAK_EDGE",
            active_streak_edge,
            _WEIGHT_ACTIVE_STREAK,
            {
                "home": home_active_streak,
                "away": away_active_streak,
                "formula": "(S_HOME - S_AWAY) / (abs(S_HOME) + abs(S_AWAY))",
            },
        ),
        _component(
            "RESULT_FORM_EDGE",
            result_form_edge,
            _WEIGHT_RESULT_FORM,
            {
                "home_score": home_result_form_score,
                "away_score": away_result_form_score,
                "formula": "home_score - away_score",
            },
        ),
        _component(
            "PERFORMANCE_FORM_EDGE",
            performance_form_edge,
            _WEIGHT_PERFORMANCE_FORM,
            {
                "home_avg_gd_l5": home_avg_gd_l5,
                "away_avg_gd_l5": away_avg_gd_l5,
                "formula": "(home_avg_gd_l5 - away_avg_gd_l5) / (abs(home_avg_gd_l5) + abs(away_avg_gd_l5))",
                "reason": performance_form_reason,
            },
        ),
    ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "home_result_l5_encoded": home_result_l5_encoded,
        "away_result_l5_encoded": away_result_l5_encoded,
        "home_result_l5_count": home_result_count,
        "away_result_l5_count": away_result_count,
        "home_gd_l5": home_gd_l5,
        "away_gd_l5": away_gd_l5,
        "active_streak": {
            "home": home_active_streak,
            "away": away_active_streak,
            "edge": active_streak_edge,
        },
        "result_form": {
            "home_score": home_result_form_score,
            "away_score": away_result_form_score,
            "edge": result_form_edge,
        },
        "performance_form": {
            "home_avg_gd_l5": home_avg_gd_l5,
            "away_avg_gd_l5": away_avg_gd_l5,
            "edge": performance_form_edge,
            "reason": performance_form_reason,
        },
        "performance_form_reason": performance_form_reason,
        "m5_edge_raw": m5_edge_raw,
        "m5_edge": m5_edge,
        "m5_abs_edge": abs(m5_edge),
        "m5_status": m5_status,
        "m5_status_reason": m5_status_reason,
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M5",
        module_name="Recent Inertia Engine",
        event_id=event_id,
        participants=participants,
        value=m5_edge,
        bias=calculate_bias(m5_edge),
        strength=classify_strength(m5_edge),
        components=components,
        raw=raw,
    )
