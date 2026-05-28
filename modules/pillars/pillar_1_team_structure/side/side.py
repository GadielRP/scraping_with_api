"""Pillar 1 - Team Structure Side Engine."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from modules.pillars.common import calculate_bias, clamp, classify_strength
from modules.pillars.context import EventContext
from modules.pillars.pillar_1_team_structure.module_1.base_strength import (
    calculate_base_strength,
)
from modules.pillars.pillar_1_team_structure.module_2.offensive_profile_engine import (
    calculate_performance_profile,
)
from modules.pillars.pillar_1_team_structure.module_3.direct_matchup_profile import (
    calculate_direct_matchup_profile,
)
from modules.pillars.pillar_1_team_structure.module_4.quality_adjusted_immediate_state_engine import (
    calculate_quality_adjusted_immediate_state_engine,
)
from modules.pillars.pillar_1_team_structure.module_5.contextual_competitive_cost_engine import (
    calculate_contextual_competitive_cost_engine,
)
from modules.pillars.pillar_1_team_structure.module_6.structural_drift_engine import (
    calculate_structural_drift_engine,
)
from modules.pillars.pillar_1_team_structure.module_7.opponent_expectation_engine import (
    calculate_m7_opponent_expectation_engine,
)

logger = logging.getLogger(__name__)

_ENGINE_VERSION = "side_engine_multilayer_v2_0"
_LAYER_A_WEIGHTS: Dict[str, float] = {
    "M1": 0.30,
    "M2": 0.20,
    "M3": 0.15,
    "M6": 0.20,
    "M7": 0.15,
}
_M4_CONTEXT_ADJ_MAX = 0.06
_EXPECTED_MODULE_IDS: List[str] = ["M1", "M2", "M3", "M4", "M5", "M6", "M7"]
_GATE_MODULES = {
    "M4": "minor_context_gate",
    "M5": "major_context_gate",
}


def _serialize_module_result(module_result) -> Dict[str, Any]:
    return {
        "module_id": module_result.module_id,
        "module_name": module_result.module_name,
        "value": module_result.value,
        "bias": module_result.bias,
        "strength": module_result.strength,
        "components": [
            {
                "name": component.name,
                "edge": component.edge,
                "bias": component.bias,
                "strength": component.strength,
                "weight": component.weight,
                "weighted_edge": component.weighted_edge,
                "raw": component.raw,
            }
            for component in module_result.components
        ],
        "raw": module_result.raw,
    }


def _module_status(module_result) -> str:
    status_key = f"{module_result.module_id.lower()}_status"
    status = module_result.raw.get(status_key, "ACTIVE")
    return str(status).upper()


def _module_status_reason(module_result) -> str:
    reason_key = f"{module_result.module_id.lower()}_status_reason"
    reason = module_result.raw.get(reason_key, "active")
    return str(reason)


def _is_module_usable(module_result) -> bool:
    status = _module_status(module_result)
    return not (
        status == "INSUFFICIENT_DATA"
        or status == "INACTIVE"
        or status.startswith("INVALID")
    )


def _effective_module_value(module_result) -> float:
    if not _is_module_usable(module_result):
        return 0.0
    return float(module_result.value or 0.0)


def _final_contextual_bias(final_balance: float, m5_edge: float) -> str:
    if final_balance > 0:
        return "HOME_CONTEXTUAL" if m5_edge != 0 else "HOME"
    if final_balance < 0:
        return "AWAY_CONTEXTUAL" if m5_edge != 0 else "AWAY"
    return "NEUTRAL"


def _context_state(core_side: float, after_m4: float, m5_edge: float, final_balance: float) -> str:
    core_bias = calculate_bias(core_side)
    final_bias = calculate_bias(final_balance)
    m5_bias = calculate_bias(m5_edge)

    if final_bias == "NEUTRAL":
        return "NEUTRAL_BALANCE"

    if m5_bias != "NEUTRAL" and core_bias != "NEUTRAL" and final_bias != core_bias:
        return "UNDERDOG_WINDOW"

    if m5_bias != "NEUTRAL" and final_bias == m5_bias and abs(m5_edge) > abs(after_m4):
        return "CONTEXT_OVERRIDE"

    if abs(m5_edge) > 0:
        return "CONTEXT_ALIGNED"

    return "STRUCTURAL_ONLY"


def _aggregate_multilayer_side_engine_v2(module_results):
    results_by_id = {module.module_id: module for module in module_results}
    expected_ids = set(_EXPECTED_MODULE_IDS)
    provided_ids = set(results_by_id)

    unexpected_ids = sorted(provided_ids - expected_ids)
    if unexpected_ids:
        raise ValueError(
            f"Pillar 1 v2 uses exactly M1-M7; unexpected modules: {unexpected_ids}"
        )

    missing_ids = sorted(expected_ids - provided_ids)
    if missing_ids:
        raise ValueError(
            f"Pillar 1 v2 aggregation requires modules M1-M7; missing: {missing_ids}"
        )

    if len(results_by_id) != len(module_results):
        raise ValueError(
            "Pillar 1 v2 uses exactly M1-M7; duplicate module ids are not allowed"
        )

    layer_a_contributions: list[Dict[str, Any]] = []
    module_statuses: Dict[str, Dict[str, str]] = {}
    active_modules: list[Dict[str, Any]] = []
    skipped_modules: list[Dict[str, Any]] = []

    for module_id in _EXPECTED_MODULE_IDS:
        module_result = results_by_id[module_id]
        status = _module_status(module_result)
        reason = _module_status_reason(module_result)
        raw_value = module_result.value
        effective_value = _effective_module_value(module_result)

        module_entry = {
            "module_id": module_id,
            "module_name": module_result.module_name,
            "status": status,
            "reason": reason,
            "raw_value": raw_value,
            "effective_value": effective_value,
        }
        module_statuses[module_id] = {"status": status, "reason": reason}
        if _is_module_usable(module_result):
            active_modules.append(module_entry)
        else:
            skipped_modules.append(module_entry)

        if module_id in _LAYER_A_WEIGHTS:
            weight = _LAYER_A_WEIGHTS[module_id]
            weighted_edge = weight * effective_value
            layer_a_contributions.append(
                {
                    "module_id": module_id,
                    "status": status,
                    "reason": reason,
                    "raw_value": raw_value,
                    "effective_value": effective_value,
                    "weight": weight,
                    "weighted_edge": weighted_edge,
                }
            )

    m1_edge = _effective_module_value(results_by_id["M1"])
    m2_edge = _effective_module_value(results_by_id["M2"])
    m3_edge = _effective_module_value(results_by_id["M3"])
    m6_edge = _effective_module_value(results_by_id["M6"])
    m7_edge = _effective_module_value(results_by_id["M7"])

    m1_contribution = _LAYER_A_WEIGHTS["M1"] * m1_edge
    m2_contribution = _LAYER_A_WEIGHTS["M2"] * m2_edge
    m3_contribution = _LAYER_A_WEIGHTS["M3"] * m3_edge
    m6_contribution = _LAYER_A_WEIGHTS["M6"] * m6_edge
    m7_contribution = _LAYER_A_WEIGHTS["M7"] * m7_edge

    p1_core_side = sum(
        [
            m1_contribution,
            m2_contribution,
            m3_contribution,
            m6_contribution,
            m7_contribution,
        ]
    )
    p1_core_bias = calculate_bias(p1_core_side)
    p1_core_strength = classify_strength(p1_core_side)

    m4_module = results_by_id["M4"]
    m4_edge = _effective_module_value(m4_module)
    m4_context_adj = clamp(m4_edge, -_M4_CONTEXT_ADJ_MAX, _M4_CONTEXT_ADJ_MAX)
    p1_after_m4 = p1_core_side + m4_context_adj

    m5_module = results_by_id["M5"]
    m5_edge = _effective_module_value(m5_module)
    m5_bias = calculate_bias(m5_edge)
    m5_strength = classify_strength(m5_edge)

    p1_final_context_balance_raw = p1_after_m4 + m5_edge
    p1_final_context_balance = clamp(p1_final_context_balance_raw)
    p1_final_bias_base = calculate_bias(p1_final_context_balance)
    p1_final_bias = _final_contextual_bias(p1_final_context_balance, m5_edge)
    p1_final_strength = classify_strength(p1_final_context_balance)
    p1_context_state = _context_state(
        p1_core_side,
        p1_after_m4,
        m5_edge,
        p1_final_context_balance,
    )

    aggregation_raw = {
        "engine_version": _ENGINE_VERSION,
        "aggregation_mode": "multilayer_side_engine",
        "legacy_aggregation_disabled": True,
        "layer_a": {
            "name": "STRUCTURAL_SIDE_ENGINE",
            "weights": dict(_LAYER_A_WEIGHTS),
            "module_ids": ["M1", "M2", "M3", "M6", "M7"],
            "contributions": layer_a_contributions,
            "p1_core_side": p1_core_side,
            "p1_core_bias": p1_core_bias,
            "p1_core_strength": p1_core_strength,
        },
        "layer_b": {
            "name": "CONTEXT_GATES",
            "m4": {
                "module_id": "M4",
                "gate_type": _GATE_MODULES["M4"],
                "status": _module_status(m4_module),
                "reason": _module_status_reason(m4_module),
                "raw_value": m4_module.value,
                "effective_value": m4_edge,
                "context_adj_max": _M4_CONTEXT_ADJ_MAX,
                "m4_context_adj": m4_context_adj,
                "formula": "CLAMP(M4_EDGE, -0.06, +0.06)",
            },
            "p1_after_m4": p1_after_m4,
            "m5": {
                "module_id": "M5",
                "gate_type": _GATE_MODULES["M5"],
                "status": _module_status(m5_module),
                "reason": _module_status_reason(m5_module),
                "raw_value": m5_module.value,
                "effective_value": m5_edge,
                "m5_bias": m5_bias,
                "m5_strength": m5_strength,
                "uses_hard_clamp": False,
            },
        },
        "final": {
            "p1_final_context_balance_raw": p1_final_context_balance_raw,
            "p1_final_context_balance": p1_final_context_balance,
            "p1_context_state": p1_context_state,
            "p1_final_bias_base": p1_final_bias_base,
            "p1_final_bias": p1_final_bias,
            "p1_final_strength": p1_final_strength,
            "value_is_final_context_balance": True,
        },
        "module_statuses": module_statuses,
        "active_modules": active_modules,
        "skipped_modules": skipped_modules,
    }

    return p1_final_context_balance, aggregation_raw


def calculate_p1_side(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> Dict[str, Any]:
    """Calculate Pillar 1 - Team Structure Side components for an event.

    Args:
        streak_analysis: A ``MatchupStreakContext`` produced by the streak
            analysis pipeline.
        event_context: An ``EventContext``.
        debug_mode: True to log extra details.

    Returns:
        A dictionary containing side pillar metadata, module results,
        and aggregated side values.
    """
    m1_result = calculate_base_strength(
        streak_analysis,
        event_context=event_context,
        debug_mode=debug_mode,
    )
    m2_result = calculate_performance_profile(
        streak_analysis,
        event_context=event_context,
        debug_mode=debug_mode,
    )
    m3_result = calculate_direct_matchup_profile(
        streak_analysis,
        event_context=event_context,
        debug_mode=debug_mode,
    )
    m4_result = calculate_quality_adjusted_immediate_state_engine(
        streak_analysis,
        event_context=event_context,
        debug_mode=debug_mode,
    )
    m5_result = calculate_contextual_competitive_cost_engine(
        streak_analysis,
        event_context=event_context,
        debug_mode=debug_mode,
    )
    m6_result = calculate_structural_drift_engine(
        streak_analysis,
        event_context=event_context,
        debug_mode=debug_mode,
    )
    m7_result = calculate_m7_opponent_expectation_engine(
        streak_analysis,
        event_context=event_context,
        debug_mode=debug_mode,
    )

    pillar_value, aggregation_raw = _aggregate_multilayer_side_engine_v2(
        [
            m1_result,
            m2_result,
            m3_result,
            m4_result,
            m5_result,
            m6_result,
            m7_result,
        ]
    )

    if debug_mode:
        logger.info(
            "--- Pillar 1 Side v2 Debug: event_id=%s participants=%s engine_version=%s ---",
            getattr(m1_result, "event_id", getattr(event_context, "event_id", 0)),
            getattr(m1_result, "participants", getattr(event_context, "participants_label", "")),
            aggregation_raw["engine_version"],
        )
        logger.info(
            "  layer_a: p1_core_side=%s bias=%s strength=%s",
            aggregation_raw["layer_a"]["p1_core_side"],
            aggregation_raw["layer_a"]["p1_core_bias"],
            aggregation_raw["layer_a"]["p1_core_strength"],
        )
        for contribution in aggregation_raw["layer_a"]["contributions"]:
            logger.info(
                "  layer_a contribution %s | status=%s reason=%s raw=%s effective=%s weight=%s weighted_edge=%s",
                contribution["module_id"],
                contribution["status"],
                contribution["reason"],
                contribution["raw_value"],
                contribution["effective_value"],
                contribution["weight"],
                contribution["weighted_edge"],
            )
        logger.info(
            "  layer_b M4: raw=%s effective=%s adjusted=%s after_m4=%s",
            aggregation_raw["layer_b"]["m4"]["raw_value"],
            aggregation_raw["layer_b"]["m4"]["effective_value"],
            aggregation_raw["layer_b"]["m4"]["m4_context_adj"],
            aggregation_raw["layer_b"]["p1_after_m4"],
        )
        logger.info(
            "  layer_b M5: raw=%s effective=%s bias=%s strength=%s",
            aggregation_raw["layer_b"]["m5"]["raw_value"],
            aggregation_raw["layer_b"]["m5"]["effective_value"],
            aggregation_raw["layer_b"]["m5"]["m5_bias"],
            aggregation_raw["layer_b"]["m5"]["m5_strength"],
        )
        logger.info(
            "  side: raw=%s clamped=%s bias=%s strength=%s context_state=%s",
            aggregation_raw["final"]["p1_final_context_balance_raw"],
            aggregation_raw["final"]["p1_final_context_balance"],
            aggregation_raw["final"]["p1_final_bias"],
            aggregation_raw["final"]["p1_final_strength"],
            aggregation_raw["final"]["p1_context_state"],
        )

    return {
        "pillar_id": "pillar_1_team_structure",
        "pillar_name": "Team Structure",
        "event_id": m1_result.event_id,
        "participants": m1_result.participants,
        "modules": [
            _serialize_module_result(m1_result),
            _serialize_module_result(m2_result),
            _serialize_module_result(m3_result),
            _serialize_module_result(m4_result),
            _serialize_module_result(m5_result),
            _serialize_module_result(m6_result),
            _serialize_module_result(m7_result),
        ],
        "value": pillar_value,
        "raw": {
            "event_context_present": True,
            "context_status": event_context.context_status,
            "engine_version": aggregation_raw["engine_version"],
            "aggregation_mode": aggregation_raw["aggregation_mode"],
            "layer_a": aggregation_raw["layer_a"],
            "layer_b": aggregation_raw["layer_b"],
            "final": aggregation_raw["final"],
            "module_statuses": aggregation_raw["module_statuses"],
            "active_modules": aggregation_raw["active_modules"],
            "skipped_modules": aggregation_raw["skipped_modules"],
            "module_weights": None,
            "module_weights_legacy_disabled": True,
            "m1_raw": m1_result.raw,
            "m2_raw": m2_result.raw,
            "m3_raw": m3_result.raw,
            "m4_raw": m4_result.raw,
            "m5_raw": m5_result.raw,
            "m6_raw": m6_result.raw,
            "m7_raw": m7_result.raw,
        },
    }
