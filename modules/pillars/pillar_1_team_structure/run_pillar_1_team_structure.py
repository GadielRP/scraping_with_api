"""Pillar 1 - Team Structure aggregator."""

from __future__ import annotations

from typing import Any, Dict

from modules.pillars.common import clamp
from modules.pillars.context import EventContext
from modules.pillars.pillar_1_team_structure.module_1.base_strength import (
    calculate_base_strength,
)
from modules.pillars.pillar_1_team_structure.module_2.performance_profile import (
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

_MODULE_WEIGHTS: Dict[str, float] = {
    "M1": 0.17,
    "M2": 0.17,
    "M3": 0.10,
    "M4": 0.14,
    "M5": 0.11,
    "M6": 0.09,
    "M7": 0.11,
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


def _aggregate_module_results(module_results):
    active_modules = []
    skipped_modules = []
    active_weight_sum = 0.0

    for module_result in module_results:
        module_id = module_result.module_id
        module_weight = float(_MODULE_WEIGHTS.get(module_id, 0.0))
        module_status = _module_status(module_result)
        module_reason = _module_status_reason(module_result)
        module_entry = {
            "module_id": module_id,
            "module_name": module_result.module_name,
            "status": module_status,
            "reason": module_reason,
            "value": module_result.value,
            "weight": module_weight,
        }

        if (
            module_status == "INSUFFICIENT_DATA"
            or module_status == "INACTIVE"
            or module_status.startswith("INVALID")
        ):
            skipped_modules.append(module_entry)
            continue

        active_modules.append(module_entry)
        active_weight_sum += module_weight

    module_weights: Dict[str, float] = {}
    pillar_value = 0.0
    if active_weight_sum > 0:
        for module_entry in active_modules:
            normalized_weight = module_entry["weight"] / active_weight_sum
            module_weights[module_entry["module_id"]] = normalized_weight
            pillar_value += module_entry["value"] * normalized_weight

    pillar_value = clamp(pillar_value)

    return pillar_value, {
        "module_weights": module_weights,
        "active_modules": active_modules,
        "skipped_modules": skipped_modules,
    }


def calculate_pillar_1_team_structure(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> Dict[str, Any]:
    """Calculate Pillar 1 - Team Structure for an event.

    Args:
        streak_analysis: A ``MatchupStreakContext`` produced by the streak
            analysis pipeline.

    Returns:
        A dictionary containing pillar metadata, the list of module results,
        and the aggregated pillar value.
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

    pillar_value, aggregation_raw = _aggregate_module_results([
        m1_result,
        m2_result,
        m3_result,
        m4_result,
        m5_result,
        m6_result,
        m7_result,
    ])

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
            "module_weights": aggregation_raw["module_weights"],
            "active_modules": aggregation_raw["active_modules"],
            "skipped_modules": aggregation_raw["skipped_modules"],
            "m1_raw": m1_result.raw,
            "m2_raw": m2_result.raw,
            "m3_raw": m3_result.raw,
            "m4_raw": m4_result.raw,
            "m5_raw": m5_result.raw,
            "m6_raw": m6_result.raw,
            "m7_raw": m7_result.raw,
        },
    }
