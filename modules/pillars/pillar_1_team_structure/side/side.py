"""Pillar 1 - Team Structure Side Engine."""

from __future__ import annotations

import logging
import math
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

_ENGINE_VERSION = "side_engine_multilayer_v3_0"
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


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== P1_SIDE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("P1_SIDE DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: str | None = None,
) -> None:
    logger.info("P1_SIDE DEBUG | %s", name)
    logger.info("P1_SIDE DEBUG |   Formula: %s", formula)
    logger.info("P1_SIDE DEBUG |   Sustitución: %s", substitution)
    logger.info("P1_SIDE DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("P1_SIDE DEBUG |   Lectura: %s", meaning)


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


def _classify_m5_strength(m5_edge: float) -> str:
    abs_edge = abs(float(m5_edge or 0.0))
    if abs_edge < 0.15:
        return "LOW"
    if abs_edge < 0.30:
        return "MODERATE"
    if abs_edge < 0.45:
        return "HIGH"
    if abs_edge < 0.60:
        return "SEVERE"
    return "OVERRIDE"


def _pressure_side_from_edge(m5_edge: float) -> str:
    if m5_edge > 0:
        return "HOME"
    if m5_edge < 0:
        return "AWAY"
    return "NONE"


def _resolve_pressure_relation(
    core_strength: str,
    core_bias: str,
    m5_pressure_side: str,
) -> str:
    if core_strength == "IGNORE":
        return "CONTEXT_ONLY"
    if core_bias == "NONE":
        return "INTERNAL_INCONSISTENCY"
    if m5_pressure_side == "NONE":
        return "NO_PRESSURE"
    if core_bias == m5_pressure_side:
        return "PRESSURE_SUPPORTS_CORE"
    return "PRESSURE_CHALLENGES_CORE"


def _resolve_final_state_and_bias(
    core_strength: str,
    core_bias: str,
    m5_strength: str,
    m5_pressure_side: str,
    pressure_relation: str,
) -> tuple[str, str, list[str]]:
    anomalies: list[str] = []

    if core_strength != "IGNORE" and core_bias == "NONE":
        anomalies.append("CORE_BIAS_NONE_WITH_OPERATIVE_CORE")
        return "NO_BET", "NONE", anomalies

    if core_strength == "IGNORE":
        if m5_pressure_side == "NONE":
            return "NO_BET", "NONE", anomalies
        if m5_strength in {"LOW", "MODERATE"}:
            return "NO_BET", "NONE", anomalies
        if m5_strength == "HIGH":
            return "CONTEXT_WINDOW", m5_pressure_side, anomalies
        if m5_strength in {"SEVERE", "OVERRIDE"}:
            return "STRONG_CONTEXT_WINDOW", m5_pressure_side, anomalies
        return "NO_BET", "NONE", anomalies

    if pressure_relation == "NO_PRESSURE":
        return "CONFIRMED", core_bias, anomalies

    if pressure_relation == "INTERNAL_INCONSISTENCY":
        anomalies.append("CORE_BIAS_NONE_WITH_OPERATIVE_CORE")
        return "NO_BET", "NONE", anomalies

    if pressure_relation == "PRESSURE_SUPPORTS_CORE":
        if core_strength == "LOW" and m5_strength == "LOW":
            return "DEGRADED", core_bias, anomalies
        return "CONFIRMED", core_bias, anomalies

    if pressure_relation == "PRESSURE_CHALLENGES_CORE":
        if core_strength == "EXTREME":
            if m5_strength in {"LOW", "MODERATE", "HIGH"}:
                return "DEGRADED", core_bias, anomalies
            if m5_strength == "SEVERE":
                return "CONFLICT", core_bias, anomalies
            if m5_strength == "OVERRIDE":
                return "STRONG_CONFLICT", core_bias, anomalies
        elif core_strength in {"HIGH", "MEDIUM"}:
            if m5_strength in {"LOW", "MODERATE"}:
                return "DEGRADED", core_bias, anomalies
            if m5_strength == "HIGH":
                return "CONFLICT", core_bias, anomalies
            if m5_strength in {"SEVERE", "OVERRIDE"}:
                return "STRONG_CONFLICT", core_bias, anomalies
        elif core_strength == "LOW":
            if m5_strength == "LOW":
                return "DEGRADED", core_bias, anomalies
            if m5_strength == "MODERATE":
                return "CONFLICT", core_bias, anomalies
            if m5_strength == "HIGH":
                return "UNDERDOG_WINDOW", m5_pressure_side, anomalies
            if m5_strength in {"SEVERE", "OVERRIDE"}:
                return "STRONG_UNDERDOG_WINDOW", m5_pressure_side, anomalies

    return "NO_BET", "NONE", anomalies


def _aggregate_multilayer_side_engine_v3(module_results):
    results_by_id = {module.module_id: module for module in module_results}
    expected_ids = set(_EXPECTED_MODULE_IDS)
    provided_ids = set(results_by_id)

    unexpected_ids = sorted(provided_ids - expected_ids)
    if unexpected_ids:
        raise ValueError(
            f"Pillar 1 v3 uses exactly M1-M7; unexpected modules: {unexpected_ids}"
        )

    missing_ids = sorted(expected_ids - provided_ids)
    if missing_ids:
        raise ValueError(
            f"Pillar 1 v3 aggregation requires modules M1-M7; missing: {missing_ids}"
        )

    if len(results_by_id) != len(module_results):
        raise ValueError(
            "Pillar 1 v3 uses exactly M1-M7; duplicate module ids are not allowed"
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
                    "module_name": module_result.module_name,
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
    p1_core_side_bias = calculate_bias(p1_core_side)
    p1_core_side_strength = classify_strength(p1_core_side)

    m4_module = results_by_id["M4"]
    m4_edge = _effective_module_value(m4_module)
    m4_context_adj = clamp(m4_edge, -_M4_CONTEXT_ADJ_MAX, _M4_CONTEXT_ADJ_MAX)
    p1_effective_core = p1_core_side + m4_context_adj
    core_strength = classify_strength(p1_effective_core)
    core_bias = "NONE" if core_strength == "IGNORE" else calculate_bias(p1_effective_core)
    if core_bias == "NEUTRAL":
        core_bias = "NONE"

    m5_module = results_by_id["M5"]
    m5_edge = _effective_module_value(m5_module)
    m5_strength = _classify_m5_strength(m5_edge)
    m5_pressure_side = (
        str(m5_module.raw.get("m5_pressure_side"))
        if isinstance(getattr(m5_module, "raw", None), dict) and m5_module.raw.get("m5_pressure_side")
        else _pressure_side_from_edge(m5_edge)
    )

    p1_final_context_balance = p1_effective_core + m5_edge
    pressure_relation = _resolve_pressure_relation(
        core_strength,
        core_bias,
        m5_pressure_side,
    )
    p1_final_state, p1_final_bias, anomalies = _resolve_final_state_and_bias(
        core_strength,
        core_bias,
        m5_strength,
        m5_pressure_side,
        pressure_relation,
    )

    aggregation_raw = {
        "engine_version": _ENGINE_VERSION,
        "aggregation_mode": "multilayer_side_engine",
        "layer_a": {
            "name": "STRUCTURAL_SIDE_ENGINE",
            "weights": dict(_LAYER_A_WEIGHTS),
            "module_ids": ["M1", "M2", "M3", "M6", "M7"],
            "contributions": layer_a_contributions,
            "p1_core_side": p1_core_side,
            "p1_core_side_bias": p1_core_side_bias,
            "p1_core_side_strength": p1_core_side_strength,
            "p1_effective_core": p1_effective_core,
            "core_bias": core_bias,
            "core_strength": core_strength,
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
            "m5": {
                "module_id": "M5",
                "gate_type": _GATE_MODULES["M5"],
                "status": _module_status(m5_module),
                "reason": _module_status_reason(m5_module),
                "raw_value": m5_module.value,
                "effective_value": m5_edge,
                "m5_strength": m5_strength,
                "m5_pressure_side": m5_pressure_side,
                "strength_profile": "P1_SIDE_M5_BLUEPRINT_THRESHOLDS",
                "uses_hard_clamp": False,
            },
        },
        "final": {
            "p1_final_context_balance": p1_final_context_balance,
            "p1_final_context_balance_formula": "P1_EFFECTIVE_CORE + M5_EDGE",
            "p1_final_context_balance_is_decision": False,
            "pressure_relation": pressure_relation,
            "p1_final_state": p1_final_state,
            "p1_final_bias": p1_final_bias,
            "decision_inputs": {
                "core_strength": core_strength,
                "core_bias": core_bias,
                "m5_strength": m5_strength,
                "m5_pressure_side": m5_pressure_side,
                "pressure_relation": pressure_relation,
            },
            "anomalies": anomalies,
            "value_is_evidence_only": True,
        },
        "module_statuses": module_statuses,
        "active_modules": active_modules,
        "skipped_modules": skipped_modules,
        "p1_core_side": p1_core_side,
        "m4_context_adj": m4_context_adj,
        "p1_effective_core": p1_effective_core,
        "core_bias": core_bias,
        "core_strength": core_strength,
        "m5_edge": m5_edge,
        "m5_strength": m5_strength,
        "m5_pressure_side": m5_pressure_side,
        "p1_final_context_balance": p1_final_context_balance,
        "pressure_relation": pressure_relation,
        "p1_final_state": p1_final_state,
        "p1_final_bias": p1_final_bias,
        "anomalies": anomalies,
        "value_is_evidence_only": True,
        "p1_final_context_balance_is_decision": False,
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

    pillar_value, aggregation_raw = _aggregate_multilayer_side_engine_v3(
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
        _debug_section("INICIO P1 SIDE MULTILAYER AGGREGATION")
        _debug_line("Event ID: %s", _fmt(getattr(m1_result, "event_id", getattr(event_context, "event_id", 0))))
        _debug_line("Participantes: %s", _fmt(getattr(m1_result, "participants", getattr(event_context, "participants_label", ""))))
        _debug_line("Configuración Global / Constantes:")
        _debug_line("  _ENGINE_VERSION: %s", _fmt(_ENGINE_VERSION))
        _debug_line("  _M4_CONTEXT_ADJ_MAX: %s", _fmt(_M4_CONTEXT_ADJ_MAX))
        _debug_line("  _LAYER_A_WEIGHTS: %s", _fmt(_LAYER_A_WEIGHTS))
        _debug_line("  _GATE_MODULES: %s", _fmt(_GATE_MODULES))

        _debug_section("LAYER A - STRUCTURAL SIDE ENGINE CONTRIBUTIONS")
        for contribution in aggregation_raw["layer_a"]["contributions"]:
            _debug_formula(
                f"{contribution['module_id']} WEIGHTED EDGE",
                "weight * effective_value",
                f"{_fmt(contribution['weight'])} * {_fmt(contribution['effective_value'])}",
                _fmt(contribution['weighted_edge']),
                f"Contribución de {contribution['module_name']} (status: {contribution['status']})"
            )
        
        _debug_formula(
            "P1_CORE_SIDE",
            " + ".join(f"{c['module_id']}_contrib" for c in aggregation_raw["layer_a"]["contributions"]),
            " + ".join(_fmt(c["weighted_edge"]) for c in aggregation_raw["layer_a"]["contributions"]),
            _fmt(aggregation_raw["p1_core_side"]),
            "Fuerza estructural base sumando módulos operativos"
        )
        _debug_line("  P1_CORE_SIDE_BIAS: %s", aggregation_raw["layer_a"]["p1_core_side_bias"])
        _debug_line("  P1_CORE_SIDE_STRENGTH: %s", aggregation_raw["layer_a"]["p1_core_side_strength"])

        _debug_section("LAYER B - CONTEXT GATES (M4, M5)")
        _debug_line("Evaluando M4 (Minor Context Gate):")
        m4_eff = aggregation_raw["layer_b"]["m4"]["effective_value"]
        _debug_formula(
            "M4_CONTEXT_ADJ",
            "CLAMP(M4_EDGE, -_M4_CONTEXT_ADJ_MAX, +_M4_CONTEXT_ADJ_MAX)",
            f"CLAMP({_fmt(m4_eff)}, -{_fmt(_M4_CONTEXT_ADJ_MAX)}, +{_fmt(_M4_CONTEXT_ADJ_MAX)})",
            _fmt(aggregation_raw["m4_context_adj"]),
            "Ajuste menor contextual (M4)"
        )

        _debug_formula(
            "P1_EFFECTIVE_CORE",
            "P1_CORE_SIDE + M4_CONTEXT_ADJ",
            f"{_fmt(aggregation_raw['p1_core_side'])} + {_fmt(aggregation_raw['m4_context_adj'])}",
            _fmt(aggregation_raw["p1_effective_core"]),
            "Core efectivo tras ajuste M4"
        )
        _debug_line("  CORE_BIAS: %s", aggregation_raw["core_bias"])
        _debug_line("  CORE_STRENGTH: %s", aggregation_raw["core_strength"])

        _debug_line("Evaluando M5 (Major Context Gate):")
        _debug_line("  M5 Edge: %s", _fmt(aggregation_raw["m5_edge"]))
        _debug_line("  M5 Strength: %s", aggregation_raw["m5_strength"])
        _debug_line("  M5 Pressure Side: %s", aggregation_raw["m5_pressure_side"])

        _debug_section("FINAL AGGREGATION & DECISION")
        _debug_formula(
            "P1_FINAL_CONTEXT_BALANCE",
            "P1_EFFECTIVE_CORE + M5_EDGE",
            f"{_fmt(aggregation_raw['p1_effective_core'])} + {_fmt(aggregation_raw['m5_edge'])}",
            _fmt(aggregation_raw["p1_final_context_balance"]),
            "Balance contextual final (evidencia numérica)"
        )
        
        _debug_line("Relación de Presión M5 vs Core (pressure_relation): %s", aggregation_raw["pressure_relation"])
        _debug_line("Estado Final (p1_final_state): %s", aggregation_raw["p1_final_state"])
        _debug_line("Bias Final (p1_final_bias): %s", aggregation_raw["p1_final_bias"])
        
        if aggregation_raw["anomalies"]:
            _debug_line("Anomalías detectadas: %s", aggregation_raw["anomalies"])
        _debug_section("FIN P1 SIDE MULTILAYER AGGREGATION")

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
            "p1_core_side": aggregation_raw["p1_core_side"],
            "m4_context_adj": aggregation_raw["m4_context_adj"],
            "p1_effective_core": aggregation_raw["p1_effective_core"],
            "core_bias": aggregation_raw["core_bias"],
            "core_strength": aggregation_raw["core_strength"],
            "m5_edge": aggregation_raw["m5_edge"],
            "m5_strength": aggregation_raw["m5_strength"],
            "m5_pressure_side": aggregation_raw["m5_pressure_side"],
            "p1_final_context_balance": aggregation_raw["p1_final_context_balance"],
            "pressure_relation": aggregation_raw["pressure_relation"],
            "p1_final_state": aggregation_raw["p1_final_state"],
            "p1_final_bias": aggregation_raw["p1_final_bias"],
            "anomalies": aggregation_raw["anomalies"],
            "value_is_evidence_only": aggregation_raw["value_is_evidence_only"],
            "p1_final_context_balance_is_decision": aggregation_raw[
                "p1_final_context_balance_is_decision"
            ],
            "m1_raw": m1_result.raw,
            "m2_raw": m2_result.raw,
            "m3_raw": m3_result.raw,
            "m4_raw": m4_result.raw,
            "m5_raw": m5_result.raw,
            "m6_raw": m6_result.raw,
            "m7_raw": m7_result.raw,
        },
    }
