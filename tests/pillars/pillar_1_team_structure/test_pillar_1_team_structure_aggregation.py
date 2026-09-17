from __future__ import annotations

from types import SimpleNamespace

import pytest

from modules.pillars.pillar_1_team_structure.run_pillar_1_team_structure import (
    _aggregate_multilayer_side_engine_v2,
)


def _module(
    module_id: str,
    value: float,
    *,
    status: str = "ACTIVE",
    reason: str = "active",
) -> SimpleNamespace:
    lower = module_id.lower()
    return SimpleNamespace(
        module_id=module_id,
        module_name=f"Module {module_id}",
        value=value,
        raw={
            f"{lower}_status": status,
            f"{lower}_status_reason": reason,
        },
    )


def _blueprint_modules(**overrides) -> list[SimpleNamespace]:
    values = {
        "M1": 0.582958473481,
        "M2": 0.019711178300,
        "M3": 0.160000000000,
        "M4": 0.080000000000,
        "M5": -0.515151515152,
        "M6": -0.515151515152,
        "M7": 0.036342105263,
    }
    statuses = {
        "M1": ("ACTIVE", "active"),
        "M2": ("ACTIVE", "active"),
        "M3": ("ACTIVE", "active"),
        "M4": ("ACTIVE", "active"),
        "M5": ("ACTIVE", "active"),
        "M6": ("ACTIVE", "active"),
        "M7": ("ACTIVE", "active"),
    }

    for module_id, payload in overrides.items():
        if "value" in payload:
            values[module_id] = payload["value"]
        if "status" in payload or "reason" in payload:
            current_status, current_reason = statuses[module_id]
            statuses[module_id] = (
                payload.get("status", current_status),
                payload.get("reason", current_reason),
            )

    return [
        _module(
            module_id,
            values[module_id],
            status=statuses[module_id][0],
            reason=statuses[module_id][1],
        )
        for module_id in ["M1", "M2", "M3", "M4", "M5", "M6", "M7"]
    ]


def test_blueprint_case_uses_multilayer_v2_formula():
    value, raw = _aggregate_multilayer_side_engine_v2(_blueprint_modules())

    assert value == pytest.approx(-0.34990072468865, abs=1e-12)
    assert raw["engine_version"] == "side_engine_multilayer_v2_0"
    assert raw["aggregation_mode"] == "multilayer_side_engine"
    assert raw["legacy_aggregation_disabled"] is True
    assert raw["layer_a"]["module_ids"] == ["M1", "M2", "M3", "M6", "M7"]
    assert raw["layer_a"]["p1_core_side"] == pytest.approx(0.10525079046335, abs=1e-12)
    assert raw["layer_a"]["p1_core_bias"] == "HOME"
    assert raw["layer_b"]["m4"]["m4_context_adj"] == pytest.approx(0.06, abs=1e-12)
    assert raw["layer_b"]["p1_after_m4"] == pytest.approx(0.16525079046335, abs=1e-12)
    assert raw["layer_b"]["m5"]["m5_bias"] == "AWAY"
    assert raw["final"]["p1_final_context_balance_raw"] == pytest.approx(-0.34990072468865, abs=1e-12)
    assert raw["final"]["p1_final_context_balance"] == pytest.approx(-0.34990072468865, abs=1e-12)
    assert raw["final"]["p1_final_bias"] == "AWAY_CONTEXTUAL"
    assert raw["final"]["p1_final_strength"] == "HIGH"
    assert raw["final"]["p1_context_state"] == "UNDERDOG_WINDOW"
    assert raw["module_statuses"]["M1"] == {"status": "ACTIVE", "reason": "active"}
    assert len(raw["layer_a"]["contributions"]) == 5
    assert len(raw["active_modules"]) == 7
    assert len(raw["skipped_modules"]) == 0


def test_m4_context_gate_clamps_positive_edge():
    value, raw = _aggregate_multilayer_side_engine_v2(
        _blueprint_modules(M4={"value": 0.50})
    )

    assert raw["layer_b"]["m4"]["m4_context_adj"] == pytest.approx(0.06, abs=1e-12)
    assert raw["layer_b"]["p1_after_m4"] == pytest.approx(0.16525079046335, abs=1e-12)
    assert value == pytest.approx(-0.34990072468865, abs=1e-12)


def test_m4_context_gate_clamps_negative_edge():
    value, raw = _aggregate_multilayer_side_engine_v2(
        _blueprint_modules(M4={"value": -0.50})
    )

    assert raw["layer_b"]["m4"]["m4_context_adj"] == pytest.approx(-0.06, abs=1e-12)
    assert raw["layer_b"]["p1_after_m4"] == pytest.approx(0.04525079046335, abs=1e-12)
    assert value == pytest.approx(-0.46990072468865, abs=1e-12)


def test_insufficient_data_module_is_zeroed_without_renormalization():
    value, raw = _aggregate_multilayer_side_engine_v2(
        _blueprint_modules(
            M6={
                "value": 0.75,
                "status": "INSUFFICIENT_DATA",
                "reason": "missing_context",
            }
        )
    )

    assert raw["layer_a"]["p1_core_side"] == pytest.approx(0.20828109349375, abs=1e-12)
    m6_contribution = next(
        contribution
        for contribution in raw["layer_a"]["contributions"]
        if contribution["module_id"] == "M6"
    )
    assert m6_contribution["effective_value"] == 0.0
    assert m6_contribution["weighted_edge"] == 0.0
    assert raw["module_statuses"]["M6"] == {
        "status": "INSUFFICIENT_DATA",
        "reason": "missing_context",
    }
    assert len(raw["active_modules"]) == 6
    assert len(raw["skipped_modules"]) == 1
    assert value == pytest.approx(-0.24687042165825, abs=1e-12)


def test_unexpected_m8_module_is_rejected():
    with pytest.raises(
        ValueError,
        match=r"Pillar 1 v2 uses exactly M1-M7; unexpected modules: \['M8'\]",
    ):
        _aggregate_multilayer_side_engine_v2(
            _blueprint_modules() + [_module("M8", 0.1)]
        )
