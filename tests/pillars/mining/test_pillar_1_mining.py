from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from modules.pillars.mining.adapters.pillar_1 import (
    P1SideMiningAdapter,
    P1TotalsMiningAdapter,
)
from modules.pillars.mining.contracts import validate_mining_run


def _event_context(*, evaluation_minute: int | None = 5):
    return SimpleNamespace(
        event_id=1001,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=evaluation_minute,
        starts_at=datetime(2026, 9, 2, 18, 0, tzinfo=timezone.utc),
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _side_result() -> dict:
    modules = []
    for module_id in ("M1", "M2", "M3", "M4", "M5", "M6", "M7"):
        modules.append(
            {
                "module_id": module_id,
                "module_name": f"Module {module_id}",
                "value": 0.12,
                "bias": "HOME",
                "strength": "LOW",
                "components": [
                    {
                        "name": "EDGE",
                        "edge": 0.12,
                        "bias": "HOME",
                        "strength": "LOW",
                        "weight": 0.5,
                        "weighted_edge": 0.06,
                        "raw": {"source": module_id},
                    }
                ],
                "raw": {
                    f"{module_id.lower()}_status": "ACTIVE",
                    f"{module_id.lower()}_status_reason": "active",
                    "sample_size": 10,
                },
            }
        )
    return {
        "pillar_id": "pillar_1_team_structure",
        "pillar_name": "Team Structure",
        "event_id": 1001,
        "participants": "Home vs Away",
        "modules": modules,
        "value": 0.15,
        "raw": {
            "engine_version": "side_engine_multilayer_v3_0",
            "layer_a": {"name": "STRUCTURAL_SIDE_ENGINE"},
            "layer_b": {"name": "CONTEXT_GATES"},
            "final": {
                "p1_final_context_balance": 0.15,
                "p1_final_bias": "HOME",
                "p1_final_state": "CONFIRMED",
            },
            "module_statuses": {},
            "active_modules": [],
            "skipped_modules": [],
            "value_is_evidence_only": True,
            "p1_final_context_balance_is_decision": False,
        },
    }


def _totals_result() -> dict:
    return {
        "pillar_id": "pillar_1_team_structure",
        "module_id": "P1_TOTALS",
        "module_name": "P1 Totals",
        "engine_version": "p1-totals-v2.5",
        "event_id": 1001,
        "participants": "Home vs Away",
        "status": "OK",
        "status_reason": None,
        "P1_TOTALS_DIRECTIONAL_SCORE": 0.2,
        "P1_TOTALS_DIRECTION": "OVER_PROFILE",
        "P1_TOTALS_STRENGTH": "MODERATE",
        "P1_TOTALS_VARIANCE_STATE": "NORMAL",
        "P1_TOTALS_INTERNAL_STATE": {"active_weight_sum": 1.0},
        "P1_TOTALS_COMPOSITE": 0.24,
        "P1_TOTALS_COMPOSITE_DIRECTION": "OVER_PROFILE",
        "P1_TOTALS_COMPOSITE_STRENGTH": "MODERATE",
        "BREAKOUT_CONDITION": True,
        "BREAKOUT_SCORE": 0.1,
        "TREND_DOMINANCE": 0.3,
        "WINDOWS_USED": {"SHORT": 3, "FULL": 20},
        "WINDOW_COMPLETENESS_BY_WINDOW": {"SHORT": 1.0, "FULL": 0.9},
        "active_layers": [
            {
                "layer": "STRUCTURAL",
                "status": "ACTIVE",
                "raw_signal": 0.2,
                "final_signal": 0.2,
                "weight": 0.45,
                "weighted_signal": 0.09,
                "ignored_reason": None,
                "raw": {"source": "structural"},
            }
        ],
        "ignored_layers": [
            {
                "layer": "TEMPORAL",
                "status": "IGNORE",
                "raw_signal": 0.01,
                "final_signal": 0.01,
                "weight": 0.3,
                "weighted_signal": 0.0,
                "ignored_reason": "ABS_SIGNAL_BELOW_0_05",
                "raw": {"source": "temporal"},
            }
        ],
        "raw": {
            "directional_components": {"P1_TOTALS_COMPOSITE": 0.24},
            "variance_components": {"P1_TOTALS_VARIANCE_STATE": "NORMAL"},
            "temporal": {"temporal_final": 0.01},
            "trend": {"trend_final": 0.3},
            "composite_breakout": {"BREAKOUT_SCORE": 0.1},
            "policy_ignore": {},
        },
    }


def test_p1_side_persists_summary_modules_components_and_full_output() -> None:
    result = _side_result()
    run = P1SideMiningAdapter().build(_event_context(), result)
    validate_mining_run(run)

    assert run.pillar_id == "pillar_1_team_structure"
    assert run.result_scope == "side"
    assert run.execution_slot == "evaluation:5"
    assert run.canonical_status == "SUCCESS"
    assert run.engine_version == "side_engine_multilayer_v3_0"
    assert run.output_payload == result
    assert len(run.units) == 15

    summary = run.units[0]
    assert summary.signal_axis == "SIDE"
    assert summary.direction == "HOME"
    assert summary.score_name == "value"
    assert summary.payload["final"]["p1_final_state"] == "CONFIRMED"

    module = next(unit for unit in run.units if unit.unit_key == "module:M1")
    component = next(
        unit for unit in run.units if unit.unit_key == "component:M1:EDGE"
    )
    assert module.parent_unit_key == "summary"
    assert module.score_name == "value"
    assert module.direction == "HOME"
    assert component.parent_unit_key == "module:M1"
    assert component.score_name == "edge"
    assert {metric.name for metric in component.metrics} == {
        "weight",
        "weighted_edge",
    }


def test_p1_side_normalizes_degraded_and_inactive_module_statuses() -> None:
    result = _side_result()
    result["modules"][1]["raw"]["m2_status"] = "DEGRADED"
    result["modules"][2]["raw"]["m3_status"] = "INACTIVE"

    run = P1SideMiningAdapter().build(_event_context(), result)

    assert run.producer_status == "PARTIAL"
    assert run.canonical_status == "PARTIAL"
    degraded = next(unit for unit in run.units if unit.unit_key == "module:M2")
    inactive = next(unit for unit in run.units if unit.unit_key == "module:M3")
    assert degraded.canonical_status == "PARTIAL"
    assert degraded.is_valid is True
    assert inactive.canonical_status == "INSUFFICIENT"
    assert inactive.is_valid is False


def test_p1_totals_persists_directional_score_composite_and_layers() -> None:
    result = _totals_result()
    run = P1TotalsMiningAdapter().build(_event_context(), result)
    validate_mining_run(run)

    assert run.pillar_id == "pillar_1_team_structure"
    assert run.result_scope == "totals"
    assert run.canonical_status == "SUCCESS"
    assert run.output_payload == result

    summary = run.units[0]
    assert summary.signal_axis == "TOTALS"
    assert summary.score_name == "P1_TOTALS_DIRECTIONAL_SCORE"
    assert summary.direction == "OVER_PROFILE"
    assert summary.strength == "MODERATE"
    assert {metric.name for metric in summary.metrics} >= {
        "P1_TOTALS_COMPOSITE",
        "BREAKOUT_SCORE",
        "TREND_DOMINANCE",
    }

    structural = next(unit for unit in run.units if unit.unit_key == "layer:STRUCTURAL")
    temporal = next(unit for unit in run.units if unit.unit_key == "layer:TEMPORAL")
    assert structural.parent_unit_key == "summary"
    assert structural.canonical_status == "SUCCESS"
    assert temporal.canonical_status == "SKIPPED"
    assert temporal.diagnostics["ignored_reason"] == "ABS_SIGNAL_BELOW_0_05"


def test_p1_adapters_use_target_fallback_only_for_evaluation_unknown() -> None:
    side_run = P1SideMiningAdapter().build(
        _event_context(evaluation_minute=None), _side_result()
    )
    totals_run = P1TotalsMiningAdapter().build(
        _event_context(evaluation_minute=None), _totals_result()
    )

    assert side_run.execution_slot == "event"
    assert totals_run.execution_slot == "event"
