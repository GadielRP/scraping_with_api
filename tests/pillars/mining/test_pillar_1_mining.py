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


def test_side_preserves_internal_classifications_as_signal_data():
    from dataclasses import replace
    result = _side_result()
    result["modules"][0]["raw"]["m1_status"] = "DEGRADED"
    result["modules"][1]["raw"]["m2_status"] = "INACTIVE"
    run = P1SideMiningAdapter().build(_event_context(), result)
    validate_mining_run(replace(run,evaluation_id="evaluation"))
    assert run.status == "ACTIVE"
    assert run.units[1].status == "COMPUTED"
    blocked=next(u for u in run.units if u.unit_key == "module:M2")
    assert blocked.status == "BLOCKED"
    assert blocked.diagnostics["reason"] == "MISSING_INPUT"
    assert run.output_payload["modules"][0]["raw"]["m1_status"] == "DEGRADED"
    assert all("raw" not in unit.payload for unit in run.units)


def test_totals_keeps_continuous_components_and_weights():
    from dataclasses import replace
    result = _totals_result()
    result.update(P1_TOTALS_DIRECTIONAL_SCORE=-.2, BREAKOUT_SCORE=.1, TREND_DOMINANCE=-1,
                  P1_TOTALS_COMPOSITE=-.23, UNDER_COUNT=3, OVER_COUNT=1,
                  raw={"composite_breakout":{"weights":{"directional":.55,"breakout":.30,"trend":.15},
                                             "alignment":"UNDER","thresholds":[.05,.15,.30,.60]}})
    run = P1TotalsMiningAdapter().build(_event_context(), result)
    validate_mining_run(replace(run,evaluation_id="evaluation"))
    metrics={m.name:m.value for m in run.units[0].metrics}
    assert str(metrics["P1_TOTALS_COMPOSITE"]) == "-0.23"
    assert str(metrics["raw.composite_breakout.weights.directional"]) == "0.55"
    assert run.output_payload["raw"]["composite_breakout"]["alignment"] == "UNDER"
