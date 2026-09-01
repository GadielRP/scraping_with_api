from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from modules.pillars.mining.adapters.pillar_2 import P2MiningAdapter
from modules.pillars.mining.adapters.pillar_3 import P3MiningAdapter
from modules.pillars.mining.contracts import validate_mining_run
from modules.pillars.mining.service import PillarMiningService


def _event_context(*, evaluation_minute: int | None = 5):
    return SimpleNamespace(
        event_id=3003,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=evaluation_minute,
        start_time_utc=datetime(2026, 8, 27, 18, 0),
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _profile() -> dict:
    return {
        "FT": {
            "PINNACLE": {
                "LINE": 2.5,
                "OVER_ODDS": 1.8,
                "UNDER_ODDS": 2.2,
                "EDGE": 0.1,
                "DIRECTION": "OVER",
            },
            "BET365": {
                "LINE": 2.5,
                "OVER_ODDS": 1.9,
                "UNDER_ODDS": 2.1,
                "EDGE": 0.05,
                "DIRECTION": "OVER",
            },
            "LINE_STRUCTURE": {"LINE_DIFF_RAW": 0.0, "LINE_GAP": 0.0},
            "BOOK_RELATION": {"RELATION": "CONVERGENCE_OVER", "GAP": 0.05},
            "REPRESENTATIVE": {"EDGE": 0.075, "DIRECTION": "OVER"},
            "CONTEXT_DIRECTION_RAW": "OPEN_BIAS",
        },
        "1H": None,
        "FT_1H": None,
    }


def _active_result() -> dict:
    periods = {
        "full_time": {
            "status": "COMPLETE",
            "missing_inputs": [],
            "invalid_inputs": [],
            "ambiguous_inputs": [],
        },
        "first_half": {
            "status": "INCOMPLETE",
            "missing_inputs": ["PIN_1H_UNDER_ODDS"],
            "invalid_inputs": [],
            "ambiguous_inputs": [],
        },
    }
    return {
        "pillar_id": "pillar_3_totals_market_context",
        "engine_version": "p3-signal-profile-v1",
        "event_id": 3003,
        "P3_STATUS": "PARTIAL",
        "status": "PARTIAL",
        "P3_TARGET_MINUTE": 5,
        "PERIODS": periods,
        "MISSING_INPUTS": ["PIN_1H_UNDER_ODDS"],
        "INVALID_INPUTS": [],
        "AMBIGUOUS_INPUTS": [],
        "P3_SIGNAL_PROFILE": _profile(),
        "modules": [{"module_id": "p3_signal_engine"}],
        "raw": {
            "inputs": {
                "PIN_FT_OU_LINE": 2.5,
                "PIN_FT_OVER_ODDS": 1.8,
                "PIN_FT_UNDER_ODDS": 2.2,
                "PIN_1H_UNDER_ODDS": None,
            },
            "input_trace": {
                "PIN_FT_OVER_ODDS": {
                    "target_minute": 5,
                    "quote_id": 123,
                }
            },
            "periods": periods,
            "extraction_diagnostics": {"selection": "canonical"},
        },
    }


class _CollectingWriter:
    def __init__(self) -> None:
        self.runs = []

    def replace_run(self, run) -> None:
        self.runs.append(run)


def test_p3_persists_profile_and_traceability_without_scalar_score() -> None:
    run = P3MiningAdapter().build(_event_context(), _active_result())
    validate_mining_run(run)

    assert run.result_scope == "totals_market_context"
    assert run.execution_slot == "evaluation:5"
    assert run.target_minute == 5
    assert run.producer_status == "PARTIAL"
    assert run.canonical_status == "PARTIAL"
    assert run.engine_version == "p3-signal-profile-v1"
    assert run.payload_schema_version == 2
    assert run.inputs["PIN_1H_UNDER_ODDS"] is None
    assert run.context["P3_TARGET_MINUTE"] == 5

    summary, module = run.units
    assert summary.score_name is None
    assert summary.score is None
    assert summary.direction is None
    assert summary.signal_axis is None
    assert summary.metrics == ()
    assert summary.payload["P3_SIGNAL_PROFILE"] == _profile()
    assert summary.dimensions == {
        "market_types": ["OVER_UNDER"],
        "market_periods": ["FULL_TIME"],
    }
    assert module.module_id == "p3_signal_engine"
    assert module.score_name is None
    assert module.score is None
    assert module.direction is None
    assert module.signal_axis is None
    assert module.metrics == ()
    assert module.payload["P3_SIGNAL_PROFILE"] == _profile()
    assert module.diagnostics["input_trace"]["PIN_FT_OVER_ODDS"]["quote_id"] == 123


@pytest.mark.parametrize("status", ["INSUFFICIENT_DATA", "ERROR"])
def test_non_success_p3_keeps_diagnostics_without_profile_or_score(status) -> None:
    result = _active_result()
    result.update(
        {
            "P3_STATUS": status,
            "status": status,
            "P3_SIGNAL_PROFILE": None,
            "modules": [],
            "error": "database unavailable" if status == "ERROR" else None,
        }
    )
    result["raw"]["reason"] = (
        "pillar_3_exception"
        if status == "ERROR"
        else "full_time_completeness_gate_failed"
    )

    run = P3MiningAdapter().build(_event_context(), result)

    assert len(run.units) == 1
    assert run.units[0].payload["P3_SIGNAL_PROFILE"] is None
    assert run.units[0].score is None
    assert run.units[0].direction is None
    assert run.units[0].metrics == ()
    assert run.diagnostics["reason"] == result["raw"]["reason"]


def test_complete_first_half_is_reflected_in_mining_dimensions() -> None:
    result = _active_result()
    result["P3_STATUS"] = "ACTIVE"
    result["status"] = "ACTIVE"
    result["PERIODS"]["first_half"] = {
        "status": "COMPLETE",
        "missing_inputs": [],
        "invalid_inputs": [],
        "ambiguous_inputs": [],
    }
    result["raw"]["periods"] = result["PERIODS"]

    run = P3MiningAdapter().build(_event_context(), result)

    assert run.canonical_status == "SUCCESS"
    assert run.units[0].dimensions["market_periods"] == [
        "FULL_TIME",
        "FIRST_HALF",
    ]


def test_target_slot_is_fallback_only_when_evaluation_minute_is_unknown() -> None:
    run = P3MiningAdapter().build(
        _event_context(evaluation_minute=None),
        _active_result(),
    )
    assert run.execution_slot == "target:5"


def test_service_persists_p3_with_the_same_status_policy_as_p2() -> None:
    writer = _CollectingWriter()
    adapters = {"pillar_3_totals_market_context": P3MiningAdapter()}
    all_service = PillarMiningService(writer, adapters, status_mode="all")
    successful_service = PillarMiningService(
        writer,
        adapters,
        status_mode="successful_only",
    )
    active = _active_result()
    active["P3_STATUS"] = "ACTIVE"
    active["status"] = "ACTIVE"

    assert all_service.persist(
        "pillar_3_totals_market_context",
        _event_context(),
        _active_result(),
    )
    assert not successful_service.persist(
        "pillar_3_totals_market_context",
        _event_context(),
        _active_result(),
    )
    assert successful_service.persist(
        "pillar_3_totals_market_context",
        _event_context(),
        active,
    )
    assert [run.canonical_status for run in writer.runs] == [
        "PARTIAL",
        "SUCCESS",
    ]


def test_pipeline_registers_both_structural_signal_profile_adapters() -> None:
    from modules.jobs.pre_start_check_job.pillar_pipeline import (
        _registered_mining_adapters,
    )

    adapters = _registered_mining_adapters()
    assert set(adapters) == {
        "pillar_2_side_market",
        "pillar_3_totals_market_context",
    }
    assert adapters["pillar_3_totals_market_context"].pillar_id == (
        "pillar_3_totals_market_context"
    )


def test_p2_and_p3_use_the_same_profile_persistence_shape() -> None:
    p2_result = {
        "engine_version": "p2-signal-profile-v1",
        "P2_STATUS": "PARTIAL",
        "status": "PARTIAL",
        "P2_TARGET_MINUTE": 5,
        "PERIODS": _active_result()["PERIODS"],
        "P2_SIGNAL_PROFILE": {"FT": {}, "1H": None},
        "modules": [{"module_id": "p2_signal_engine"}],
        "raw": _active_result()["raw"],
    }
    p2_run = P2MiningAdapter().build(_event_context(), p2_result)
    p3_run = P3MiningAdapter().build(_event_context(), _active_result())

    assert p2_run.payload_schema_version == p3_run.payload_schema_version == 2
    assert [unit.unit_type for unit in p2_run.units] == [
        unit.unit_type for unit in p3_run.units
    ] == ["summary", "module"]
    for p2_unit, p3_unit in zip(p2_run.units, p3_run.units):
        assert p2_unit.score_name is p3_unit.score_name is None
        assert p2_unit.score is p3_unit.score is None
        assert p2_unit.direction is p3_unit.direction is None
        assert p2_unit.signal_axis is p3_unit.signal_axis is None
        assert p2_unit.metrics == p3_unit.metrics == ()
