from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from modules.pillars.mining.adapters.pillar_2 import P2MiningAdapter
from modules.pillars.mining.contracts import (
    PillarMiningRun,
    PillarMiningUnit,
    validate_mining_run,
)
from modules.pillars.mining.service import PillarMiningService


def _event_context(*, evaluation_minute: int | None = 5):
    return SimpleNamespace(
        event_id=2002,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=evaluation_minute,
        start_time_utc=datetime(2026, 8, 22, 18, 0),
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _profile() -> dict:
    return {
        "FT": {
            "1X2": {"DIRECTION": "HOME", "BOOK_RELATION": "CONVERGENCE_HOME"},
            "AH": {"DIRECTION": "NEUTRAL"},
            "CROSS_MARKET": {"FT_1X2_AH_RELATION": "NEUTRAL"},
        },
        "1H": None,
        "FT_1H": None,
        "EXCHANGE": {"DIRECTION": "HOME", "BACK_LAY_RELATION": "CONVERGENCE_HOME"},
        "BOOK_EXCHANGE": {"RELATION": "CONVERGENCE_HOME"},
    }


def _active_result() -> dict:
    periods = {
        "full_time": {"status": "COMPLETE", "missing_inputs": [], "invalid_inputs": [], "ambiguous_inputs": []},
        "first_half": {"status": "INCOMPLETE", "missing_inputs": [], "invalid_inputs": [], "ambiguous_inputs": []},
    }
    return {
        "pillar_id": "pillar_2_side_market",
        "engine_version": "p2-signal-profile-v1",
        "event_id": 2002,
        "P2_STATUS": "ACTIVE",
        "status": "ACTIVE",
        "P2_TARGET_MINUTE": 5,
        "PERIODS": periods,
        "P2_SIGNAL_PROFILE": _profile(),
        "modules": [{"module_id": "p2_signal_engine"}],
        "raw": {
            "inputs": {
                "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE": 2.0,
                "BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE": None,
            },
            "input_trace": {
                "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE": {
                    "target_minute": 5,
                    "quote_id": 123,
                }
            },
            "periods": periods,
        },
    }


class _CollectingWriter:
    def __init__(self) -> None:
        self.runs = []

    def replace_run(self, run) -> None:
        self.runs.append(run)


def test_active_p2_persists_profile_and_traceability_without_scalar_score() -> None:
    run = P2MiningAdapter().build(_event_context(), _active_result())
    validate_mining_run(run)

    assert run.execution_slot == "evaluation:5"
    assert run.target_minute == 5
    assert run.canonical_status == "SUCCESS"
    assert run.engine_version == "p2-signal-profile-v1"
    assert run.inputs["BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE"] is None

    summary, module = run.units
    assert summary.score_name is None
    assert summary.score is None
    assert summary.direction is None
    assert summary.signal_axis is None
    assert summary.payload["P2_SIGNAL_PROFILE"] == _profile()
    assert module.module_id == "p2_signal_engine"
    assert module.score_name is None
    assert module.score is None
    assert module.direction is None
    assert module.signal_axis is None
    assert module.metrics == ()
    assert module.payload["P2_SIGNAL_PROFILE"] == _profile()
    assert module.diagnostics["input_trace"]["PIN_HOME_1X2_FULL_TIME_ODDS_PRICE"]["quote_id"] == 123


@pytest.mark.parametrize("status", ["INSUFFICIENT_DATA", "ERROR"])
def test_non_success_p2_keeps_diagnostics_without_creating_profile_or_score(status) -> None:
    result = _active_result()
    result.update(
        {
            "P2_STATUS": status,
            "status": status,
            "P2_SIGNAL_PROFILE": None,
            "modules": [],
            "error": "database unavailable" if status == "ERROR" else None,
            "MISSING_INPUTS": ["PIN_HOME_1X2_FULL_TIME_ODDS_PRICE"],
        }
    )
    result["raw"]["reason"] = "pillar_2_exception" if status == "ERROR" else "full_time_completeness_gate_failed"

    run = P2MiningAdapter().build(_event_context(), result)

    assert len(run.units) == 1
    assert run.units[0].score is None
    assert run.units[0].direction is None
    assert run.units[0].metrics == ()
    assert run.diagnostics["missing_inputs"] == ["PIN_HOME_1X2_FULL_TIME_ODDS_PRICE"]
    assert run.diagnostics["reason"] == result["raw"]["reason"]


def test_partial_p2_persists_granular_first_half_profile() -> None:
    result = _active_result()
    result["P2_STATUS"] = "PARTIAL"
    result["status"] = "PARTIAL"
    partial_first_half = {
        "1X2": {
            "PIN_EDGE": None,
            "PIN_DIRECTION": None,
            "B365_EDGE": 0.08,
            "B365_DIRECTION": "HOME",
            "BOOK_RELATION": None,
            "BOOK_GAP": None,
            "REP_EDGE": None,
            "DIRECTION": None,
        },
        "AH": {"PIN_EDGE": 0.02, "B365_EDGE": 0.01, "REP_EDGE": 0.015},
        "CROSS_MARKET": {
            "1H_1X2_AH_RELATION": None,
            "1H_CROSS_MARKET_GAP": None,
        },
    }
    result["P2_SIGNAL_PROFILE"]["1H"] = partial_first_half
    result["PERIODS"]["first_half"]["status"] = "INCOMPLETE"
    result["PERIODS"]["first_half"]["missing_inputs"] = [
        "PIN_AWAY_1X2_1H_ODDS_PRICE"
    ]
    result["raw"]["periods"] = result["PERIODS"]

    run = P2MiningAdapter().build(_event_context(), result)

    assert run.producer_status == "PARTIAL"
    assert run.canonical_status == "PARTIAL"
    assert run.units[0].dimensions["market_periods"] == ["FULL_TIME"]
    assert run.units[0].payload["P2_SIGNAL_PROFILE"]["1H"] == partial_first_half
    assert run.units[1].payload["P2_SIGNAL_PROFILE"]["1H"] == partial_first_half
    assert run.diagnostics["periods"]["first_half"]["status"] == "INCOMPLETE"


def test_target_slot_is_fallback_only_when_evaluation_minute_is_unknown() -> None:
    run = P2MiningAdapter().build(
        _event_context(evaluation_minute=None),
        _active_result(),
    )
    assert run.execution_slot == "target:5"


def test_service_is_adapter_driven_and_applies_status_modes() -> None:
    writer = _CollectingWriter()
    adapters = {"pillar_2_side_market": P2MiningAdapter()}
    all_service = PillarMiningService(writer, adapters, status_mode="all")
    successful_service = PillarMiningService(
        writer, adapters, status_mode="successful_only"
    )
    disabled_service = PillarMiningService(writer, adapters, enabled=False)
    insufficient = _active_result()
    insufficient.update(
        {
            "P2_STATUS": "INSUFFICIENT_DATA",
            "status": "INSUFFICIENT_DATA",
            "P2_SIGNAL_PROFILE": None,
            "modules": [],
        }
    )

    assert all_service.persist("pillar_2_side_market", _event_context(), insufficient)
    assert not successful_service.persist(
        "pillar_2_side_market", _event_context(), insufficient
    )
    assert successful_service.persist(
        "pillar_2_side_market", _event_context(), _active_result()
    )
    assert not disabled_service.persist(
        "pillar_2_side_market", _event_context(), _active_result()
    )
    assert [run.canonical_status for run in writer.runs] == [
        "INSUFFICIENT",
        "SUCCESS",
    ]


def test_contract_rejects_duplicate_units_and_unknown_parents() -> None:
    base = P2MiningAdapter().build(_event_context(), _active_result())
    with pytest.raises(ValueError, match="duplicate mining unit key"):
        validate_mining_run(
            PillarMiningRun(**{**base.__dict__, "units": (base.units[0], base.units[0])})
        )

    orphan = PillarMiningUnit(
        unit_type="component",
        unit_key="orphan",
        parent_unit_key="missing",
        producer_status="ACTIVE",
        canonical_status="SUCCESS",
    )
    with pytest.raises(ValueError, match="unknown parent"):
        validate_mining_run(
            PillarMiningRun(**{**base.__dict__, "units": (orphan,)})
        )


def test_pipeline_registers_structural_signal_profile_adapters() -> None:
    from modules.jobs.pre_start_check_job.pillar_pipeline import (
        _registered_mining_adapters,
    )

    adapters = _registered_mining_adapters()
    assert set(adapters) == {
        "pillar_1_team_structure_side",
        "pillar_1_team_structure_totals",
        "pillar_2_side_market",
        "pillar_3_totals_market_context",
    }
    assert adapters["pillar_2_side_market"].pillar_id == "pillar_2_side_market"


def test_pipeline_mining_failure_is_non_blocking() -> None:
    from modules.jobs.pre_start_check_job.pillar_pipeline import EventPillarProcessor

    class _FailingService:
        def persist(self, pillar_id, event_context, result):
            raise RuntimeError("mining database unavailable")

    processor = EventPillarProcessor(event_repo=None, mining_service=_FailingService())
    processor._persist_mining_result(
        "pillar_2_side_market",
        _event_context(),
        _active_result(),
    )
