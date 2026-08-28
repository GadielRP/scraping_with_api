from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from modules.pillars.mining.adapters.pillar_2 import (
    P2_RAW_METRIC_NAMES,
    P2MiningAdapter,
)
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


def _active_result() -> dict:
    metrics = {name: index / 100 for index, name in enumerate(P2_RAW_METRIC_NAMES, 1)}
    metrics.update(
        {
            "P2_DIRECTION_RAW": "HOME",
            "BOOK_DIRECTION_FT": "HOME",
            "BOOK_DIRECTION_1H": "AWAY",
            "FT_1H_SAME_DIRECTION": False,
            "DISLOCATION": True,
            "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE": 100.0,
            "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE": 90.0,
            "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE": 80.0,
            "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE": 70.0,
            "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE": 120.0,
            "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE": 110.0,
            "SIDE_MARKET_EDGE": 0.123456,
        }
    )
    return {
        "pillar_id": "pillar_2_side_market",
        "engine_version": "p2_raw_v1",
        "P2_STATUS": "ACTIVE",
        "status": "ACTIVE",
        "P2_TARGET_MINUTE": 5,
        "PERIODS": {
            "full_time": {
                "status": "COMPLETE",
                "missing_inputs": [],
                "invalid_inputs": [],
                "ambiguous_inputs": [],
            },
            "first_half": {
                "status": "COMPLETE",
                "missing_inputs": [],
                "invalid_inputs": [],
                "ambiguous_inputs": [],
            },
        },
        "modules": [{"module_id": "p2_raw_engine"}],
        **metrics,
        "raw": {
            "module_count": 1,
            "module_ids": ["p2_raw_engine"],
            "p2_raw_engine": {
                "baseline_weights": {"W_PIN": 0.5, "W_B365": 0.5},
                "mining_context": {"market_type": "1X2"},
                "inputs": {"PIN_HOME_1X2_FULL_TIME_ODDS_PRICE": 2.0},
                "input_trace": {
                    "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE": {
                        "target_minute": 5,
                        "quote_id": 123,
                    }
                },
            },
        },
    }


class _CollectingWriter:
    def __init__(self) -> None:
        self.runs = []

    def replace_run(self, run) -> None:
        self.runs.append(run)


def test_active_p2_maps_hierarchy_all_metrics_and_liquidity() -> None:
    run = P2MiningAdapter().build(_event_context(), _active_result())
    validate_mining_run(run)

    assert run.execution_slot == "evaluation:5"
    assert run.target_minute == 5
    assert run.canonical_status == "SUCCESS"
    assert run.context["competition_id"] == 99
    assert run.inputs["PIN_HOME_1X2_FULL_TIME_ODDS_PRICE"] == 2.0
    assert run.output_payload["P2_STATUS"] == "ACTIVE"

    summary, module = run.units
    assert summary.unit_type == "summary"
    assert summary.score == Decimal("0.123456")
    assert summary.direction == "HOME"
    assert summary.is_valid is None
    assert summary.dimensions["market_periods"] == ["FULL_TIME", "FIRST_HALF"]
    assert module.parent_unit_key == "summary"
    assert {metric.name for metric in module.metrics} == set(P2_RAW_METRIC_NAMES)
    metric_values = {metric.name: metric.value for metric in module.metrics}
    assert metric_values["BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE"] == Decimal("100.0")
    assert metric_values["BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE"] == Decimal("90.0")
    assert metric_values["BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE"] == Decimal("80.0")
    assert metric_values["BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE"] == Decimal("70.0")
    assert metric_values["BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE"] == Decimal("120.0")
    assert metric_values["BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE"] == Decimal("110.0")
    assert (
        module.diagnostics["input_trace"]
        ["PIN_HOME_1X2_FULL_TIME_ODDS_PRICE"]["quote_id"]
        == 123
    )


@pytest.mark.parametrize("status", ["INSUFFICIENT_DATA", "ERROR"])
def test_non_success_p2_keeps_diagnostics_without_inventing_metrics(status) -> None:
    result = {
        "engine_version": "p2_raw_v1",
        "P2_STATUS": status,
        "status": status,
        "P2_TARGET_MINUTE": None,
        "modules": [],
        "MISSING_INPUTS": ["PIN_SIDE"],
        "INVALID_INPUTS": ["B365_SIDE"],
        "AMBIGUOUS_INPUTS": [],
        "error": "database unavailable" if status == "ERROR" else None,
        "raw": {"reason": "pillar_2_exception" if status == "ERROR" else "gate_failed"},
    }

    run = P2MiningAdapter().build(_event_context(), result)

    assert len(run.units) == 1
    assert run.units[0].metrics == ()
    assert run.units[0].score is None
    assert run.units[0].direction is None
    assert run.units[0].is_valid is None
    assert run.execution_slot == "evaluation:5"
    assert run.diagnostics["missing_inputs"] == ["PIN_SIDE"]
    assert run.diagnostics["invalid_inputs"] == ["B365_SIDE"]
    assert run.diagnostics["reason"] == result["raw"]["reason"]


def _partial_result() -> dict:
    result = _active_result()
    result["P2_STATUS"] = "PARTIAL"
    result["status"] = "PARTIAL"
    result["PIN_1H_SIDE_EDGE"] = None
    result["B365_1H_SIDE_EDGE"] = None
    result["BOOK_1H_GAP"] = None
    result["BOOK_1H_EDGE"] = None
    result["PIN_AH_1H_EDGE"] = None
    result["B365_AH_1H_EDGE"] = None
    result["AH_1H_LINE_GAP"] = None
    result["AH_1H_PRICE_GAP"] = None
    result["BOOK_DIRECTION_1H"] = None
    result["FT_1H_GAP"] = None
    result["FT_1H_SAME_DIRECTION"] = None
    result["MISSING_INPUTS"] = ["PIN_AH_1H_LINE"]
    result["AMBIGUOUS_INPUTS"] = ["PIN_AH_1H_LINE"]
    result["PERIODS"] = {
        "full_time": {
            "status": "COMPLETE",
            "missing_inputs": [],
            "invalid_inputs": [],
            "ambiguous_inputs": [],
        },
        "first_half": {
            "status": "AMBIGUOUS",
            "missing_inputs": [],
            "invalid_inputs": [],
            "ambiguous_inputs": ["PIN_AH_1H_LINE"],
        },
    }
    result["raw"]["reason"] = "first_half_incomplete"
    result["raw"]["periods"] = result["PERIODS"]
    result["raw"]["excluded_metrics"] = {
        "PIN_1H_SIDE_EDGE": "first_half_incomplete",
        "FT_1H_GAP": "cross_period_requires_both_periods",
    }
    result["raw"]["p2_raw_engine"]["excluded_metrics"] = result["raw"]["excluded_metrics"]
    result["raw"]["p2_raw_engine"]["periods"] = result["PERIODS"]
    return result


def test_partial_p2_persists_full_time_signal_and_period_diagnostics() -> None:
    run = P2MiningAdapter().build(_event_context(), _partial_result())
    validate_mining_run(run)

    assert run.producer_status == "PARTIAL"
    assert run.canonical_status == "PARTIAL"
    assert run.units[0].score == Decimal("0.123456")
    assert run.units[0].direction == "HOME"
    assert run.units[0].dimensions["market_periods"] == ["FULL_TIME"]
    assert run.diagnostics["periods"]["full_time"]["status"] == "COMPLETE"
    assert run.diagnostics["periods"]["first_half"]["status"] == "AMBIGUOUS"
    assert run.diagnostics["excluded_metrics"]["PIN_1H_SIDE_EDGE"] == (
        "first_half_incomplete"
    )
    metric_names = {metric.name for metric in run.units[1].metrics}
    assert "SIDE_MARKET_EDGE" in metric_names
    assert "PIN_1H_SIDE_EDGE" not in metric_names
    assert "FT_1H_GAP" not in metric_names


def test_insufficient_full_time_persists_period_diagnostics_without_score() -> None:
    result = {
        "engine_version": "p2-raw-ft-1h-periodized-v2",
        "P2_STATUS": "INSUFFICIENT_DATA",
        "status": "INSUFFICIENT_DATA",
        "P2_TARGET_MINUTE": 5,
        "modules": [],
        "MISSING_INPUTS": [],
        "INVALID_INPUTS": [],
        "AMBIGUOUS_INPUTS": ["PIN_AH_FULL_TIME_LINE"],
        "PERIODS": {
            "full_time": {
                "status": "AMBIGUOUS",
                "missing_inputs": [],
                "invalid_inputs": [],
                "ambiguous_inputs": ["PIN_AH_FULL_TIME_LINE"],
            },
            "first_half": {
                "status": "COMPLETE",
                "missing_inputs": [],
                "invalid_inputs": [],
                "ambiguous_inputs": [],
            },
        },
        "raw": {
            "reason": "full_time_completeness_gate_failed",
            "periods": {
                "full_time": {
                    "status": "AMBIGUOUS",
                    "missing_inputs": [],
                    "invalid_inputs": [],
                    "ambiguous_inputs": ["PIN_AH_FULL_TIME_LINE"],
                },
                "first_half": {
                    "status": "COMPLETE",
                    "missing_inputs": [],
                    "invalid_inputs": [],
                    "ambiguous_inputs": [],
                },
            },
        },
    }

    run = P2MiningAdapter().build(_event_context(), result)

    assert run.canonical_status == "INSUFFICIENT"
    assert run.units[0].score is None
    assert run.units[0].direction is None
    assert run.diagnostics["periods"]["full_time"]["status"] == "AMBIGUOUS"
    assert run.diagnostics["periods"]["first_half"]["status"] == "COMPLETE"


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
    insufficient = {
        "engine_version": "p2_raw_v1",
        "P2_STATUS": "INSUFFICIENT_DATA",
        "P2_TARGET_MINUTE": None,
        "raw": {"reason": "gate_failed"},
    }

    assert all_service.persist("pillar_2_side_market", _event_context(), insufficient)
    assert not successful_service.persist(
        "pillar_2_side_market", _event_context(), insufficient
    )
    assert successful_service.persist(
        "pillar_2_side_market", _event_context(), _active_result()
    )
    assert all_service.persist("pillar_2_side_market", _event_context(), _partial_result())
    assert not successful_service.persist(
        "pillar_2_side_market", _event_context(), _partial_result()
    )
    assert not disabled_service.persist(
        "pillar_2_side_market", _event_context(), _active_result()
    )
    assert [run.canonical_status for run in writer.runs] == [
        "INSUFFICIENT",
        "SUCCESS",
        "PARTIAL",
    ]


def test_service_rejects_unknown_mode_and_unregistered_pillar() -> None:
    with pytest.raises(ValueError, match="status_mode"):
        PillarMiningService(_CollectingWriter(), {}, status_mode="sometimes")
    with pytest.raises(ValueError, match="no mining adapter"):
        PillarMiningService(_CollectingWriter(), {}).persist(
            "pillar_9", _event_context(), {}
        )


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
