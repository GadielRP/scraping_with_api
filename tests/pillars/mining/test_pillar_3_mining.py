from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from modules.pillars.mining.adapters.pillar_3 import (
    P3MiningAdapter,
    p3_raw_metric_names,
)
from modules.pillars.mining.contracts import validate_mining_run
from modules.pillars.mining.service import PillarMiningService


def _event_context(evaluation_minute: int | None = 0):
    return SimpleNamespace(
        event_id=3003,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=evaluation_minute,
        season_id=77,
        season_name="2026",
        season_year=2026,
        start_time_utc=datetime(2026, 8, 27, 18, 0),
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _result(status: str = "ACTIVE") -> dict:
    metrics = {
        "PIN_TOTAL_LINE": 2.5,
        "PIN_OVER_PRICE": 1.8,
        "PIN_UNDER_PRICE": 2.2,
        "PIN_TOTAL_FULL_TIME_EDGE": 0.1,
        "PIN_TOTAL_FULL_TIME_DIRECTION_RAW": "OVER",
        "B365_TOTAL_LINE": 2.5,
        "B365_OVER_PRICE": 1.9,
        "B365_UNDER_PRICE": 2.1,
        "B365_TOTAL_FULL_TIME_EDGE": 0.05,
        "B365_TOTAL_FULL_TIME_DIRECTION_RAW": "OVER",
        "TOTAL_FULL_TIME_LINE_DIFF_RAW": 0.0,
        "TOTAL_FULL_TIME_LINE_GAP": 0.0,
        "TOTAL_FULL_TIME_PRICE_GAP": 0.05,
        "W_PIN_TOTALS_FULL_TIME": 0.5,
        "W_B365_TOTALS_FULL_TIME": 0.5,
        "TOTALS_MARKET_FULL_TIME_EDGE": 0.075,
        "P3_FULL_TIME_DIRECTION_RAW": "OVER",
        "CONTEXT_FULL_TIME_DIRECTION_RAW": "OPEN_BIAS",
        "Q_COMPLETE_TOTALS_FULL_TIME": 1.0,
    }
    return {
        "pillar_id": "pillar_3_totals_market_context",
        "engine_version": "p3-raw-totals-periodized-v2",
        "P3_STATUS": status,
        "status": status,
        "EVENT_ID": 3003,
        "PERIOD": "Full Time",
        "PERIOD_SCOPE": "FULL_TIME",
        "TARGET_MINUTE": 0,
        "modules": [{"module_id": "p3_raw_engine"}],
        **metrics,
        "raw": {
            "module_ids": ["p3_raw_engine"],
            "p3_raw_engine": {
                "baseline_weights": {"W_PIN_TOTALS_FULL_TIME": 0.5, "W_B365_TOTALS_FULL_TIME": 0.5},
                "mining_context": {"market_group": "Over/Under"},
                "inputs": {"PIN_TOTAL_LINE": 2.5},
                "input_trace": {"PIN_OVER_PRICE": {"quote_id": 123, "target_minute": 0}},
            },
        },
    }


class _CollectingWriter:
    def __init__(self):
        self.runs = []

    def replace_run(self, run) -> None:
        self.runs.append(run)


def test_active_p3_maps_metrics_context_season_and_trace() -> None:
    run = P3MiningAdapter().build(_event_context(), _result())
    validate_mining_run(run)

    assert run.pillar_id == "pillar_3_totals_market_context"
    assert run.result_scope == "totals_market_context_full_time"
    assert run.execution_slot == "evaluation:0"
    assert run.canonical_status == "SUCCESS"
    assert run.context["season_id"] == 77
    assert run.context["season_name"] == "2026"
    assert run.context["season_year"] == 2026
    summary, module = run.units
    assert summary.signal_axis == "TOTALS"
    assert summary.direction == "OVER"
    assert module.parent_unit_key == "summary"
    assert {metric.name for metric in module.metrics} == set(p3_raw_metric_names())
    assert summary.score_name == "TOTALS_MARKET_FULL_TIME_EDGE"
    assert run.context["PERIOD_SCOPE"] == "FULL_TIME"
    assert module.diagnostics["input_trace"]["PIN_OVER_PRICE"]["quote_id"] == 123


@pytest.mark.parametrize(
    ("producer_status", "canonical_status"),
    [("PARTIAL", "PARTIAL"), ("INSUFFICIENT_DATA", "INSUFFICIENT"), ("ERROR", "ERROR")],
)
def test_p3_statuses_are_normalized_without_inventing_score(
    producer_status,
    canonical_status,
) -> None:
    result = {
        "P3_STATUS": producer_status,
        "status": producer_status,
        "TARGET_MINUTE": 0,
        "modules": [],
        "raw": {"reason": "test"},
    }
    run = P3MiningAdapter().build(_event_context(), result)

    assert run.canonical_status == canonical_status
    assert run.units[0].score is None
    assert run.units[0].direction is None
    assert run.units[0].metrics == ()


def test_status_mode_persists_all_but_successful_only_skips_partial() -> None:
    writer = _CollectingWriter()
    adapters = {"pillar_3_totals_market_context": P3MiningAdapter()}
    all_service = PillarMiningService(writer, adapters, status_mode="all")
    successful_service = PillarMiningService(
        writer,
        adapters,
        status_mode="successful_only",
    )
    partial = _result("PARTIAL")

    assert all_service.persist(
        "pillar_3_totals_market_context",
        _event_context(),
        partial,
    )
    assert not successful_service.persist(
        "pillar_3_totals_market_context",
        _event_context(),
        partial,
    )
    assert successful_service.persist(
        "pillar_3_totals_market_context",
        _event_context(),
        _result(),
    )
