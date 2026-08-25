from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from modules.pillars.mining.pillar_2_adapter import (
    P2_RAW_METRIC_NAMES,
    build_p2_mining_observation,
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
        competition=SimpleNamespace(
            competition_id=99,
            display_name="League",
        ),
    )


def _active_result() -> dict:
    metrics = {
        name: index / 100
        for index, name in enumerate(P2_RAW_METRIC_NAMES, start=1)
    }
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
        "modules": [{"module_id": "p2_raw_engine"}],
        **metrics,
        "raw": {
            "module_count": 1,
            "module_ids": ["p2_raw_engine"],
            "p2_raw_engine": {
                "baseline_weights": {"W_PIN": 0.5, "W_B365": 0.5},
                "mining_context": {
                    "event_id": 2002,
                    "sport": "Football",
                    "competition_id": 99,
                    "competition": "League",
                    "market_type": "1X2",
                    "minutes_to_start": 5,
                    "P2_TARGET_MINUTE": 5,
                },
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
        self.observations = []

    def upsert(self, observation) -> None:
        self.observations.append(observation)


def test_active_p2_maps_every_raw_metric_and_trace() -> None:
    observation = build_p2_mining_observation(_event_context(), _active_result())

    assert set(observation.metrics) == set(P2_RAW_METRIC_NAMES)
    assert observation.score_name == "SIDE_MARKET_EDGE"
    assert observation.score == Decimal("0.123456")
    assert observation.direction == "HOME"
    assert observation.status == "ACTIVE"
    assert observation.is_successful is True
    assert observation.is_valid is None
    assert observation.observation_slot == "target:5"
    assert observation.context["competition_id"] == 99
    assert observation.inputs["PIN_HOME_1X2_FULL_TIME_ODDS_PRICE"] == 2.0
    assert (
        observation.diagnostics["engine"]["input_trace"]
        ["PIN_HOME_1X2_FULL_TIME_ODDS_PRICE"]["quote_id"]
        == 123
    )
    assert observation.context["event_start_time_utc"] == "2026-08-22T18:00:00"
    assert {
        name: observation.metrics[name]
        for name in (
            "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE",
            "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE",
            "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE",
            "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE",
            "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE",
            "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE",
        )
    } == {
        "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE": 100.0,
        "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE": 90.0,
        "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE": 80.0,
        "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE": 70.0,
        "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE": 120.0,
        "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE": 110.0,
    }


@pytest.mark.parametrize("status", ["INSUFFICIENT_DATA", "ERROR"])
def test_non_active_p2_keeps_diagnostics_without_inventing_metrics(status) -> None:
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
        "raw": {
            "reason": "pillar_2_exception" if status == "ERROR" else "gate_failed",
            "target_minutes_present": [],
        },
    }

    observation = build_p2_mining_observation(_event_context(), result)

    assert observation.metrics == {}
    assert observation.score is None
    assert observation.direction is None
    assert observation.is_successful is False
    assert observation.is_valid is None
    assert observation.observation_slot == "evaluation:5"
    assert observation.diagnostics["missing_inputs"] == ["PIN_SIDE"]
    assert observation.diagnostics["invalid_inputs"] == ["B365_SIDE"]
    assert observation.diagnostics["reason"] == result["raw"]["reason"]


def test_service_status_modes_and_disabled_toggle() -> None:
    writer = _CollectingWriter()
    all_service = PillarMiningService(writer, status_mode="all")
    active_service = PillarMiningService(writer, status_mode="active_only")
    disabled_service = PillarMiningService(writer, enabled=False)

    insufficient = {
        "engine_version": "p2_raw_v1",
        "P2_STATUS": "INSUFFICIENT_DATA",
        "P2_TARGET_MINUTE": None,
        "raw": {"reason": "gate_failed"},
    }

    assert all_service.persist_p2(_event_context(), insufficient) is True
    assert active_service.persist_p2(_event_context(), insufficient) is False
    assert active_service.persist_p2(_event_context(), _active_result()) is True
    assert disabled_service.persist_p2(_event_context(), _active_result()) is False
    assert [item.status for item in writer.observations] == [
        "INSUFFICIENT_DATA",
        "ACTIVE",
    ]


def test_service_rejects_unknown_status_mode() -> None:
    with pytest.raises(ValueError, match="status_mode"):
        PillarMiningService(_CollectingWriter(), status_mode="sometimes")
