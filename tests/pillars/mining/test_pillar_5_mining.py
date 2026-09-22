from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from modules.pillars.mining.adapters.pillar_5 import P5MiningAdapter
from modules.pillars.mining.contracts import validate_mining_run


def _event():
    return SimpleNamespace(
        event_id=5505,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=5,
        starts_at=datetime(2026, 9, 21, 18, 0, tzinfo=timezone.utc),
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _profile(bookmaker: str, bookie_id: int, *, valid: bool) -> dict:
    return {
        "bookmaker": bookmaker,
        "bookie_id": bookie_id,
        "P5_STATUS": "ACTIVE" if valid else "INSUFFICIENT_DATA",
        "P5_VALID": valid,
        "P5_DIRECTION": "HOME" if valid else "NONE",
        "P5": 0.2 if valid else 0,
        "P5_STRENGTH": "WEAK" if valid else "NONE",
        "market_group": "1X2",
        "market_period": "Full Time",
        "market_shape": "THREE_WAY",
        "target_minute": 5,
        "memory_status": "ACTIVE" if valid else "INSUFFICIENT_DATA",
        "reason": None if valid else "minimum_sample_size_not_met",
        "sample_size": 4 if valid else 1,
        "wins_home": 3 if valid else 1,
        "wins_draw": 0,
        "wins_away": 1 if valid else 0,
        "HIST_EDGE": 0.625 if valid else None,
        "MSRI_RAW": 0.25390625 if valid else None,
        "MSRI_SIGNAL": 0.5 if valid else None,
        "SAMPLE_WEIGHT": 0.4 if valid else None,
        "historical_matches": [],
        "diagnostics": {},
    }


def _result() -> dict:
    profiles = {
        "sofascore": _profile("sofascore", 1, valid=False),
        "pinnacle": _profile("pinnacle", 302, valid=True),
        "bet365": _profile("bet365", 3, valid=False),
    }
    return {
        "pillar_id": "pillar_5",
        "engine_version": "p5_price_memory_v3_0",
        "P5_TARGET_MINUTE": 5,
        "P5_STATUS": "PARTIAL",
        "status": "PARTIAL",
        "P5_EXTRACTION_STATUS": "PARTIAL",
        "P5_MEMORY_PROFILES": profiles,
        "PERIODS": {"full_time": {"status": "PARTIAL"}},
        "MISSING_INPUTS": [],
        "INVALID_INPUTS": [],
        "AMBIGUOUS_INPUTS": [],
        "modules": [{"module_id": "p5_memory_engine"}],
        "raw": {
            "inputs": {"PIN_HOME_1X2_FULL_TIME_ODDS_PRICE": 1.9},
            "input_trace": {},
            "memory_diagnostics": {},
            "betfair_exposure": {
                "bookie_id": 4,
                "inputs": {"BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE": 1.91},
                "input_trace": {},
                "participates_in_score": False,
            },
        },
    }


def test_p5_adapter_persists_independent_bookmaker_units() -> None:
    run = P5MiningAdapter().build(_event(), _result())
    validate_mining_run(run)

    assert run.pillar_id == "pillar_5"
    assert run.result_scope == "exact_price_memory"
    assert run.canonical_status == "PARTIAL"
    assert run.payload_schema_version == 3
    assert [unit.unit_key for unit in run.units] == [
        "summary",
        "p5_memory_engine",
        "bookmaker:1",
        "bookmaker:302",
        "bookmaker:3",
        "bookmaker:4",
    ]
    pinnacle = next(unit for unit in run.units if unit.unit_key == "bookmaker:302")
    assert pinnacle.score_name == "P5"
    assert float(pinnacle.score) == 0.2
    assert pinnacle.direction == "HOME"
    assert pinnacle.is_valid is True
    assert pinnacle.payload["sample_size"] == 4

    betfair = next(unit for unit in run.units if unit.unit_key == "bookmaker:4")
    assert betfair.score is None
    assert betfair.dimensions["diagnostic_only"] is True
    assert betfair.payload["participates_in_score"] is False
