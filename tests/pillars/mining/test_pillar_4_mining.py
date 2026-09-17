from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from modules.pillars.mining.adapters.pillar_4 import P4MiningAdapter
from modules.pillars.mining.contracts import validate_mining_run


def _event():
    return SimpleNamespace(
        event_id=4404,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=5,
        start_time_utc=datetime(2026, 9, 12, 18, 0, tzinfo=timezone.utc),
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _result():
    profile = {
        "META": {"TARGET_MINUTE": 5},
        "ADAPTIVE_VIEW": {
            "SOURCE_MODE": "PERSISTED_SNAPSHOTS",
            "SERIES": [
                {
                    "SERIES_ID": "s1",
                    "MARKET": {
                        "MARKET_GROUP": "Over/Under",
                        "MARKET_PERIOD": "Full Time",
                        "VIEW": "ADAPTIVE_VIEW",
                        "BOOKIE_ID": 302,
                        "SOURCE": "oddspapi",
                        "VALUE_TYPE": "ODDS_PRICE",
                    },
                }
            ],
        },
        "CHECKPOINT_VIEW": {"SOURCE_MODE": "FIXED_CHECKPOINTS", "SERIES": []},
    }
    periods = {"TOTALS_FULL_TIME": {"status": "COMPLETE"}}
    return {
        "pillar_id": "pillar_4_temporal_market_drift",
        "engine_version": "p4-signal-profile-v1",
        "P4_TARGET_MINUTE": 5,
        "P4_STATUS": "ACTIVE",
        "status": "ACTIVE",
        "PERIODS": periods,
        "MISSING_INPUTS": [],
        "INVALID_INPUTS": [],
        "AMBIGUOUS_INPUTS": [],
        "P4_SIGNAL_PROFILE": profile,
        "modules": [{"module_id": "p4_signal_engine"}],
        "raw": {
            "inputs": {"s1": [2.0, 1.9]},
            "input_trace": {"s1": {"QUOTE_ID": 10}},
            "periods": periods,
            "extraction_diagnostics": {},
        },
    }


def test_p4_adapter_persists_profile_without_scalar_score() -> None:
    run = P4MiningAdapter().build(_event(), _result())
    validate_mining_run(run)

    assert run.pillar_id == "pillar_4_temporal_market_drift"
    assert run.result_scope == "temporal_market_drift"
    assert run.execution_slot == "evaluation:5"
    assert run.payload_schema_version == 2
    assert run.canonical_status == "SUCCESS"
    assert [unit.unit_type for unit in run.units] == ["summary", "module"]
    assert all(unit.score is None for unit in run.units)
    assert all(unit.metrics == () for unit in run.units)
    assert run.units[0].payload["P4_SIGNAL_PROFILE"] == _result()["P4_SIGNAL_PROFILE"]
    assert run.units[1].module_id == "p4_signal_engine"
    assert run.units[0].dimensions["series_ids"] == ["s1"]
    assert run.units[0].dimensions["bookie_ids"] == [302]
    assert run.units[0].dimensions["views"] == ["ADAPTIVE", "CHECKPOINT"]


def test_p4_adapter_preserves_null_profile_and_diagnostics() -> None:
    result = _result()
    result.update(
        {
            "P4_STATUS": "INSUFFICIENT_DATA",
            "status": "INSUFFICIENT_DATA",
            "P4_SIGNAL_PROFILE": None,
            "modules": [],
            "MISSING_INPUTS": ["OPERATIVE_TARGET:5"],
        }
    )
    result["raw"]["reason"] = "operative_target_unavailable"

    run = P4MiningAdapter().build(_event(), result)

    assert run.canonical_status == "INSUFFICIENT"
    assert len(run.units) == 1
    assert run.units[0].payload["P4_SIGNAL_PROFILE"] is None
    assert run.diagnostics["missing_inputs"] == ["OPERATIVE_TARGET:5"]
