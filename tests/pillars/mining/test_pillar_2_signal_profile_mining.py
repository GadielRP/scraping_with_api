"""The common schema v5 writer is verified across all pillars."""

from tests.pillars.test_market_evaluation import event, evaluate, quotes
from modules.pillars.mining.adapters import P2MiningAdapter
from modules.pillars.mining.contracts import validate_mining_run
from dataclasses import replace
from modules.pillars.mining.serialization import to_json_value


def test_p2_writes_versioned_signals_without_profile_copies():
    result = evaluate(2, quotes())
    run = P2MiningAdapter().build(event(), result)
    validate_mining_run(replace(run, evaluation_id="test-evaluation"))
    assert run.payload_schema_version == 5
    assert run.engine_version == result["engine_version"]
    assert "inputs" not in run.output_payload
    assert "signals" not in run.output_payload
    assert run.inputs == to_json_value(result["inputs"])
    assert len(run.units) == len(result["signals"]) + 1
    assert all("analysis" not in unit.payload for unit in run.units)
