"""The common schema v4 writer is verified across all pillars."""

from tests.pillars.test_market_evaluation import event, evaluate, quotes
from modules.pillars.mining.adapters import P4MiningAdapter
from modules.pillars.mining.contracts import validate_mining_run


def test_p4_writes_versioned_signals_without_profile_copies():
    result = evaluate(4, quotes())
    run = P4MiningAdapter().build(event(), result)
    validate_mining_run(run)
    assert run.payload_schema_version == 4
    assert run.engine_version == result["engine_version"]
    assert "inputs" not in run.output_payload
    assert "signals" not in run.output_payload
    assert run.inputs == result["inputs"]
    assert len(run.units) == len(result["signals"]) + 1
    assert all("analysis" not in unit.payload for unit in run.units)
