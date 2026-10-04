"""Boundary and audit regressions for the unified market evaluation contract."""

import logging
from datetime import timedelta

import pytest

from modules.pillars.market_evaluation import prepare_event_markets
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.trajectory_selection import TargetMinuteSelection
from modules.pillars.pillar_4.run_pillar_4 import calculate_pillar_4
from modules.pillars.mining.adapters import P4MiningAdapter, P5MiningAdapter
from modules.pillars.mining.contracts import validate_mining_run
from tests.pillars.test_market_evaluation import START, FT, FT_OT, event, quotes, evaluate


def run_p4(rows, *, evaluation_as_of=None, debug_mode=False):
    context = build_odds_trajectory_context(
        rows, target_minutes_expected=[120, 30, 5], event_starts_at=START,
        evaluation_minute=5, evaluation_as_of=evaluation_as_of,
    )
    target = TargetMinuteSelection(5)
    evaluation = prepare_event_markets(context, target, event())
    result = calculate_pillar_4(event(), context, target, market_evaluation=evaluation,
                                debug_mode=debug_mode)
    return result, evaluation


def prices(result, book=302, view="ADAPTIVE_VIEW"):
    return [series for series in result["analysis"].values()
            if series["market"]["BOOKIE_ID"] == book
            and series["market"]["VALUE_TYPE"] == "ODDS_PRICE"
            and series["market"]["VIEW"] == view]


@pytest.mark.parametrize("pillar", [2, 3, 4, 5])
def test_unusable_overtime_does_not_displace_regulation(pillar):
    regulation = quotes(minute=30) + quotes() + quotes("Over/Under")
    endpoint = [
        {**row, "source_collected_at": START - timedelta(minutes=20)}
        for row in quotes("Over/Under", FT_OT, outcomes=("Over",), market_id=71)
    ]
    tick_after_endpoint = quotes(
        "Over/Under", FT_OT, outcomes=("Over",), market_id=71, minute=10, price="2.1"
    )
    result = evaluate(pillar, regulation + endpoint + tick_after_endpoint)
    assert result["selected_full_time_period"] == FT
    assert result["status"] == "ACTIVE"
    assert result["selection"]["candidates"][0]["usable"] is False


def test_later_collection_keeps_history_but_later_market_state_is_excluded():
    history = [
        {**row, "collected_at": START - timedelta(minutes=4, seconds=45)}
        for row in quotes(minute=30, price="2.2")
    ]
    after_endpoint = [
        {**row, "collected_at": START - timedelta(minutes=4, seconds=40),
         "source_collected_at": START - timedelta(minutes=4, seconds=50)}
        for row in quotes(minute=4, price="9")
    ]
    result, evaluation = run_p4(
        quotes() + history + after_endpoint,
        evaluation_as_of=START - timedelta(minutes=4, seconds=30),
    )
    assert all(series["traceability"]["OBSERVATION_COUNT"] == 2 for series in prices(result))
    assert all(series["raw_temporal_features"]["NET_MOVE_RAW"] == pytest.approx(-.2)
               for series in prices(result))
    # P4 details must not mutate the envelope shared with P2/P3/P5.
    assert "operative_as_of" not in evaluation.selection["checkpoint"]


def test_one_point_without_snapshot_id_is_not_counted_twice():
    result, _ = run_p4([{**row, "snapshot_id": None} for row in quotes()])
    assert result["status"] == "INSUFFICIENT_DATA"
    assert all(series["traceability"]["OBSERVATION_COUNT"] == 1 for series in prices(result))
    assert all(series["raw_temporal_features"]["NET_MOVE_RAW"] is None
               for series in prices(result))


def test_missing_endpoint_does_not_imply_line_contract_ended():
    rows = quotes() + quotes("Asian Handicap", minute=120)
    rows += quotes("Asian Handicap", minute=30)
    result, _ = run_p4(rows)
    handicap = [series for series in prices(result)
                if series["market"]["MARKET_GROUP"] == "Asian Handicap"]
    assert handicap
    assert all(series["status"] == "INSUFFICIENT_DATA" for series in handicap)
    assert all(series["raw_temporal_features"]["NET_MOVE_RAW"] is None
               for series in handicap)


def test_late_betfair_endpoint_and_single_point_bet365_are_preserved():
    rows = quotes("Asian Handicap", minute=30) + quotes("Asian Handicap")
    rows += quotes("Asian Handicap", book=3)
    rows += [
        {**row, "collected_at": START - timedelta(minutes=4, seconds=45),
         "source_collected_at": START - timedelta(minutes=4, seconds=45)}
        for row in quotes("Asian Handicap", book=4, side="back")
    ]
    result, _ = run_p4(rows, evaluation_as_of=START - timedelta(minutes=4, seconds=30))
    assert result["status"] == "ACTIVE"
    for book in (3, 4):
        assert prices(result, book)
        for series in prices(result, book):
            assert series["traceability"]["OPERATIVE_ENDPOINT_PRESENT"]
            assert series["status"] == "INSUFFICIENT_DATA"
            assert series["raw_temporal_features"]["NET_MOVE_RAW"] is None


def test_derived_signals_inherit_contracts_and_mining_dimensions():
    rows = []
    for book, side in ((302, None), (3, None), (4, "back"), (4, "lay")):
        for minute in (30, 5):
            rows += quotes(book=book, side=side, minute=minute)
    result, _ = run_p4(rows)
    derived = [signal for signal in result["signals"]
               if signal["status"] == "COMPUTED"
               and signal["evidence"]["market"]["BOOKIE_ID"] is None]
    assert derived
    assert all(signal["contract_refs"] for signal in derived)
    run = P4MiningAdapter().build(event(), result)
    validate_mining_run(run)
    keys = {signal["key"] for signal in derived}
    units = [unit for unit in run.units if unit.payload.get("signal_key") in keys]
    assert all(unit.dimensions["contract_refs"] and unit.market_period == FT for unit in units)


def test_debug_logging_explains_timing_source_and_endpoint(caplog):
    rows = quotes("Asian Handicap", minute=30) + quotes("Asian Handicap")
    rows += quotes("Asian Handicap", book=3)
    with caplog.at_level(logging.INFO):
        run_p4(rows, debug_mode=True)
    assert "P4 timing" in caplog.text
    assert "nominal=" in caplog.text and "latest checkpoint=" in caplog.text
    assert "book=book (3)" in caplog.text
    assert "status=INSUFFICIENT_DATA | observations=1" in caplog.text
    assert "endpoint price=" in caplog.text and "net move=None" in caplog.text
    assert "P4 coverage" in caplog.text


@pytest.mark.parametrize("regular", [True, False])
def test_p5_preserves_partial_exchange_exposure_without_memory_signals(regular):
    rows = quotes() if regular else []
    exchange = quotes(book=4, side="back", outcomes=("1", "2"))
    exchange = [{**row, "exchange_size": "125.5"} for row in exchange]
    result = evaluate(5, rows + exchange)
    assert result["status"] == ("ACTIVE" if regular else "INSUFFICIENT_DATA")
    exposure = [item for item in result["inputs"].values() if item.get("role") == "DIAGNOSTIC"]
    assert len(exposure) == 2
    assert all(item["exchange_size"] == 125.5 and item["trace"]["bookie_id"] == 4
               and item["trace"]["snapshot_id"] is not None for item in exposure)
    diagnostic = next(item for item in result["diagnostics"]
                      if item.get("kind") == "exchange_exposure" and item["exchange_side"] == "back")
    assert diagnostic["status"] == "INCOMPLETE" and diagnostic["missing"] == ["x"]
    assert diagnostic["participates_in_score"] is False
    assert not any(set(signal["input_refs"]) & set(diagnostic["input_refs"])
                   for signal in result["signals"])
    run = P5MiningAdapter().build(event(), result)
    validate_mining_run(run)
    assert all(ref in run.inputs for ref in diagnostic["input_refs"])
    assert diagnostic in run.output_payload["diagnostics"]
