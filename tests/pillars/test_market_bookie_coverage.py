"""Regression coverage for independently usable bookmakers in P2 and P3."""

import pytest

from modules.pillars import market_snapshot_extractor
from modules.pillars.market_coverage import PeriodDiagnostics
from modules.pillars.mining.adapters.pillar_2 import P2MiningAdapter
from modules.pillars.mining.adapters.pillar_3 import P3MiningAdapter
from modules.pillars.mining.contracts import validate_mining_run
from tests.pillars.pillar_2_side_market import test_pillar_2_raw as side
from tests.pillars.pillar_3_totals_market_context import (
    test_pillar_3_signal_profile as totals,
)


@pytest.fixture(autouse=True)
def selected_minute(monkeypatch):
    monkeypatch.setitem(
        market_snapshot_extractor.HARDCODED_TARGET_MINUTE_BY_FLOW,
        "pre_start_signal_profile",
        None,
    )


@pytest.mark.parametrize(
    "bookie_id,bookie_name", [(302, "pinnacle"), (3, "bet365"), (4, "betfair")]
)
@pytest.mark.parametrize(
    "pillar,helper,adapter", [(2, side, P2MiningAdapter), (3, totals, P3MiningAdapter)]
)
def test_single_complete_bookie_produces_and_persists_partial_profile(
    pillar, helper, adapter, bookie_id, bookie_name
):
    rows = helper._complete_rows(
        **({"include_betfair_ou": True} if pillar == 3 else {})
    )
    result = helper._calculate([row for row in rows if row["bookie_id"] == bookie_id])
    assert result["status"] == "PARTIAL"
    coverage = result["PERIODS"]["full_time"]
    assert coverage["status"] == "PARTIAL"
    assert coverage["available_bookies"] == [bookie_name]
    assert coverage["bookies"][bookie_name]["status"] == "COMPLETE"
    profile = result[f"P{pillar}_SIGNAL_PROFILE"]
    if pillar == 2:
        if bookie_id == 4:
            assert profile["EXCHANGE"]["REP_EDGE"] is not None
        else:
            prefix = "PIN" if bookie_id == 302 else "B365"
            assert profile["FT"]["1X2"][f"{prefix}_EDGE"] is not None
            assert profile["EXCHANGE"] is None
        assert profile["BOOK_EXCHANGE"] is None
        assert profile["FT"]["1X2"]["BOOK_RELATION"] is None
    else:
        if bookie_id == 4:
            assert profile["BETFAIR_FT_OU"]["REPRESENTATIVE"]["EDGE"] is not None
        else:
            assert profile["FT"][bookie_name.upper()]["EDGE"] is not None
        assert profile["FT"]["BOOK_RELATION"]["RELATION"] is None
    run = adapter().build(helper._event_context(), result)
    validate_mining_run(run)
    assert len(run.units) == 2
    assert "FULL_TIME" in run.units[0].dimensions["market_periods"]
    assert run.diagnostics["periods"]["full_time"]["available_bookies"] == [bookie_name]


@pytest.mark.parametrize("pillar,helper", [(2, side), (3, totals)])
def test_different_complete_bookies_in_ft_and_first_half_are_retained(pillar, helper):
    rows = [
        row
        for row in helper._complete_rows()
        if row["bookie_id"] == (302 if row["market_period"] == "Full Time" else 3)
    ]
    result = helper._calculate(rows)
    assert result["status"] == "PARTIAL"
    assert result["PERIODS"]["full_time"]["available_bookies"] == ["pinnacle"]
    assert result["PERIODS"]["first_half"]["available_bookies"] == ["bet365"]
    profile = result[f"P{pillar}_SIGNAL_PROFILE"]
    assert profile["FT"] is not None and profile["1H"] is not None
    assert profile["FT_1H"] is None


@pytest.mark.parametrize("helper", [side, totals])
def test_first_half_cannot_substitute_for_missing_full_time(helper):
    result = helper._calculate(
        [row for row in helper._complete_rows() if row["market_period"] == "1st Half"]
    )
    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["modules"] == []
    assert result["raw"]["inputs"]
    assert result["raw"]["input_trace"]


def test_p2_each_bookie_can_use_its_own_spread_family():
    rows = side._complete_rows()
    for row in rows:
        if row["bookie_id"] == 3 and row["market_group"] == "Asian Handicap":
            row["market_group"] = "Handicap"
            row["market_name"] = "Handicap " + row["market_period"]
    result = side._calculate(rows)
    assert result["status"] == "ACTIVE"
    assert result["PERIODS"]["full_time"]["bookies"]["bet365"]["status"] == "COMPLETE"
    ft = result["P2_SIGNAL_PROFILE"]["FT"]
    assert ft["AH"]["PIN_EDGE"] is not None
    assert ft["HANDICAP"]["B365_EDGE"] is not None
    assert ft["AH"]["REP_EDGE"] is None
    assert ft["HANDICAP"]["REP_EDGE"] is None


def test_p2_cannot_complete_one_bookie_using_another_bookies_markets():
    rows = [
        row
        for row in side._complete_rows()
        if row["market_period"] == "Full Time"
        and (
            (row["bookie_id"] == 302 and row["market_group"] == "1X2")
            or (row["bookie_id"] == 3 and row["market_group"] == "Asian Handicap")
        )
    ]
    result = side._calculate(rows)
    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["PERIODS"]["full_time"]["available_bookies"] == []
    assert result["raw"]["inputs"]["PIN_HOME_1X2_FULL_TIME_ODDS_PRICE"] == 2.0


@pytest.mark.parametrize("bad_price", [0, -2, "NaN", "Infinity"])
@pytest.mark.parametrize("pillar,helper", [(2, side), (3, totals)])
def test_invalid_bookie_does_not_poison_complete_counterpart(pillar, helper, bad_price):
    rows = helper._complete_rows()
    for row in rows:
        if row["bookie_id"] == 302 and row["market_period"] == "Full Time":
            row["odds_value"] = bad_price
    result = helper._calculate(rows)
    assert result["status"] == "PARTIAL"
    coverage = result["PERIODS"]["full_time"]
    assert coverage["bookies"]["pinnacle"]["status"] == "INVALID"
    assert "bet365" in coverage["available_bookies"]
    assert result["INVALID_INPUTS"]


def test_p2_missing_draw_does_not_complete_betfair_1x2():
    rows = [
        row
        for row in side._complete_rows()
        if not (row["bookie_id"] == 4 and row["choice_name"] == "x")
    ]
    result = side._calculate(rows)
    assert result["status"] == "PARTIAL"
    assert result["PERIODS"]["full_time"]["available_bookies"] == ["pinnacle", "bet365"]
    assert result["P2_SIGNAL_PROFILE"]["EXCHANGE"] is None


def test_totals_keeps_different_period_contracts_without_comparing_them():
    rows = totals._complete_rows()
    for row in rows:
        if row["bookie_id"] == 302 and row["market_period"] == "Full Time":
            row["market_period"] = "Full Time Including Overtime"
            row["market_name"] = "Over/Under Full Time Including Overtime"
    result = totals._calculate(rows)
    ft = result["P3_SIGNAL_PROFILE"]["FT"]
    assert ft["PINNACLE"]["EDGE"] is not None and ft["BET365"]["EDGE"] is not None
    assert ft["BOOK_RELATION"]["RELATION"] is None
    assert ft["REPRESENTATIVE"]["EDGE"] is None


def test_shared_diagnostics_preserve_ambiguity_locally():
    coverage = PeriodDiagnostics.from_bookies(
        {
            "pinnacle": PeriodDiagnostics.from_gate(
                complete=False, ambiguous_inputs=["line"]
            ),
            "bet365": PeriodDiagnostics.from_gate(complete=True),
        }
    )
    assert coverage.usable
    assert coverage.status == "PARTIAL"
    assert coverage.to_dict()["bookies"]["pinnacle"]["status"] == "AMBIGUOUS"


@pytest.mark.parametrize("period", ["Full Time", "1st Half"])
def test_p2_complete_common_handicap_is_preferred_over_one_books_ah(period):
    rows = side._complete_rows()
    spreads = [
        dict(row, market_group="Handicap", market_name=f"Handicap {period}")
        for row in rows
        if row["market_period"] == period and row["market_group"] == "Asian Handicap"
    ]
    rows = [
        row
        for row in rows
        if not (
            row["market_period"] == period
            and row["market_group"] == "Asian Handicap"
            and row["bookie_id"] == 3
        )
    ]
    result = side._calculate(rows + spreads)
    assert result["status"] == "ACTIVE"
    period_key = "FT" if period == "Full Time" else "1H"
    block = result["P2_SIGNAL_PROFILE"][period_key]
    assert block["AH"]["PIN_EDGE"] is not None
    assert block["HANDICAP"]["REP_EDGE"] is not None
    assert block["CROSS_MARKET"][f"{period_key}_1X2_HANDICAP_RELATION"] is not None


@pytest.mark.parametrize("helper", [side, totals])
def test_unique_complete_candidate_wins_over_incomplete_alternative(helper):
    rows = helper._complete_rows()
    if helper is side:
        side._add_market(
            rows,
            minute=5,
            market_group="Asian Handicap",
            market_period="Full Time",
            market_name="Asian Handicap Full Time",
            choice_group="-2.5",
            bookie_id=302,
            prices={"1": 1.95},
        )
    else:
        rows += totals._book_rows(
            bookie_id=302,
            bookie_name="Pinnacle",
            line="3.5",
            over=1.9,
            under=None,
            target_minute=5,
            period="Full Time",
            market_name="Over/Under Full Time",
            market_id=9999,
        )
    result = helper._calculate(rows)
    assert result["status"] == "ACTIVE"
    assert result["PERIODS"]["full_time"]["bookies"]["pinnacle"]["status"] == "COMPLETE"


def test_totals_betfair_different_back_lay_periods_are_not_complete():
    rows = totals._complete_rows(include_betfair_ou=True)
    for row in rows:
        if row["bookie_id"] == 4 and row["exchange_side"] == "lay":
            row["market_period"] = "Full Time Including Overtime"
            row["market_name"] = "Over/Under Full Time Including Overtime"
    result = totals._calculate(rows)
    assert result["PERIODS"]["exchange_ou"]["status"] == "INCOMPLETE"
    assert (
        result["P3_SIGNAL_PROFILE"]["BETFAIR_FT_OU"]["REPRESENTATIVE"]["EDGE"] is None
    )
