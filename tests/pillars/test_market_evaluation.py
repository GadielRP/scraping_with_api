"""Behavioral acceptance tests for the shared market policy and real engines."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from modules.pillars.market_evaluation import (
    CANONICAL_CONTRACTS,
    FT,
    FT_OT,
    FIRST_HALF,
    prepare_event_markets,
)
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.trajectory_selection import select_target_minute
from modules.pillars.pillar_2_side_market.run_pillar_2 import calculate_pillar_2
from modules.pillars.pillar_3_totals_market_context.run_pillar_3 import (
    calculate_pillar_3,
)
from modules.pillars.pillar_4.run_pillar_4 import calculate_pillar_4
from modules.pillars.pillar_5.run_pillar_5 import calculate_pillar_5
from modules.pillars.pillar_5.calculation_models import MemorySample

START = datetime(2026, 10, 1, 18, tzinfo=timezone.utc)


def event():
    return SimpleNamespace(
        event_id=900,
        starts_at=START,
        minutes_until_start=5,
        sport="Football",
        participants_label="Home vs Away",
    )


def quotes(
    group="1X2",
    period=FT,
    book=302,
    *,
    outcomes=None,
    minute=5,
    side=None,
    price="2.0",
    line=None,
    market_id=70
):
    outcomes = outcomes or (
        ("Over", "Under")
        if group == "Over/Under"
        else ("1", "x", "2") if group == "1X2" else ("1", "2")
    )
    if line is None and group not in ("1X2", "Home/Away"):
        line = "2.5" if group == "Over/Under" else "-0.5"
    key = next(
        (key for key, pair in CANONICAL_CONTRACTS.items() if pair == (group, period)),
        "unknown",
    )
    rows = []
    for i, choice in enumerate(outcomes):
        quote = market_id * 1000 + book * 10 + i + (100000 if side == "lay" else 0)
        rows.append(
            {
                "event_id": 900,
                "market_id": market_id,
                "canonical_market_key": key,
                "market_name": "Provider name intentionally differs",
                "market_group": group,
                "market_period": period,
                "line_value": line,
                "bookie_id": book,
                "bookie_name": "book",
                "source": "oddspapi",
                "is_live": False,
                "choice_name": choice,
                "choice_id": quote,
                "quote_id": quote,
                "exchange_side": side,
                "exchange_level": 0,
                "main_line": True,
                "odds_value": price,
                "snapshot_id": quote * 1000 + minute,
                "collected_at": START - timedelta(minutes=minute),
                "source_collected_at": START - timedelta(minutes=minute),
                "observed_minutes_before_start": minute,
                "trajectory_minutes_before_start": Decimal(minute),
            }
        )
    return rows


class Memory:
    def __init__(self, n=3):
        self.n, self.keys = n, []

    def summarize(self, *, key, **kwargs):
        self.keys.append(key)
        return MemorySample(key, self.n, self.n, 0, 0)


def evaluate(pillar, rows, memory=None):
    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=[120, 30, 5],
        event_starts_at=START,
        evaluation_minute=5,
    )
    target = select_target_minute(
        context,
        flow_id="pre_start_signal_profile",
        expected_event_id=900,
        allowed_target_minutes=[120, 30, 5],
        evaluation_minute=5,
    )
    evaluation = prepare_event_markets(context, target, event())
    fn = {
        2: calculate_pillar_2,
        3: calculate_pillar_3,
        4: calculate_pillar_4,
        5: calculate_pillar_5,
    }[pillar]
    kwargs = {"memory_repository": memory or Memory()} if pillar == 5 else {}
    return fn(
        event(),
        context,
        target_selection=target,
        market_evaluation=evaluation,
        **kwargs
    )


@pytest.mark.parametrize(
    "pillar,group,period",
    [
        (2, "1X2", FT),
        (2, "Home/Away", FT_OT),
        (2, "Asian Handicap", FT),
        (2, "Handicap", FT_OT),
        (2, "1X2", FIRST_HALF),
        (3, "Over/Under", FT),
        (3, "Over/Under", FIRST_HALF),
        (5, "1X2", FT),
    ],
)
def test_one_book_and_one_family_is_success(pillar, group, period):
    result = evaluate(pillar, quotes(group, period))
    assert result["status"] == "ACTIVE"
    assert result["payload_schema_version"] == 4
    assert result["policy_version"] == "market-evaluation-v1"
    assert result["evidence"]["computed_signals"] > 0


@pytest.mark.parametrize("pillar,group", [(2, "1X2"), (3, "Over/Under")])
def test_back_only_is_independent_and_size_is_optional(pillar, group):
    result = evaluate(pillar, quotes(group, book=4, side="back"))
    assert result["status"] == "ACTIVE"
    assert any(
        (
            "BACK_EDGE" in s["evidence"].get("metric", "")
            or ".BACK.EDGE" in s["evidence"].get("metric", "")
        )
        and s["status"] == "COMPUTED"
        for s in result["signals"]
    )
    assert any(s["status"] == "BLOCKED" for s in result["signals"])


def test_overtime_choice_is_global_and_does_not_maximize_coverage():
    rows = quotes() + quotes("Over/Under") + quotes(book=3) + quotes("Home/Away", FT_OT)
    results = [evaluate(p, rows) for p in (2, 3, 4, 5)]
    assert {r["selected_full_time_period"] for r in results} == {FT_OT}
    assert results[1]["status"] == "INSUFFICIENT_DATA"
    assert all(r["selection"]["reason"] == "OVERTIME_PRIORITY" for r in results)


def test_incomplete_overtime_falls_back_with_reason():
    result = evaluate(2, quotes() + quotes("Home/Away", FT_OT, outcomes=("1",)))
    assert result["selected_full_time_period"] == FT
    assert result["selection"]["reason"] == "REGULATION_FALLBACK"
    assert result["selection"]["candidates"][0]["usable"] is False


def test_draw_is_contract_coverage_and_only_required_by_three_way_memory():
    rows = quotes(outcomes=("1", "2"))
    p2, p5 = evaluate(2, rows), evaluate(5, rows)
    assert p2["status"] == "ACTIVE"
    cell = next(
        c
        for c in p2["coverage"]
        if c["bookie_id"] == 302 and c["family"] == "1X2" and c["period"] == FT
    )
    assert cell["status"] == "INCOMPLETE" and cell["missing"] == ["x"]
    assert p5["status"] == "INSUFFICIENT_DATA"


def test_distinct_moneyline_contracts_have_independent_memories():
    memory = Memory()
    result = evaluate(5, quotes() + quotes("Home/Away", market_id=71), memory)
    assert result["status"] == "ACTIVE"
    assert len(result["analysis"]) == 2
    assert {key.market_shape for key in memory.keys} == {"TWO_WAY", "THREE_WAY"}


@pytest.mark.parametrize("price", ["1", "0", "-1", "NaN", "Infinity"])
def test_invalid_values_do_not_create_neutral_signals(price):
    result = evaluate(2, quotes(price=price))
    assert result["status"] == "INSUFFICIENT_DATA"
    assert not any(s["status"] == "COMPUTED" for s in result["signals"])


def test_live_and_unknown_contracts_do_not_enter_selection():
    rows = quotes("Home/Away", FT_OT)
    rows = [{**row, "is_live": True} for row in rows]
    result = evaluate(2, quotes() + rows)
    assert result["selected_full_time_period"] == FT
    assert any(d.get("reason") == "UNSUPPORTED_CONTRACT" for d in result["diagnostics"])


@pytest.mark.parametrize("n", [0, 1, 2, 3])
def test_memory_minimum_and_no_history_fallback(n):
    result = evaluate(5, quotes() + quotes("Home/Away", FT_OT), Memory(n))
    assert result["selected_full_time_period"] == FT_OT
    assert result["status"] == ("ACTIVE" if n >= 3 else "INSUFFICIENT_DATA")
    assert all("historical_matches" not in p for p in result["analysis"].values())


def test_multiple_line_contracts_are_evaluated_independently():
    rows = (
        quotes("Over/Under", line="2.5")
        + quotes("Over/Under", line="3.5", market_id=71)
        + quotes("Over/Under", book=3, line="2.5")
    )
    result = evaluate(3, rows)
    assert result["status"] == "ACTIVE"
    assert len(result["analysis"]) >= 2
    pin = [
        s
        for s in result["signals"]
        if s["evidence"].get("metric") == "FT.PINNACLE.EDGE"
        and s["status"] == "COMPUTED"
    ]
    assert len(pin) == 2


def test_temporal_one_observation_is_blocked_and_two_are_active():
    one = quotes("Over/Under", outcomes=("Over",))
    assert evaluate(4, one)["status"] == "INSUFFICIENT_DATA"
    result = evaluate(
        4, one + quotes("Over/Under", outcomes=("Over",), minute=30, price="2.2")
    )
    assert result["status"] == "ACTIVE"
    assert any(
        s["key"].endswith("NET_MOVE_RAW") and s["status"] == "COMPUTED"
        for s in result["signals"]
    )


def test_exchange_temporal_only_can_succeed():
    rows = quotes("Over/Under", outcomes=("Over",), book=4, side="back") + quotes(
        "Over/Under", outcomes=("Over",), book=4, side="back", minute=30
    )
    assert evaluate(4, rows)["status"] == "ACTIVE"


def test_error_in_one_memory_unit_keeps_other_result():
    class FailingMemory(Memory):
        def summarize(self, *, key, **kwargs):
            if key.bookie_id == 302:
                raise RuntimeError("lookup failed")
            return super().summarize(key=key, **kwargs)

    result = evaluate(5, quotes() + quotes(book=3), FailingMemory())
    assert result["status"] == "ACTIVE"
    assert result["execution_status"] == "COMPLETED_WITH_ERRORS"


def test_fragments_from_different_provider_contracts_are_not_combined():
    rows = quotes(outcomes=("1",), market_id=70) + quotes(outcomes=("2",), market_id=71)
    result = evaluate(2, rows)
    assert result["status"] == "INSUFFICIENT_DATA"
    result = evaluate(2, rows + quotes(book=3))
    assert result["status"] == "ACTIVE"
    assert any(s["reason"] == "AMBIGUOUS_CANDIDATE" for s in result["signals"])


@pytest.mark.parametrize("pillar,group", [(2, "1X2"), (3, "Over/Under")])
def test_ambiguous_lay_keeps_back_reading(pillar, group):
    rows = quotes(group, book=4, side="back") + quotes(group, book=4, side="lay")
    rows += [
        {**row, "source": "second-source"} for row in quotes(group, book=4, side="lay")
    ]
    result = evaluate(pillar, rows)
    assert result["status"] == "ACTIVE"
    assert any(s["reason"] == "AMBIGUOUS_CANDIDATE" for s in result["signals"])


def test_exchange_depth_is_preserved_per_outcome():
    rows = quotes("Over/Under", book=4, side="back")
    rows[1]["exchange_level"] = 1
    result = evaluate(3, rows)
    assert result["status"] == "ACTIVE"
    prices = [p for p in result["inputs"].values() if p.get("kind") == "PRICE"]
    assert {p["trace"]["exchange_level"] for p in prices} == {0, 1}


def test_first_five_innings_keeps_its_period_and_label():
    from modules.pillars.market_evaluation import FIRST_FIVE

    result = evaluate(2, quotes("Home/Away", FIRST_FIVE))
    assert result["status"] == "ACTIVE"
    metrics = [
        s["evidence"].get("metric", "")
        for s in result["signals"]
        if s["status"] == "COMPUTED"
    ]
    assert any("FIRST_FIVE_INNINGS" in metric for metric in metrics)
    assert not any("1H" in metric for metric in metrics)
    assert {p["trace"]["market_period"] for p in result["inputs"].values()} == {
        FIRST_FIVE
    }


@pytest.mark.parametrize("pillar,group", [(2, "Asian Handicap"), (3, "Over/Under")])
def test_price_pair_without_line_is_usable_but_not_a_complete_contract(pillar, group):
    rows = [{**row, "line_value": None} for row in quotes(group)]
    result = evaluate(pillar, rows)
    assert result["status"] == "ACTIVE"
    assert any(
        c["status"] == "INCOMPLETE" and c["bookie_id"] == 302
        for c in result["coverage"]
    )


def test_line_separation_has_real_line_provenance_without_valid_prices():
    result = evaluate(
        3,
        quotes("Over/Under", price="1", line="2.5")
        + quotes("Over/Under", book=3, price="1", line="3.5"),
    )
    assert result["status"] == "ACTIVE"
    signal = next(
        s
        for s in result["signals"]
        if s["status"] == "COMPUTED" and s["evidence"]["metric"].endswith("LINE_GAP")
    )
    assert len(signal["input_refs"]) == 2
    assert all(result["inputs"][ref]["kind"] == "LINE" for ref in signal["input_refs"])
    assert len(signal["contract_refs"]) == 2


def test_one_temporal_observation_does_not_publish_a_zero_movement():
    result = evaluate(4, quotes("Over/Under", outcomes=("Over",)))
    assert result["status"] == "INSUFFICIENT_DATA"
    for series in result["analysis"].values():
        assert series["raw_temporal_features"]["NET_MOVE_RAW"] is None


def test_missing_endpoint_blocks_only_temporal_series():
    rows = quotes("Over/Under", outcomes=("Over",), minute=120) + quotes(
        "Over/Under", outcomes=("Over",), minute=30
    )
    result = evaluate(4, rows + quotes(outcomes=("1", "2")))
    assert result["status"] == "INSUFFICIENT_DATA"
    assert any(s["reason"] == "MISSING_ENDPOINT" for s in result["signals"])


def test_temporal_reading_can_select_overtime_without_price_pair():
    rows = (
        quotes()
        + quotes("Over/Under", FT_OT, outcomes=("Over",), book=4)
        + quotes("Over/Under", FT_OT, outcomes=("Over",), book=4, minute=30)
    )
    result = evaluate(4, rows)
    assert result["selected_full_time_period"] == FT_OT
    assert result["status"] == "ACTIVE"


def test_future_availability_does_not_enter_temporal_movement():
    rows = quotes("Over/Under", outcomes=("Over",)) + quotes(
        "Over/Under", outcomes=("Over",), minute=30
    )
    future = quotes("Over/Under", outcomes=("Over",), minute=20, price="9")
    future = [{**r, "collected_at": START - timedelta(minutes=4)} for r in future]
    baseline, result = evaluate(4, rows), evaluate(4, rows + future)
    computed = lambda r: {
        s["key"]: s["value"] for s in r["signals"] if s["status"] == "COMPUTED"
    }
    assert computed(baseline) == computed(result)
    assert any(d.get("excluded_future_points", 0) > 0 for d in result["diagnostics"])


def test_result_states_are_independent_of_neutral_values_and_coverage():
    from modules.pillars.evaluation_contracts import EvaluationResult, SignalResult

    def build(signals, skipped=False):
        return EvaluationResult(
            900, "pillar_2_side_market", "v3", 5, {}, signals, (), skipped=skipped
        ).to_dict()

    assert build((SignalResult("neutral", "COMPUTED", 0),))["status"] == "ACTIVE"
    assert (
        build((SignalResult("missing", "BLOCKED", reason="MISSING_INPUT"),))["status"]
        == "INSUFFICIENT_DATA"
    )
    assert (
        build((SignalResult("failed", "ERROR", reason="CALCULATION_ERROR"),))[
            "execution_status"
        ]
        == "FAILED"
    )
    assert build((), True)["status"] == "SKIPPED"
    with pytest.raises(ValueError):
        SignalResult("missing", "COMPUTED")


def test_line_contract_transition_is_explicit_and_does_not_join_price_series():
    rows = quotes(
        "Over/Under", FT_OT, outcomes=("Over",), minute=30, line="2.5", market_id=70
    )
    rows += quotes("Over/Under", FT_OT, outcomes=("Over",), line="3.5", market_id=71)
    result = evaluate(4, rows)
    assert result["selected_full_time_period"] == FT_OT
    assert result["status"] == "ACTIVE"
    signal = next(
        s
        for s in result["signals"]
        if s["key"].endswith("NET_MOVE_RAW")
        and s["status"] == "COMPUTED"
        and s["evidence"]["market"]["VALUE_TYPE"] == "LINE"
    )
    assert signal["value"] == 1
    assert len(signal["contract_refs"]) == 2
    ended_prices = [
        series for series in result["analysis"].values()
        if series["market"]["VALUE_TYPE"] == "ODDS_PRICE"
        and series["market"]["CHOICE_GROUP"] == "2.5"
    ]
    assert ended_prices
    assert all(series["status"] == "CONTRACT_ENDED" for series in ended_prices)
    assert not any(
        s["key"].endswith("NET_MOVE_RAW")
        and s["status"] == "COMPUTED"
        and s["evidence"]["market"]["VALUE_TYPE"] == "ODDS_PRICE"
        for s in result["signals"]
    )


def test_shared_contract_registry_only_uses_existing_catalog_contracts():
    from infrastructure.persistence.catalogs.canonical_market_types import (
        CANONICAL_MARKET_TYPE_SEEDS,
    )

    for key, (family, period) in CANONICAL_CONTRACTS.items():
        existing = CANONICAL_MARKET_TYPE_SEEDS[key]
        assert (
            existing["canonical_market_group"],
            existing["canonical_market_period"],
        ) == (family, period)


@pytest.mark.parametrize("pillar", [2, 3, 4, 5])
def test_absent_context_returns_insufficient_data(pillar):
    from modules.pillars.trajectory_selection import TargetMinuteSelection

    fn = {
        2: calculate_pillar_2,
        3: calculate_pillar_3,
        4: calculate_pillar_4,
        5: calculate_pillar_5,
    }[pillar]
    kwargs = {"memory_repository": Memory()} if pillar == 5 else {}
    result = fn(
        event(),
        None,
        target_selection=TargetMinuteSelection(None, "odds_trajectory_unavailable"),
        **kwargs
    )
    assert result["status"] == "INSUFFICIENT_DATA"


def test_registering_a_bookmaker_does_not_enable_p4_capabilities(monkeypatch):
    import modules.pillars.market_evaluation as policy

    extra = policy.Bookmaker("new-book", 777)
    monkeypatch.setattr(policy, "BOOKMAKERS", (*policy.BOOKMAKERS, extra))
    monkeypatch.setattr(policy, "BOOKMAKER_IDS", policy.BOOKMAKER_IDS | {extra.id})
    rows = (
        quotes()
        + quotes("Over/Under", book=777, minute=30)
        + quotes("Over/Under", book=777)
    )
    result = evaluate(4, rows)
    assert result["status"] == "INSUFFICIENT_DATA"
    assert not any(s["status"] == "COMPUTED" for s in result["signals"])
    assert all(
        c["status"] == "NOT_APPLICABLE"
        for c in result["coverage"]
        if c["bookie_id"] == 777
    )
    overtime = quotes("Over/Under", FT_OT, book=777, minute=30) + quotes(
        "Over/Under", FT_OT, book=777
    )
    assert evaluate(4, quotes() + overtime)["selected_full_time_period"] == FT


def test_invalid_optional_sizes_limits_and_lines_keep_valid_p4_price_metrics():
    import json

    rows = quotes("Over/Under", line="NaN", minute=30) + quotes(
        "Over/Under", line="NaN"
    )
    rows = [{**r, "source_limit": "NaN", "exchange_size": "Infinity"} for r in rows]
    result = evaluate(4, rows)
    assert result["status"] == "ACTIVE"
    json.dumps(result, allow_nan=False)
    assert any(
        "invalid_source_limit" in value
        for d in result["diagnostics"]
        for value in d.get("invalid", [])
    )
    assert not any(
        s["evidence"].get("market", {}).get("VALUE_TYPE") == "LINE"
        and s["status"] == "COMPUTED"
        for s in result["signals"]
    )
