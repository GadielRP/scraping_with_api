"""Coverage is independent from analytical success for every configured bookmaker."""

import pytest
from modules.pillars.mining.adapters import P2MiningAdapter, P3MiningAdapter
from modules.pillars.mining.contracts import validate_mining_run
from tests.pillars.test_market_evaluation import (
    evaluate,
    event,
    quotes,
    FT,
    FT_OT,
    FIRST_HALF,
)


@pytest.mark.parametrize(
    "pillar,group,adapter",
    [(2, "1X2", P2MiningAdapter), (3, "Over/Under", P3MiningAdapter)],
)
@pytest.mark.parametrize("book", [302, 3, 4])
def test_one_book_produces_success_without_required_counterparts(
    pillar, group, adapter, book
):
    rows = quotes(group, book=book, side="back" if book == 4 else None)
    result = evaluate(pillar, rows)
    assert result["status"] == "ACTIVE"
    run = adapter().build(event(), result)
    validate_mining_run(run)
    assert run.canonical_status == "SUCCESS"
    assert any(c["status"] == "MISSING" for c in result["coverage"])


@pytest.mark.parametrize("pillar,group", [(2, "1X2"), (3, "Over/Under")])
def test_different_books_in_ft_and_half_remain_independent(pillar, group):
    result = evaluate(pillar, quotes(group) + quotes(group, FIRST_HALF, 3))
    profile = next(iter(result["analysis"].values()))
    assert result["status"] == "ACTIVE"
    assert profile["FT_1H"] is None


@pytest.mark.parametrize("pillar,group", [(2, "1X2"), (3, "Over/Under")])
@pytest.mark.parametrize("bad_price", ["1", "0", "-2", "NaN", "Infinity"])
def test_invalid_book_does_not_poison_valid_counterpart(pillar, group, bad_price):
    result = evaluate(pillar, quotes(group, price=bad_price) + quotes(group, book=3))
    assert result["status"] == "ACTIVE"
    assert any(
        c["status"] == "INVALID"
        for c in result["coverage"]
        if c["bookie_id"] == 302 and c["period"] == FT
    )


def test_same_contract_source_ambiguity_is_local():
    duplicate = [
        {**row, "source": "another", "quote_id": row["quote_id"] + 10000}
        for row in quotes("Over/Under")
    ]
    result = evaluate(
        3, quotes("Over/Under") + duplicate + quotes("Over/Under", book=3)
    )
    assert result["status"] == "ACTIVE"
    assert any(
        c["status"] == "AMBIGUOUS"
        for c in result["coverage"]
        if c["bookie_id"] == 302 and c["period"] == FT
    )


def test_periods_are_not_mixed_even_between_exchange_sides():
    result = evaluate(
        3,
        quotes("Over/Under", book=4, side="back")
        + quotes("Over/Under", FT_OT, book=4, side="lay"),
    )
    assert result["selected_full_time_period"] == FT_OT
    profile = next(iter(result["analysis"].values()))
    assert profile["BETFAIR_FT_OU"]["BACK_LAY_RELATION"] is None
    assert profile["BETFAIR_FT_OU"]["REPRESENTATIVE"]["EDGE"] is None
