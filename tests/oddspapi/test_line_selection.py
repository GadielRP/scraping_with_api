"""Contract tests for current-line selection shared by persistence and cache."""

from copy import deepcopy
from decimal import Decimal

import pytest

from infrastructure.persistence.repositories.market_mapping_repository import (
    CanonicalMarketResolution,
    CanonicalOutcomeResolution,
    MarketMappingIndex,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.mainline_outcome_extractor import (
    OddspapiMainlineOutcomeExtractor,
)
from modules.jobs.pre_start_check_job.providers.oddspapi.exchange_outcome_selector import (
    OddspapiExchangeOutcomeSelector,
)
from modules.odds_ingestion.adapters.oddspapi_market_adapter import OddspapiMarketAdapter
from modules.odds_ingestion.oddspapi_line_selection import line_liquidity, select_current_lines


def _fixture(specs):
    """Each spec supplies a complete catalog, independently of observed payload."""
    index = MarketMappingIndex({}, {})
    markets = {}
    for mid, spec in enumerate(specs, start=1):
        period = spec.get("period", "Full Time")
        group = spec.get("group", "Over/Under")
        prices = spec.get("prices", [1.9, 2.0])
        choices = [str(choice) for choice in range(len(prices))]
        resolution = CanonicalMarketResolution(
            resolved=True, mapping_id=mid, canonical_market_key=f"{group}_{period}",
            canonical_market_name=f"{group} {period}", canonical_market_group=group,
            canonical_market_period=period, requires_choice_group=True,
            source_handicap=spec.get("line", str(mid)),
        )
        index.market_mappings[("oddspapi", "13", str(mid))] = resolution
        outcomes = {}
        for ci, (choice, price) in enumerate(zip(choices, prices)):
            oid = f"{mid}-{ci}"
            index.outcome_mappings[(mid, oid)] = CanonicalOutcomeResolution(
                resolved=True, canonical_choice_name=choice,
            )
            outcomes[oid] = {"players": {"0": {
                "price": price, "mainLine": spec.get("mainline", True),
                "active": spec.get("active", True),
                "limit": spec.get("limits", [None] * len(prices))[ci],
            }}}
        if spec.get("incomplete"):
            outcomes.pop(next(iter(outcomes)))
        markets[str(mid)] = {
            "marketActive": spec.get("market_active", True), "outcomes": outcomes,
        }
    payload = {"fixtureId": "232565", "sportId": "13", "bookmakerOdds": {
        "bet365": {"markets": markets},
    }}
    return payload, index


def _select(specs):
    payload, index = _fixture(specs)
    return select_current_lines(
        payload["bookmakerOdds"]["bet365"]["markets"],
        market_mapping_index=index, source_sport_id="13",
    )


def test_nearest_prices_win_before_liquidity():
    selection = _select([
        {"prices": [1.44, 2.05], "limits": [100000, 100000]},
        {"prices": [1.99, 2.05], "limits": [1, 1]},
    ])
    assert selection.selected_market_ids == {"2"}
    assert selection.diagnostics[-1]["selected"]["priceGap"] == "0.06"


def test_mainline_priority_precedes_price_balance():
    assert _select([
        {"prices": [1.44, 2.05]},
        {"prices": [2, 2], "mainline": False},
    ]).selected_market_ids == {"1"}


def test_adapter_does_not_report_discarded_complete_line_as_incomplete():
    payload, index = _fixture([{}, {}])
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=index,
        persist_main_line_only=True,
    )

    assert not adapted.get("diagnostics", {}).get("skipped_incomplete_markets")
    assert [
        market["choiceGroup"]
        for market in adapted["bookmakers"][0]["markets"]
    ] == ["1"]


@pytest.mark.parametrize("bad", [
    {"active": False}, {"market_active": False}, {"incomplete": True},
    {"prices": ["NaN", 2]}, {"prices": ["Infinity", 2]},
    {"prices": [True, 2]}, {"prices": [1, 2]}, {"prices": [0, 2]},
    {"prices": [None, 2]}, {"line": "NaN"},
])
def test_valid_active_complete_alternative_wins_over_bad_mainline(bad):
    assert _select([bad, {"mainline": False}]).selected_market_ids == {"2"}


def test_unknown_catalog_completeness_is_rejected():
    payload, index = _fixture([{}])
    index.outcome_mappings.clear()
    result = select_current_lines(payload["bookmakerOdds"]["bet365"]["markets"],
                                  market_mapping_index=index, source_sport_id="13")
    assert not result.selected_market_ids
    assert result.diagnostics[0]["reason"] == "unknown_complete_choice_set"


def test_normalized_limit_matches_supplied_example():
    value = line_liquidity([(Decimal("1.452"), 24889), (Decimal("2.95"), 11250)])
    assert value.base_limit == Decimal("11249.914")
    assert float(value.consistency) == pytest.approx(0.9999847111)
    assert float(value.effective_base_limit) == pytest.approx(11249.7420013)


@pytest.mark.parametrize("count", [2, 3])
def test_limit_tiebreak_is_not_a_sum(count):
    result = line_liquidity([(Decimal(2), 100)] * count)
    assert result.effective_base_limit == 100


def test_larger_effective_limit_breaks_equal_price_gap():
    assert _select([
        {"prices": [1.8, 2], "limits": [100, 100]},
        {"prices": [1.8, 2], "limits": [1000, 1000]},
    ]).selected_market_ids == {"2"}


def test_consistency_penalizes_unbalanced_limits():
    assert _select([
        {"prices": [2, 2], "limits": [10000, 1]},
        {"prices": [2, 2], "limits": [100, 100]},
    ]).selected_market_ids == {"2"}


@pytest.mark.parametrize("bad_limit", [None, -1, "NaN", "Infinity", True])
def test_partial_or_invalid_liquidity_never_uses_only_available_choices(bad_limit):
    assert line_liquidity([(Decimal(2), 10000), (Decimal(2), bad_limit)]) is None
    assert _select([
        {"prices": [2, 2], "limits": [10000, bad_limit]},
        {"prices": [2, 2], "limits": [0, 0]},
    ]).selected_market_ids == {"2"}


def test_three_way_price_range_and_median():
    assert _select([
        {"prices": [2, 2, 4], "limits": [10000] * 3},
        {"prices": [2.9, 3, 3.1], "limits": [10, 20, 30]},
    ]).selected_market_ids == {"2"}
    value = line_liquidity([(Decimal(3), limit) for limit in (10, 20, 30)])
    assert value.base_limit == 20
    assert float(value.effective_base_limit) == pytest.approx(20 / 3)


def test_full_tie_is_stable_when_json_order_changes():
    payload, index = _fixture([{"line": "1.5"}, {"line": "-1.5"}])
    markets = payload["bookmakerOdds"]["bet365"]["markets"]
    for ordered in (markets, dict(reversed(list(markets.items())))):
        assert select_current_lines(ordered, market_mapping_index=index,
                                    source_sport_id="13").selected_market_ids == {"2"}


def test_periods_and_market_families_are_independent():
    assert _select([{}, {"period": "1st Half"}, {"group": "Handicap"}]).selected_market_ids == {"1", "2", "3"}


def test_exact_decimal_ranking_precedes_repository_rounding():
    assert _select([{"prices": [1.9001, 2]}, {"prices": [1.9002, 2]}]).selected_market_ids == {"2"}


@pytest.mark.parametrize("mainline", [True, False])
def test_adapter_and_cache_choose_identical_outcomes_without_mutating_payload(mainline):
    payload, index = _fixture([
        {"prices": [2.62, 1.444], "line": "-1.5", "mainline": mainline},
        {"prices": [1.526, 2.5], "line": "1.5", "mainline": mainline},
    ])
    payload["bookmakerOdds"]["pinnacle"] = deepcopy(payload["bookmakerOdds"]["bet365"])
    payload["bookmakerOdds"]["pinnacle"]["markets"].pop("2")
    original = deepcopy(payload)
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload, market_mapping_index=index, persist_main_line_only=True,
    )
    cached = OddspapiMainlineOutcomeExtractor.extract(payload, market_mapping_index=index)
    persisted = {(book["slug"], choice["sourceMarketId"], choice["sourceOutcomeId"])
                 for book in adapted["bookmakers"] for market in book["markets"] for choice in market["choices"]}
    assert persisted == {(row["bookmaker_slug"], row["source_market_id"], row["source_outcome_id"]) for row in cached}
    assert {(book, mid) for book, mid, _ in persisted} == {("bet365", "2"), ("pinnacle", "1")}
    assert all(choice["mainLine"] for book in adapted["bookmakers"] for market in book["markets"] for choice in market["choices"])
    assert payload == original


def test_current_lines_require_activity_even_when_suspended_observations_are_allowed():
    payload, index = _fixture([{"active": False}, {}])
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload, market_mapping_index=index, persist_main_line_only=True, require_active_quotes=False,
    )
    cached = OddspapiMainlineOutcomeExtractor.extract(
        payload, market_mapping_index=index, require_active_quotes=False,
    )
    assert [market["choiceGroup"] for market in adapted["bookmakers"][0]["markets"]] == ["2"]
    assert {row["source_market_id"] for row in cached} == {"2"}


def test_historical_ingestion_preserves_cached_line_despite_new_price_balance():
    payload, index = _fixture([{"prices": [1.4, 3]}, {"prices": [2, 2]}])
    for market in payload["bookmakerOdds"]["bet365"]["markets"].values():
        for outcome in market["outcomes"].values():
            outcome["players"]["0"].pop("mainLine")
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload, market_mapping_index=index, use_mainline_cache=True,
        persist_main_line_only=True, mainline_outcome_ids_by_bookmaker={"bet365": {"1-0", "1-1"}},
    )
    assert [market["choiceGroup"] for market in adapted["bookmakers"][0]["markets"]] == ["1"]


def test_exchange_history_budget_is_spent_only_on_selected_line():
    payload, index = _fixture([
        {"prices": [1.4, 3], "mainline": False},
        {"prices": [2, 2], "mainline": False},
    ])
    payload["bookmakerOdds"]["betfair-ex"] = payload["bookmakerOdds"].pop("bet365")
    plan = OddspapiExchangeOutcomeSelector.select(
        payload, exchange_bookmakers=["betfair-ex"], market_mapping_index=index,
        allowed_market_keys=None, max_outcomes=2,
    )
    assert {selection.source_market_id for selection in plan.selections} == {"2"}
    assert len(plan.selections) == 2
    assert plan.truncated == 0
