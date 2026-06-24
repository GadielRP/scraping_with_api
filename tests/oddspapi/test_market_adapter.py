import json
from pathlib import Path

from infrastructure.persistence.repositories.market_mapping_repository import (
    CanonicalMarketResolution,
    CanonicalOutcomeResolution,
    MarketMappingIndex,
)
from modules.odds_ingestion.adapters.oddspapi_market_adapter import OddspapiMarketAdapter
from modules.odds_ingestion.adapters.sofascore_market_adapter import SofaScoreMarketAdapter


def player(bookmaker_outcome_id, price, active=True, exchange_meta=None):
    result = {
        "active": active,
        "bookmakerOutcomeId": bookmaker_outcome_id,
        "price": price,
        "changedAt": "2026-06-19T00:00:00Z",
        "mainLine": True,
        "limit": 100,
    }
    if exchange_meta is not None:
        result["exchangeMeta"] = exchange_meta
    return result


def market(outcomes):
    return {
        "marketActive": True,
        "outcomes": {
            str(index): {"players": {"0": value}}
            for index, value in enumerate(outcomes, start=1)
        },
    }


def response(markets, slug="pinnacle"):
    return {
        "fixtureId": "fixture-1",
        "sportId": "10",
        "bookmakerOdds": {slug: {"markets": markets}},
    }


def mapped_index(
    *,
    source_market_id="101",
    source_sport_id="10",
    source_handicap=None,
    requires_choice_group=False,
    canonical_market_key="1x2_full_time",
    canonical_market_name="Full-time",
    canonical_market_group="1X2",
    canonical_market_period="Full-time",
    market_family="side",
    outcome_pairs=(("101", "1"), ("102", "X"), ("103", "2")),
):
    mapping_id = 1
    return MarketMappingIndex(
        market_mappings={
            ("oddspapi", source_sport_id, source_market_id): CanonicalMarketResolution(
                resolved=True,
                mapping_id=mapping_id,
                canonical_market_key=canonical_market_key,
                canonical_market_name=canonical_market_name,
                canonical_market_group=canonical_market_group,
                canonical_market_period=canonical_market_period,
                market_family=market_family,
                requires_choice_group=requires_choice_group,
                source_handicap=source_handicap,
                reason="resolved_from_db_mapping",
            )
        },
        outcome_mappings={
            (mapping_id, source_outcome_id): CanonicalOutcomeResolution(
                resolved=True,
                canonical_choice_name=canonical_choice_name,
                display_order=index,
                reason="resolved_from_db_mapping",
            )
            for index, (source_outcome_id, canonical_choice_name) in enumerate(
                outcome_pairs,
                start=1,
            )
        },
    )


def test_moneyline_home_draw_away_and_bookmaker_catalog():
    payload = response(
        {"100": market([player("home", 1.9), player("draw", 3.4), player("away", 4.2)])}
    )
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        bookmaker_catalog=[{"slug": "pinnacle", "bookmakerName": "Pinnacle Sports"}],
        market_mapping_index=mapped_index(
            source_market_id="100",
            outcome_pairs=(("1", "1"), ("2", "X"), ("3", "2")),
        ),
    )
    bookmaker = adapted["bookmakers"][0]
    assert bookmaker["name"] == "Pinnacle Sports"
    assert [choice["name"] for choice in bookmaker["markets"][0]["choices"]] == ["1", "X", "2"]


def test_moneyline_without_draw_is_kept():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response({"100": market([player("home", 1.6), player("away", 2.3)])}),
        market_mapping_index=mapped_index(
            source_market_id="100",
            outcome_pairs=(("1", "1"), ("2", "2")),
        ),
    )
    assert [choice["name"] for choice in adapted["bookmakers"][0]["markets"][0]["choices"]] == [
        "1",
        "2",
    ]


def test_totals_are_grouped_by_line_without_catalog():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response({"200": market([player("62.5/over", 1.909), player("62.5/under", 1.925)])}),
        market_mapping_index=mapped_index(
            source_market_id="200",
            requires_choice_group=True,
            source_handicap="62.5",
            canonical_market_key="total_full_time",
            canonical_market_name="Total",
            canonical_market_group="Over/Under",
            market_family="total",
            outcome_pairs=(("1", "Over"), ("2", "Under")),
        ),
    )
    normalized = adapted["bookmakers"][0]["markets"][0]
    assert normalized["marketGroup"] == "Over/Under"
    assert normalized["choiceGroup"] == "62.5"
    assert [choice["name"] for choice in normalized["choices"]] == ["Over", "Under"]


def test_spreads_are_normalized_without_catalog():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response({"300": market([player("-3.5/home", 2.01), player("-3.5/away", 1.833)])}),
        market_mapping_index=mapped_index(
            source_market_id="300",
            requires_choice_group=True,
            source_handicap="-3.5",
            canonical_market_key="asian_handicap_full_time",
            canonical_market_name="Asian handicap",
            canonical_market_group="Asian handicap",
            market_family="spread",
            outcome_pairs=(("1", "1"), ("2", "2")),
        ),
    )
    normalized = adapted["bookmakers"][0]["markets"][0]
    assert normalized["marketGroup"] == "Asian handicap"
    assert normalized["choiceGroup"] == "-3.5"
    assert [choice["name"] for choice in normalized["choices"]] == ["1", "2"]


def test_mapping_index_resolves_market_and_outcomes():
    payload = response(
        {
            "101": {
                "marketActive": True,
                "outcomes": {
                    "101": {"players": {"0": player("opaque-home", 1.91)}},
                    "102": {"players": {"0": player("opaque-draw", 3.2)}},
                    "103": {"players": {"0": player("opaque-away", 4.4)}},
                },
            }
        }
    )
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=mapped_index(),
    )

    normalized = adapted["bookmakers"][0]["markets"][0]
    assert normalized["marketName"] == "Full-time"
    assert normalized["marketGroup"] == "1X2"
    assert normalized["marketPeriod"] == "Full-time"
    assert [choice["name"] for choice in normalized["choices"]] == ["1", "X", "2"]


def test_mapping_choice_group_comes_only_from_mapping_handicap():
    payload = response(
        {
            "1010": {
                "marketActive": True,
                "outcomes": {
                    "1010": {"players": {"0": player("62.5/over", 1.91)}},
                    "1011": {"players": {"0": player("62.5/under", 1.93)}},
                },
            }
        }
    )
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=mapped_index(
            source_market_id="1010",
            requires_choice_group=True,
            source_handicap="0.5",
            canonical_market_key="total_full_time",
            canonical_market_name="Total",
            canonical_market_group="Over/Under",
            market_family="total",
            outcome_pairs=(("1010", "Over"), ("1011", "Under")),
        ),
    )

    normalized = adapted["bookmakers"][0]["markets"][0]
    assert normalized["choiceGroup"] == "0.5"
    assert [choice["name"] for choice in normalized["choices"]] == ["Over", "Under"]


def test_mapping_mode_does_not_use_bookmaker_outcome_id_to_override_line():
    payload = response(
        {
            "1010": {
                "marketActive": True,
                "outcomes": {
                    "1010": {"players": {"0": player("62.5/over", 1.91)}},
                    "1011": {"players": {"0": player("62.5/under", 1.93)}},
                },
            }
        }
    )
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=mapped_index(
            source_market_id="1010",
            requires_choice_group=True,
            source_handicap="0",
            canonical_market_key="total_full_time",
            canonical_market_name="Total",
            canonical_market_group="Over/Under",
            market_family="total",
            outcome_pairs=(("1010", "Over"), ("1011", "Under")),
        ),
    )

    normalized = adapted["bookmakers"][0]["markets"][0]
    assert normalized["choiceGroup"] == "0"


def test_mapping_mode_skips_unmapped_market():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response({"999": market([player("home", 1.9)])}),
        market_mapping_index=mapped_index(),
    )

    assert adapted["bookmakers"] == []
    assert adapted["diagnostics"]["unmapped_markets"][0]["sourceMarketId"] == "999"


def test_mapping_mode_skips_unmapped_outcome():
    payload = response(
        {
            "101": {
                "marketActive": True,
                "outcomes": {
                    "101": {"players": {"0": player("opaque-home", 1.91)}},
                    "999": {"players": {"0": player("opaque-unknown", 9.99)}},
                },
            }
        }
    )
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=mapped_index(
            outcome_pairs=(("101", "1"),),
        ),
    )

    normalized = adapted["bookmakers"][0]["markets"][0]
    assert [choice["name"] for choice in normalized["choices"]] == ["1"]
    assert adapted["diagnostics"]["unmapped_outcomes"][0]["sourceOutcomeId"] == "999"


def test_mapping_mode_requires_handicap_when_mapping_demands_line():
    payload = response(
        {
            "1010": {
                "marketActive": True,
                "outcomes": {
                    "1010": {"players": {"0": player("opaque-over", 1.91)}},
                    "1011": {"players": {"0": player("opaque-under", 1.93)}},
                },
            }
        }
    )
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=mapped_index(
            source_market_id="1010",
            requires_choice_group=True,
            source_handicap=None,
            canonical_market_key="total_full_time",
            canonical_market_name="Total",
            canonical_market_group="Over/Under",
            market_family="total",
            outcome_pairs=(("1010", "Over"), ("1011", "Under")),
        ),
    )

    assert adapted["bookmakers"] == []
    assert adapted["diagnostics"]["skipped_missing_handicap"][0]["sourceMarketId"] == "1010"


def test_market_mapping_index_is_required():
    try:
        OddspapiMarketAdapter.from_odds_response(
            response({"100": market([player("home", 1.8)])}),
        )
    except ValueError as exc:
        assert "market_mapping_index" in str(exc)
    else:
        raise AssertionError("Expected market mapping index to be required")


def test_inactive_and_missing_price_players_are_ignored_and_choices_are_deduplicated():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response(
            {
                "100": market(
                    [
                        player("home", 1.8),
                        player("home", 1.9),
                        player("draw", 3.0, active=False),
                        player("away", None),
                    ]
                )
            }
        ),
        market_mapping_index=mapped_index(
            source_market_id="100",
            outcome_pairs=(("1", "1"), ("2", "X"), ("3", "2")),
        ),
    )
    choices = adapted["bookmakers"][0]["markets"][0]["choices"]
    assert [(choice["name"], choice["decimalValue"]) for choice in choices] == [("1", 1.8)]


def test_groups_markets_under_each_bookmaker():
    payload = {
        "fixtureId": "fixture-1",
        "sportId": "10",
        "bookmakerOdds": {
            "pinnacle": {"markets": {"1": market([player("home", 1.8)])}},
            "bet365": {"markets": {"2": market([player("away", 2.1)])}},
            "empty": {"markets": {}},
        },
    }
    index = MarketMappingIndex(
        market_mappings={
            ("oddspapi", "10", "1"): CanonicalMarketResolution(
                resolved=True,
                mapping_id=1,
                canonical_market_key="1x2_full_time",
                canonical_market_name="Full-time",
                canonical_market_group="1X2",
                canonical_market_period="Full-time",
                market_family="side",
                requires_choice_group=False,
                source_handicap=None,
                reason="resolved_from_db_mapping",
            ),
            ("oddspapi", "10", "2"): CanonicalMarketResolution(
                resolved=True,
                mapping_id=2,
                canonical_market_key="1x2_full_time",
                canonical_market_name="Full-time",
                canonical_market_group="1X2",
                canonical_market_period="Full-time",
                market_family="side",
                requires_choice_group=False,
                source_handicap=None,
                reason="resolved_from_db_mapping",
            ),
        },
        outcome_mappings={
            (1, "1"): CanonicalOutcomeResolution(resolved=True, canonical_choice_name="1", display_order=1),
            (2, "1"): CanonicalOutcomeResolution(resolved=True, canonical_choice_name="2", display_order=1),
        },
    )
    adapted = OddspapiMarketAdapter.from_odds_response(payload, market_mapping_index=index)
    assert [bookmaker["slug"] for bookmaker in adapted["bookmakers"]] == ["pinnacle", "bet365"]


def test_sportsbook_choice_does_not_include_exchange_quotes():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response({"100": market([player("home", 1.8)])}),
        market_mapping_index=mapped_index(
            source_market_id="100",
            outcome_pairs=(("1", "1"),),
        ),
    )

    choice = adapted["bookmakers"][0]["markets"][0]["choices"][0]
    assert "exchangeQuotes" not in choice

    exchange_without_meta = OddspapiMarketAdapter.from_odds_response(
        response({"100": market([player("home", 1.8)])}, slug="betfair-ex"),
        market_mapping_index=mapped_index(
            source_market_id="100",
            outcome_pairs=(("1", "1"),),
        ),
    )
    exchange_choice = exchange_without_meta["bookmakers"][0]["markets"][0]["choices"][0]
    assert "exchangeQuotes" not in exchange_choice


def test_exchange_choice_normalizes_back_and_lay_ladders():
    exchange_meta = {
        "availableToBack": [
            {"price": 4.8, "size": 64.9},
            {"price": 4.7, "size": 2091.25},
        ],
        "availableToLay": [
            {"price": 5.0, "size": 100.92},
            {"price": 5.1, "size": 103.6},
        ],
        "tradedVolume": 10618.74,
    }
    adapted = OddspapiMarketAdapter.from_odds_response(
        response(
            {"100": market([player("away", 4.8, exchange_meta=exchange_meta)])},
            slug="betfair-ex",
        ),
        market_mapping_index=mapped_index(
            source_market_id="100",
            outcome_pairs=(("1", "2"),),
        ),
    )

    choice = adapted["bookmakers"][0]["markets"][0]["choices"][0]
    assert choice["name"] == "2"
    assert choice["decimalValue"] == 4.8
    assert choice["exchangeQuotes"] == [
        {"side": "back", "level": 0, "price": 4.8, "size": 64.9},
        {"side": "back", "level": 1, "price": 4.7, "size": 2091.25},
        {"side": "lay", "level": 0, "price": 5.0, "size": 100.92},
        {"side": "lay", "level": 1, "price": 5.1, "size": 103.6},
    ]


def test_exchange_ladder_tolerates_missing_sides_and_skips_invalid_prices():
    back_only = OddspapiMarketAdapter.from_odds_response(
        response(
            {
                "100": market(
                    [
                        player(
                            "home",
                            1.8,
                            exchange_meta={
                                "availableToBack": [
                                    {"price": "invalid", "size": 10},
                                    {"price": 1.8, "size": "invalid"},
                                ]
                            },
                        )
                    ]
                )
            },
            slug="betfair-ex",
        ),
        market_mapping_index=mapped_index(
            source_market_id="100",
            outcome_pairs=(("1", "1"),),
        ),
    )
    lay_only = OddspapiMarketAdapter.from_odds_response(
        response(
            {
                "100": market(
                    [
                        player(
                            "away",
                            2.2,
                            exchange_meta={"availableToLay": [{"price": 2.24, "size": 50}]},
                        )
                    ]
                )
            },
            slug="betfair-ex",
        ),
        market_mapping_index=mapped_index(
            source_market_id="100",
            outcome_pairs=(("1", "2"),),
        ),
    )

    back_quotes = back_only["bookmakers"][0]["markets"][0]["choices"][0][
        "exchangeQuotes"
    ]
    lay_quotes = lay_only["bookmakers"][0]["markets"][0]["choices"][0][
        "exchangeQuotes"
    ]
    assert back_quotes == [{"side": "back", "level": 1, "price": 1.8, "size": None}]
    assert lay_quotes == [{"side": "lay", "level": 0, "price": 2.24, "size": 50.0}]


def _load_sofascore_fixture(name: str) -> dict:
    fixture_path = Path(__file__).resolve().parents[2] / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


def test_sofascore_adapter_preserves_all_market_types_from_real_fixture():
    adapted = SofaScoreMarketAdapter.from_event_odds_response(
        _load_sofascore_fixture("61507_odds_response.json"),
        home_team="IFK Mariehamn",
        away_team="HJK",
    )

    markets = adapted["markets"]
    assert len(markets) == 17

    market_names = [market["marketName"] for market in markets]
    assert market_names.count("Full time") == 1
    assert market_names.count("Double chance") == 1
    assert market_names.count("1st half") == 1
    assert market_names.count("Draw no bet") == 1
    assert market_names.count("Both teams to score") == 1
    assert market_names.count("Asian handicap") == 1
    assert market_names.count("Corners 2-Way") == 1
    assert market_names.count("First team to score") == 1
    assert market_names.count("Match goals") == 9

    asian_handicap = next(market for market in markets if market["marketName"] == "Asian handicap")
    assert asian_handicap["choiceGroup"] == "1.5"
    assert [choice["name"] for choice in asian_handicap["choices"]] == ["1", "2"]
    assert asian_handicap["choices"][0]["sourceOutcomeId"] == "1468771326"
    assert asian_handicap["choices"][0]["sourceMarketId"] == "196042959"

    first_team_to_score = next(
        market for market in markets if market["marketName"] == "First team to score"
    )
    assert [choice["name"] for choice in first_team_to_score["choices"]] == [
        "IFK Mariehamn",
        "No goal",
        "HJK",
    ]


def test_sofascore_adapter_accepts_market_dict_containers():
    payload = {
        "markets": {
            "market-a": {
                "marketName": "Special market",
                "marketGroup": "Special market",
                "marketPeriod": "Full-time",
                "choices": [
                    {
                        "name": "(2.5) Team A",
                        "fractionalValue": "2/1",
                        "sourceId": 11,
                    },
                    {
                        "name": "(-2.5) Team B",
                        "fractionalValue": "3/1",
                        "sourceId": 12,
                    },
                ],
            }
        }
    }

    adapted = SofaScoreMarketAdapter.from_event_odds_response(payload)

    assert len(adapted["markets"]) == 1
    market = adapted["markets"][0]
    assert market["marketName"] == "Special market"
    assert market["choiceGroup"] == "2.5"
    assert [choice["name"] for choice in market["choices"]] == ["(2.5) Team A", "(-2.5) Team B"]
    assert market["choices"][0]["sourceOutcomeId"] == "11"


def test_sofascore_adapter_maps_parenthesized_team_names_to_1_and_2():
    payload = {
        "markets": {
            "market-a": {
                "marketName": "Asian handicap",
                "marketGroup": "Asian Handicap",
                "marketPeriod": "Full-time",
                "choices": [
                    {
                        "name": "(1.5) IFK Mariehamn",
                        "fractionalValue": "9/10",
                        "sourceId": 11,
                    },
                    {
                        "name": "(-1.5) HJK",
                        "fractionalValue": "9/10",
                        "sourceId": 12,
                    },
                ],
            }
        }
    }

    adapted = SofaScoreMarketAdapter.from_event_odds_response(
        payload,
        home_team="IFK Mariehamn",
        away_team="HJK",
    )

    market = adapted["markets"][0]
    assert market["choiceGroup"] == "1.5"
    assert [choice["name"] for choice in market["choices"]] == ["1", "2"]
    assert market["choices"][0]["sourceOutcomeId"] == "11"
