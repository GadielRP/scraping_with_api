import json
from pathlib import Path

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
        "bookmakerOdds": {slug: {"markets": markets}},
    }


def test_moneyline_home_draw_away_and_bookmaker_catalog():
    payload = response(
        {"100": market([player("home", 1.9), player("draw", 3.4), player("away", 4.2)])}
    )
    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        bookmaker_catalog=[{"slug": "pinnacle", "bookmakerName": "Pinnacle Sports"}],
    )
    bookmaker = adapted["bookmakers"][0]
    assert bookmaker["name"] == "Pinnacle Sports"
    assert [choice["name"] for choice in bookmaker["markets"][0]["choices"]] == ["1", "X", "2"]


def test_moneyline_without_draw_is_kept():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response({"100": market([player("home", 1.6), player("away", 2.3)])})
    )
    assert [choice["name"] for choice in adapted["bookmakers"][0]["markets"][0]["choices"]] == [
        "1",
        "2",
    ]


def test_totals_are_grouped_by_line_without_catalog():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response({"200": market([player("62.5/over", 1.909), player("62.5/under", 1.925)])})
    )
    normalized = adapted["bookmakers"][0]["markets"][0]
    assert normalized["marketGroup"] == "Over/Under"
    assert normalized["choiceGroup"] == "62.5"
    assert [choice["name"] for choice in normalized["choices"]] == ["Over", "Under"]


def test_spreads_are_normalized_without_catalog():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response({"300": market([player("-3.5/home", 2.01), player("-3.5/away", 1.833)])})
    )
    normalized = adapted["bookmakers"][0]["markets"][0]
    assert normalized["marketGroup"] == "Asian handicap"
    assert normalized["choiceGroup"] == "-3.5"
    assert [choice["name"] for choice in normalized["choices"]] == ["1", "2"]


def test_catalog_maps_outcome_ids_and_period():
    payload = response(
        {"101": market([player("opaque-home", 1.5555), player("opaque-away", 2.4)])}
    )
    catalog = [
        {
            "marketId": 101,
            "marketName": "Full Time Result",
            "marketType": "moneyline",
            "period": "fulltime",
            "handicap": 0,
            "outcomes": [
                {"outcomeId": 1, "outcomeName": "Home"},
                {"outcomeId": 2, "outcomeName": "Away"},
            ],
        }
    ]
    normalized = OddspapiMarketAdapter.from_odds_response(payload, market_catalog=catalog)[
        "bookmakers"
    ][0]["markets"][0]
    assert normalized["marketPeriod"] == "Full-time"
    assert [choice["decimalValue"] for choice in normalized["choices"]] == [1.556, 2.4]


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
        )
    )
    choices = adapted["bookmakers"][0]["markets"][0]["choices"]
    assert [(choice["name"], choice["decimalValue"]) for choice in choices] == [("1", 1.8)]


def test_groups_markets_under_each_bookmaker():
    payload = {
        "fixtureId": "fixture-1",
        "bookmakerOdds": {
            "pinnacle": {"markets": {"1": market([player("home", 1.8)])}},
            "bet365": {"markets": {"2": market([player("away", 2.1)])}},
            "empty": {"markets": {}},
        },
    }
    adapted = OddspapiMarketAdapter.from_odds_response(payload)
    assert [bookmaker["slug"] for bookmaker in adapted["bookmakers"]] == ["pinnacle", "bet365"]


def test_sportsbook_choice_does_not_include_exchange_quotes():
    adapted = OddspapiMarketAdapter.from_odds_response(
        response({"100": market([player("home", 1.8)])})
    )

    choice = adapted["bookmakers"][0]["markets"][0]["choices"][0]
    assert "exchangeQuotes" not in choice

    exchange_without_meta = OddspapiMarketAdapter.from_odds_response(
        response({"100": market([player("home", 1.8)])}, slug="betfair-ex")
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
        )
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
        )
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
        )
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
