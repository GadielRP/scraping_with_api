from modules.odds_ingestion.adapters.oddspapi_market_adapter import OddspapiMarketAdapter


def player(bookmaker_outcome_id, price, active=True):
    return {
        "active": active,
        "bookmakerOutcomeId": bookmaker_outcome_id,
        "price": price,
        "changedAt": "2026-06-19T00:00:00Z",
        "mainLine": True,
        "limit": 100,
    }


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
