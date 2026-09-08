"""Tests for the shared normalized OddsPapi market-filter component."""

from modules.odds_ingestion.oddspapi_market_filter import (
    OddspapiNormalizedMarketFilter,
)


def test_filter_response_applies_market_aliases_without_mutating_input():
    response = {
        "fixtureId": "fixture-1",
        "bookmakers": [
            {
                "slug": "pinnacle",
                "name": "Pinnacle Sports",
                "markets": [
                    {
                        "canonicalMarketKey": "1x2_full_time",
                        "marketGroup": "1X2",
                        "marketPeriod": "Full Time",
                    },
                    {
                        "canonicalMarketKey": "home_away_full_time",
                        "marketGroup": "Home/Away",
                        "marketPeriod": "Full Time",
                    },
                ],
            }
        ],
    }

    filtered = OddspapiNormalizedMarketFilter.filter_response(
        response,
        allowed_market_keys={"home_away_full_time"},
        allowed_market_groups={"ml"},
        allowed_market_periods={"Match"},
    )

    assert [bookmaker["slug"] for bookmaker in filtered["bookmakers"]] == ["pinnacle"]
    assert [market["marketGroup"] for market in filtered["bookmakers"][0]["markets"]] == ["Home/Away"]
    assert [market["marketPeriod"] for market in filtered["bookmakers"][0]["markets"]] == ["Full Time"]
    assert len(response["bookmakers"][0]["markets"]) == 2


def test_filter_response_accepts_canonical_market_key_case_insensitively():
    response = {
        "fixtureId": "fixture-1",
        "bookmakers": [
            {
                "slug": "pinnacle",
                "markets": [
                    {"canonicalMarketKey": "1x2_full_time"},
                    {"canonicalMarketKey": "over_under_full_time"},
                ],
            }
        ],
    }

    filtered = OddspapiNormalizedMarketFilter.filter_response(
        response,
        allowed_market_keys={"OVER_UNDER_FULL_TIME"},
    )

    assert [
        market["canonicalMarketKey"]
        for market in filtered["bookmakers"][0]["markets"]
    ] == ["over_under_full_time"]
