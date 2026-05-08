"""Small verification for SofaScore market adapter shapes."""

from __future__ import annotations

from modules.odds_ingestion import SofaScoreMarketAdapter


def main() -> int:
    missing_name = SofaScoreMarketAdapter.from_daily_odds_entry(
        {
            "choices": [
                {"name": "1", "initialFractionalValue": "1/2", "fractionalValue": "4/9"},
                {"name": "2", "initialFractionalValue": "4/1", "fractionalValue": "9/2"},
            ]
        }
    )
    assert missing_name == {"markets": []}

    three_way = SofaScoreMarketAdapter.from_daily_odds_entry(
        {
            "marketName": "Full time",
            "marketPeriod": "Full Time",
            "choices": [
                {"name": "1", "initialFractionalValue": "1/2", "fractionalValue": "4/9"},
                {"name": "X", "initialFractionalValue": "2/1", "fractionalValue": "21/10"},
                {"name": "2", "initialFractionalValue": "4/1", "fractionalValue": "9/2"},
            ]
        }
    )
    market = three_way["markets"][0]
    assert market["marketName"] == "Full time"
    assert "marketGroup" not in market
    assert market["marketPeriod"] == "Full-time"
    assert [choice["name"] for choice in market["choices"]] == ["1", "X", "2"]

    two_way = SofaScoreMarketAdapter.from_dropping_odds_map_entry(
        {
            "odds": {
                "marketName": "Home/Away",
                "marketGroup": "",
                "choices": [
                    {"name": "1", "initialFractionalValue": "1/2", "fractionalValue": "4/9"},
                    {"name": "2", "initialFractionalValue": "4/1", "fractionalValue": "9/2"},
                ]
            }
        }
    )
    market = two_way["markets"][0]
    assert market["marketName"] == "Home/Away"
    assert "marketGroup" not in market
    assert "marketPeriod" not in market
    assert [choice["name"] for choice in market["choices"]] == ["1", "2"]

    print("Market ingestion adapter verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
