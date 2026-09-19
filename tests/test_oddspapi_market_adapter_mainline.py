"""Adapter coverage for mainLine cache annotation and filtering."""

from __future__ import annotations

from types import SimpleNamespace

from modules.odds_ingestion.adapters.oddspapi_market_adapter import OddspapiMarketAdapter


class _MarketResolution(SimpleNamespace):
    pass


class _OutcomeResolution(SimpleNamespace):
    pass


def _index():
    return SimpleNamespace(
        market_mappings={},
        outcome_mappings={},
    )


def test_adapter_applies_cached_mainline_and_filters_non_mainline(monkeypatch):
    index = _index()

    def resolve_market(_index, **_kwargs):
        return _MarketResolution(
            resolved=True,
            reason=None,
            mapping_id=1,
            requires_line_value=True,
            source_handicap=2.5,
            canonical_market_key="over_under_full_time",
            canonical_market_name="Over/Under",
            canonical_market_group="Totals",
            canonical_market_period="Full Time",
        )

    def resolve_outcome(_index, **kwargs):
        outcome_id = str(kwargs["source_outcome_id"])
        return _OutcomeResolution(
            resolved=True,
            reason=None,
            canonical_choice_name="Over" if outcome_id == "11" else "Under",
        )

    monkeypatch.setattr(
        "modules.odds_ingestion.adapters.oddspapi_market_adapter."
        "MarketMappingRepository.resolve_market",
        resolve_market,
    )
    monkeypatch.setattr(
        "modules.odds_ingestion.adapters.oddspapi_market_adapter."
        "MarketMappingRepository.resolve_outcome",
        resolve_outcome,
    )
    monkeypatch.setattr(
        OddspapiMarketAdapter,
        "_expected_choice_names",
        staticmethod(lambda *_args, **_kwargs: {"Over", "Under"}),
    )

    payload = {
        "fixtureId": "fx-1",
        "sportId": "10",
        "bookmakerOdds": {
            "pinnacle": {
                "slug": "pinnacle",
                "markets": {
                    "101": {
                        "marketActive": True,
                        "isLive": False,
                        "outcomes": {
                            "11": {"players": {"0": {"price": 1.9, "active": True}}},
                            "12": {"players": {"0": {"price": 1.95, "active": True}}},
                        },
                    },
                    "102": {
                        "marketActive": True,
                        "isLive": False,
                        "outcomes": {
                            "21": {
                                "players": {
                                    "0": {
                                        "price": 1.8,
                                        "active": True,
                                        "mainLine": False,
                                    }
                                }
                            },
                            "22": {
                                "players": {
                                    "0": {
                                        "price": 2.0,
                                        "active": True,
                                        "mainLine": False,
                                    }
                                }
                            },
                        },
                    },
                },
            }
        },
    }

    adapted = OddspapiMarketAdapter.from_odds_response(
        payload,
        market_mapping_index=index,
        mainline_outcome_ids={"11", "12"},
        persist_main_line_only=True,
    )

    markets = adapted["bookmakers"][0]["markets"]
    assert len(markets) == 1
    choices = {choice["name"]: choice["mainLine"] for choice in markets[0]["choices"]}
    assert choices == {"Over": True, "Under": True}
