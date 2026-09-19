from modules.odds_ingestion.adapters.sofascore_market_adapter import SofaScoreMarketAdapter
from modules.odds_ingestion.canonical_market_normalizer import CanonicalMarketNormalizer
from infrastructure.persistence.repositories.canonical_market_type_repository import (
    CanonicalMarketTypeRepository,
    CanonicalMarketTypeResolution,
)


def test_sofascore_line_value_survives_adapter_and_canonical_normalizer(monkeypatch):
    canonical_type = CanonicalMarketTypeResolution(
        canonical_market_key="over_under_full_time",
        canonical_market_name="Over/Under Full Time",
        canonical_market_group="Over/Under",
        canonical_market_period="Full Time",
        market_family="total",
        requires_line_value=True,
        enabled_for_ingestion=True,
        market_type_id=12,
    )
    monkeypatch.setattr(
        CanonicalMarketTypeRepository,
        "build_index",
        staticmethod(lambda enabled_only=True: {canonical_type.canonical_market_key: canonical_type}),
    )

    adapted = SofaScoreMarketAdapter.from_event_odds_response(
        {
            "eventId": 16317959,
            "markets": [
                {
                    "marketId": 9,
                    "marketName": "Match goals",
                    "marketGroup": "Match goals",
                    "marketPeriod": "Full-time",
                    "choiceGroup": "2.5",
                    "isLive": True,
                    "choices": [
                        {"name": "Over", "sourceId": 1, "decimalValue": 1.9},
                        {"name": "Under", "sourceId": 2, "decimalValue": 1.9},
                    ],
                }
            ],
        }
    )

    assert adapted["markets"][0]["lineValue"] == "2.5"

    normalized = CanonicalMarketNormalizer.normalize_sofascore_response(adapted)

    assert normalized["diagnostics"]["skipped_missing_line_value"] == []
    assert normalized["markets"][0]["lineValue"] == "2.5"
    assert normalized["markets"][0]["canonicalMarketKey"] == "over_under_full_time"
