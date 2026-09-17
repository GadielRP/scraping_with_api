from __future__ import annotations

import logging

from infrastructure.persistence.repositories.market_repository import MarketSaveResult
from modules.odds_ingestion import market_odds_ingestion_service as service_module
from modules.odds_ingestion.market_odds_ingestion_service import MarketOddsIngestionService


def test_save_from_dropping_odds_map_entry_persists_passed_source(monkeypatch, caplog):
    captured = {}

    monkeypatch.setattr(
        service_module.SofaScoreMarketAdapter,
        "from_dropping_odds_map_entry",
        staticmethod(lambda odds_map_entry: {"markets": [{"marketName": "1X2", "choices": [{}, {}]}]}),
    )
    monkeypatch.setattr(
        service_module.CanonicalMarketNormalizer,
        "normalize_sofascore_response",
        staticmethod(lambda adapted_response: adapted_response),
    )

    def fake_save_canonical_bookmaker_batches(event_id, bookmaker_batches, *, source=None):
        batch = bookmaker_batches[0] if bookmaker_batches else {}
        captured["event_id"] = event_id
        captured["bookie_id"] = batch.get("bookie_id")
        captured["source"] = source
        captured["odds_response"] = {"markets": batch.get("markets", [])}
        return MarketSaveResult(markets_saved=1, choices_saved=2, snapshots_saved=2)

    monkeypatch.setattr(
        service_module.MarketRepository,
        "save_canonical_bookmaker_batches",
        staticmethod(fake_save_canonical_bookmaker_batches),
    )
    monkeypatch.setattr(
        service_module.DualProcessOddsRepository,
        "event_has_dual_process_odds",
        staticmethod(lambda event_id: False),
    )

    with caplog.at_level(logging.INFO, logger="modules.odds_ingestion.market_odds_ingestion_service"):
        result = MarketOddsIngestionService.save_from_dropping_odds_map_entry(
            115707,
            {"eventId": 115707},
            source="sofascore",
        )

    assert result.event_id == 115707
    assert result.source == "sofascore"
    assert captured["event_id"] == 115707
    assert captured["bookie_id"] == 1
    assert captured["source"] == "sofascore"
    assert (
        "Saved 1 markets, 2 choices and 2 snapshots for event 115707 (source=sofascore)"
        in caplog.text
    )
