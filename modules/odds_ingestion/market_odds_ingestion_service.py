"""Central market-odds ingestion service for active runtime jobs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional

from infrastructure.persistence.repositories import DualProcessOddsRepository, MarketRepository

from .adapters.sofascore_market_adapter import SofaScoreMarketAdapter

logger = logging.getLogger(__name__)


@dataclass
class MarketIngestionResult:
    event_id: int
    source: str
    markets_saved: int = 0
    choices_saved: int = 0
    snapshots_saved: int = 0
    dual_process_market_available: bool = False
    skipped: bool = False
    reason: Optional[str] = None


class MarketOddsIngestionService:
    @staticmethod
    def save_from_event_odds_response(
        event_id: int,
        odds_response: Dict,
        source: str = "sofascore_event_odds",
    ) -> MarketIngestionResult:
        normalized_response = SofaScoreMarketAdapter.from_event_odds_response(odds_response)
        return MarketOddsIngestionService._save_normalized(event_id, normalized_response, source)

    @staticmethod
    def save_from_dropping_odds_map_entry(
        event_id: int,
        odds_map_entry: Dict,
        source: str = "sofascore_dropping_odds",
    ) -> MarketIngestionResult:
        normalized_response = SofaScoreMarketAdapter.from_dropping_odds_map_entry(odds_map_entry)
        return MarketOddsIngestionService._save_normalized(event_id, normalized_response, source)

    @staticmethod
    def save_from_daily_odds_entry(
        event_id: int,
        daily_odds_entry: Dict,
        source: str = "sofascore_daily_discovery",
    ) -> MarketIngestionResult:
        normalized_response = SofaScoreMarketAdapter.from_daily_odds_entry(daily_odds_entry)
        return MarketOddsIngestionService._save_normalized(event_id, normalized_response, source)

    @staticmethod
    def _save_normalized(event_id: int, normalized_response: Dict, source: str) -> MarketIngestionResult:
        if not normalized_response or not normalized_response.get("markets"):
            reason = "no normalized markets found"
            logger.info("Skipped market odds ingestion for event %s: %s", event_id, reason)
            return MarketIngestionResult(event_id=event_id, source=source, skipped=True, reason=reason)

        try:
            logger.info(f"\nsource: {source}")
            markets_saved = MarketRepository.save_markets_from_response(event_id, normalized_response, bookie_id=1)
            dual_process_available = DualProcessOddsRepository.event_has_dual_process_odds(event_id)

            result = MarketIngestionResult(
                event_id=event_id,
                source=source,
                markets_saved=markets_saved,
                dual_process_market_available=dual_process_available,
                skipped=markets_saved <= 0,
                reason=None if markets_saved > 0 else "no markets saved",
            )

            if markets_saved > 0:
                logger.info("Market odds saved for event %s: %s markets (%s)", event_id, markets_saved, source)
                if not dual_process_available:
                    logger.warning(
                        "Market odds saved for event %s, but no dual-process-compatible market found. "
                        "Check Config.MARKETS_DUAL_PROCESS / PERIODS_DUAL_PROCESS and saved market metadata.",
                        event_id,
                    )
            else:
                logger.info("Skipped market odds ingestion for event %s: no markets saved", event_id)

            return result
        except Exception as exc:
            logger.error("Error ingesting market odds for event %s (%s): %s", event_id, source, exc)
            return MarketIngestionResult(
                event_id=event_id,
                source=source,
                skipped=True,
                reason=str(exc),
            )
