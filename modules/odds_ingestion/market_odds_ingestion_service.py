"""Central market-odds ingestion service for active runtime jobs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional

from infrastructure.persistence.repositories import (
    BookieRepository,
    DualProcessOddsRepository,
    MarketRepository,
)
from modules.oddspapi import OddspapiEventResolver

from .adapters.oddspapi_market_adapter import OddspapiMarketAdapter
from .adapters.sofascore_market_adapter import SofaScoreMarketAdapter

logger = logging.getLogger(__name__)


@dataclass
class MarketIngestionResult:
    event_id: Optional[int]
    source: str

    markets_detected: int = 0
    choices_detected: int = 0
    snapshots_detected: int = 0

    markets_saved: int = 0
    choices_saved: int = 0
    snapshots_saved: int = 0

    bookies_detected: int = 0
    bookies_processed: int = 0
    bookies_created: int = 0
    bookies_reused: int = 0
    bookie_mappings_created: int = 0
    bookie_mappings_updated: int = 0

    event_mappings_created: int = 0

    dual_process_market_available: bool = False
    skipped: bool = False
    reason: Optional[str] = None

    @property
    def mappings_created(self) -> int:
        return self.event_mappings_created

    @property
    def bookies_saved(self) -> int:
        return self.bookies_processed


class MarketOddsIngestionService:
    @staticmethod
    def save_from_oddspapi_response(
        odds_response: dict,
        market_catalog: dict | list | None = None,
        bookmaker_catalog: dict | list | None = None,
        source: str = "oddspapi_odds",
        dry_run: bool = False,
    ) -> MarketIngestionResult:
        if dry_run:
            resolution = OddspapiEventResolver.resolve_from_odds_response(
                odds_response,
                create_mappings=False,
            )
        else:
            resolution = OddspapiEventResolver.resolve_from_odds_response(odds_response)
        if not resolution.resolved:
            return MarketIngestionResult(
                event_id=None,
                source=source,
                skipped=True,
                reason=resolution.skipped_reason,
            )

        adapted = OddspapiMarketAdapter.from_odds_response(
            odds_response,
            market_catalog=market_catalog,
            bookmaker_catalog=bookmaker_catalog,
        )
        bookmakers = adapted.get("bookmakers", [])
        markets_detected = sum(len(bookmaker.get("markets", [])) for bookmaker in bookmakers)
        choices_detected = sum(
            len(market.get("choices", []))
            for bookmaker in bookmakers
            for market in bookmaker.get("markets", [])
        )
        snapshots_detected = choices_detected
        event_mappings_created = len(resolution.created_mappings)
        bookies_detected = len(bookmakers)

        if not bookmakers or markets_detected == 0:
            return MarketIngestionResult(
                event_id=resolution.canonical_event_id,
                source=source,
                markets_detected=markets_detected,
                choices_detected=choices_detected,
                snapshots_detected=snapshots_detected,
                event_mappings_created=event_mappings_created,
                skipped=True,
                reason="no normalized markets found",
            )

        if dry_run:
            return MarketIngestionResult(
                event_id=resolution.canonical_event_id,
                source=source,
                markets_detected=markets_detected,
                choices_detected=choices_detected,
                snapshots_detected=snapshots_detected,
                bookies_detected=bookies_detected,
                event_mappings_created=event_mappings_created,
            )

        markets_saved = 0
        choices_saved = 0
        snapshots_saved = 0
        bookies_processed = 0
        bookies_created = 0
        bookies_reused = 0
        bookie_mappings_created = 0
        bookie_mappings_updated = 0
        try:
            for bookmaker in bookmakers:
                resolution_result = BookieRepository.resolve_bookie_from_source(
                    source="oddspapi",
                    source_bookie_name=bookmaker.get("name"),
                    source_bookie_slug=bookmaker.get("slug"),
                    allow_create=False,
                )
                if not resolution_result.resolved or resolution_result.bookie is None:
                    logger.warning(
                        "Skipping unresolved OddsPapi bookmaker slug=%s name=%s",
                        bookmaker.get("slug"),
                        bookmaker.get("name"),
                    )
                    continue

                bookies_processed += 1
                if resolution_result.created:
                    bookies_created += 1
                else:
                    bookies_reused += 1
                if resolution_result.mapping_created:
                    bookie_mappings_created += 1
                if resolution_result.mapping_updated:
                    bookie_mappings_updated += 1

                save_result = MarketRepository.save_markets_from_response_with_stats(
                    event_id=resolution.canonical_event_id,
                    odds_response={"markets": bookmaker.get("markets", [])},
                    bookie_id=resolution_result.bookie.bookie_id,
                    source="oddspapi",
                )
                markets_saved += save_result.markets_saved
                choices_saved += save_result.choices_saved
                snapshots_saved += save_result.snapshots_saved

            if bookies_processed == 0:
                dual_process_available = DualProcessOddsRepository.event_has_dual_process_odds(
                    resolution.canonical_event_id
                )
                return MarketIngestionResult(
                    event_id=resolution.canonical_event_id,
                    source=source,
                    markets_detected=markets_detected,
                    choices_detected=choices_detected,
                    snapshots_detected=snapshots_detected,
                    bookies_detected=bookies_detected,
                    event_mappings_created=event_mappings_created,
                    dual_process_market_available=dual_process_available,
                    skipped=True,
                    reason="no resolved canonical bookies",
                )

            dual_process_available = DualProcessOddsRepository.event_has_dual_process_odds(
                resolution.canonical_event_id
            )
            return MarketIngestionResult(
                event_id=resolution.canonical_event_id,
                source=source,
                markets_detected=markets_detected,
                choices_detected=choices_detected,
                snapshots_detected=snapshots_detected,
                markets_saved=markets_saved,
                choices_saved=choices_saved,
                snapshots_saved=snapshots_saved,
                bookies_detected=bookies_detected,
                bookies_processed=bookies_processed,
                bookies_created=bookies_created,
                bookies_reused=bookies_reused,
                bookie_mappings_created=bookie_mappings_created,
                bookie_mappings_updated=bookie_mappings_updated,
                event_mappings_created=event_mappings_created,
                dual_process_market_available=dual_process_available,
                skipped=markets_saved <= 0,
                reason=None if markets_saved > 0 else "no markets saved",
            )
        except Exception as exc:
            logger.error(
                "Error ingesting OddsPapi markets for event %s: %s",
                resolution.canonical_event_id,
                exc,
            )
            return MarketIngestionResult(
                event_id=resolution.canonical_event_id,
                source=source,
                markets_detected=markets_detected,
                choices_detected=choices_detected,
                snapshots_detected=snapshots_detected,
                markets_saved=markets_saved,
                choices_saved=choices_saved,
                snapshots_saved=snapshots_saved,
                bookies_detected=bookies_detected,
                bookies_processed=bookies_processed,
                bookies_created=bookies_created,
                bookies_reused=bookies_reused,
                bookie_mappings_created=bookie_mappings_created,
                bookie_mappings_updated=bookie_mappings_updated,
                event_mappings_created=event_mappings_created,
                skipped=True,
                reason=str(exc),
            )

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
            save_result = MarketRepository.save_markets_from_response_with_stats(
                event_id,
                normalized_response,
                bookie_id=1,
                source=source,
            )
            dual_process_available = DualProcessOddsRepository.event_has_dual_process_odds(event_id)

            result = MarketIngestionResult(
                event_id=event_id,
                source=source,
                markets_saved=save_result.markets_saved,
                choices_saved=save_result.choices_saved,
                snapshots_saved=save_result.snapshots_saved,
                dual_process_market_available=dual_process_available,
                skipped=save_result.markets_saved <= 0,
                reason=None if save_result.markets_saved > 0 else "no markets saved",
            )

            if save_result.markets_saved > 0:
                logger.info("Market odds saved for event %s: %s markets (%s)", event_id, save_result.markets_saved, source)
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
