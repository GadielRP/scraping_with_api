"""Central market-odds ingestion service for active runtime jobs."""

from __future__ import annotations

import logging
import pprint
from dataclasses import dataclass
from typing import Dict, Optional

from infrastructure.persistence.repositories import (
    BookieRepository,
    DualProcessOddsRepository,
    MarketMappingRepository,
    MarketRepository,
)
from infrastructure.persistence.repositories.market_mapping_repository import MarketMappingIndex
from modules.oddspapi import OddspapiEventResolver

from .adapters.oddspapi_market_adapter import OddspapiMarketAdapter
from .adapters.sofascore_market_adapter import SofaScoreMarketAdapter
from .canonical_market_normalizer import CanonicalMarketNormalizer

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

    unmapped_markets_detected: int = 0
    unmapped_outcomes_detected: int = 0
    skipped_missing_handicap_detected: int = 0
    skipped_missing_choice_group_detected: int = 0

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
    def _normalize_source(source: str, default: str) -> str:
        return str(source or default).strip().lower()

    @staticmethod
    def filter_normalized_oddspapi_response_by_groups_and_periods(
        normalized_response: Dict,
        allowed_market_groups: Optional[list[str] | set[str] | tuple[str, ...]] = None,
        allowed_market_periods: Optional[list[str] | set[str] | tuple[str, ...]] = None,
    ) -> Dict:
        if not normalized_response or not normalized_response.get("bookmakers"):
            return normalized_response

        normalized_groups = MarketOddsIngestionService._normalize_market_group_filters(allowed_market_groups)
        normalized_periods = MarketOddsIngestionService._normalize_market_period_filters(allowed_market_periods)

        if normalized_groups is None and normalized_periods is None:
            return normalized_response

        filtered_bookmakers = []
        for bookmaker in normalized_response.get("bookmakers", []):
            filtered_markets = []
            for market in bookmaker.get("markets", []):
                market_group = str(market.get("marketGroup") or "").strip()
                market_period = str(market.get("marketPeriod") or "").strip()

                if normalized_groups is not None and market_group not in normalized_groups:
                    continue
                if normalized_periods is not None and market_period not in normalized_periods:
                    continue
                filtered_markets.append(market)

            if filtered_markets:
                filtered_bookmaker = dict(bookmaker)
                filtered_bookmaker["markets"] = filtered_markets
                filtered_bookmakers.append(filtered_bookmaker)

        filtered = {
            "fixtureId": normalized_response.get("fixtureId"),
            "bookmakers": filtered_bookmakers,
        }
        if "diagnostics" in normalized_response:
            filtered["diagnostics"] = normalized_response.get("diagnostics")
        return filtered

    @staticmethod
    def filter_normalized_oddspapi_response(
        normalized_response: Dict,
        allowed_market_groups: Optional[list[str] | set[str] | tuple[str, ...]] = None,
        allowed_market_periods: Optional[list[str] | set[str] | tuple[str, ...]] = None,
    ) -> Dict:
        return MarketOddsIngestionService.filter_normalized_oddspapi_response_by_groups_and_periods(
            normalized_response,
            allowed_market_groups=allowed_market_groups,
            allowed_market_periods=allowed_market_periods,
        )

    @staticmethod
    def _normalize_market_group_filters(
        allowed_market_groups: Optional[list[str] | set[str] | tuple[str, ...]],
    ) -> Optional[set[str]]:
        if not allowed_market_groups:
            return None

        normalized: set[str] = set()
        for item in allowed_market_groups:
            if item is None:
                continue
            text = str(item).strip()
            if not text:
                continue
            lowered = text.lower()
            if lowered == "1x2":
                normalized.add("1X2")
            elif lowered in {"home/away", "ml", "moneyline"}:
                normalized.add("Home/Away")
            elif lowered in {"over/under", "total", "totals"}:
                normalized.add("Over/Under")
            elif lowered in {"asian handicap", "ah", "spread"}:
                normalized.add("Asian handicap")
            else:
                normalized.add(text)
        return normalized or None

    @staticmethod
    def _normalize_market_period_filters(
        allowed_market_periods: Optional[list[str] | set[str] | tuple[str, ...]],
    ) -> Optional[set[str]]:
        if not allowed_market_periods:
            return None

        normalized: set[str] = set()
        for item in allowed_market_periods:
            if item is None:
                continue
            text = str(item).strip()
            if not text:
                continue
            lowered = text.lower()
            if lowered in {"match", "ft", "full time", "fulltime"}:
                normalized.add("Full Time")
            else:
                normalized.add(text)
        return normalized or None

    @staticmethod
    def save_from_oddspapi_response(
        odds_response: dict,
        bookmaker_catalog: dict | list | None = None,
        source: str = "oddspapi_odds",
        dry_run: bool = False,
        allowed_market_groups: Optional[list[str] | set[str] | tuple[str, ...]] = None,
        allowed_market_periods: Optional[list[str] | set[str] | tuple[str, ...]] = None,
        market_mapping_index: MarketMappingIndex | None = None,
    ) -> MarketIngestionResult:
        source = MarketOddsIngestionService._normalize_source(source, "oddspapi_odds")
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

        if market_mapping_index is None:
            market_mapping_index = MarketMappingRepository.build_index(
                source="oddspapi",
                enabled_only=True,
            )
        if not market_mapping_index.market_mappings:
            return MarketIngestionResult(
                event_id=resolution.canonical_event_id,
                source=source,
                skipped=True,
                reason="market_mapping_index_unavailable",
                event_mappings_created=len(resolution.created_mappings),
            )

        adapted = OddspapiMarketAdapter.from_odds_response(
            odds_response,
            bookmaker_catalog=bookmaker_catalog,
            market_mapping_index=market_mapping_index,
            source="oddspapi",
        )
        adapted = MarketOddsIngestionService.filter_normalized_oddspapi_response_by_groups_and_periods(
            adapted,
            allowed_market_groups=allowed_market_groups,
            allowed_market_periods=allowed_market_periods,
        )
        diagnostics = adapted.get("diagnostics") or {}
        unmapped_markets_detected = len(diagnostics.get("unmapped_markets") or [])
        unmapped_outcomes_detected = len(diagnostics.get("unmapped_outcomes") or [])
        skipped_missing_handicap_detected = len(
            diagnostics.get("skipped_missing_handicap") or []
        )
        if diagnostics:
            logger.info("OddsPapi market mapping diagnostics: %s", diagnostics)
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
                unmapped_markets_detected=unmapped_markets_detected,
                unmapped_outcomes_detected=unmapped_outcomes_detected,
                skipped_missing_handicap_detected=skipped_missing_handicap_detected,
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
                unmapped_markets_detected=unmapped_markets_detected,
                unmapped_outcomes_detected=unmapped_outcomes_detected,
                skipped_missing_handicap_detected=skipped_missing_handicap_detected,
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
                    unmapped_markets_detected=unmapped_markets_detected,
                    unmapped_outcomes_detected=unmapped_outcomes_detected,
                    skipped_missing_handicap_detected=skipped_missing_handicap_detected,
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
                unmapped_markets_detected=unmapped_markets_detected,
                unmapped_outcomes_detected=unmapped_outcomes_detected,
                skipped_missing_handicap_detected=skipped_missing_handicap_detected,
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
                unmapped_markets_detected=unmapped_markets_detected,
                unmapped_outcomes_detected=unmapped_outcomes_detected,
                skipped_missing_handicap_detected=skipped_missing_handicap_detected,
                skipped=True,
                reason=str(exc),
            )

    @staticmethod
    def save_from_event_odds_response(
        event_id: int,
        odds_response: Dict,
        source: str = "sofascore",
        home_team: str | None = None,
        away_team: str | None = None,
        debug_mode: bool = False,
    ) -> MarketIngestionResult:
        source = MarketOddsIngestionService._normalize_source(source, "sofascore")
        adapted_response = SofaScoreMarketAdapter.from_event_odds_response(
            odds_response,
            home_team=home_team,
            away_team=away_team,
        )
        if debug_mode:
            logger.info(
                "CanonicalMarketNormalizer input for event %s (source=%s):\n%s",
                event_id,
                source,
                pprint.pformat(adapted_response, indent=2, width=120, sort_dicts=False),
            )
        canonical_response = CanonicalMarketNormalizer.normalize_sofascore_response(
            adapted_response,
            home_team=home_team,
            away_team=away_team,
        )
        return MarketOddsIngestionService._save_normalized(event_id, canonical_response, source)

    @staticmethod
    def save_from_dropping_odds_map_entry(
        event_id: int,
        odds_map_entry: Dict,
        source: str = "sofascore_dropping_odds",
    ) -> MarketIngestionResult:
        source = MarketOddsIngestionService._normalize_source(source, "sofascore_dropping_odds")
        adapted_response = SofaScoreMarketAdapter.from_dropping_odds_map_entry(odds_map_entry)
        canonical_response = CanonicalMarketNormalizer.normalize_sofascore_response(adapted_response)
        return MarketOddsIngestionService._save_normalized(event_id, canonical_response, source)

    @staticmethod
    def save_from_daily_odds_entry(
        event_id: int,
        daily_odds_entry: Dict,
        source: str = "sofascore_daily_discovery",
    ) -> MarketIngestionResult:
        source = MarketOddsIngestionService._normalize_source(source, "sofascore_daily_discovery")
        adapted_response = SofaScoreMarketAdapter.from_daily_odds_entry(daily_odds_entry)
        canonical_response = CanonicalMarketNormalizer.normalize_sofascore_response(adapted_response)
        return MarketOddsIngestionService._save_normalized(event_id, canonical_response, source)

    @staticmethod
    def _save_normalized(event_id: int, normalized_response: Dict, source: str) -> MarketIngestionResult:
        source = MarketOddsIngestionService._normalize_source(source, "unknown")
        diagnostics = (normalized_response or {}).get("diagnostics") or {}
        unmapped_markets = len(diagnostics.get("unmapped_markets") or [])
        unmapped_choices = len(diagnostics.get("unmapped_choices") or [])
        skipped_missing_choice_group = len(diagnostics.get("skipped_missing_choice_group") or [])
        markets = (normalized_response or {}).get("markets") or []
        choices_detected = sum(len(market.get("choices") or []) for market in markets)
        if not normalized_response or not normalized_response.get("markets"):
            reason = "no normalized markets found"
            logger.info("Skipped market odds ingestion for event %s: %s", event_id, reason)
            return MarketIngestionResult(
                event_id=event_id,
                source=source,
                unmapped_markets_detected=unmapped_markets,
                unmapped_outcomes_detected=unmapped_choices,
                skipped_missing_choice_group_detected=skipped_missing_choice_group,
                skipped=True,
                reason=reason,
            )

        try:
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
                markets_detected=len(markets),
                choices_detected=choices_detected,
                snapshots_detected=choices_detected,
                markets_saved=save_result.markets_saved,
                choices_saved=save_result.choices_saved,
                snapshots_saved=save_result.snapshots_saved,
                unmapped_markets_detected=unmapped_markets,
                unmapped_outcomes_detected=unmapped_choices,
                skipped_missing_choice_group_detected=skipped_missing_choice_group,
                dual_process_market_available=dual_process_available,
                skipped=save_result.markets_saved <= 0,
                reason=None if save_result.markets_saved > 0 else "no markets saved",
            )

            if save_result.markets_saved > 0:
                logger.info(
                    "Market odds saved for event %s: %s markets (source=%s)",
                    event_id,
                    save_result.markets_saved,
                    source,
                )
                if not dual_process_available:
                    logger.warning(
                        "Market odds saved for event %s, but no dual-process-compatible market found. "
                        "Check Config.MARKETS_DUAL_PROCESS / PERIODS_DUAL_PROCESS and saved market metadata.",
                        event_id,
                    )
            else:
                logger.info(
                    "Skipped market odds ingestion for event %s: no markets saved (source=%s)",
                    event_id,
                    source,
                )

            return result
        except Exception as exc:
            logger.error("Error ingesting market odds for event %s (%s): %s", event_id, source, exc)
            return MarketIngestionResult(
                event_id=event_id,
                source=source,
                skipped=True,
                reason=str(exc),
            )
