"""Pure in-memory pre-selector for OddsPapi /historical-odds payloads.

Discards unmapped and non-mainline markets/outcomes BEFORE passing the payload
to the historical odds reader and change detector, while preserving the raw HTTP
response for debug auditing and keeping downstream adapter validations intact.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from copy import copy
from dataclasses import dataclass, field
import logging
from typing import Any

from infrastructure.persistence.repositories.market_mapping_repository import (
    MarketMappingIndex,
    MarketMappingRepository,
)
from modules.oddspapi.format_utils import normalize_source_id
from modules.oddspapi.mainline_cache_ids import resolve_mainline_outcome_ids
from modules.odds_ingestion.oddspapi_market_filter import (
    OddspapiNormalizedMarketFilter,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HistoricalPayloadSelectionContext:
    """Context parameters required to pre-select historical markets/outcomes."""

    source_sport_id: str | int | None = None
    market_mapping_index: MarketMappingIndex | None = None
    source: str = "oddspapi"
    allowed_market_keys: Sequence[str] | set[str] | None = None
    allowed_market_groups: Sequence[str] | set[str] | None = None
    allowed_market_periods: Sequence[str] | set[str] | None = None
    mainline_outcome_ids_by_bookmaker: Mapping[str, Collection[object]] | None = None
    mainline_fallback_bookmakers: Sequence[str] | None = None
    use_mainline_cache: bool = False
    persist_main_line_only: bool = False


@dataclass
class HistoricalPayloadSelectionResult:
    """Result of historical payload pre-selection."""

    payload: dict
    diagnostics: dict = field(default_factory=dict)
    raw_markets_seen: int = 0
    raw_outcomes_seen: int = 0
    selected_markets: int = 0
    selected_outcomes: int = 0
    skipped_unmapped_markets: int = 0
    skipped_unmapped_outcomes: int = 0
    skipped_non_mainline_markets: int = 0
    bypassed: bool = False
    bypass_reason: str | None = None


class HistoricalPayloadSelector:
    """Filter raw /historical-odds trees using in-memory mappings and mainline cache."""

    @classmethod
    def select(
        cls,
        raw_payload: dict | None,
        context: HistoricalPayloadSelectionContext | None,
    ) -> HistoricalPayloadSelectionResult:
        """Pre-filter raw historical payload without mutating input.

        If context is incomplete (missing mapping index, missing sport id, or
        missing mainline cache when mainline filtering is requested), returns
        the original payload with bypassed=True to prevent accidental data loss.
        """
        payload = raw_payload if isinstance(raw_payload, dict) else {}

        if context is None:
            return HistoricalPayloadSelectionResult(
                payload=payload,
                bypassed=True,
                bypass_reason="missing_context",
            )

        if context.market_mapping_index is None:
            return HistoricalPayloadSelectionResult(
                payload=payload,
                bypassed=True,
                bypass_reason="missing_market_mapping_index",
            )

        sport_id = (
            context.source_sport_id
            if context.source_sport_id is not None
            else payload.get("sportId")
        )
        if sport_id is None:
            return HistoricalPayloadSelectionResult(
                payload=payload,
                bypassed=True,
                bypass_reason="missing_source_sport_id",
            )

        if context.use_mainline_cache and context.persist_main_line_only:
            if context.mainline_outcome_ids_by_bookmaker is None:
                return HistoricalPayloadSelectionResult(
                    payload=payload,
                    bypassed=True,
                    bypass_reason="missing_mainline_cache",
                )
            has_any_cache = any(
                bool(ids)
                for ids in context.mainline_outcome_ids_by_bookmaker.values()
            )
            if not has_any_cache:
                return HistoricalPayloadSelectionResult(
                    payload=payload,
                    bypassed=True,
                    bypass_reason="empty_mainline_cache",
                )

        raw_bookmakers = payload.get("bookmakers")
        if not isinstance(raw_bookmakers, dict):
            return HistoricalPayloadSelectionResult(
                payload=payload,
                diagnostics={"bypassed": False, "reason": "no_bookmakers"},
            )

        allowed_keys = OddspapiNormalizedMarketFilter.normalize_market_key_filters(
            context.allowed_market_keys
        )
        allowed_groups = OddspapiNormalizedMarketFilter.normalize_market_group_filters(
            context.allowed_market_groups
        )
        allowed_periods = OddspapiNormalizedMarketFilter.normalize_market_period_filters(
            context.allowed_market_periods
        )
        fallback_priority = tuple(
            str(slug).strip()
            for slug in (context.mainline_fallback_bookmakers or ())
            if str(slug).strip()
        )

        raw_markets_seen = 0
        raw_outcomes_seen = 0
        selected_markets = 0
        selected_outcomes = 0
        skipped_unmapped_markets = 0
        skipped_unmapped_outcomes = 0
        skipped_non_mainline_markets = 0

        selected_bookmakers: dict[str, dict] = {}
        cache_sources_by_bookmaker: dict[str, str | None] = {}

        for raw_slug, bookmaker_data in raw_bookmakers.items():
            if not isinstance(bookmaker_data, dict):
                continue
            markets = bookmaker_data.get("markets")
            if not isinstance(markets, dict):
                continue

            slug = str(raw_slug).strip().lower()
            cached_mainline_ids: set[str] = set()
            cache_source_slug = None

            if context.use_mainline_cache and context.persist_main_line_only:
                cached_mainline_ids, cache_source_slug = resolve_mainline_outcome_ids(
                    slug,
                    context.mainline_outcome_ids_by_bookmaker,
                    fallback_priority,
                )
                cache_sources_by_bookmaker[slug] = cache_source_slug

            selected_markets_dict: dict[str, dict] = {}

            for source_market_id, market_data in markets.items():
                if not isinstance(market_data, dict):
                    continue
                raw_markets_seen += 1
                raw_outcomes = market_data.get("outcomes")
                if isinstance(raw_outcomes, dict):
                    raw_outcomes_seen += sum(
                        1
                        for outcome_data in raw_outcomes.values()
                        if isinstance(outcome_data, dict)
                    )

                market_resolution = MarketMappingRepository.resolve_market(
                    context.market_mapping_index,
                    source=context.source,
                    source_sport_id=sport_id,
                    source_market_id=source_market_id,
                )
                if not market_resolution.resolved:
                    skipped_unmapped_markets += 1
                    continue

                canonical_key = str(
                    market_resolution.canonical_market_key or ""
                ).strip().lower()
                market_group = str(
                    market_resolution.canonical_market_group or ""
                ).strip()
                market_period = str(
                    market_resolution.canonical_market_period or ""
                ).strip()

                if allowed_keys is not None and canonical_key not in allowed_keys:
                    continue
                if allowed_groups is not None and market_group not in allowed_groups:
                    continue
                if allowed_periods is not None and market_period not in allowed_periods:
                    continue

                if not isinstance(raw_outcomes, dict):
                    continue

                selected_outcomes_dict: dict[str, dict] = {}
                has_mainline = False

                for source_outcome_id, outcome_data in raw_outcomes.items():
                    if not isinstance(outcome_data, dict):
                        continue

                    outcome_resolution = MarketMappingRepository.resolve_outcome(
                        context.market_mapping_index,
                        market_source_mapping_id=market_resolution.mapping_id,
                        source_outcome_id=source_outcome_id,
                    )
                    if not outcome_resolution.resolved:
                        skipped_unmapped_outcomes += 1
                        continue

                    norm_outcome_id = normalize_source_id(source_outcome_id)
                    if norm_outcome_id in cached_mainline_ids:
                        has_mainline = True

                    if outcome_data.get("mainLine") is True:
                        has_mainline = True
                    players = outcome_data.get("players")
                    if isinstance(players, dict):
                        for player in players.values():
                            if isinstance(player, dict) and player.get("mainLine") is True:
                                has_mainline = True
                            elif isinstance(player, list):
                                for tick in player:
                                    if isinstance(tick, dict) and tick.get("mainLine") is True:
                                        has_mainline = True
                                        break

                    selected_outcomes_dict[str(source_outcome_id)] = outcome_data

                if not selected_outcomes_dict:
                    continue

                if context.use_mainline_cache and context.persist_main_line_only:
                    if not has_mainline:
                        skipped_non_mainline_markets += 1
                        continue

                filtered_market = dict(market_data)
                filtered_market["outcomes"] = selected_outcomes_dict
                selected_markets_dict[str(source_market_id)] = filtered_market
                selected_markets += 1
                selected_outcomes += len(selected_outcomes_dict)

            if selected_markets_dict:
                filtered_bookmaker = dict(bookmaker_data)
                filtered_bookmaker["markets"] = selected_markets_dict
                selected_bookmakers[raw_slug] = filtered_bookmaker

        filtered_payload = dict(payload)
        filtered_payload["bookmakers"] = selected_bookmakers

        diagnostics = {
            "raw_markets_seen": raw_markets_seen,
            "raw_outcomes_seen": raw_outcomes_seen,
            "selected_markets": selected_markets,
            "selected_outcomes": selected_outcomes,
            "skipped_unmapped_markets": skipped_unmapped_markets,
            "skipped_unmapped_outcomes": skipped_unmapped_outcomes,
            "skipped_non_mainline_markets": skipped_non_mainline_markets,
            "bypassed": False,
            "bypass_reason": None,
            "cache_sources_by_bookmaker": cache_sources_by_bookmaker,
        }

        return HistoricalPayloadSelectionResult(
            payload=filtered_payload,
            diagnostics=diagnostics,
            raw_markets_seen=raw_markets_seen,
            raw_outcomes_seen=raw_outcomes_seen,
            selected_markets=selected_markets,
            selected_outcomes=selected_outcomes,
            skipped_unmapped_markets=skipped_unmapped_markets,
            skipped_unmapped_outcomes=skipped_unmapped_outcomes,
            skipped_non_mainline_markets=skipped_non_mainline_markets,
            bypassed=False,
            bypass_reason=None,
        )
