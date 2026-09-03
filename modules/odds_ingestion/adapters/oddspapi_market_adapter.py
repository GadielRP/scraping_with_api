"""Normalize OddsPapi bookmaker odds into the repository market contract."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import Any, Iterable

from infrastructure.persistence.repositories.market_mapping_repository import (
    MarketMappingIndex,
    MarketMappingRepository,
)
from modules.oddspapi.exchange_quotes import best_exchange_quotes
from modules.oddspapi.format_utils import format_line, normalize_source_id
from modules.oddspapi.mainline_cache_ids import resolve_mainline_outcome_ids
from modules.oddspapi.quote_activity import should_skip_inactive_market
from modules.odds_ingestion.oddspapi_line_selection import (
    LineSelection,
    select_current_lines,
)


class OddspapiMarketAdapter:
    _EXCHANGE_BOOKMAKER_SLUGS = {"betfair-ex"}

    @staticmethod
    def _entries(value: Any) -> Iterable[tuple[str, dict]]:
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, dict):
                    yield str(key), item
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if not isinstance(item, dict):
                    continue
                key = (
                    item.get("slug")
                    or item.get("marketId")
                    or item.get("outcomeId")
                    or index
                )
                yield str(key), item

    @staticmethod
    def _catalog_items(catalog: Any, wrapper_key: str) -> list[dict]:
        if isinstance(catalog, dict) and isinstance(catalog.get(wrapper_key), list):
            return [item for item in catalog[wrapper_key] if isinstance(item, dict)]
        if isinstance(catalog, list):
            return [item for item in catalog if isinstance(item, dict)]
        if isinstance(catalog, dict):
            result = []
            for key, item in catalog.items():
                if not isinstance(item, dict):
                    continue
                normalized = dict(item)
                normalized.setdefault("slug", key)
                result.append(normalized)
            return result
        return []

    @staticmethod
    def _format_line(value: Any) -> str | None:
        return format_line(value)

    @staticmethod
    def _bookmaker_name(slug: str, bookmaker_index: dict[str, dict]) -> str:
        catalog_entry = bookmaker_index.get(slug)
        if catalog_entry:
            name = str(catalog_entry.get("bookmakerName") or "").strip()
            if name:
                return name
        return slug.replace("-", " ").replace("_", " ").title()

    @staticmethod
    def _is_exchange_bookmaker(slug: str, exchange_meta: Any) -> bool:
        return isinstance(exchange_meta, dict) or slug in OddspapiMarketAdapter._EXCHANGE_BOOKMAKER_SLUGS

    @staticmethod
    def _append_diagnostic(diagnostics: dict, key: str, payload: dict) -> None:
        diagnostics.setdefault(key, []).append(payload)

    @staticmethod
    def _expected_choice_names(
        market_mapping_index: MarketMappingIndex,
        mapping_id: int | None,
    ) -> set[str]:
        if mapping_id is None:
            return set()

        expected_choice_names = set()
        for outcome_key, resolution in market_mapping_index.outcome_mappings.items():
            candidate_mapping_id = outcome_key[0]
            if candidate_mapping_id != mapping_id or not resolution.resolved:
                continue
            if resolution.canonical_choice_name in (None, ""):
                continue
            expected_choice_names.add(str(resolution.canonical_choice_name))
        return expected_choice_names

    @staticmethod
    def from_odds_response(
        odds_response: dict,
        bookmaker_catalog: dict | list | None = None,
        market_mapping_index: MarketMappingIndex | None = None,
        source: str = "oddspapi",
        mainline_outcome_ids: set[str] | None = None,
        mainline_outcome_ids_by_bookmaker: Mapping[str, Collection[str]] | None = None,
        mainline_fallback_bookmakers: Sequence[str] | None = None,
        use_mainline_cache: bool = False,
        persist_main_line_only: bool = False,
        require_active_quotes: bool = True,
    ) -> dict:
        if market_mapping_index is None:
            raise ValueError("market_mapping_index is required")

        payload = odds_response if isinstance(odds_response, dict) else {}
        fixture_id = payload.get("fixtureId")
        source_sport_id = payload.get("sportId")
        global_mainline_ids = {
            str(outcome_id).strip()
            for outcome_id in (mainline_outcome_ids or set())
            if str(outcome_id).strip()
        }
        fallback_priority = tuple(
            str(slug).strip()
            for slug in (mainline_fallback_bookmakers or ())
            if str(slug).strip()
        )
        diagnostics = {
            "unmapped_markets": [],
            "unmapped_outcomes": [],
            "skipped_missing_handicap": [],
            "skipped_incomplete_markets": [],
            "skipped_missing_mainline_cache": [],
            "mainline_cache_fallbacks_used": [],
        }

        bookmaker_items = OddspapiMarketAdapter._catalog_items(bookmaker_catalog, "bookmakers")
        bookmaker_index = {
            str(item.get("slug") or "").strip().lower(): item
            for item in bookmaker_items
            if str(item.get("slug") or "").strip()
        }

        normalized_bookmakers = []
        for bookmaker_key, bookmaker_data in OddspapiMarketAdapter._entries(
            payload.get("bookmakerOdds", {})
        ):
            slug = str(bookmaker_data.get("slug") or bookmaker_key).strip().lower()
            markets_data = bookmaker_data.get("markets")
            if not slug or not markets_data:
                continue

            cached_mainline_ids = set(global_mainline_ids)
            cache_source_slug = None
            if use_mainline_cache and mainline_outcome_ids_by_bookmaker is not None:
                cached_mainline_ids, cache_source_slug = resolve_mainline_outcome_ids(
                    slug,
                    mainline_outcome_ids_by_bookmaker,
                    fallback_priority,
                )
                if not cached_mainline_ids:
                    OddspapiMarketAdapter._append_diagnostic(
                        diagnostics,
                        "skipped_missing_mainline_cache",
                        {
                            "bookmakerSlug": slug,
                            "fallbackPriority": list(fallback_priority),
                            "reason": "no_mainline_cache_or_fallback",
                        },
                    )
                    continue
                if cache_source_slug and cache_source_slug != slug:
                    OddspapiMarketAdapter._append_diagnostic(
                        diagnostics,
                        "mainline_cache_fallbacks_used",
                        {
                            "bookmakerSlug": slug,
                            "cacheSourceSlug": cache_source_slug,
                        },
                    )

            selection = LineSelection()
            if (
                not use_mainline_cache
                and not global_mainline_ids
                and mainline_outcome_ids_by_bookmaker is None
            ):
                selection = select_current_lines(
                    markets_data,
                    market_mapping_index=market_mapping_index,
                    source_sport_id=source_sport_id,
                    source=source,
                    is_live=bool(payload.get("isLive", False)),
                )
                for decision in selection.diagnostics:
                    OddspapiMarketAdapter._append_diagnostic(
                        diagnostics, "line_selection", {"bookmakerSlug": slug, **decision},
                    )

            grouped_markets: dict[tuple, dict] = {}
            for source_market_id, market_data in OddspapiMarketAdapter._entries(markets_data):
                if should_skip_inactive_market(
                    market_data,
                    require_active_quotes=require_active_quotes,
                ):
                    continue

                market_resolution = MarketMappingRepository.resolve_market(
                    market_mapping_index,
                    source=source,
                    source_sport_id=source_sport_id,
                    source_market_id=source_market_id,
                )
                normalized_market_id = normalize_source_id(source_market_id)
                if not market_resolution.resolved:
                    OddspapiMarketAdapter._append_diagnostic(
                        diagnostics,
                        "unmapped_markets",
                        {
                            "sourceMarketId": normalized_market_id,
                            "sourceSportId": normalize_source_id(source_sport_id),
                            "reason": market_resolution.reason,
                        },
                    )
                    continue

                choice_group = None
                if market_resolution.requires_choice_group:
                    choice_group = OddspapiMarketAdapter._format_line(
                        market_resolution.source_handicap
                    )
                    if choice_group is None:
                        OddspapiMarketAdapter._append_diagnostic(
                            diagnostics,
                            "skipped_missing_handicap",
                            {
                                "sourceMarketId": normalized_market_id,
                                "canonicalMarketKey": market_resolution.canonical_market_key,
                                "reason": "missing_required_handicap",
                            },
                        )
                        continue

                market_key = (
                    market_resolution.canonical_market_name,
                    market_resolution.canonical_market_group,
                    market_resolution.canonical_market_period,
                    choice_group,
                    bool(market_data.get("isLive", payload.get("isLive", False))),
                )

                # Current-line selection already classified this source market.
                # Do not run the completeness diagnostic again for a discarded
                # alternative: its non-mainline choices are intentionally not
                # collected when persist_main_line_only is enabled. A rejected
                # candidate remains represented by line_selection diagnostics.
                if normalized_market_id in selection.excluded_market_ids:
                    continue

                source_market_choices = []
                available_choice_names = set()

                for source_outcome_id, outcome_data in OddspapiMarketAdapter._entries(
                    market_data.get("outcomes", {})
                ):
                    outcome_resolution = MarketMappingRepository.resolve_outcome(
                        market_mapping_index,
                        market_source_mapping_id=market_resolution.mapping_id,
                        source_outcome_id=source_outcome_id,
                    )
                    normalized_outcome_id = normalize_source_id(source_outcome_id)
                    if not outcome_resolution.resolved:
                        OddspapiMarketAdapter._append_diagnostic(
                            diagnostics,
                            "unmapped_outcomes",
                            {
                                "sourceMarketId": normalized_market_id,
                                "sourceOutcomeId": normalized_outcome_id,
                                "reason": outcome_resolution.reason,
                            },
                        )
                        continue

                    players = outcome_data.get("players")
                    if not players and "price" in outcome_data:
                        players = [outcome_data]

                    for _, player in OddspapiMarketAdapter._entries(players or {}):
                        if require_active_quotes and player.get("active") is False:
                            continue
                        price = player.get("price")
                        if price is None or price == "":
                            continue
                        try:
                            decimal_value = round(float(price), 3)
                        except (TypeError, ValueError):
                            continue
                        initial_decimal_value = None
                        initial_price = player.get("initialPrice")
                        if initial_price not in (None, ""):
                            try:
                                initial_decimal_value = round(
                                    float(initial_price),
                                    3,
                                )
                            except (TypeError, ValueError):
                                initial_decimal_value = None

                        choice_name = outcome_resolution.canonical_choice_name
                        # Completeness describes the provider payload, not the
                        # later mainLine persistence policy. Keep this separate
                        # from source_market_choices, which may intentionally
                        # omit non-mainline choices.
                        available_choice_names.add(str(choice_name))
                        if any(
                            choice["name"] == choice_name
                            for choice in source_market_choices
                        ):
                            continue

                        main_line = player.get("mainLine")
                        if normalized_market_id in selection.selected_market_ids:
                            main_line = True
                        elif (
                            main_line is None
                            and normalized_outcome_id in cached_mainline_ids
                        ):
                            main_line = True
                        if persist_main_line_only and main_line is not True:
                            continue

                        choice = {
                            "name": choice_name,
                            "decimalValue": decimal_value,
                            "initialDecimalValue": initial_decimal_value,
                            "initialChangedAt": player.get("initialChangedAt"),
                            "initialLimit": player.get("initialLimit"),
                            "sourceMarketId": normalized_market_id,
                            "sourceOutcomeId": normalized_outcome_id,
                            "bookmakerOutcomeId": player.get("bookmakerOutcomeId"),
                            "changedAt": player.get("changedAt"),
                            "mainLine": main_line,
                            "limit": player.get("limit"),
                        }
                        moment_quotes = player.get("momentQuotes")
                        if isinstance(moment_quotes, list) and moment_quotes:
                            choice["momentQuotes"] = moment_quotes
                        exchange_meta = player.get("exchangeMeta")
                        if OddspapiMarketAdapter._is_exchange_bookmaker(
                            slug, exchange_meta
                        ):
                            if isinstance(exchange_meta, dict):
                                # Persist only top-of-book back + best lay.
                                choice["exchangeQuotes"] = best_exchange_quotes(
                                    back_price=decimal_value,
                                    back_size=player.get("limit"),
                                    exchange_meta=exchange_meta,
                                )
                            elif slug in OddspapiMarketAdapter._EXCHANGE_BOOKMAKER_SLUGS:
                                # /historical-odds identifies the exchange bookmaker
                                # but deliberately omits exchangeMeta. Its single
                                # price can therefore only represent the back
                                # instrument; never create a side-agnostic quote or
                                # infer a lay opening price from it.
                                choice["exchangeQuotes"] = [
                                    {
                                        "side": "back",
                                        "level": 0,
                                        "price": decimal_value,
                                        "size": player.get("limit"),
                                    }
                                ]
                        source_market_choices.append(choice)

                expected_choice_names = OddspapiMarketAdapter._expected_choice_names(
                    market_mapping_index,
                    market_resolution.mapping_id,
                )
                detected_choice_names = {
                    str(choice_name)
                    for choice_name in available_choice_names
                }
                missing_choice_names = sorted(
                    expected_choice_names - detected_choice_names
                )
                if missing_choice_names:
                    OddspapiMarketAdapter._append_diagnostic(
                        diagnostics,
                        "skipped_incomplete_markets",
                        {
                            "sourceMarketId": normalized_market_id,
                            "canonicalMarketKey": (
                                market_resolution.canonical_market_key
                            ),
                            "expectedChoices": sorted(expected_choice_names),
                            "detectedChoices": sorted(detected_choice_names),
                            "missingChoices": missing_choice_names,
                            "reason": "missing_active_mapped_choices",
                        },
                    )
                    continue
                if not source_market_choices:
                    continue
                normalized_market = grouped_markets.setdefault(
                    market_key,
                    {
                        "canonicalMarketKey": (
                            market_resolution.canonical_market_key
                        ),
                        "marketName": market_resolution.canonical_market_name,
                        "marketGroup": market_resolution.canonical_market_group,
                        "marketPeriod": market_resolution.canonical_market_period,
                        "choiceGroup": choice_group,
                        "isLive": market_key[-1],
                        "choices": [],
                    },
                )
                existing_choice_names = {
                    str(choice["name"])
                    for choice in normalized_market["choices"]
                }
                normalized_market["choices"].extend(
                    choice
                    for choice in source_market_choices
                    if str(choice["name"]) not in existing_choice_names
                )

            markets = [market for market in grouped_markets.values() if market["choices"]]
            if markets:
                normalized_bookmakers.append(
                    {
                        "slug": slug,
                        "name": OddspapiMarketAdapter._bookmaker_name(slug, bookmaker_index),
                        "markets": markets,
                    }
                )

        result = {"fixtureId": fixture_id, "bookmakers": normalized_bookmakers}
        if any(diagnostics.values()):
            result["diagnostics"] = diagnostics
        return result
