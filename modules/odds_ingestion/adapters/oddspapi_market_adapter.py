"""Normalize OddsPapi bookmaker odds into the repository market contract."""

from __future__ import annotations

from typing import Any, Iterable

from infrastructure.persistence.repositories.market_mapping_repository import (
    MarketMappingIndex,
    MarketMappingRepository,
)
from modules.oddspapi.exchange_quotes import best_exchange_quotes
from modules.oddspapi.format_utils import format_line, normalize_source_id


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
    def from_odds_response(
        odds_response: dict,
        bookmaker_catalog: dict | list | None = None,
        market_mapping_index: MarketMappingIndex | None = None,
        source: str = "oddspapi",
    ) -> dict:
        if market_mapping_index is None:
            raise ValueError("market_mapping_index is required")

        payload = odds_response if isinstance(odds_response, dict) else {}
        fixture_id = payload.get("fixtureId")
        source_sport_id = payload.get("sportId")
        diagnostics = {
            "unmapped_markets": [],
            "unmapped_outcomes": [],
            "skipped_missing_handicap": [],
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

            grouped_markets: dict[tuple, dict] = {}
            for source_market_id, market_data in OddspapiMarketAdapter._entries(markets_data):
                if market_data.get("marketActive") is False:
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
                normalized_market = grouped_markets.setdefault(
                    market_key,
                    {
                        "marketName": market_resolution.canonical_market_name,
                        "marketGroup": market_resolution.canonical_market_group,
                        "marketPeriod": market_resolution.canonical_market_period,
                        "choiceGroup": choice_group,
                        "isLive": market_key[-1],
                        "choices": [],
                    },
                )

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
                        if player.get("active") is False:
                            continue
                        price = player.get("price")
                        if price is None or price == "":
                            continue
                        try:
                            decimal_value = round(float(price), 3)
                        except (TypeError, ValueError):
                            continue

                        choice_name = outcome_resolution.canonical_choice_name
                        if any(choice["name"] == choice_name for choice in normalized_market["choices"]):
                            continue

                        choice = {
                            "name": choice_name,
                            "decimalValue": decimal_value,
                            "sourceMarketId": normalized_market_id,
                            "sourceOutcomeId": normalized_outcome_id,
                            "bookmakerOutcomeId": player.get("bookmakerOutcomeId"),
                            "changedAt": player.get("changedAt"),
                            "mainLine": player.get("mainLine"),
                            "limit": player.get("limit"),
                        }
                        exchange_meta = player.get("exchangeMeta")
                        if OddspapiMarketAdapter._is_exchange_bookmaker(
                            slug, exchange_meta
                        ) and isinstance(exchange_meta, dict):
                            # Persist only top-of-book back + best lay.
                            choice["exchangeQuotes"] = best_exchange_quotes(
                                back_price=decimal_value,
                                back_size=player.get("limit"),
                                exchange_meta=exchange_meta,
                            )
                        normalized_market["choices"].append(choice)

                if not normalized_market["choices"]:
                    grouped_markets.pop(market_key, None)

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
