"""Extract mainLine=true outcomes from an OddsPapi /odds payload."""

from __future__ import annotations

from typing import Any, Iterable

from infrastructure.persistence.repositories.market_mapping_repository import (
    MarketMappingIndex,
    MarketMappingRepository,
)
from modules.oddspapi.format_utils import normalize_source_id
from modules.oddspapi.quote_activity import should_skip_inactive_market


class OddspapiMainlineOutcomeExtractor:
    """Collect cacheable mainline outcomes without mutating the source payload."""

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
    def _players(outcome_data: dict) -> list[dict]:
        players = outcome_data.get("players")
        if not players and "price" in outcome_data:
            players = [outcome_data]
        return [player for _, player in OddspapiMainlineOutcomeExtractor._entries(players or {})]

    @classmethod
    def extract(
        cls,
        odds_response: dict,
        *,
        exchange_bookmakers: list[str] | None = None,
        require_active_quotes: bool = True,
        market_mapping_index: MarketMappingIndex | None = None,
        source_sport_id: str | int | None = None,
    ) -> list[dict]:
        payload = odds_response if isinstance(odds_response, dict) else {}
        if source_sport_id is None:
            source_sport_id = payload.get("sportId")
        exchange_slugs = {
            str(slug).strip().lower()
            for slug in exchange_bookmakers or []
            if str(slug).strip()
        }
        results: list[dict] = []
        seen: set[tuple[str, str, str]] = set()

        for bookmaker_key, bookmaker_data in cls._entries(
            payload.get("bookmakerOdds", {})
        ):
            slug = str(bookmaker_data.get("slug") or bookmaker_key).strip().lower()
            if not slug:
                continue
            is_exchange = slug in exchange_slugs

            for source_market_id, market_data in cls._entries(
                bookmaker_data.get("markets", {})
            ):
                if should_skip_inactive_market(
                    market_data,
                    require_active_quotes=require_active_quotes,
                ):
                    continue
                normalized_market_id = normalize_source_id(source_market_id)
                if normalized_market_id is None:
                    continue

                canonical_market_key = None
                if market_mapping_index is not None:
                    market_resolution = MarketMappingRepository.resolve_market(
                        market_mapping_index,
                        source="oddspapi",
                        source_sport_id=source_sport_id,
                        source_market_id=normalized_market_id,
                    )
                    if not market_resolution.resolved or not market_resolution.canonical_market_key:
                        continue
                    canonical_market_key = market_resolution.canonical_market_key

                for source_outcome_id, outcome_data in cls._entries(
                    market_data.get("outcomes", {})
                ):
                    players = cls._players(outcome_data)
                    if not any(player.get("mainLine") is True for player in players):
                        continue
                    normalized_outcome_id = normalize_source_id(source_outcome_id)
                    if normalized_outcome_id is None:
                        continue
                    key = (slug, normalized_market_id, normalized_outcome_id)
                    if key in seen:
                        continue
                    seen.add(key)
                    results.append(
                        {
                            "bookmaker_slug": slug,
                            "source_market_id": normalized_market_id,
                            "source_outcome_id": normalized_outcome_id,
                            "canonical_market_key": canonical_market_key,
                            "is_exchange": is_exchange,
                        }
                    )
        return results
