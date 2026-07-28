"""Merge historical opening quotes into the current OddsPapi response."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable


class OddspapiHistoricalOddsEnricher:
    """Enrich matching current players without replacing current prices."""

    _INITIAL_FIELDS = (
        "initialPrice",
        "initialChangedAt",
        "initialLimit",
    )

    @staticmethod
    def _entries(value: Any) -> Iterable[tuple[str, dict]]:
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, dict):
                    yield str(key), item
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, dict):
                    yield str(index), item

    @classmethod
    def merge_initial_prices(
        cls,
        current_response: dict,
        historical_normalized_response: dict | None,
    ) -> dict:
        enriched = deepcopy(
            current_response if isinstance(current_response, dict) else {}
        )
        historical = (
            historical_normalized_response
            if isinstance(historical_normalized_response, dict)
            else {}
        )
        current_bookmakers = enriched.get("bookmakerOdds")
        historical_bookmakers = historical.get("bookmakerOdds")
        if not isinstance(current_bookmakers, dict):
            return enriched
        if not isinstance(historical_bookmakers, dict):
            return enriched

        for bookmaker_slug, historical_bookmaker in cls._entries(
            historical_bookmakers
        ):
            current_bookmaker = current_bookmakers.get(bookmaker_slug)
            if not isinstance(current_bookmaker, dict):
                continue
            current_markets = current_bookmaker.get("markets")
            historical_markets = historical_bookmaker.get("markets")
            if not isinstance(current_markets, dict):
                continue

            for market_id, historical_market in cls._entries(historical_markets):
                current_market = current_markets.get(market_id)
                if not isinstance(current_market, dict):
                    continue
                current_outcomes = current_market.get("outcomes")
                historical_outcomes = historical_market.get("outcomes")
                if not isinstance(current_outcomes, dict):
                    continue

                for outcome_id, historical_outcome in cls._entries(
                    historical_outcomes
                ):
                    current_outcome = current_outcomes.get(outcome_id)
                    if not isinstance(current_outcome, dict):
                        continue
                    current_players = current_outcome.get("players")
                    historical_players = historical_outcome.get("players")
                    if not isinstance(current_players, dict):
                        continue

                    for player_id, historical_player in cls._entries(
                        historical_players
                    ):
                        current_player = current_players.get(player_id)
                        if not isinstance(current_player, dict):
                            continue
                        for field_name in cls._INITIAL_FIELDS:
                            value = historical_player.get(field_name)
                            if value not in (None, ""):
                                current_player[field_name] = value

        return enriched
