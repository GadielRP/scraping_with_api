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

    @classmethod
    def merge_bookmaker_odds(
        cls,
        base_response: dict | None,
        overlay_response: dict | None,
    ) -> dict:
        """Deep-merge overlay bookmakerOdds into base (live exchange attach)."""
        enriched = deepcopy(base_response if isinstance(base_response, dict) else {})
        overlay = overlay_response if isinstance(overlay_response, dict) else {}
        overlay_bookmakers = overlay.get("bookmakerOdds")
        if not isinstance(overlay_bookmakers, dict) or not overlay_bookmakers:
            return enriched

        if "fixtureId" not in enriched and overlay.get("fixtureId") is not None:
            enriched["fixtureId"] = overlay.get("fixtureId")
        if "sportId" not in enriched and overlay.get("sportId") is not None:
            enriched["sportId"] = overlay.get("sportId")

        base_bookmakers = enriched.setdefault("bookmakerOdds", {})
        if not isinstance(base_bookmakers, dict):
            enriched["bookmakerOdds"] = deepcopy(overlay_bookmakers)
            return enriched

        for bookmaker_slug, overlay_bookmaker in cls._entries(overlay_bookmakers):
            if not isinstance(overlay_bookmaker, dict):
                continue
            base_bookmaker = base_bookmakers.setdefault(bookmaker_slug, {})
            if not isinstance(base_bookmaker, dict):
                base_bookmakers[bookmaker_slug] = deepcopy(overlay_bookmaker)
                continue
            if "slug" not in base_bookmaker and overlay_bookmaker.get("slug"):
                base_bookmaker["slug"] = overlay_bookmaker.get("slug")

            overlay_markets = overlay_bookmaker.get("markets")
            if not isinstance(overlay_markets, dict):
                continue
            base_markets = base_bookmaker.setdefault("markets", {})
            if not isinstance(base_markets, dict):
                base_bookmaker["markets"] = deepcopy(overlay_markets)
                continue

            for market_id, overlay_market in cls._entries(overlay_markets):
                if not isinstance(overlay_market, dict):
                    continue
                base_market = base_markets.setdefault(market_id, {})
                if not isinstance(base_market, dict):
                    base_markets[market_id] = deepcopy(overlay_market)
                    continue
                if "marketActive" not in base_market:
                    base_market["marketActive"] = overlay_market.get(
                        "marketActive",
                        True,
                    )

                overlay_outcomes = overlay_market.get("outcomes")
                if not isinstance(overlay_outcomes, dict):
                    continue
                base_outcomes = base_market.setdefault("outcomes", {})
                if not isinstance(base_outcomes, dict):
                    base_market["outcomes"] = deepcopy(overlay_outcomes)
                    continue

                for outcome_id, overlay_outcome in cls._entries(overlay_outcomes):
                    if not isinstance(overlay_outcome, dict):
                        continue
                    base_outcome = base_outcomes.setdefault(outcome_id, {})
                    if not isinstance(base_outcome, dict):
                        base_outcomes[outcome_id] = deepcopy(overlay_outcome)
                        continue

                    overlay_players = overlay_outcome.get("players")
                    if not isinstance(overlay_players, dict):
                        continue
                    base_players = base_outcome.setdefault("players", {})
                    if not isinstance(base_players, dict):
                        base_outcome["players"] = deepcopy(overlay_players)
                        continue

                    for player_id, overlay_player in cls._entries(overlay_players):
                        if not isinstance(overlay_player, dict):
                            continue
                        base_player = base_players.get(player_id)
                        if not isinstance(base_player, dict):
                            base_players[player_id] = deepcopy(overlay_player)
                            continue
                        base_player.update(deepcopy(overlay_player))

        return enriched
