"""Convert OddsPapi historical odds into the current-odds transport contract."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class OddspapiHistoricalOddsNormalizer:
    """Select opening/final quotes while preserving the existing ingestion path."""

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime | None:
        if value in (None, ""):
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _has_valid_price(entry: dict) -> bool:
        if entry.get("price") in (None, ""):
            return False
        try:
            float(entry.get("price"))
        except (TypeError, ValueError):
            return False
        return True

    @classmethod
    def _ordered_quotes(cls, value: Any) -> list[dict]:
        if isinstance(value, dict):
            values = [value]
        elif isinstance(value, list):
            values = [item for item in value if isinstance(item, dict)]
        else:
            return []

        indexed = list(enumerate(values))
        indexed.sort(
            key=lambda item: (
                cls._parse_timestamp(item[1].get("createdAt"))
                or datetime.min.replace(tzinfo=timezone.utc),
                -item[0],
            )
        )
        return [item for _, item in indexed]

    @classmethod
    def _normalize_player_history(
        cls,
        value: Any,
        *,
        minimum_initial_span_minutes: float = 0.0,
        require_active_quotes: bool = True,
    ) -> dict | None:
        quotes = [
            quote
            for quote in cls._ordered_quotes(value)
            if (
                cls._has_valid_price(quote)
                and cls._parse_timestamp(quote.get("createdAt")) is not None
            )
        ]
        if not quotes:
            return None

        # ``require_active_quotes`` mirrors ODDSPAPI_PRE_START_REQUIRE_ACTIVE_QUOTES:
        # when True, both opening and current come from the active timeline;
        # when False, use every priced observation (needed for suspended lines
        # that still publish prices with active=false).
        if require_active_quotes:
            candidate_quotes = [
                quote for quote in quotes if quote.get("active") is not False
            ]
        else:
            candidate_quotes = quotes
        if not candidate_quotes:
            return None

        opening = candidate_quotes[0]
        latest = candidate_quotes[-1]
        normalized = dict(latest)
        if require_active_quotes:
            # Selected from the active pool; keep the current-odds contract.
            normalized["active"] = True
        normalized["changedAt"] = latest.get("createdAt")
        opening_at = cls._parse_timestamp(opening.get("createdAt"))
        latest_at = cls._parse_timestamp(latest.get("createdAt"))
        minimum_span_seconds = max(
            0.0,
            float(minimum_initial_span_minutes or 0.0),
        ) * 60.0
        has_credible_opening = (
            opening_at is not None
            and latest_at is not None
            and (latest_at - opening_at).total_seconds() >= minimum_span_seconds
        )
        if has_credible_opening:
            normalized["initialPrice"] = opening.get("price")
            normalized["initialChangedAt"] = opening.get("createdAt")
            normalized["initialLimit"] = opening.get("limit")
        return normalized

    @classmethod
    def normalize(
        cls,
        historical_response: dict,
        *,
        source_sport_id: str | int | None,
        minimum_initial_span_minutes: float = 0.0,
        require_active_quotes: bool = True,
    ) -> dict:
        payload = historical_response if isinstance(historical_response, dict) else {}
        normalized_bookmakers: dict[str, dict] = {}
        bookmakers = payload.get("bookmakers")
        if not isinstance(bookmakers, dict):
            bookmakers = {}

        for bookmaker_slug, bookmaker_data in bookmakers.items():
            if not isinstance(bookmaker_data, dict):
                continue
            markets = bookmaker_data.get("markets")
            if not isinstance(markets, dict):
                continue
            normalized_markets: dict[str, dict] = {}

            for market_id, market_data in markets.items():
                if not isinstance(market_data, dict):
                    continue
                outcomes = market_data.get("outcomes")
                if not isinstance(outcomes, dict):
                    continue
                normalized_outcomes: dict[str, dict] = {}

                for outcome_id, outcome_data in outcomes.items():
                    if not isinstance(outcome_data, dict):
                        continue
                    players = outcome_data.get("players")
                    if not isinstance(players, dict):
                        continue
                    normalized_players = {
                        str(player_id): normalized
                        for player_id, history in players.items()
                        if (
                            normalized := cls._normalize_player_history(
                                history,
                                minimum_initial_span_minutes=(
                                    minimum_initial_span_minutes
                                ),
                                require_active_quotes=require_active_quotes,
                            )
                        )
                        is not None
                    }
                    if normalized_players:
                        normalized_outcomes[str(outcome_id)] = {
                            "players": normalized_players
                        }

                if normalized_outcomes:
                    normalized_markets[str(market_id)] = {
                        "marketActive": True,
                        "outcomes": normalized_outcomes,
                    }

            if normalized_markets:
                normalized_bookmakers[str(bookmaker_slug)] = {
                    "markets": normalized_markets
                }

        return {
            "fixtureId": payload.get("fixtureId"),
            "sportId": source_sport_id,
            "bookmakerOdds": normalized_bookmakers,
        }
