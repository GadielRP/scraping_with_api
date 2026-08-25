"""Reduce an ordered historical tick series to prices-in-force at key moments.

The live reader walks the ``/historical-odds`` tree once and feeds this
reducer the same ordered ticks used for opening/latest. This module does not
traverse raw payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

from shared.timezone_utils import convert_local_to_utc, convert_utc_to_local


@dataclass(frozen=True)
class HistoricalOddsAsOfQuote:
    """One reconstructed observation for a single outcome at one key moment."""

    bookmaker_slug: str
    source_market_id: str
    source_outcome_id: str
    player_id: str
    minutes_until_start: int
    price: float
    created_at: str
    collected_at: datetime
    limit: Any = None
    active: Any = None


class OddspapiHistoricalOddsAsOf:
    """Select last-in-force quotes from already-ordered tick series."""

    @staticmethod
    def start_time_as_utc(start_time: datetime | None) -> datetime | None:
        """Normalize the canonical event start to an aware UTC boundary.

        Canonical event datetimes are stored as local-naive values despite the
        legacy ``start_time_utc`` field name. A timezone-aware value is treated
        according to its own offset.
        """
        if start_time is None:
            return None
        if start_time.tzinfo is None:
            return convert_local_to_utc(start_time).replace(tzinfo=timezone.utc)
        return start_time.astimezone(timezone.utc)

    @classmethod
    def targets_from_start(
        cls,
        start_time: datetime | None,
        moments: Sequence[int],
    ) -> list[tuple[int, datetime, datetime]]:
        """Return ``(minutes, target_utc, collected_at_local)`` for non-negative moments."""
        if start_time is None:
            return []
        start_utc = cls.start_time_as_utc(start_time)
        if start_utc is None:
            return []
        if start_time.tzinfo is None:
            start_local = start_time
        else:
            start_local = convert_utc_to_local(start_time)
        targets: list[tuple[int, datetime, datetime]] = []
        for moment in moments:
            try:
                minutes = int(moment)
            except (TypeError, ValueError):
                continue
            if minutes < 0:
                continue
            delta = timedelta(minutes=minutes)
            targets.append(
                (
                    minutes,
                    start_utc - delta,
                    start_local - delta,
                )
            )
        return targets

    @classmethod
    def attach_to_normalized_payload(
        cls,
        normalized_payload: dict | None,
        as_of_quotes: Sequence[HistoricalOddsAsOfQuote],
    ) -> dict | None:
        """Copy reconstructed ticks onto matching normalized players as momentQuotes."""
        if not isinstance(normalized_payload, dict) or not as_of_quotes:
            return normalized_payload
        grouped: dict[tuple[str, str, str, str], list[dict]] = {}
        for quote in as_of_quotes:
            grouped.setdefault(
                (
                    quote.bookmaker_slug,
                    quote.source_market_id,
                    quote.source_outcome_id,
                    quote.player_id,
                ),
                [],
            ).append(
                {
                    "minutesUntilStart": quote.minutes_until_start,
                    "price": quote.price,
                    "createdAt": quote.created_at,
                    "collectedAt": quote.collected_at,
                    "limit": quote.limit,
                    "active": quote.active,
                }
            )
        bookmakers = normalized_payload.get("bookmakerOdds")
        if not isinstance(bookmakers, dict):
            return normalized_payload
        for key, moment_quotes in grouped.items():
            slug, market_id, outcome_id, player_id = key
            bookmaker = bookmakers.get(slug)
            if not isinstance(bookmaker, dict):
                continue
            markets = bookmaker.get("markets")
            if not isinstance(markets, dict):
                continue
            market = markets.get(market_id)
            if not isinstance(market, dict):
                continue
            outcomes = market.get("outcomes")
            if not isinstance(outcomes, dict):
                continue
            outcome = outcomes.get(outcome_id)
            if not isinstance(outcome, dict):
                continue
            players = outcome.get("players")
            if not isinstance(players, dict):
                continue
            player = players.get(player_id)
            if not isinstance(player, dict):
                continue
            player["momentQuotes"] = moment_quotes
        return normalized_payload

    @classmethod
    def from_ordered_ticks(
        cls,
        ticks: Sequence[tuple[datetime, dict]],
        *,
        targets: Sequence[tuple[int, datetime, datetime]],
        bookmaker_slug: str,
        source_market_id: str,
        source_outcome_id: str,
        player_id: str,
        require_active_quotes: bool = True,
    ) -> list[HistoricalOddsAsOfQuote]:
        """Select last-in-force quotes from an already-ordered tick series."""
        if not targets:
            return []
        if require_active_quotes:
            filtered = [
                (created_at, quote)
                for created_at, quote in ticks
                if quote.get("active") is not False
            ]
        else:
            filtered = list(ticks)
        if not filtered:
            return []

        selected: list[HistoricalOddsAsOfQuote] = []
        tick_index = 0
        current: tuple[datetime, dict] | None = None
        ordered_targets = sorted(targets, key=lambda item: item[1])
        for minutes, target_utc, collected_at in ordered_targets:
            while tick_index < len(filtered) and filtered[tick_index][0] <= target_utc:
                current = filtered[tick_index]
                tick_index += 1
            if current is None:
                continue
            created_at, quote = current
            try:
                price = round(float(quote.get("price")), 3)
            except (TypeError, ValueError):
                continue
            selected.append(
                HistoricalOddsAsOfQuote(
                    bookmaker_slug=bookmaker_slug,
                    source_market_id=source_market_id,
                    source_outcome_id=source_outcome_id,
                    player_id=player_id,
                    minutes_until_start=minutes,
                    price=price,
                    created_at=str(quote.get("createdAt") or ""),
                    collected_at=collected_at,
                    limit=quote.get("limit"),
                    active=quote.get("active"),
                )
            )
        return selected
