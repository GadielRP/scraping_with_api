"""Reduce sanitized historical ticks to sustained, adaptively anchored changes."""

from __future__ import annotations

from datetime import datetime, timedelta
from math import isclose, isfinite
from typing import Callable, Sequence

from modules.oddspapi.historical_odds_quote import HistoricalOddsAsOfQuote


class OddspapiHistoricalOddsChangeDetector:
    """Pure per-series reduction; no requests, persistence or policy lookups."""

    @staticmethod
    def sanitize_ticks(
        ticks: Sequence[tuple[datetime, dict]],
        *,
        kickoff_utc: datetime,
        min_price: float = 1.01,
    ) -> list[tuple[datetime, dict]]:
        """Keep ordered pre-kickoff observations, borrowing the original dicts."""
        cleaned = []
        for tick in ticks:
            created_at, quote = tick
            if created_at > kickoff_utc:
                break
            if quote.get("active") is False:
                continue
            try:
                price = float(quote.get("price"))
            except (TypeError, ValueError, OverflowError):
                continue
            if isfinite(price) and price > min_price:
                cleaned.append(tick)
        return cleaned

    @staticmethod
    def is_history_span_sufficient(
        ticks: Sequence[tuple[datetime, dict]],
        kickoff_utc: datetime,
        min_hours: float = 24.0,
    ) -> bool:
        return bool(ticks) and (
            kickoff_utc - ticks[0][0]
        ).total_seconds() >= min_hours * 3600

    @staticmethod
    def _is_significant(price: float, anchor: float, magnitude_pct: float) -> bool:
        change = abs(price - anchor) * 100
        threshold = anchor * magnitude_pct
        # Decimal odds such as 2.0 -> 2.4 must qualify at exactly 20% despite
        # floating-point representation. Do not round prices before comparison.
        return change >= threshold or isclose(change, threshold, rel_tol=1e-12)

    @classmethod
    def detect_significant_changes(
        cls,
        ticks: Sequence[tuple[datetime, dict]],
        *,
        kickoff_utc: datetime,
        bookmaker_slug: str,
        source_market_id: str,
        source_outcome_id: str,
        player_id: str,
        to_local: Callable[[datetime], datetime],
        min_change_magnitude_pct: float = 20.0,
        min_history_hours: float = 24.0,
        flash_reversal_minutes: float = 3.0,
    ) -> list[HistoricalOddsAsOfQuote] | None:
        """Consume already sanitized ticks; None requests fixed-moment fallback.

        Inspect reversal windows by index, without allocating slices. Rejected
        episodes resume at their reversal; confirmed ticks advance one position
        so subsequent changes can still be measured against the new anchor.
        The caller supplies timezone conversion so this reducer never reads
        global configuration. Input datetimes must be aware and UTC ordered.
        """
        if not ticks:
            return []
        if not cls.is_history_span_sufficient(ticks, kickoff_utc, min_history_hours):
            return None

        window = timedelta(minutes=flash_reversal_minutes)
        closing_boundary = kickoff_utc - window
        anchor = float(ticks[0][1]["price"])
        selected: list[HistoricalOddsAsOfQuote] = []
        index = 1
        while index < len(ticks):
            closing = ticks[index][0] > closing_boundary
            if closing:
                index = len(ticks) - 1
            created_at, quote = ticks[index]
            price = float(quote["price"])
            if not cls._is_significant(price, anchor, min_change_magnitude_pct):
                index += 1
                continue

            if not closing:
                reversal = index + 1
                deadline = created_at + window
                while reversal < len(ticks) and ticks[reversal][0] < deadline:
                    if not cls._is_significant(
                        float(ticks[reversal][1]["price"]),
                        anchor,
                        min_change_magnitude_pct,
                    ):
                        break
                    reversal += 1
                if reversal < len(ticks) and ticks[reversal][0] < deadline:
                    index = reversal
                    continue

            selected.append(
                HistoricalOddsAsOfQuote(
                    bookmaker_slug=bookmaker_slug,
                    source_market_id=source_market_id,
                    source_outcome_id=source_outcome_id,
                    player_id=player_id,
                    minutes_until_start=(kickoff_utc - created_at).total_seconds() / 60,
                    price=round(price, 3),
                    created_at=str(quote.get("createdAt") or ""),
                    collected_at=to_local(created_at),
                    limit=quote.get("limit"),
                    active=quote.get("active"),
                )
            )
            anchor = price
            index += 1
        return selected
