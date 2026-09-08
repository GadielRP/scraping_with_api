"""Market filtering for normalized OddsPapi payloads.

This module owns only the canonical market-filter rules. It deliberately does
not resolve events, adapt provider payloads, read historical series, or write
to the database. Both the current ``/odds`` path and the historical preselector
can therefore share the same filter normalization without coupling those
pipelines to the persistence service.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Dict, Optional


class OddspapiNormalizedMarketFilter:
    """Apply configured canonical market filters to adapted OddsPapi data."""

    @staticmethod
    def normalize_market_key_filters(
        allowed_market_keys: Sequence[str] | set[str] | None,
    ) -> set[str] | None:
        if not allowed_market_keys:
            return None
        normalized = {
            str(item).strip().lower()
            for item in allowed_market_keys
            if item is not None and str(item).strip()
        }
        return normalized or None

    @staticmethod
    def normalize_market_group_filters(
        allowed_market_groups: Sequence[str] | set[str] | None,
    ) -> set[str] | None:
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
    def normalize_market_period_filters(
        allowed_market_periods: Sequence[str] | set[str] | None,
    ) -> set[str] | None:
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

    @classmethod
    def filter_response(
        cls,
        normalized_response: Dict,
        *,
        allowed_market_keys: Optional[list[str] | set[str] | tuple[str, ...]] = None,
        allowed_market_groups: Optional[list[str] | set[str] | tuple[str, ...]] = None,
        allowed_market_periods: Optional[list[str] | set[str] | tuple[str, ...]] = None,
    ) -> Dict:
        """Return a filtered normalized response without mutating its input."""
        if not normalized_response or not normalized_response.get("bookmakers"):
            return normalized_response

        normalized_keys = cls.normalize_market_key_filters(allowed_market_keys)
        normalized_groups = cls.normalize_market_group_filters(allowed_market_groups)
        normalized_periods = cls.normalize_market_period_filters(allowed_market_periods)

        if normalized_keys is None and normalized_groups is None and normalized_periods is None:
            return normalized_response

        filtered_bookmakers = []
        for bookmaker in normalized_response.get("bookmakers", []):
            filtered_markets = []
            for market in bookmaker.get("markets", []):
                market_key = str(market.get("canonicalMarketKey") or "").strip().lower()
                market_group = str(market.get("marketGroup") or "").strip()
                market_period = str(market.get("marketPeriod") or "").strip()

                if normalized_keys is not None and market_key not in normalized_keys:
                    continue
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
