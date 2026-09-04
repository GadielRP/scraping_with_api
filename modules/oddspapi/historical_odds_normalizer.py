"""Convert OddsPapi historical odds into the current-odds transport contract."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence


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
    def ordered_priced_ticks(cls, value: Any) -> list[tuple[datetime, dict]]:
        """Return priced quotes ordered by ``createdAt``, with parsed timestamps."""
        ticks: list[tuple[datetime, dict]] = []
        for quote in cls._ordered_quotes(value):
            if not cls._has_valid_price(quote):
                continue
            created_at = cls._parse_timestamp(quote.get("createdAt"))
            if created_at is None:
                continue
            ticks.append((created_at, quote))
        return ticks

    @classmethod
    def from_ordered_ticks(
        cls,
        ticks: Sequence[tuple[datetime, dict]],
        *,
        minimum_initial_span_minutes: float = 0.0,
        require_active_quotes: bool = True,
        current_cutoff_utc: datetime | None = None,
    ) -> dict | None:
        """Reduce an ordered series to opening + latest pre-cutoff quote.

        ``current_cutoff_utc`` is inclusive. Historical responses may contain
        in-play ticks when requested shortly after kickoff; those ticks must
        not become the canonical pre-match ``current`` price.
        """
        cutoff_utc = cls._parse_timestamp(current_cutoff_utc)
        opening = latest = None
        for created_at, quote in ticks:
            if cutoff_utc is not None and created_at > cutoff_utc:
                break
            if require_active_quotes and quote.get("active") is False:
                continue
            if opening is None:
                opening = quote
            latest = quote
        if latest is None:
            return None

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
        current_cutoff_utc: datetime | None = None,
    ) -> dict:
        from modules.oddspapi.historical_odds_reader import OddspapiHistoricalOddsReader

        return OddspapiHistoricalOddsReader.read(
            historical_response,
            source_sport_id=source_sport_id,
            as_of_targets=(),
            minimum_initial_span_minutes=minimum_initial_span_minutes,
            require_active_quotes=require_active_quotes,
            current_cutoff_utc=current_cutoff_utc,
        ).normalized_payload
