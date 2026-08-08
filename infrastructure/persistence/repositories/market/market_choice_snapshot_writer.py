"""Append-only persistence for snapshots linked to one exact quote."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from infrastructure.persistence.models import (
        MarketChoiceQuote,
        MarketChoiceSnapshot,
    )


class MarketChoiceSnapshotWriter:
    """Append historical ticks while preserving exact quote lineage."""

    @staticmethod
    def append(
        session: Session,
        *,
        quote: MarketChoiceQuote,
        odds_value,
        collected_at: datetime,
        source_collected_at: datetime | None = None,
        source_limit=None,
        exchange_size=None,
    ) -> MarketChoiceSnapshot:
        """Create one snapshot from persisted quote identity and tick values."""
        from infrastructure.persistence.models import (
            MarketChoiceQuote,
            MarketChoiceSnapshot,
        )

        if not isinstance(quote, MarketChoiceQuote):
            raise TypeError("quote must be a MarketChoiceQuote")
        if quote.choice_id is None:
            raise ValueError("quote.choice_id is required to append a snapshot")
        if odds_value is None:
            raise ValueError("odds_value is required to append a snapshot")
        if collected_at is None:
            raise ValueError("collected_at is required to append a snapshot")

        source = str(quote.source or "").strip().lower()
        if not source:
            raise ValueError("quote.source is required to append a snapshot")
        if quote.source != source:
            raise ValueError(
                "quote.source must be normalized before appending a snapshot"
            )

        exchange_side = (
            str(quote.exchange_side).strip().lower()
            if quote.exchange_side is not None
            else None
        )
        if exchange_side not in {None, "back", "lay"}:
            raise ValueError(f"Unsupported quote exchange_side={exchange_side!r}")
        if exchange_side is None and exchange_size is not None:
            raise ValueError("exchange_size requires a side-specific quote")

        exchange_level = quote.exchange_level if exchange_side is not None else None
        if exchange_level is not None and exchange_level < 0:
            raise ValueError("quote.exchange_level cannot be negative")

        snapshot = MarketChoiceSnapshot(
            choice_id=quote.choice_id,
            quote=quote,
            odds_value=odds_value,
            collected_at=collected_at,
            source=source,
            source_collected_at=source_collected_at,
            source_market_id=quote.source_market_id,
            source_outcome_id=quote.source_outcome_id,
            bookmaker_outcome_id=quote.bookmaker_outcome_id,
            main_line=quote.main_line,
            source_limit=source_limit,
            exchange_side=exchange_side,
            exchange_level=exchange_level,
            exchange_size=exchange_size,
        )
        session.add(snapshot)
        return snapshot


__all__ = ["MarketChoiceSnapshotWriter"]
