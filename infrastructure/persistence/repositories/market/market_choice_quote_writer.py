"""Upsert collaborator for MarketChoiceQuote (current-state price cache).

A quote row is keyed by (choice_id, source, exchange_side, exchange_level),
where exchange_side is None for non-exchange bookies (no back/lay split) -
the same NULL convention as Market.choice_group - and can receive its
``initial_*`` and ``current_*`` slots from independent write cycles (e.g.
opening odds arrive at T-120, a later poll only refreshes ``current_odds``
at T-5). ``upsert`` merges into whichever slots have new data instead of
requiring both to arrive together.

See docs/refactors/db-schema-odds-refactor.md (Fase 1-2) for the schema
rationale and infrastructure/persistence/models.py::MarketChoiceQuote for the
table definition.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, MutableMapping, Optional

from infrastructure.persistence.repositories.market.odds_movement import compute_movement

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from infrastructure.persistence.models import MarketChoiceQuote


class MarketChoiceQuoteWriter:
    """Creates/updates one MarketChoiceQuote row per (choice, source, side, level)."""

    @staticmethod
    def identity_key(
        *,
        choice_id: int,
        source: str,
        exchange_side: Optional[str] = None,
        exchange_level: int = 0,
    ) -> tuple[int, str, Optional[str], int]:
        """Return the normalized identity shared by preload and upsert."""
        if choice_id is None:
            raise ValueError("choice_id is required to identify a quote")
        normalized_source = str(source or "").strip().lower()
        if not normalized_source:
            raise ValueError("source is required to identify a quote")
        normalized_side = (
            str(exchange_side).strip().lower() if exchange_side else None
        )
        if normalized_side not in {None, "back", "lay"}:
            raise ValueError(f"Unsupported exchange_side={normalized_side!r}")
        normalized_level = int(exchange_level or 0)
        if normalized_level < 0:
            raise ValueError("exchange_level cannot be negative")
        return (
            int(choice_id),
            normalized_source,
            normalized_side,
            normalized_level,
        )

    @staticmethod
    def upsert(
        session: Session,
        *,
        quote_index: MutableMapping[
            tuple[int, str, Optional[str], int],
            MarketChoiceQuote,
        ],
        choice_id: int,
        source: str,
        exchange_side: Optional[str] = None,
        exchange_level: int = 0,
        initial_price=None,
        initial_captured_at: Optional[datetime] = None,
        current_price=None,
        current_captured_at: Optional[datetime] = None,
        main_line: Optional[bool] = None,
        source_market_id: Optional[str] = None,
        source_outcome_id: Optional[str] = None,
        bookmaker_outcome_id: Optional[str] = None,
        source_limit=None,
        overwrite_initial: bool = False,
    ):
        """Create or refresh a quote using a caller-preloaded identity map."""
        from infrastructure.persistence.models import MarketChoiceQuote

        if initial_price is None and current_price is None:
            return None

        identity = MarketChoiceQuoteWriter.identity_key(
            choice_id=choice_id,
            source=source,
            exchange_side=exchange_side,
            exchange_level=exchange_level,
        )
        _, normalized_source, normalized_side, normalized_level = identity
        quote = quote_index.get(identity)
        if quote is None:
            quote = MarketChoiceQuote(
                choice_id=choice_id,
                source=normalized_source,
                exchange_side=normalized_side,
                exchange_level=normalized_level,
            )
            session.add(quote)
            quote_index[identity] = quote

        if main_line is not None:
            quote.main_line = main_line
        if source_market_id is not None:
            quote.source_market_id = source_market_id
        if source_outcome_id is not None:
            quote.source_outcome_id = source_outcome_id
        if bookmaker_outcome_id is not None:
            quote.bookmaker_outcome_id = bookmaker_outcome_id
        if source_limit is not None:
            quote.source_limit = source_limit

        if initial_price is not None and (quote.initial_odds is None or overwrite_initial):
            quote.initial_odds = initial_price
            quote.initial_captured_at = initial_captured_at

        if current_price is not None:
            quote.current_odds = current_price
            quote.current_updated_at = current_captured_at

        quote.movement = compute_movement(
            explicit_change=None,
            initial_odds=quote.initial_odds,
            current_odds=quote.current_odds,
        )
        return quote


__all__ = ["MarketChoiceQuoteWriter"]
