"""Upsert collaborator for MarketChoiceQuote (current-state price cache).

A quote row is keyed by (choice_id, source, exchange_side, exchange_level),
where exchange_side is None for non-exchange bookies (no back/lay split) -
the same NULL convention as Market.choice_group - and can receive its
``initial_*`` and ``current_*`` slots from independent write cycles (e.g.
opening odds arrive at T-120, a later poll only refreshes ``current_odds``
at T-5). ``upsert`` merges into whichever slots have new data instead of
requiring both to arrive together.

Temporal ordering and fill-only backfill rules live in
``market_choice_quote_merge_policy`` so live ingestion and Phase 4b share one
decision path.

See docs/refactors/db-schema-odds-refactor.md (Fase 1-2) and
docs/refactors/db-schema-odds-refactor-phase-4b.md §5.1.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, MutableMapping, Optional

from infrastructure.persistence.repositories.market.market_choice_quote_merge_policy import (
    QuoteCandidateState,
    QuoteMergeDecision,
    QuoteMergeMode,
    decide_quote_merge,
    existing_state_from_quote,
)
from infrastructure.persistence.repositories.market.odds_movement import compute_movement

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from infrastructure.persistence.models import MarketChoiceQuote


@dataclass(frozen=True)
class QuoteUpsertResult:
    """Auditable outcome of one ``MarketChoiceQuoteWriter.upsert`` call."""

    quote: Optional["MarketChoiceQuote"]
    decision: QuoteMergeDecision

    @property
    def applied(self) -> bool:
        return self.quote is not None and self.decision.has_mutations


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
        explicit_change=None,
        overwrite_initial: bool = False,
        mode: QuoteMergeMode = QuoteMergeMode.LIVE,
    ) -> Optional[QuoteUpsertResult]:
        """Create or refresh a quote using a caller-preloaded identity map.

        Returns ``None`` when no price candidate is provided. Otherwise returns
        a ``QuoteUpsertResult`` whose ``.quote`` is the ORM row (callers that
        previously consumed the bare quote should use ``result.quote``).
        """
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

        candidate = QuoteCandidateState(
            initial_price=initial_price,
            initial_captured_at=initial_captured_at,
            current_price=current_price,
            current_captured_at=current_captured_at,
            main_line=main_line,
            source_market_id=source_market_id,
            source_outcome_id=source_outcome_id,
            bookmaker_outcome_id=bookmaker_outcome_id,
            source_limit=source_limit,
            overwrite_initial=overwrite_initial,
        )
        decision = decide_quote_merge(
            existing=existing_state_from_quote(quote),
            candidate=candidate,
            mode=mode,
        )

        if quote is None:
            quote = MarketChoiceQuote(
                choice_id=choice_id,
                source=normalized_source,
                exchange_side=normalized_side,
                exchange_level=normalized_level,
            )
            session.add(quote)
            quote_index[identity] = quote

        MarketChoiceQuoteWriter._apply_decision(
            quote,
            decision,
            explicit_change=explicit_change,
        )
        return QuoteUpsertResult(quote=quote, decision=decision)

    @staticmethod
    def _apply_decision(
        quote: "MarketChoiceQuote",
        decision: QuoteMergeDecision,
        *,
        explicit_change=None,
    ) -> None:
        if decision.apply_initial:
            quote.initial_odds = decision.initial_odds
            quote.initial_captured_at = decision.initial_captured_at
        elif decision.apply_initial_timestamp_only:
            quote.initial_captured_at = decision.initial_captured_at

        if decision.apply_current:
            quote.current_odds = decision.current_odds
            quote.current_updated_at = decision.current_updated_at

        for field_name, value in decision.metadata_updates.items():
            setattr(quote, field_name, value)

        if decision.apply_source_limit:
            quote.source_limit = decision.source_limit

        if decision.recalculate_movement:
            quote.movement = compute_movement(
                explicit_change=explicit_change,
                initial_odds=quote.initial_odds,
                current_odds=quote.current_odds,
            )


__all__ = ["MarketChoiceQuoteWriter", "QuoteUpsertResult"]
