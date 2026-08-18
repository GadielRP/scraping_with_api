"""Pure temporal merge policy for MarketChoiceQuote upserts.

Shared by live ingestion and the Phase 4b historical backfill so both paths
evaluate the same rules. This module must not import SQLAlchemy or mutate ORM
objects — callers apply the returned ``QuoteMergeDecision``.

See docs/refactors/db-schema-odds-refactor-phase-4b.md §5.1.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class QuoteMergeMode(str, Enum):
    LIVE = "live"
    BACKFILL_FILL_ONLY = "backfill_fill_only"


@dataclass(frozen=True)
class QuoteExistingState:
    """Immutable view of an existing quote row (or empty for insert)."""

    initial_odds: Any = None
    initial_captured_at: Optional[datetime] = None
    current_odds: Any = None
    current_updated_at: Optional[datetime] = None
    main_line: Optional[bool] = None
    source_market_id: Optional[str] = None
    source_outcome_id: Optional[str] = None
    bookmaker_outcome_id: Optional[str] = None
    source_limit: Any = None
    exists: bool = False


@dataclass(frozen=True)
class QuoteCandidateState:
    """Immutable candidate values offered by one upsert call."""

    initial_price: Any = None
    initial_captured_at: Optional[datetime] = None
    current_price: Any = None
    current_captured_at: Optional[datetime] = None
    main_line: Optional[bool] = None
    source_market_id: Optional[str] = None
    source_outcome_id: Optional[str] = None
    bookmaker_outcome_id: Optional[str] = None
    source_limit: Any = None
    overwrite_initial: bool = False


@dataclass(frozen=True)
class QuoteMergeDecision:
    """Allowed mutations derived from existing state + candidate + mode."""

    apply_initial: bool = False
    initial_odds: Any = None
    initial_captured_at: Optional[datetime] = None
    apply_initial_timestamp_only: bool = False
    apply_current: bool = False
    current_odds: Any = None
    current_updated_at: Optional[datetime] = None
    apply_source_limit: bool = False
    source_limit: Any = None
    metadata_updates: dict[str, Any] = field(default_factory=dict)
    applied_fields: tuple[str, ...] = ()
    stale_fields: tuple[str, ...] = ()
    conflicts: tuple[str, ...] = ()
    recalculate_movement: bool = False
    is_create: bool = False

    @property
    def has_conflicts(self) -> bool:
        return bool(self.conflicts)

    @property
    def has_mutations(self) -> bool:
        return bool(self.applied_fields) or self.is_create


def _values_equal(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return left == right


def _decide_current(
    *,
    existing: QuoteExistingState,
    candidate: QuoteCandidateState,
    mode: QuoteMergeMode,
) -> tuple[bool, Any, Optional[datetime], str | None, str | None]:
    """Return (apply, value, timestamp, stale_reason, conflict_reason).

    Live writes the current from this ingest. ``current_updated_at`` is persist
    time, not OddsPapi ``changedAt``; those clocks are not compared here.
    Backfill still orders by timestamp so it cannot clobber a live current.
    """
    if candidate.current_price is None:
        return False, None, None, None, None

    if existing.current_odds is None:
        return True, candidate.current_price, candidate.current_captured_at, None, None

    if mode is QuoteMergeMode.LIVE:
        if _values_equal(existing.current_odds, candidate.current_price):
            return False, None, None, None, None
        return True, candidate.current_price, candidate.current_captured_at, None, None

    existing_ts = existing.current_updated_at
    candidate_ts = candidate.current_captured_at

    if existing_ts is not None and candidate_ts is None:
        return False, None, None, "current", None

    if existing_ts is None and candidate_ts is not None:
        return False, None, None, "current", None

    if existing_ts is None and candidate_ts is None:
        if _values_equal(existing.current_odds, candidate.current_price):
            return False, None, None, None, None
        return False, None, None, None, "current"

    assert existing_ts is not None and candidate_ts is not None
    if candidate_ts > existing_ts:
        return True, candidate.current_price, candidate_ts, None, None
    if candidate_ts < existing_ts:
        return False, None, None, "current", None
    if _values_equal(existing.current_odds, candidate.current_price):
        return False, None, None, None, None
    return False, None, None, None, "current"


def _decide_initial(
    *,
    existing: QuoteExistingState,
    candidate: QuoteCandidateState,
    mode: QuoteMergeMode,
) -> tuple[bool, bool, Any, Optional[datetime], str | None]:
    """Return (apply_full, apply_ts_only, value, timestamp, conflict_reason)."""
    if candidate.initial_price is None:
        return False, False, None, None, None

    if existing.initial_odds is None:
        return True, False, candidate.initial_price, candidate.initial_captured_at, None

    if candidate.overwrite_initial and mode is QuoteMergeMode.LIVE:
        return True, False, candidate.initial_price, candidate.initial_captured_at, None

    if _values_equal(existing.initial_odds, candidate.initial_price):
        if (
            existing.initial_captured_at is None
            and candidate.initial_captured_at is not None
        ):
            return False, True, existing.initial_odds, candidate.initial_captured_at, None
        return False, False, None, None, None

    if mode is QuoteMergeMode.BACKFILL_FILL_ONLY:
        return False, False, None, None, "initial"
    # Live without overwrite_initial: keep existing, no conflict (fill-or-skip).
    return False, False, None, None, None


def _decide_metadata_field(
    *,
    field_name: str,
    existing_value: Any,
    candidate_value: Any,
) -> tuple[Optional[Any], Optional[str]]:
    """Return (update_value_or_None, conflict_or_None)."""
    if candidate_value is None:
        return None, None
    if existing_value is None:
        return candidate_value, None
    if existing_value == candidate_value:
        return None, None
    return None, field_name


def decide_quote_merge(
    *,
    existing: QuoteExistingState,
    candidate: QuoteCandidateState,
    mode: QuoteMergeMode = QuoteMergeMode.LIVE,
) -> QuoteMergeDecision:
    """Compute a pure merge decision without mutating anything."""
    if candidate.initial_price is None and candidate.current_price is None:
        return QuoteMergeDecision()

    applied: list[str] = []
    stale: list[str] = []
    conflicts: list[str] = []
    metadata_updates: dict[str, Any] = {}

    apply_initial, apply_ts_only, initial_odds, initial_ts, initial_conflict = (
        _decide_initial(existing=existing, candidate=candidate, mode=mode)
    )
    if initial_conflict:
        conflicts.append(initial_conflict)
    if apply_initial:
        applied.append("initial_odds")
        if initial_ts is not None:
            applied.append("initial_captured_at")
    elif apply_ts_only:
        applied.append("initial_captured_at")

    apply_current, current_odds, current_ts, current_stale, current_conflict = (
        _decide_current(existing=existing, candidate=candidate, mode=mode)
    )
    if current_stale:
        stale.append(current_stale)
    if current_conflict:
        conflicts.append(current_conflict)
    if apply_current:
        applied.append("current_odds")
        if current_ts is not None:
            applied.append("current_updated_at")

    for field_name, existing_value, candidate_value in (
        ("main_line", existing.main_line, candidate.main_line),
        ("source_market_id", existing.source_market_id, candidate.source_market_id),
        ("source_outcome_id", existing.source_outcome_id, candidate.source_outcome_id),
        (
            "bookmaker_outcome_id",
            existing.bookmaker_outcome_id,
            candidate.bookmaker_outcome_id,
        ),
    ):
        update_value, conflict = _decide_metadata_field(
            field_name=field_name,
            existing_value=existing_value,
            candidate_value=candidate_value,
        )
        if conflict:
            conflicts.append(f"metadata_{conflict}")
        elif update_value is not None:
            metadata_updates[field_name] = update_value
            applied.append(field_name)

    apply_source_limit = False
    source_limit = None
    if candidate.source_limit is not None:
        if apply_current:
            apply_source_limit = True
            source_limit = candidate.source_limit
            applied.append("source_limit")
        elif existing.source_limit is None:
            apply_source_limit = True
            source_limit = candidate.source_limit
            applied.append("source_limit")

    recalculate_movement = apply_initial or apply_current
    is_create = not existing.exists

    return QuoteMergeDecision(
        apply_initial=apply_initial,
        initial_odds=initial_odds,
        initial_captured_at=initial_ts if (apply_initial or apply_ts_only) else None,
        apply_initial_timestamp_only=apply_ts_only,
        apply_current=apply_current,
        current_odds=current_odds,
        current_updated_at=current_ts if apply_current else None,
        apply_source_limit=apply_source_limit,
        source_limit=source_limit,
        metadata_updates=metadata_updates,
        applied_fields=tuple(applied),
        stale_fields=tuple(stale),
        conflicts=tuple(conflicts),
        recalculate_movement=recalculate_movement,
        is_create=is_create,
    )


def existing_state_from_quote(quote: Any | None) -> QuoteExistingState:
    """Build an existing-state snapshot from an ORM quote or None."""
    if quote is None:
        return QuoteExistingState(exists=False)
    return QuoteExistingState(
        initial_odds=quote.initial_odds,
        initial_captured_at=quote.initial_captured_at,
        current_odds=quote.current_odds,
        current_updated_at=quote.current_updated_at,
        main_line=quote.main_line,
        source_market_id=quote.source_market_id,
        source_outcome_id=quote.source_outcome_id,
        bookmaker_outcome_id=quote.bookmaker_outcome_id,
        source_limit=quote.source_limit,
        exists=True,
    )


__all__ = [
    "QuoteCandidateState",
    "QuoteExistingState",
    "QuoteMergeDecision",
    "QuoteMergeMode",
    "decide_quote_merge",
    "existing_state_from_quote",
]
