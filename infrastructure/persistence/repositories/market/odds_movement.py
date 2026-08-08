"""Pure odds-movement computation, shared by choice and quote writers.

Extracted from ``MarketRepository._choice_change`` (see
docs/refactors/db-schema-odds-refactor.md §7) so both ``MarketChoiceWriter``
and ``MarketChoiceQuoteWriter`` can compute the same -1/0/+1 movement
indicator without depending on the monolithic repository class.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Optional

_VALID_EXPLICIT_MOVEMENTS = {Decimal("-1"), Decimal("0"), Decimal("1")}


def _to_decimal_or_none(value) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def compute_movement(
    *,
    explicit_change=None,
    initial_odds=None,
    current_odds=None,
) -> Optional[int]:
    """Return -1/0/+1 movement, preferring an explicit provider-supplied value.

    Falls back to comparing initial vs current odds when no valid explicit
    value is given. Returns None when there isn't enough information yet
    (e.g. only initial_odds has arrived so far) — callers should treat None
    as "unknown", not as "unchanged".
    """
    normalized_explicit = _to_decimal_or_none(explicit_change)
    if normalized_explicit in _VALID_EXPLICIT_MOVEMENTS:
        return int(normalized_explicit)

    initial = _to_decimal_or_none(initial_odds)
    current = _to_decimal_or_none(current_odds)
    if initial is None or current is None:
        return None
    if current > initial:
        return 1
    if current < initial:
        return -1
    return 0


__all__ = ["compute_movement"]
