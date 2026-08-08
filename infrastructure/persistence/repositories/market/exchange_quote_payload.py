"""Shared wire contract for exchange (back/lay) quotes.

Both Oddspapi and OddsPortal adapters need to describe a two-sided exchange
price (Betfair back/lay) to the persistence layer. Before this refactor each
provider used a different, ad-hoc shape (Oddspapi: a loose ``exchangeQuotes``
list of dicts; OddsPortal: duplicate ``Market`` rows keyed by
``choice_group='Back'/'Lay'``). ``ExchangeQuotePayload`` is the single typed
shape both adapters build going forward, consumed uniformly by
``MarketChoiceQuoteWriter``.

See docs/refactors/db-schema-odds-refactor.md §5-§7 for the full rationale.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

#: Non-exchange bookies (single price, no back/lay split) use side=None,
#: the same NULL-for-"not applicable" convention as Market.choice_group.
#: Real-world uniqueness for NULL sides is enforced with a functional
#: COALESCE(exchange_side, '') index - see MarketChoiceQuote in models.py.
_VALID_SIDES = {None, "back", "lay"}


@dataclass(frozen=True, slots=True)
class ExchangeQuotePayload:
    """One priced instrument for a choice: a specific source, side and depth level."""

    side: Optional[str] = None
    level: int = 0
    price: Optional[float] = None
    size: Optional[float] = None
    main_line: Optional[bool] = None

    def __post_init__(self) -> None:
        normalized_side = str(self.side).strip().lower() if self.side else None
        if normalized_side not in _VALID_SIDES:
            raise ValueError(
                f"Invalid exchange_side={self.side!r}; expected one of {sorted(filter(None, _VALID_SIDES))} or None"
            )
        object.__setattr__(self, "side", normalized_side)

    def as_dict(self) -> dict:
        return {
            "side": self.side,
            "level": self.level,
            "price": self.price,
            "size": self.size,
            "mainLine": self.main_line,
        }


__all__ = ["ExchangeQuotePayload"]
