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

#: Sentinel used for non-exchange bookies (single price, no back/lay split).
#: Must match the DB default on MarketChoiceQuote.exchange_side, since
#: Postgres treats NULL != NULL in UNIQUE constraints.
SINGLE_SIDE = "single"

_VALID_SIDES = {SINGLE_SIDE, "back", "lay"}


@dataclass(frozen=True, slots=True)
class ExchangeQuotePayload:
    """One priced instrument for a choice: a specific source, side and depth level."""

    side: str = SINGLE_SIDE
    level: int = 0
    price: Optional[float] = None
    size: Optional[float] = None
    main_line: Optional[bool] = None

    def __post_init__(self) -> None:
        normalized_side = str(self.side or SINGLE_SIDE).strip().lower()
        if normalized_side not in _VALID_SIDES:
            raise ValueError(
                f"Invalid exchange_side={self.side!r}; expected one of {sorted(_VALID_SIDES)}"
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


__all__ = ["ExchangeQuotePayload", "SINGLE_SIDE"]
