"""Typed read contracts for quote-aware market consumers.

The persistence layer returns domain values and provenance, never formatter
glyphs.  Alert text, debug JSON and other presentation concerns live in their
respective consumers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional, Tuple


@dataclass(frozen=True, slots=True)
class QuoteFieldOrigin:
    quote_id: int
    source: str
    captured_at: Optional[datetime]


@dataclass(frozen=True, slots=True)
class ExternalChoiceQuote:
    choice_id: int
    choice_name: str
    exchange_level: Optional[int]
    initial: Optional[Decimal]
    current: Optional[Decimal]
    movement: Optional[int]
    initial_origin: Optional[QuoteFieldOrigin]
    current_origin: Optional[QuoteFieldOrigin]


@dataclass(frozen=True, slots=True)
class ExternalMarketQuoteBlock:
    market_id: int
    bookie_id: int
    bookie_name: str
    market_name: str
    market_group: Optional[str]
    market_period: str
    choice_group: Optional[str]
    is_live: bool
    aggregation: Literal["field_priority", "exchange"]
    source: Optional[str]
    exchange_side: Optional[str]
    contributing_sources: Tuple[str, ...]
    choices: Tuple[ExternalChoiceQuote, ...]


@dataclass(frozen=True, slots=True)
class MarketQuoteReadDiagnostic:
    code: str
    blocking: bool
    market_id: Optional[int] = None
    choice_id: Optional[int] = None
    quote_ids: Tuple[int, ...] = ()
    detail: Optional[str] = None


@dataclass(frozen=True, slots=True)
class ExternalMarketQuoteReadResult:
    event_id: int
    blocks: Tuple[ExternalMarketQuoteBlock, ...] = ()
    diagnostics: Tuple[MarketQuoteReadDiagnostic, ...] = ()

    @property
    def has_blocking_diagnostics(self) -> bool:
        return any(item.blocking for item in self.diagnostics)


__all__ = [
    "ExternalChoiceQuote",
    "ExternalMarketQuoteBlock",
    "ExternalMarketQuoteReadResult",
    "MarketQuoteReadDiagnostic",
    "QuoteFieldOrigin",
]
