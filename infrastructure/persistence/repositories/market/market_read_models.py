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


@dataclass(frozen=True, slots=True)
class MarketReadShadowDifference:
    code: str
    blocking: bool
    identity: str
    detail: Optional[str] = None


@dataclass(frozen=True, slots=True)
class MarketReadShadowComparison:
    event_id: int
    legacy_blocks: int
    quote_blocks: int
    differences: Tuple[MarketReadShadowDifference, ...] = ()
    legacy_duration_ms: float = 0.0
    quote_duration_ms: float = 0.0

    @property
    def blocking_count(self) -> int:
        return sum(1 for item in self.differences if item.blocking)

    def counts_by_code(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in self.differences:
            counts[item.code] = counts.get(item.code, 0) + 1
        return counts

    def as_log_fields(self) -> dict:
        return {
            "event": "odds_quote_read_shadow",
            "consumer": "external_alerts",
            "event_id": self.event_id,
            "legacy_blocks": self.legacy_blocks,
            "quote_blocks": self.quote_blocks,
            "duration_legacy_ms": round(self.legacy_duration_ms, 3),
            "duration_quotes_ms": round(self.quote_duration_ms, 3),
            "diff_counts": self.counts_by_code(),
            "diff_blocking": self.blocking_count,
            "blocking_codes": sorted(
                {item.code for item in self.differences if item.blocking}
            ),
        }


__all__ = [
    "ExternalChoiceQuote",
    "ExternalMarketQuoteBlock",
    "ExternalMarketQuoteReadResult",
    "MarketQuoteReadDiagnostic",
    "MarketReadShadowComparison",
    "MarketReadShadowDifference",
    "QuoteFieldOrigin",
]
