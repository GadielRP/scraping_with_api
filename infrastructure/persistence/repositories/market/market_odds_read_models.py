"""Provider-neutral odds read contracts with provenance attached to each price."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .odds_movement import compute_movement


@dataclass(frozen=True, slots=True)
class QuotePriceOrigin:
    quote_id: int
    source: str
    exchange_side: str | None = None
    exchange_level: int = 0
    main_line: bool | None = None
    source_market_id: str | None = None
    source_outcome_id: str | None = None
    bookmaker_outcome_id: str | None = None
    source_limit: Decimal | None = None
    # Quote clocks: opening date from the provider versus local current update.
    initial_captured_at: datetime | None = None
    current_updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class OddsPrice:
    value: Decimal
    origin: QuotePriceOrigin


@dataclass(frozen=True, slots=True)
class ChoiceOddsState:
    choice_id: int
    choice_name: str
    opening: OddsPrice | None
    current: OddsPrice | None

    @property
    def movement(self) -> int | None:
        return compute_movement(
            initial_odds=self.opening.value if self.opening else None,
            current_odds=self.current.value if self.current else None,
        )


@dataclass(frozen=True, slots=True)
class MarketOddsState:
    event_id: int
    market_id: int
    bookie_id: int
    bookie_name: str
    market_type_id: int | None
    canonical_market_key: str | None
    market_name: str
    market_group: str | None
    market_period: str
    market_family: str | None
    # For spreads this is the HOME line; for totals it is the threshold.
    line_value: Decimal | None
    is_live: bool
    choices: tuple[ChoiceOddsState, ...]
    # Conventional prices compose sources. Exchange states retain source/side.
    source: str | None = None
    exchange_side: str | None = None


@dataclass(frozen=True, slots=True)
class MarketOddsReadDiagnostic:
    code: str
    blocking: bool
    market_id: int | None = None
    choice_id: int | None = None
    quote_ids: tuple[int, ...] = ()
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class MarketOddsReadResult:
    event_id: int
    markets: tuple[MarketOddsState, ...] = ()
    diagnostics: tuple[MarketOddsReadDiagnostic, ...] = ()

    @property
    def has_blocking_diagnostics(self) -> bool:
        return any(item.blocking for item in self.diagnostics)
