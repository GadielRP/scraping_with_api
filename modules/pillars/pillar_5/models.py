"""Typed contracts used by the Pillar 5 snapshot policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from modules.pillars.market_coverage import PeriodDiagnostics
from modules.pillars.market_snapshot_extractor import QuotePoint

from .periods import (
    EXCHANGE_ODDS_INPUT_NAMES,
    EXCHANGE_SIZE_TRACE_INPUT_NAMES,
    FULL_TIME_PRICE_MEMORY_SCOPE,
    PriceMemoryPeriodScope,
    resolve_pillar_status,
)


@dataclass(frozen=True, slots=True)
class TwoWayMarketSnapshot:
    home: QuotePoint
    away: QuotePoint

    def is_complete(self) -> bool:
        return self.home is not None and self.away is not None


@dataclass(frozen=True, slots=True)
class ThreeWayMarketSnapshot:
    home: QuotePoint
    draw: QuotePoint | None
    away: QuotePoint

    def is_complete(self) -> bool:
        return self.home is not None and self.away is not None


@dataclass(frozen=True, slots=True)
class ExchangeSnapshot:
    back: ThreeWayMarketSnapshot | None
    lay: ThreeWayMarketSnapshot | None

    def is_complete(self) -> bool:
        return (
            self.back is not None
            and self.back.is_complete()
            and self.lay is not None
            and self.lay.is_complete()
        )


@dataclass(frozen=True, slots=True)
class P5FullTimeSnapshot:
    """Consolidated market reading for Pillar 5 Full Time price memory."""

    period: str | None
    period_scope: PriceMemoryPeriodScope
    pinnacle: ThreeWayMarketSnapshot | None = None
    bet365: ThreeWayMarketSnapshot | None = None
    sofascore: ThreeWayMarketSnapshot | None = None
    betfair: ExchangeSnapshot | None = None

    def has_any_input(self) -> bool:
        return any(
            item is not None
            for item in (self.pinnacle, self.bet365, self.sofascore, self.betfair)
        )

    def input_values(self) -> dict[str, float | None]:
        values: dict[str, float | None] = {
            name: None for name in self.period_scope.input_names()
        }

        # Standard bookies
        for book_snap, book_spec in (
            (self.pinnacle, self.period_scope.pinnacle),
            (self.bet365, self.period_scope.bet365),
            (self.sofascore, self.period_scope.sofascore),
        ):
            if book_snap is not None:
                if book_snap.home is not None:
                    values[book_spec.home] = float(book_snap.home.odds_price)
                if book_snap.away is not None:
                    values[book_spec.away] = float(book_snap.away.odds_price)
                if book_snap.draw is not None and book_spec.draw is not None:
                    values[book_spec.draw] = float(book_snap.draw.odds_price)

        # Betfair Exchange
        if self.betfair is not None:
            if self.betfair.back is not None:
                back = self.betfair.back
                if back.home is not None:
                    values["BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE"] = float(back.home.odds_price)
                    if back.home.exchange_size is not None:
                        values["BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE"] = float(back.home.exchange_size)
                if back.draw is not None:
                    values["BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE"] = float(back.draw.odds_price)
                    if back.draw.exchange_size is not None:
                        values["BF_DRAW_BACK_1X2_FULL_TIME_EXCHANGE_SIZE"] = float(back.draw.exchange_size)
                if back.away is not None:
                    values["BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE"] = float(back.away.odds_price)
                    if back.away.exchange_size is not None:
                        values["BF_AWAY_BACK_1X2_FULL_TIME_EXCHANGE_SIZE"] = float(back.away.exchange_size)

            if self.betfair.lay is not None:
                lay = self.betfair.lay
                if lay.home is not None:
                    values["BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE"] = float(lay.home.odds_price)
                    if lay.home.exchange_size is not None:
                        values["BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE"] = float(lay.home.exchange_size)
                if lay.draw is not None:
                    values["BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE"] = float(lay.draw.odds_price)
                    if lay.draw.exchange_size is not None:
                        values["BF_DRAW_LAY_1X2_FULL_TIME_EXCHANGE_SIZE"] = float(lay.draw.exchange_size)
                if lay.away is not None:
                    values["BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE"] = float(lay.away.odds_price)
                    if lay.away.exchange_size is not None:
                        values["BF_AWAY_LAY_1X2_FULL_TIME_EXCHANGE_SIZE"] = float(lay.away.exchange_size)

        return values

    def traces(self) -> dict[str, dict[str, Any]]:
        traces: dict[str, dict[str, Any]] = {}
        for book_snap, book_spec in (
            (self.pinnacle, self.period_scope.pinnacle),
            (self.bet365, self.period_scope.bet365),
            (self.sofascore, self.period_scope.sofascore),
        ):
            if book_snap is not None:
                if book_snap.home is not None:
                    traces[book_spec.home] = book_snap.home.trace.to_dict()
                if book_snap.away is not None:
                    traces[book_spec.away] = book_snap.away.trace.to_dict()
                if book_snap.draw is not None and book_spec.draw is not None:
                    traces[book_spec.draw] = book_snap.draw.trace.to_dict()

        if self.betfair is not None:
            if self.betfair.back is not None:
                back = self.betfair.back
                if back.home is not None:
                    traces["BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE"] = back.home.trace.to_dict()
                if back.draw is not None:
                    traces["BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE"] = back.draw.trace.to_dict()
                if back.away is not None:
                    traces["BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE"] = back.away.trace.to_dict()
            if self.betfair.lay is not None:
                lay = self.betfair.lay
                if lay.home is not None:
                    traces["BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE"] = lay.home.trace.to_dict()
                if lay.draw is not None:
                    traces["BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE"] = lay.draw.trace.to_dict()
                if lay.away is not None:
                    traces["BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE"] = lay.away.trace.to_dict()

        return traces


@dataclass(frozen=True)
class P5ExtractionResult:
    """Aggregated output of snapshot extraction for Pillar 5."""

    target_minute: int | None
    full_time_snapshot: P5FullTimeSnapshot | None
    full_time: PeriodDiagnostics
    abort_reason: str | None = None
    missing_inputs: tuple[str, ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()
    extraction_diagnostics: dict[str, Any] = field(default_factory=dict)

    @property
    def snapshot(self) -> P5FullTimeSnapshot | None:
        return self.full_time_snapshot if self.full_time.usable else None

    @property
    def status(self) -> str:
        return resolve_pillar_status(
            required_complete=self.full_time.status == "COMPLETE",
            required_usable=self.full_time.usable,
            optional_complete=True,
        )

    def period_diagnostics(self) -> dict[str, Any]:
        return {"full_time": self.full_time.to_dict()}


__all__ = [
    "ExchangeSnapshot",
    "P5ExtractionResult",
    "P5FullTimeSnapshot",
    "ThreeWayMarketSnapshot",
    "TwoWayMarketSnapshot",
]
