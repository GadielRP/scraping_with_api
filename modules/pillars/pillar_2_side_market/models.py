"""Typed contracts used by the Pillar 2 snapshot policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from modules.pillars.market_snapshot_extractor import QuotePoint

from .periods import (
    EXCHANGE_AH_LINE_INPUT_NAME,
    EXCHANGE_AH_ODDS_INPUT_NAMES,
    EXCHANGE_AH_SIZE_TRACE_INPUT_NAMES,
    EXCHANGE_AH_1H_LINE_INPUT_NAME,
    EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
    EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
    FIRST_HALF_SIDE_SCOPE,
    resolve_period_status,
)


@dataclass(frozen=True, slots=True)
class PeriodDiagnostics:
    """Local completeness outcome for one independently validated period."""

    status: str
    missing_inputs: tuple[str, ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "missing_inputs": list(self.missing_inputs),
            "invalid_inputs": list(self.invalid_inputs),
            "ambiguous_inputs": list(self.ambiguous_inputs),
        }

    @classmethod
    def from_gate(
        cls,
        *,
        complete: bool,
        missing_inputs: tuple[str, ...] | list[str] | set[str] = (),
        invalid_inputs: tuple[str, ...] | list[str] | set[str] = (),
        ambiguous_inputs: tuple[str, ...] | list[str] | set[str] = (),
    ) -> "PeriodDiagnostics":
        missing = tuple(sorted(missing_inputs))
        invalid = tuple(sorted(invalid_inputs))
        ambiguous = tuple(sorted(ambiguous_inputs))
        return cls(
            status=resolve_period_status(
                complete=complete,
                missing_inputs=missing,
                invalid_inputs=invalid,
                ambiguous_inputs=ambiguous,
            ),
            missing_inputs=missing,
            invalid_inputs=invalid,
            ambiguous_inputs=ambiguous,
        )

    @classmethod
    def empty(cls) -> "PeriodDiagnostics":
        return cls.from_gate(complete=False)


@dataclass(frozen=True, slots=True)
class TwoWayMarketSnapshot:
    home: QuotePoint
    away: QuotePoint


@dataclass(frozen=True, slots=True)
class ThreeWayMarketSnapshot:
    home: QuotePoint
    draw: QuotePoint
    away: QuotePoint


@dataclass(frozen=True, slots=True)
class AsianHandicapSnapshot(TwoWayMarketSnapshot):
    home_line: Decimal


@dataclass(frozen=True, slots=True)
class PartialTwoWayMarketSnapshot:
    """One bookmaker branch that may retain only one side of a 1H market."""

    home: QuotePoint | None
    away: QuotePoint | None

    def is_complete(self) -> bool:
        return self.home is not None and self.away is not None

    def has_any_input(self) -> bool:
        return self.home is not None or self.away is not None


@dataclass(frozen=True, slots=True)
class PartialAsianHandicapSnapshot(PartialTwoWayMarketSnapshot):
    home_line: Decimal | None

    def is_complete(self) -> bool:
        return self.home is not None and self.away is not None and self.home_line is not None

    def has_any_input(self) -> bool:
        return self.home is not None or self.away is not None or self.home_line is not None


@dataclass(frozen=True, slots=True)
class PartialAsianHandicapExchangeSnapshot:
    """Optional Betfair AH FT readings, retaining BACK and LAY independently."""

    back: PartialAsianHandicapSnapshot | None
    lay: PartialAsianHandicapSnapshot | None

    @property
    def line(self) -> Decimal | None:
        lines = [branch.home_line for branch in (self.back, self.lay) if branch is not None]
        for value in lines:
            if value is not None:
                return value
        return None

    @property
    def lines_match(self) -> bool:
        return (
            self.back is not None
            and self.lay is not None
            and self.back.home_line is not None
            and self.back.home_line == self.lay.home_line
        )

    def has_any_input(self) -> bool:
        return any(branch is not None and branch.has_any_input() for branch in (self.back, self.lay))

    def input_values(
        self,
        *,
        line_name: str = EXCHANGE_AH_LINE_INPUT_NAME,
        odds_names: tuple[str, ...] = EXCHANGE_AH_ODDS_INPUT_NAMES,
        size_names: tuple[str, ...] = EXCHANGE_AH_SIZE_TRACE_INPUT_NAMES,
    ) -> dict[str, Decimal | None]:
        def point(side: str, choice: str) -> QuotePoint | None:
            branch = getattr(self, side)
            return None if branch is None else getattr(branch, choice)

        def price(value: QuotePoint | None) -> Decimal | None:
            return None if value is None else value.odds_price

        def size(value: QuotePoint | None) -> Decimal | None:
            return None if value is None else value.exchange_size

        return {
            line_name: self.line,
            odds_names[0]: price(point("back", "home")),
            odds_names[1]: price(point("back", "away")),
            odds_names[2]: price(point("lay", "home")),
            odds_names[3]: price(point("lay", "away")),
            size_names[0]: size(point("back", "home")),
            size_names[1]: size(point("back", "away")),
            size_names[2]: size(point("lay", "home")),
            size_names[3]: size(point("lay", "away")),
        }

    def input_trace(
        self,
        *,
        line_name: str = EXCHANGE_AH_LINE_INPUT_NAME,
        odds_names: tuple[str, ...] = EXCHANGE_AH_ODDS_INPUT_NAMES,
        size_names: tuple[str, ...] = EXCHANGE_AH_SIZE_TRACE_INPUT_NAMES,
    ) -> dict[str, dict[str, Any]]:
        def point(side: str, choice: str) -> QuotePoint | None:
            branch = getattr(self, side)
            return None if branch is None else getattr(branch, choice)

        points = {
            odds_names[0]: point("back", "home"),
            odds_names[1]: point("back", "away"),
            odds_names[2]: point("lay", "home"),
            odds_names[3]: point("lay", "away"),
            size_names[0]: point("back", "home"),
            size_names[1]: point("back", "away"),
            size_names[2]: point("lay", "home"),
            size_names[3]: point("lay", "away"),
        }
        line_point = next((value for value in points.values() if value is not None), None)
        if line_point is not None:
            points[line_name] = line_point
        return {name: point.trace.to_dict() for name, point in points.items() if point is not None}


@dataclass(frozen=True, slots=True)
class ExchangeSnapshot:
    back: ThreeWayMarketSnapshot
    lay: ThreeWayMarketSnapshot


@dataclass(frozen=True, slots=True)
class P2FullTimeSnapshot:
    """Complete Full Time block required to produce the P2 signal."""

    pinnacle_1x2: TwoWayMarketSnapshot # PIN
    bet365_1x2: TwoWayMarketSnapshot # B365
    pinnacle_ah: AsianHandicapSnapshot
    bet365_ah: AsianHandicapSnapshot
    betfair_1x2: ExchangeSnapshot # BF
    betfair_ah: PartialAsianHandicapExchangeSnapshot | None = None

    def input_values(self) -> dict[str, Decimal | None]:
        bf_back = self.betfair_1x2.back
        bf_lay = self.betfair_1x2.lay
        values = {
            "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE": self.pinnacle_1x2.home.odds_price,
            "PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE": self.pinnacle_1x2.away.odds_price,
            "B365_HOME_1X2_FULL_TIME_ODDS_PRICE": self.bet365_1x2.home.odds_price,
            "B365_AWAY_1X2_FULL_TIME_ODDS_PRICE": self.bet365_1x2.away.odds_price,
            "PIN_AH_FULL_TIME_LINE": self.pinnacle_ah.home_line,
            "PIN_AH_HOME_FULL_TIME_ODDS_PRICE": self.pinnacle_ah.home.odds_price,
            "PIN_AH_AWAY_FULL_TIME_ODDS_PRICE": self.pinnacle_ah.away.odds_price,
            "B365_AH_FULL_TIME_LINE": self.bet365_ah.home_line,
            "B365_AH_HOME_FULL_TIME_ODDS_PRICE": self.bet365_ah.home.odds_price,
            "B365_AH_AWAY_FULL_TIME_ODDS_PRICE": self.bet365_ah.away.odds_price,
            "BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE": bf_back.home.odds_price,
            "BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE": bf_lay.home.odds_price,
            "BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE": bf_back.draw.odds_price,
            "BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE": bf_lay.draw.odds_price,
            "BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE": bf_back.away.odds_price,
            "BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE": bf_lay.away.odds_price,
            "BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE": bf_back.home.exchange_size,
            "BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE": bf_lay.home.exchange_size,
            "BF_DRAW_BACK_1X2_FULL_TIME_EXCHANGE_SIZE": bf_back.draw.exchange_size,
            "BF_DRAW_LAY_1X2_FULL_TIME_EXCHANGE_SIZE": bf_lay.draw.exchange_size,
            "BF_AWAY_BACK_1X2_FULL_TIME_EXCHANGE_SIZE": bf_back.away.exchange_size,
            "BF_AWAY_LAY_1X2_FULL_TIME_EXCHANGE_SIZE": bf_lay.away.exchange_size,
        }
        ah = self.betfair_ah
        values.update(
            ah.input_values()
            if ah is not None
            else {
                name: None
                for name in (
                    EXCHANGE_AH_LINE_INPUT_NAME,
                    *EXCHANGE_AH_ODDS_INPUT_NAMES,
                    *EXCHANGE_AH_SIZE_TRACE_INPUT_NAMES,
                )
            }
        )
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        bf_back = self.betfair_1x2.back
        bf_lay = self.betfair_1x2.lay
        points = {
            "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE": self.pinnacle_1x2.home,
            "PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE": self.pinnacle_1x2.away,
            "B365_HOME_1X2_FULL_TIME_ODDS_PRICE": self.bet365_1x2.home,
            "B365_AWAY_1X2_FULL_TIME_ODDS_PRICE": self.bet365_1x2.away,
            "PIN_AH_FULL_TIME_LINE": self.pinnacle_ah.home,
            "PIN_AH_HOME_FULL_TIME_ODDS_PRICE": self.pinnacle_ah.home,
            "PIN_AH_AWAY_FULL_TIME_ODDS_PRICE": self.pinnacle_ah.away,
            "B365_AH_FULL_TIME_LINE": self.bet365_ah.home,
            "B365_AH_HOME_FULL_TIME_ODDS_PRICE": self.bet365_ah.home,
            "B365_AH_AWAY_FULL_TIME_ODDS_PRICE": self.bet365_ah.away,
            "BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE": bf_back.home,
            "BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE": bf_lay.home,
            "BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE": bf_back.draw,
            "BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE": bf_lay.draw,
            "BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE": bf_back.away,
            "BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE": bf_lay.away,
            "BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE": bf_back.home,
            "BF_HOME_LAY_1X2_FULL_TIME_EXCHANGE_SIZE": bf_lay.home,
            "BF_DRAW_BACK_1X2_FULL_TIME_EXCHANGE_SIZE": bf_back.draw,
            "BF_DRAW_LAY_1X2_FULL_TIME_EXCHANGE_SIZE": bf_lay.draw,
            "BF_AWAY_BACK_1X2_FULL_TIME_EXCHANGE_SIZE": bf_back.away,
            "BF_AWAY_LAY_1X2_FULL_TIME_EXCHANGE_SIZE": bf_lay.away,
        }
        ah = self.betfair_ah
        if ah is not None:
            return {**{name: point.trace.to_dict() for name, point in points.items()}, **ah.input_trace()}
        return {name: point.trace.to_dict() for name, point in points.items()}


@dataclass(frozen=True, slots=True)
class P2FirstHalfSnapshot:
    """Granular First Half block used only to enrich a valid Full Time signal."""

    pinnacle_1x2: PartialTwoWayMarketSnapshot | None
    bet365_1x2: PartialTwoWayMarketSnapshot | None
    pinnacle_ah: PartialAsianHandicapSnapshot | None
    bet365_ah: PartialAsianHandicapSnapshot | None
    betfair_ah: PartialAsianHandicapExchangeSnapshot | None = None

    def is_complete(self) -> bool:
        return all(
            branch is not None and branch.is_complete()
            for branch in (
                self.pinnacle_1x2,
                self.bet365_1x2,
                self.pinnacle_ah,
                self.bet365_ah,
            )
        )

    def has_any_input(self) -> bool:
        return any(
            branch is not None and branch.has_any_input()
            for branch in (
                self.pinnacle_1x2,
                self.bet365_1x2,
                self.pinnacle_ah,
                self.bet365_ah,
                self.betfair_ah,
            )
        )

    def input_values(self) -> dict[str, Decimal | None]:
        values = {name: None for name in FIRST_HALF_SIDE_SCOPE.input_names()}
        branches = (
            (self.pinnacle_1x2, None, "PIN_HOME_1X2_1H_ODDS_PRICE", "PIN_AWAY_1X2_1H_ODDS_PRICE"),
            (self.bet365_1x2, None, "B365_HOME_1X2_1H_ODDS_PRICE", "B365_AWAY_1X2_1H_ODDS_PRICE"),
            (self.pinnacle_ah, "PIN_AH_1H_LINE", "PIN_AH_1H_HOME_PRICE", "PIN_AH_1H_AWAY_PRICE"),
            (self.bet365_ah, "B365_AH_1H_LINE", "B365_AH_1H_HOME_PRICE", "B365_AH_1H_AWAY_PRICE"),
        )
        for branch, line_name, home_name, away_name in branches:
            if branch is None:
                continue
            values[home_name] = None if branch.home is None else branch.home.odds_price
            values[away_name] = None if branch.away is None else branch.away.odds_price
            if line_name is not None:
                assert isinstance(branch, PartialAsianHandicapSnapshot)
                values[line_name] = branch.home_line
        if self.betfair_ah is not None:
            values.update(
                self.betfair_ah.input_values(
                    line_name=EXCHANGE_AH_1H_LINE_INPUT_NAME,
                    odds_names=EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
                    size_names=EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
                )
            )
        else:
            values.update({
                name: None
                for name in (
                    EXCHANGE_AH_1H_LINE_INPUT_NAME,
                    *EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
                    *EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
                )
            })
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        traces: dict[str, dict[str, Any]] = {}
        branches = (
            (self.pinnacle_1x2, None, "PIN_HOME_1X2_1H_ODDS_PRICE", "PIN_AWAY_1X2_1H_ODDS_PRICE"),
            (self.bet365_1x2, None, "B365_HOME_1X2_1H_ODDS_PRICE", "B365_AWAY_1X2_1H_ODDS_PRICE"),
            (self.pinnacle_ah, "PIN_AH_1H_LINE", "PIN_AH_1H_HOME_PRICE", "PIN_AH_1H_AWAY_PRICE"),
            (self.bet365_ah, "B365_AH_1H_LINE", "B365_AH_1H_HOME_PRICE", "B365_AH_1H_AWAY_PRICE"),
        )
        for branch, line_name, home_name, away_name in branches:
            if branch is None:
                continue
            if branch.home is not None:
                traces[home_name] = branch.home.trace.to_dict()
            if branch.away is not None:
                traces[away_name] = branch.away.trace.to_dict()
            line_anchor = branch.home or branch.away
            if line_name is not None and line_anchor is not None:
                traces[line_name] = line_anchor.trace.to_dict()
        if self.betfair_ah is not None:
            traces.update(
                self.betfair_ah.input_trace(
                    line_name=EXCHANGE_AH_1H_LINE_INPUT_NAME,
                    odds_names=EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
                    size_names=EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
                )
            )
        return traces


@dataclass(frozen=True, slots=True)
class P2MarketSnapshot:
    """Canonical P2 snapshot: Full Time is required, First Half is optional."""

    target_minute: int
    full_time: P2FullTimeSnapshot
    first_half: P2FirstHalfSnapshot | None = None

    def input_values(self) -> dict[str, Decimal | None]:
        values: dict[str, Decimal | None] = dict(self.full_time.input_values())
        if self.first_half is not None:
            values.update(self.first_half.input_values())
            return values
        for name in FIRST_HALF_SIDE_SCOPE.input_names():
            values[name] = None
        for name in (
            EXCHANGE_AH_1H_LINE_INPUT_NAME,
            *EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
            *EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
        ):
            values[name] = None
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        traces = dict(self.full_time.input_trace())
        if self.first_half is not None:
            traces.update(self.first_half.input_trace())
        return traces


@dataclass(frozen=True, slots=True)
class P2ExtractionResult:
    """Outcome of strict Full Time validation and granular First Half extraction."""

    target_minute: int | None
    full_time: PeriodDiagnostics
    first_half: PeriodDiagnostics
    exchange_ah: PeriodDiagnostics = field(default_factory=PeriodDiagnostics.empty)
    exchange_ah_1h: PeriodDiagnostics = field(default_factory=PeriodDiagnostics.empty)
    full_time_snapshot: P2FullTimeSnapshot | None = None
    first_half_snapshot: P2FirstHalfSnapshot | None = None
    exchange_ah_snapshot: PartialAsianHandicapExchangeSnapshot | None = None
    abort_reason: str | None = None

    @property
    def snapshot(self) -> P2MarketSnapshot | None:
        if self.target_minute is None or self.full_time_snapshot is None:
            return None
        return P2MarketSnapshot(
            target_minute=self.target_minute,
            full_time=self.full_time_snapshot,
            first_half=self.first_half_snapshot,
        )

    def period_diagnostics(self) -> dict[str, Any]:
        return {
            "full_time": self.full_time.to_dict(),
            "first_half": self.first_half.to_dict(),
            "exchange_ah": self.exchange_ah.to_dict(),
            "exchange_ah_1h": self.exchange_ah_1h.to_dict(),
        }

    @property
    def missing_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(set(self.full_time.missing_inputs) | set(self.first_half.missing_inputs) | set(self.exchange_ah.missing_inputs) | set(self.exchange_ah_1h.missing_inputs))
        )

    @property
    def invalid_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(set(self.full_time.invalid_inputs) | set(self.first_half.invalid_inputs) | set(self.exchange_ah.invalid_inputs) | set(self.exchange_ah_1h.invalid_inputs))
        )

    @property
    def ambiguous_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                set(self.full_time.ambiguous_inputs)
                | set(self.first_half.ambiguous_inputs)
                | set(self.exchange_ah.ambiguous_inputs)
                | set(self.exchange_ah_1h.ambiguous_inputs)
            )
        )
