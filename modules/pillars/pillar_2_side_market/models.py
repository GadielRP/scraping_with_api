"""Typed contracts used by the Pillar 2 snapshot policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from modules.pillars.market_snapshot_extractor import QuotePoint
from modules.pillars.market_coverage import PeriodDiagnostics

from .periods import (
    EXCHANGE_AH_LINE_INPUT_NAME,
    EXCHANGE_AH_ODDS_INPUT_NAMES,
    EXCHANGE_AH_SIZE_TRACE_INPUT_NAMES,
    EXCHANGE_AH_1H_LINE_INPUT_NAME,
    EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
    EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
    EXCHANGE_HANDICAP_LINE_INPUT_NAME,
    EXCHANGE_HANDICAP_ODDS_INPUT_NAMES,
    EXCHANGE_HANDICAP_SIZE_TRACE_INPUT_NAMES,
    EXCHANGE_HANDICAP_1H_LINE_INPUT_NAME,
    EXCHANGE_HANDICAP_1H_ODDS_INPUT_NAMES,
    EXCHANGE_HANDICAP_1H_SIZE_TRACE_INPUT_NAMES,
    FIRST_HALF_SIDE_SCOPE,
    FULL_TIME_SIDE_SCOPE,
    SidePeriodScope,
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
    draw: QuotePoint
    away: QuotePoint


@dataclass(frozen=True, slots=True)
class AsianHandicapSnapshot(TwoWayMarketSnapshot):
    home_line: Decimal

    def is_complete(self) -> bool:
        return (
            self.home is not None
            and self.away is not None
            and self.home_line is not None
        )


@dataclass(frozen=True, slots=True)
class HandicapSnapshot(AsianHandicapSnapshot):
    """Specific snapshot contract for standard/run line Handicap markets."""

    pass


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
        return (
            self.home is not None
            and self.away is not None
            and self.home_line is not None
        )

    def has_any_input(self) -> bool:
        return (
            self.home is not None or self.away is not None or self.home_line is not None
        )


@dataclass(frozen=True, slots=True)
class PartialHandicapSnapshot(PartialAsianHandicapSnapshot):
    """Specific partial snapshot for standard/run line Handicap markets."""

    pass


@dataclass(frozen=True, slots=True)
class PartialAsianHandicapExchangeSnapshot:
    """Optional Betfair AH FT readings, retaining BACK and LAY independently."""

    back: PartialAsianHandicapSnapshot | None
    lay: PartialAsianHandicapSnapshot | None

    @property
    def line(self) -> Decimal | None:
        lines = [
            branch.home_line for branch in (self.back, self.lay) if branch is not None
        ]
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
        return any(
            branch is not None and branch.has_any_input()
            for branch in (self.back, self.lay)
        )

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
        line_point = next(
            (value for value in points.values() if value is not None), None
        )
        if line_point is not None:
            points[line_name] = line_point
        return {
            name: point.trace.to_dict()
            for name, point in points.items()
            if point is not None
        }


@dataclass(frozen=True, slots=True)
class ExchangeSnapshot:
    back: ThreeWayMarketSnapshot | TwoWayMarketSnapshot
    lay: ThreeWayMarketSnapshot | TwoWayMarketSnapshot


@dataclass(frozen=True, slots=True)
class P2FullTimeSnapshot:
    """Independent Full Time bookie blocks, including unavailable branches."""

    pinnacle_1x2: TwoWayMarketSnapshot | None
    bet365_1x2: TwoWayMarketSnapshot | None
    pinnacle_ah: AsianHandicapSnapshot | None
    bet365_ah: AsianHandicapSnapshot | None
    pinnacle_handicap: HandicapSnapshot | None
    bet365_handicap: HandicapSnapshot | None
    betfair_1x2: ExchangeSnapshot | None
    betfair_ah: PartialAsianHandicapExchangeSnapshot | None = None
    betfair_handicap: PartialAsianHandicapExchangeSnapshot | None = None
    spread_market_type: str = "asian_handicap"

    def input_values(self) -> dict[str, Decimal | None]:
        values, _ = _side_inputs(self, FULL_TIME_SIDE_SCOPE, include_trace=False)
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        _, traces = _side_inputs(self, FULL_TIME_SIDE_SCOPE, include_trace=True)
        return traces


@dataclass(frozen=True, slots=True)
class P2FirstHalfSnapshot:
    """Granular First Half block used only to enrich a valid Full Time signal."""

    pinnacle_1x2: PartialTwoWayMarketSnapshot | None
    bet365_1x2: PartialTwoWayMarketSnapshot | None
    pinnacle_ah: PartialAsianHandicapSnapshot | None
    bet365_ah: PartialAsianHandicapSnapshot | None
    pinnacle_handicap: PartialHandicapSnapshot | None
    bet365_handicap: PartialHandicapSnapshot | None
    betfair_ah: PartialAsianHandicapExchangeSnapshot | None = None
    betfair_handicap: PartialAsianHandicapExchangeSnapshot | None = None
    spread_market_type: str = "asian_handicap"

    def is_complete(self) -> bool:
        return all(
            side_bookie_complete(
                getattr(self, f"{book}_1x2"),
                getattr(self, f"{book}_ah"),
                getattr(self, f"{book}_handicap"),
            )
            for book in ("pinnacle", "bet365")
        )

    def has_any_input(self) -> bool:
        return any(
            branch is not None and branch.has_any_input()
            for branch in (
                self.pinnacle_1x2,
                self.bet365_1x2,
                self.pinnacle_ah,
                self.bet365_ah,
                self.pinnacle_handicap,
                self.bet365_handicap,
                self.betfair_ah,
                self.betfair_handicap,
            )
        )

    def input_values(self) -> dict[str, Decimal | None]:
        values, _ = _side_inputs(self, FIRST_HALF_SIDE_SCOPE, include_trace=False)
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        _, traces = _side_inputs(self, FIRST_HALF_SIDE_SCOPE, include_trace=True)
        return traces


def side_bookie_complete(
    one_x_two: TwoWayMarketSnapshot | PartialTwoWayMarketSnapshot | None,
    asian_handicap: AsianHandicapSnapshot | PartialAsianHandicapSnapshot | None,
    handicap: HandicapSnapshot | PartialHandicapSnapshot | None,
) -> bool:
    return (
        one_x_two is not None
        and one_x_two.is_complete()
        and any(
            branch is not None and branch.is_complete()
            for branch in (asian_handicap, handicap)
        )
    )


def _side_inputs(
    snapshot: P2FullTimeSnapshot | P2FirstHalfSnapshot,
    scope: SidePeriodScope,
    *,
    include_trace: bool,
) -> tuple[dict[str, Decimal | None], dict[str, dict[str, Any]]]:
    values = {name: None for name in scope.input_names()}
    traces = {}

    def assign(name: str, point: QuotePoint | None, value: Decimal | None) -> None:
        values[name] = value
        if include_trace and point is not None:
            traces[name] = point.trace.to_dict()

    for family, spec in (
        ("1x2", scope.one_x_two),
        ("ah", scope.asian_handicap),
        ("handicap", scope.handicap),
    ):
        if spec is None:
            continue
        for book in ("pinnacle", "bet365"):
            branch = getattr(snapshot, f"{book}_{family}")
            if branch is None:
                continue
            for side in ("home", "away"):
                point = getattr(branch, side)
                assign(
                    getattr(spec, f"{book}_{side}"),
                    point,
                    None if point is None else point.odds_price,
                )
            line_name = getattr(spec, f"{book}_line")
            if line_name is not None:
                assign(line_name, branch.home or branch.away, branch.home_line)
    if scope.includes_exchange and snapshot.betfair_1x2 is not None:
        for side in ("back", "lay"):
            branch = getattr(snapshot.betfair_1x2, side)
            for choice in ("home", "draw", "away"):
                point = getattr(branch, choice, None)
                prefix = f"BF_{choice.upper()}_{side.upper()}_1X2_FULL_TIME"
                assign(
                    f"{prefix}_ODDS_PRICE",
                    point,
                    None if point is None else point.odds_price,
                )
                assign(
                    f"{prefix}_EXCHANGE_SIZE",
                    point,
                    None if point is None else point.exchange_size,
                )
    optional = (
        (
            (
                snapshot.betfair_ah,
                EXCHANGE_AH_LINE_INPUT_NAME,
                EXCHANGE_AH_ODDS_INPUT_NAMES,
                EXCHANGE_AH_SIZE_TRACE_INPUT_NAMES,
            ),
            (
                snapshot.betfair_handicap,
                EXCHANGE_HANDICAP_LINE_INPUT_NAME,
                EXCHANGE_HANDICAP_ODDS_INPUT_NAMES,
                EXCHANGE_HANDICAP_SIZE_TRACE_INPUT_NAMES,
            ),
        )
        if scope.includes_exchange
        else (
            (
                snapshot.betfair_ah,
                EXCHANGE_AH_1H_LINE_INPUT_NAME,
                EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
                EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
            ),
            (
                snapshot.betfair_handicap,
                EXCHANGE_HANDICAP_1H_LINE_INPUT_NAME,
                EXCHANGE_HANDICAP_1H_ODDS_INPUT_NAMES,
                EXCHANGE_HANDICAP_1H_SIZE_TRACE_INPUT_NAMES,
            ),
        )
    )
    for exchange, line, odds, sizes in optional:
        values.update(
            exchange.input_values(line_name=line, odds_names=odds, size_names=sizes)
            if exchange
            else {name: None for name in (line, *odds, *sizes)}
        )
        if include_trace and exchange is not None:
            traces.update(
                exchange.input_trace(line_name=line, odds_names=odds, size_names=sizes)
            )
    return values, traces


@dataclass(frozen=True, slots=True)
class P2MarketSnapshot:
    """Canonical P2 snapshot: Full Time is required, First Half is optional."""

    target_minute: int
    full_time: P2FullTimeSnapshot
    first_half: P2FirstHalfSnapshot | None = None

    def input_values(self) -> dict[str, Decimal | None]:
        values = self.full_time.input_values()
        if self.first_half is not None:
            values.update(self.first_half.input_values())
            return values
        for name in FIRST_HALF_SIDE_SCOPE.input_names():
            values[name] = None
        for name in (
            EXCHANGE_AH_1H_LINE_INPUT_NAME,
            *EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
            *EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
            EXCHANGE_HANDICAP_1H_LINE_INPUT_NAME,
            *EXCHANGE_HANDICAP_1H_ODDS_INPUT_NAMES,
            *EXCHANGE_HANDICAP_1H_SIZE_TRACE_INPUT_NAMES,
        ):
            values[name] = None
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        traces = self.full_time.input_trace()
        if self.first_half is not None:
            traces.update(self.first_half.input_trace())
        return traces


@dataclass(frozen=True, slots=True)
class P2ExtractionResult:
    """Per-bookie coverage and audited inputs at the pipeline-selected minute."""

    target_minute: int | None
    full_time: PeriodDiagnostics
    first_half: PeriodDiagnostics
    exchange_ah: PeriodDiagnostics = field(default_factory=PeriodDiagnostics.empty)
    exchange_ah_1h: PeriodDiagnostics = field(default_factory=PeriodDiagnostics.empty)
    exchange_handicap: PeriodDiagnostics = field(
        default_factory=PeriodDiagnostics.empty
    )
    exchange_handicap_1h: PeriodDiagnostics = field(
        default_factory=PeriodDiagnostics.empty
    )
    full_time_snapshot: P2FullTimeSnapshot | None = None
    first_half_snapshot: P2FirstHalfSnapshot | None = None
    exchange_ah_snapshot: PartialAsianHandicapExchangeSnapshot | None = None
    exchange_handicap_snapshot: PartialAsianHandicapExchangeSnapshot | None = None
    abort_reason: str | None = None
    extraction_diagnostics: dict[str, Any] = field(default_factory=dict)

    @property
    def snapshot(self) -> P2MarketSnapshot | None:
        if (
            self.target_minute is None
            or self.full_time_snapshot is None
            or not self.full_time.usable
        ):
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
            "exchange_handicap": self.exchange_handicap.to_dict(),
            "exchange_handicap_1h": self.exchange_handicap_1h.to_dict(),
        }

    @property
    def missing_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                set(self.full_time.missing_inputs)
                | set(self.first_half.missing_inputs)
                | set(self.exchange_ah.missing_inputs)
                | set(self.exchange_ah_1h.missing_inputs)
                | set(self.exchange_handicap.missing_inputs)
                | set(self.exchange_handicap_1h.missing_inputs)
            )
        )

    @property
    def invalid_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                set(self.full_time.invalid_inputs)
                | set(self.first_half.invalid_inputs)
                | set(self.exchange_ah.invalid_inputs)
                | set(self.exchange_ah_1h.invalid_inputs)
                | set(self.exchange_handicap.invalid_inputs)
                | set(self.exchange_handicap_1h.invalid_inputs)
            )
        )

    @property
    def ambiguous_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                set(self.full_time.ambiguous_inputs)
                | set(self.first_half.ambiguous_inputs)
                | set(self.exchange_ah.ambiguous_inputs)
                | set(self.exchange_ah_1h.ambiguous_inputs)
                | set(self.exchange_handicap.ambiguous_inputs)
                | set(self.exchange_handicap_1h.ambiguous_inputs)
            )
        )
