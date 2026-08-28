"""Typed contracts used by the Pillar 2 extractor and RAW engine."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from modules.pillars.market_snapshot_extractor import QuotePoint

from .periods import FIRST_HALF_SIDE_SCOPE, resolve_period_status


@dataclass(frozen=True)
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
        snapshot: object | None,
        missing_inputs: tuple[str, ...] | list[str] | set[str] = (),
        invalid_inputs: tuple[str, ...] | list[str] | set[str] = (),
        ambiguous_inputs: tuple[str, ...] | list[str] | set[str] = (),
    ) -> "PeriodDiagnostics":
        missing = tuple(sorted(missing_inputs))
        invalid = tuple(sorted(invalid_inputs))
        ambiguous = tuple(sorted(ambiguous_inputs))
        return cls(
            status=resolve_period_status(
                snapshot=snapshot,
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
        return cls.from_gate(snapshot=None)


@dataclass(frozen=True)
class TwoWayMarketSnapshot:
    home: QuotePoint
    away: QuotePoint


@dataclass(frozen=True)
class ThreeWayMarketSnapshot:
    home: QuotePoint
    draw: QuotePoint
    away: QuotePoint


@dataclass(frozen=True)
class AsianHandicapSnapshot(TwoWayMarketSnapshot):
    home_line: Decimal


@dataclass(frozen=True)
class ExchangeSnapshot:
    back: ThreeWayMarketSnapshot
    lay: ThreeWayMarketSnapshot


@dataclass(frozen=True)
class P2FullTimeSnapshot:
    """Complete Full Time block required to produce the P2 signal."""

    pinnacle_1x2: TwoWayMarketSnapshot
    bet365_1x2: TwoWayMarketSnapshot
    pinnacle_ah: AsianHandicapSnapshot
    bet365_ah: AsianHandicapSnapshot
    betfair_1x2: ExchangeSnapshot

    def input_values(self) -> dict[str, Decimal]:
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
            "BF_HOME_BACK_FULL_TIME_ODDS_PRICE": bf_back.home.odds_price,
            "BF_HOME_LAY_FULL_TIME_ODDS_PRICE": bf_lay.home.odds_price,
            "BF_DRAW_BACK_FULL_TIME_ODDS_PRICE": bf_back.draw.odds_price,
            "BF_DRAW_LAY_FULL_TIME_ODDS_PRICE": bf_lay.draw.odds_price,
            "BF_AWAY_BACK_FULL_TIME_ODDS_PRICE": bf_back.away.odds_price,
            "BF_AWAY_LAY_FULL_TIME_ODDS_PRICE": bf_lay.away.odds_price,
            "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.home.exchange_size,
            "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.home.exchange_size,
            "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.draw.exchange_size,
            "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.draw.exchange_size,
            "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.away.exchange_size,
            "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.away.exchange_size,
        }
        # Exchange sizes have passed the Full Time completeness gate and cannot
        # be None. The assertion narrows their type without inventing liquidity.
        assert all(value is not None for value in values.values())
        return values  # type: ignore[return-value]

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
            "BF_HOME_BACK_FULL_TIME_ODDS_PRICE": bf_back.home,
            "BF_HOME_LAY_FULL_TIME_ODDS_PRICE": bf_lay.home,
            "BF_DRAW_BACK_FULL_TIME_ODDS_PRICE": bf_back.draw,
            "BF_DRAW_LAY_FULL_TIME_ODDS_PRICE": bf_lay.draw,
            "BF_AWAY_BACK_FULL_TIME_ODDS_PRICE": bf_back.away,
            "BF_AWAY_LAY_FULL_TIME_ODDS_PRICE": bf_lay.away,
            "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.home,
            "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.home,
            "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.draw,
            "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.draw,
            "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.away,
            "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.away,
        }
        return {name: point.trace.to_dict() for name, point in points.items()}


@dataclass(frozen=True)
class P2FirstHalfSnapshot:
    """Complete First Half block used only to enrich a valid Full Time signal."""

    pinnacle_1x2: TwoWayMarketSnapshot
    bet365_1x2: TwoWayMarketSnapshot
    pinnacle_ah: AsianHandicapSnapshot
    bet365_ah: AsianHandicapSnapshot

    def input_values(self) -> dict[str, Decimal]:
        return {
            "PIN_HOME_1X2_1H_ODDS_PRICE": self.pinnacle_1x2.home.odds_price,
            "PIN_AWAY_1X2_1H_ODDS_PRICE": self.pinnacle_1x2.away.odds_price,
            "B365_HOME_1X2_1H_ODDS_PRICE": self.bet365_1x2.home.odds_price,
            "B365_AWAY_1X2_1H_ODDS_PRICE": self.bet365_1x2.away.odds_price,
            "PIN_AH_1H_LINE": self.pinnacle_ah.home_line,
            "PIN_AH_1H_HOME_PRICE": self.pinnacle_ah.home.odds_price,
            "PIN_AH_1H_AWAY_PRICE": self.pinnacle_ah.away.odds_price,
            "B365_AH_1H_LINE": self.bet365_ah.home_line,
            "B365_AH_1H_HOME_PRICE": self.bet365_ah.home.odds_price,
            "B365_AH_1H_AWAY_PRICE": self.bet365_ah.away.odds_price,
        }

    def input_trace(self) -> dict[str, dict[str, Any]]:
        points = {
            "PIN_HOME_1X2_1H_ODDS_PRICE": self.pinnacle_1x2.home,
            "PIN_AWAY_1X2_1H_ODDS_PRICE": self.pinnacle_1x2.away,
            "B365_HOME_1X2_1H_ODDS_PRICE": self.bet365_1x2.home,
            "B365_AWAY_1X2_1H_ODDS_PRICE": self.bet365_1x2.away,
            "PIN_AH_1H_LINE": self.pinnacle_ah.home,
            "PIN_AH_1H_HOME_PRICE": self.pinnacle_ah.home,
            "PIN_AH_1H_AWAY_PRICE": self.pinnacle_ah.away,
            "B365_AH_1H_LINE": self.bet365_ah.home,
            "B365_AH_1H_HOME_PRICE": self.bet365_ah.home,
            "B365_AH_1H_AWAY_PRICE": self.bet365_ah.away,
        }
        return {name: point.trace.to_dict() for name, point in points.items()}


@dataclass(frozen=True)
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
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        traces = dict(self.full_time.input_trace())
        if self.first_half is not None:
            traces.update(self.first_half.input_trace())
        return traces


@dataclass(frozen=True)
class P2ExtractionResult:
    """Outcome of the independent Full Time and First Half completeness gates."""

    target_minute: int | None
    full_time: PeriodDiagnostics
    first_half: PeriodDiagnostics
    full_time_snapshot: P2FullTimeSnapshot | None = None
    first_half_snapshot: P2FirstHalfSnapshot | None = None
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
        }

    @property
    def missing_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(set(self.full_time.missing_inputs) | set(self.first_half.missing_inputs))
        )

    @property
    def invalid_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(set(self.full_time.invalid_inputs) | set(self.first_half.invalid_inputs))
        )

    @property
    def ambiguous_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                set(self.full_time.ambiguous_inputs)
                | set(self.first_half.ambiguous_inputs)
            )
        )
