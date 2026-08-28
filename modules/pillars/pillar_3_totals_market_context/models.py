"""Typed contracts used by the Pillar 3 extractor and RAW engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from modules.pillars.market_snapshot_extractor import QuotePoint

from .periods import TotalsPeriodScope, resolve_period_status


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
class TotalsBookSnapshot:
    """The single structural totals line resolved for one bookmaker."""

    market_period: str
    line: Decimal | None
    over: QuotePoint | None
    under: QuotePoint | None


@dataclass(frozen=True)
class P3PeriodSnapshot:
    """One independently extracted totals period; bookmaker branches may be partial."""

    period: str | None
    period_scope: TotalsPeriodScope
    pinnacle: TotalsBookSnapshot | None
    bet365: TotalsBookSnapshot | None

    def input_values(self) -> dict[str, Decimal | None]:
        return {
            "PIN_TOTAL_LINE": self.pinnacle.line if self.pinnacle else None,
            "PIN_OVER_PRICE": (
                self.pinnacle.over.odds_price
                if self.pinnacle and self.pinnacle.over
                else None
            ),
            "PIN_UNDER_PRICE": (
                self.pinnacle.under.odds_price
                if self.pinnacle and self.pinnacle.under
                else None
            ),
            "B365_TOTAL_LINE": self.bet365.line if self.bet365 else None,
            "B365_OVER_PRICE": (
                self.bet365.over.odds_price if self.bet365 and self.bet365.over else None
            ),
            "B365_UNDER_PRICE": (
                self.bet365.under.odds_price
                if self.bet365 and self.bet365.under
                else None
            ),
        }

    def input_trace(self) -> dict[str, dict[str, Any]]:
        points = {
            "PIN_OVER_PRICE": self.pinnacle.over if self.pinnacle else None,
            "PIN_UNDER_PRICE": self.pinnacle.under if self.pinnacle else None,
            "B365_OVER_PRICE": self.bet365.over if self.bet365 else None,
            "B365_UNDER_PRICE": self.bet365.under if self.bet365 else None,
        }
        return {
            name: point.trace.to_dict()
            for name, point in points.items()
            if point is not None
        }

    def has_any_input(self) -> bool:
        return any(value is not None for value in self.input_values().values())


@dataclass(frozen=True)
class P3MarketSnapshot:
    """Canonical P3 snapshot: Full Time is required; later periods stay optional."""

    target_minute: int
    full_time: P3PeriodSnapshot

    def input_values(self) -> dict[str, Decimal | None]:
        return self.full_time.input_values()

    def input_trace(self) -> dict[str, dict[str, Any]]:
        return self.full_time.input_trace()


@dataclass(frozen=True)
class P3ExtractionResult:
    """Outcome of the independent totals-period completeness gates."""

    target_minute: int | None
    full_time: PeriodDiagnostics
    full_time_snapshot: P3PeriodSnapshot | None = None
    abort_reason: str | None = None
    extraction_diagnostics: dict[str, Any] = field(default_factory=dict)

    @property
    def snapshot(self) -> P3MarketSnapshot | None:
        if self.target_minute is None or self.full_time_snapshot is None:
            return None
        if not self.full_time_snapshot.has_any_input():
            return None
        return P3MarketSnapshot(
            target_minute=self.target_minute,
            full_time=self.full_time_snapshot,
        )

    def period_diagnostics(self) -> dict[str, Any]:
        return {"full_time": self.full_time.to_dict()}

    @property
    def missing_inputs(self) -> tuple[str, ...]:
        return self.full_time.missing_inputs

    @property
    def invalid_inputs(self) -> tuple[str, ...]:
        return self.full_time.invalid_inputs

    @property
    def ambiguous_inputs(self) -> tuple[str, ...]:
        return self.full_time.ambiguous_inputs
