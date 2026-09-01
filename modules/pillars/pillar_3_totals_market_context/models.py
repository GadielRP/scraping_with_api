"""Typed snapshot contracts used by the Pillar 3 extraction policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from modules.pillars.market_snapshot_extractor import QuotePoint

from .periods import (
    FIRST_HALF_TOTALS_SCOPE,
    TotalsBookInputSpec,
    TotalsPeriodScope,
    resolve_period_status,
)


@dataclass(frozen=True, slots=True)
class PeriodDiagnostics:
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
class TotalsBookSnapshot:
    market_period: str
    line: Decimal | None
    over: QuotePoint | None
    under: QuotePoint | None

    def is_complete(self) -> bool:
        return self.line is not None and self.over is not None and self.under is not None

    def has_any_input(self) -> bool:
        return self.line is not None or self.over is not None or self.under is not None

    def input_values(self, spec: TotalsBookInputSpec) -> dict[str, Decimal | None]:
        return {
            spec.line: self.line,
            spec.over: self.over.odds_price if self.over is not None else None,
            spec.under: self.under.odds_price if self.under is not None else None,
        }

    def input_trace(self, spec: TotalsBookInputSpec) -> dict[str, dict[str, Any]]:
        line_anchor = self.over or self.under
        points = {
            spec.line: line_anchor,
            spec.over: self.over,
            spec.under: self.under,
        }
        return {
            name: point.trace.to_dict()
            for name, point in points.items()
            if point is not None
        }


@dataclass(frozen=True, slots=True)
class P3PeriodSnapshot:
    period: str | None
    period_scope: TotalsPeriodScope
    pinnacle: TotalsBookSnapshot | None
    bet365: TotalsBookSnapshot | None

    def is_complete(self) -> bool:
        return (
            self.pinnacle is not None
            and self.pinnacle.is_complete()
            and self.bet365 is not None
            and self.bet365.is_complete()
        )

    def has_any_input(self) -> bool:
        return any(
            book is not None and book.has_any_input()
            for book in (self.pinnacle, self.bet365)
        )

    def input_values(self) -> dict[str, Decimal | None]:
        values = {name: None for name in self.period_scope.input_names()}
        if self.pinnacle is not None:
            values.update(self.pinnacle.input_values(self.period_scope.pinnacle))
        if self.bet365 is not None:
            values.update(self.bet365.input_values(self.period_scope.bet365))
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        traces: dict[str, dict[str, Any]] = {}
        if self.pinnacle is not None:
            traces.update(self.pinnacle.input_trace(self.period_scope.pinnacle))
        if self.bet365 is not None:
            traces.update(self.bet365.input_trace(self.period_scope.bet365))
        return traces


@dataclass(frozen=True, slots=True)
class P3MarketSnapshot:
    target_minute: int
    full_time: P3PeriodSnapshot
    first_half: P3PeriodSnapshot | None

    def input_values(self) -> dict[str, Decimal | None]:
        values = self.full_time.input_values()
        if self.first_half is None:
            values.update({name: None for name in FIRST_HALF_TOTALS_SCOPE.input_names()})
        else:
            values.update(self.first_half.input_values())
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        traces = self.full_time.input_trace()
        if self.first_half is not None:
            traces.update(self.first_half.input_trace())
        return traces


@dataclass(frozen=True, slots=True)
class P3ExtractionResult:
    target_minute: int | None
    full_time: PeriodDiagnostics
    first_half: PeriodDiagnostics
    full_time_snapshot: P3PeriodSnapshot | None = None
    first_half_snapshot: P3PeriodSnapshot | None = None
    abort_reason: str | None = None
    extraction_diagnostics: dict[str, Any] = field(default_factory=dict)

    @property
    def snapshot(self) -> P3MarketSnapshot | None:
        if (
            self.target_minute is None
            or self.full_time_snapshot is None
            or not self.full_time_snapshot.is_complete()
        ):
            return None
        return P3MarketSnapshot(
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
            sorted(set(self.full_time.missing_inputs + self.first_half.missing_inputs))
        )

    @property
    def invalid_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(set(self.full_time.invalid_inputs + self.first_half.invalid_inputs))
        )

    @property
    def ambiguous_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(set(self.full_time.ambiguous_inputs + self.first_half.ambiguous_inputs))
        )


__all__ = [
    "P3ExtractionResult",
    "P3MarketSnapshot",
    "P3PeriodSnapshot",
    "PeriodDiagnostics",
    "TotalsBookSnapshot",
]
