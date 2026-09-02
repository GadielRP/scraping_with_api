"""Typed snapshot contracts used by the Pillar 3 extraction policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from modules.pillars.market_snapshot_extractor import QuotePoint

from .periods import (
    EXCHANGE_OU_LINE_INPUT_NAME,
    EXCHANGE_OU_ODDS_INPUT_NAMES,
    EXCHANGE_OU_SIZE_TRACE_INPUT_NAMES,
    EXCHANGE_OU_1H_LINE_INPUT_NAME,
    EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
    EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES,
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
class TotalsExchangeSnapshot:
    """Optional Betfair O/U BACK and LAY readings for one period."""

    back: TotalsBookSnapshot | None
    lay: TotalsBookSnapshot | None

    @property
    def line(self) -> Decimal | None:
        for branch in (self.back, self.lay):
            if branch is not None and branch.line is not None:
                return branch.line
        return None

    @property
    def lines_match(self) -> bool:
        return (
            self.back is not None
            and self.lay is not None
            and self.back.line is not None
            and self.back.line == self.lay.line
        )

    def has_any_input(self) -> bool:
        return any(branch is not None and branch.has_any_input() for branch in (self.back, self.lay))

    def input_values(
        self,
        *,
        line_name: str = EXCHANGE_OU_LINE_INPUT_NAME,
        odds_names: tuple[str, ...] = EXCHANGE_OU_ODDS_INPUT_NAMES,
        size_names: tuple[str, ...] = EXCHANGE_OU_SIZE_TRACE_INPUT_NAMES,
    ) -> dict[str, Decimal | None]:
        def price(branch: TotalsBookSnapshot | None, field: str) -> Decimal | None:
            point = None if branch is None else getattr(branch, field)
            return None if point is None else point.odds_price

        def size(branch: TotalsBookSnapshot | None, field: str) -> Decimal | None:
            point = None if branch is None else getattr(branch, field)
            return None if point is None else point.exchange_size

        return {
            line_name: self.line,
            odds_names[0]: price(self.back, "over"),
            odds_names[1]: price(self.back, "under"),
            odds_names[2]: price(self.lay, "over"),
            odds_names[3]: price(self.lay, "under"),
            size_names[0]: size(self.back, "over"),
            size_names[1]: size(self.back, "under"),
            size_names[2]: size(self.lay, "over"),
            size_names[3]: size(self.lay, "under"),
        }

    def input_trace(
        self,
        *,
        line_name: str = EXCHANGE_OU_LINE_INPUT_NAME,
        odds_names: tuple[str, ...] = EXCHANGE_OU_ODDS_INPUT_NAMES,
        size_names: tuple[str, ...] = EXCHANGE_OU_SIZE_TRACE_INPUT_NAMES,
    ) -> dict[str, dict[str, Any]]:
        points = {
            odds_names[0]: None if self.back is None else self.back.over,
            odds_names[1]: None if self.back is None else self.back.under,
            odds_names[2]: None if self.lay is None else self.lay.over,
            odds_names[3]: None if self.lay is None else self.lay.under,
            size_names[0]: None if self.back is None else self.back.over,
            size_names[1]: None if self.back is None else self.back.under,
            size_names[2]: None if self.lay is None else self.lay.over,
            size_names[3]: None if self.lay is None else self.lay.under,
        }
        line_point = next((point for point in points.values() if point is not None), None)
        if line_point is not None:
            points[line_name] = line_point
        return {name: point.trace.to_dict() for name, point in points.items() if point is not None}

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
    exchange_ou: TotalsExchangeSnapshot | None = None
    exchange_ou_1h: TotalsExchangeSnapshot | None = None

    def input_values(self) -> dict[str, Decimal | None]:
        values = self.full_time.input_values()
        values.update(
            self.exchange_ou.input_values()
            if self.exchange_ou is not None
            else {
                name: None
                for name in (
                    EXCHANGE_OU_LINE_INPUT_NAME,
                    *EXCHANGE_OU_ODDS_INPUT_NAMES,
                    *EXCHANGE_OU_SIZE_TRACE_INPUT_NAMES,
                )
            }
        )
        values.update(
            self.exchange_ou_1h.input_values(
                line_name=EXCHANGE_OU_1H_LINE_INPUT_NAME,
                odds_names=EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
                size_names=EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES,
            )
            if self.exchange_ou_1h is not None
            else {
                name: None
                for name in (
                    EXCHANGE_OU_1H_LINE_INPUT_NAME,
                    *EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
                    *EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES,
                )
            }
        )
        if self.first_half is None:
            values.update({name: None for name in FIRST_HALF_TOTALS_SCOPE.input_names()})
        else:
            values.update(self.first_half.input_values())
        return values

    def input_trace(self) -> dict[str, dict[str, Any]]:
        traces = self.full_time.input_trace()
        if self.exchange_ou is not None:
            traces.update(self.exchange_ou.input_trace())
        if self.exchange_ou_1h is not None:
            traces.update(
                self.exchange_ou_1h.input_trace(
                    line_name=EXCHANGE_OU_1H_LINE_INPUT_NAME,
                    odds_names=EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
                    size_names=EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES,
                )
            )
        if self.first_half is not None:
            traces.update(self.first_half.input_trace())
        return traces


@dataclass(frozen=True, slots=True)
class P3ExtractionResult:
    target_minute: int | None
    full_time: PeriodDiagnostics
    first_half: PeriodDiagnostics
    exchange_ou: PeriodDiagnostics = field(default_factory=PeriodDiagnostics.empty)
    exchange_ou_1h: PeriodDiagnostics = field(default_factory=PeriodDiagnostics.empty)
    full_time_snapshot: P3PeriodSnapshot | None = None
    first_half_snapshot: P3PeriodSnapshot | None = None
    exchange_ou_snapshot: TotalsExchangeSnapshot | None = None
    exchange_ou_1h_snapshot: TotalsExchangeSnapshot | None = None
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
            exchange_ou=self.exchange_ou_snapshot,
            exchange_ou_1h=self.exchange_ou_1h_snapshot,
        )

    def period_diagnostics(self) -> dict[str, Any]:
        return {
            "full_time": self.full_time.to_dict(),
            "first_half": self.first_half.to_dict(),
            "exchange_ou": self.exchange_ou.to_dict(),
            "exchange_ou_1h": self.exchange_ou_1h.to_dict(),
        }

    @property
    def missing_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(set(self.full_time.missing_inputs + self.first_half.missing_inputs + self.exchange_ou.missing_inputs + self.exchange_ou_1h.missing_inputs))
        )

    @property
    def invalid_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(set(self.full_time.invalid_inputs + self.first_half.invalid_inputs + self.exchange_ou.invalid_inputs + self.exchange_ou_1h.invalid_inputs))
        )

    @property
    def ambiguous_inputs(self) -> tuple[str, ...]:
        return tuple(
            sorted(set(self.full_time.ambiguous_inputs + self.first_half.ambiguous_inputs + self.exchange_ou.ambiguous_inputs + self.exchange_ou_1h.ambiguous_inputs))
        )


__all__ = [
    "P3ExtractionResult",
    "P3MarketSnapshot",
    "P3PeriodSnapshot",
    "PeriodDiagnostics",
    "TotalsBookSnapshot",
    "TotalsExchangeSnapshot",
]
