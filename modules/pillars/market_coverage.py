"""Coverage contracts shared by market pillars, independent of their formulas."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


PERIOD_STATUS_COMPLETE = "COMPLETE"
PERIOD_STATUS_PARTIAL = "PARTIAL"
PERIOD_STATUS_AMBIGUOUS = "AMBIGUOUS"
PERIOD_STATUS_INVALID = "INVALID"
PERIOD_STATUS_INCOMPLETE = "INCOMPLETE"


def resolve_period_status(
    *,
    complete: bool,
    missing_inputs: Iterable[str] = (),
    invalid_inputs: Iterable[str] = (),
    ambiguous_inputs: Iterable[str] = (),
) -> str:
    if complete:
        return PERIOD_STATUS_COMPLETE
    if any(ambiguous_inputs):
        return PERIOD_STATUS_AMBIGUOUS
    if any(invalid_inputs):
        return PERIOD_STATUS_INVALID
    return PERIOD_STATUS_INCOMPLETE


def resolve_pillar_status(
    *,
    required_complete: bool,
    optional_complete: bool,
    required_usable: bool | None = None,
) -> str:
    usable = required_complete if required_usable is None else required_usable
    if not usable:
        return "INSUFFICIENT_DATA"
    return "ACTIVE" if required_complete and optional_complete else "PARTIAL"


@dataclass(frozen=True, slots=True)
class PeriodDiagnostics:
    """A complete bookie remains usable despite another bookie's failures."""

    status: str
    missing_inputs: tuple[str, ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()
    bookies: Mapping[str, PeriodDiagnostics] = field(default_factory=dict)

    @property
    def usable(self) -> bool:
        return self.status in {PERIOD_STATUS_COMPLETE, PERIOD_STATUS_PARTIAL}

    def to_dict(self) -> dict[str, Any]:
        result = {
            "status": self.status,
            "missing_inputs": list(self.missing_inputs),
            "invalid_inputs": list(self.invalid_inputs),
            "ambiguous_inputs": list(self.ambiguous_inputs),
        }
        if self.bookies:
            result["bookies"] = {
                name: item.to_dict() for name, item in self.bookies.items()
            }
            result["available_bookies"] = [
                name for name, item in self.bookies.items() if item.usable
            ]
        return result

    @classmethod
    def from_gate(
        cls,
        *,
        complete: bool,
        missing_inputs: Iterable[str] = (),
        invalid_inputs: Iterable[str] = (),
        ambiguous_inputs: Iterable[str] = (),
    ) -> PeriodDiagnostics:
        missing = tuple(sorted(set(missing_inputs)))
        invalid = tuple(sorted(set(invalid_inputs)))
        ambiguous = tuple(sorted(set(ambiguous_inputs)))
        return cls(
            resolve_period_status(
                complete=complete,
                missing_inputs=missing,
                invalid_inputs=invalid,
                ambiguous_inputs=ambiguous,
            ),
            missing,
            invalid,
            ambiguous,
        )

    @classmethod
    def from_bookies(
        cls,
        bookies: Mapping[str, PeriodDiagnostics],
        *,
        required: Iterable[str] | None = None,
    ) -> PeriodDiagnostics:
        required_bookies = (
            bookies if required is None else {name: bookies[name] for name in required}
        )
        aggregate = cls.from_gate(
            complete=bool(required_bookies)
            and all(
                item.status == PERIOD_STATUS_COMPLETE
                for item in required_bookies.values()
            ),
            missing_inputs=(
                name
                for item in required_bookies.values()
                for name in item.missing_inputs
            ),
            invalid_inputs=(
                name
                for item in required_bookies.values()
                for name in item.invalid_inputs
            ),
            ambiguous_inputs=(
                name
                for item in required_bookies.values()
                for name in item.ambiguous_inputs
            ),
        )
        status = aggregate.status
        if status != PERIOD_STATUS_COMPLETE and any(
            item.usable for item in bookies.values()
        ):
            status = PERIOD_STATUS_PARTIAL
        return cls(
            status,
            aggregate.missing_inputs,
            aggregate.invalid_inputs,
            aggregate.ambiguous_inputs,
            dict(bookies),
        )

    @classmethod
    def empty(cls) -> PeriodDiagnostics:
        return cls.from_gate(complete=False)
