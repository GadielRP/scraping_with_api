"""Versioned, persistence-neutral results for market pillars (P2–P5)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

POLICY_VERSION = "market-evaluation-v1"
PAYLOAD_SCHEMA_VERSION = 4
SignalStatus = Literal["COMPUTED", "BLOCKED", "ERROR"]
CoverageStatus = Literal[
    "COMPLETE",
    "INCOMPLETE",
    "MISSING",
    "INVALID",
    "AMBIGUOUS",
    "EXCLUDED",
    "NOT_APPLICABLE",
]


@dataclass(frozen=True, slots=True)
class SignalResult:
    key: str
    status: SignalStatus
    value: Any = None
    reason: str | None = None
    input_refs: tuple[str, ...] = ()
    contract_refs: tuple[str, ...] = ()
    evidence: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status == "COMPUTED" and self.value is None:
            raise ValueError("computed signals require a value; neutral zero is valid")
        if self.status != "COMPUTED" and self.value is not None:
            raise ValueError(
                "blocked or failed signals cannot expose a calculated value"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "status": self.status,
            "value": self.value,
            "reason": self.reason,
            "input_refs": list(self.input_refs),
            "contract_refs": list(self.contract_refs),
            "evidence": self.evidence,
        }


@dataclass(frozen=True, slots=True)
class CoverageCell:
    key: str
    bookie_id: int
    family: str
    period: str
    status: CoverageStatus
    contract_refs: tuple[str, ...] = ()
    missing: tuple[str, ...] = ()
    reason: str | None = None
    line_value: str | None = None
    exchange_side: str | None = None
    observed_status: CoverageStatus | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "bookie_id": self.bookie_id,
            "family": self.family,
            "period": self.period,
            "status": self.status,
            "contract_refs": list(self.contract_refs),
            "missing": list(self.missing),
            "reason": self.reason,
            "line_value": self.line_value,
            "exchange_side": self.exchange_side,
            "observed_status": self.observed_status,
        }


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    event_id: int
    pillar_id: str
    engine_version: str
    target_minute: int | None
    selection: dict[str, Any]
    signals: tuple[SignalResult, ...]
    coverage: tuple[CoverageCell, ...]
    inputs: dict[str, Any] = field(default_factory=dict)
    contracts: dict[str, Any] = field(default_factory=dict)
    analysis: dict[str, Any] = field(default_factory=dict)
    diagnostics: tuple[dict[str, Any], ...] = ()
    skipped: bool = False

    @property
    def status(self) -> str:
        if self.skipped:
            return "SKIPPED"
        if any(s.status == "COMPUTED" for s in self.signals):
            return "ACTIVE"
        return (
            "ERROR"
            if any(s.status == "ERROR" for s in self.signals)
            else "INSUFFICIENT_DATA"
        )

    def to_dict(self) -> dict[str, Any]:
        errors = any(s.status == "ERROR" for s in self.signals)
        return {
            "event_id": self.event_id,
            "pillar_id": self.pillar_id,
            "engine_version": self.engine_version,
            "payload_schema_version": PAYLOAD_SCHEMA_VERSION,
            "policy_version": POLICY_VERSION,
            "target_minute": self.target_minute,
            "selected_full_time_period": self.selection.get("selected_period"),
            "selection": self.selection,
            "checkpoint": self.selection.get(
                "checkpoint", {"target_minute": self.target_minute}
            ),
            "status": self.status,
            "execution_status": (
                "SKIPPED"
                if self.skipped
                else (
                    "FAILED"
                    if self.status == "ERROR"
                    else "COMPLETED_WITH_ERRORS" if errors else "COMPLETED"
                )
            ),
            "signals": [s.to_dict() for s in self.signals],
            "coverage": [c.to_dict() for c in self.coverage],
            "evidence": {
                "computed_signals": sum(s.status == "COMPUTED" for s in self.signals),
                "blocked_signals": sum(s.status == "BLOCKED" for s in self.signals),
            },
            "inputs": self.inputs,
            "contracts": self.contracts,
            "analysis": self.analysis,
            "diagnostics": list(self.diagnostics),
        }


def read_stored_result(payload: dict[str, Any], schema_version: int) -> dict[str, Any]:
    """History is returned with its stored semantics, never reclassified."""
    if schema_version < 1 or schema_version > PAYLOAD_SCHEMA_VERSION:
        raise ValueError(f"unsupported result schema: {schema_version}")
    return payload
