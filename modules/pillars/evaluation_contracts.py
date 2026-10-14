"""Versioned, persistence-neutral results for market pillars (P2–P5)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict
from .instrumentation import SIGNAL_REASONS, SIGNAL_STATUSES, signal_diagnostics

POLICY_VERSION = "market-evaluation-v3-source-reconstruction"
PAYLOAD_SCHEMA_VERSION = 5
INTERPRETATION_VERSION = 1
EXECUTIVE_SUMMARY_VERSION = 1
InterpretationState = Literal[
    "BOOK_CONSENSUS",
    "BOOK_DISAGREEMENT",
    "NEUTRAL_STRUCTURE",
    "PARTIAL_DIRECTIONAL_AGREEMENT",
    "SINGLE_BOOK_READING",
    "INCOMPARABLE_CONTRACT",
    "MISSING_INPUT",
    "INVALID_VALUE",
    "AMBIGUOUS_CANDIDATE",
    "CALCULATION_ERROR",
    "BACK_LAY_CONSENSUS",
    "BACK_LAY_DIVERGENCE",
    "SINGLE_EXCHANGE_READING",
    "CROSS_SOURCE_CONSENSUS",
    "CROSS_SOURCE_CONFLICT",
    "PARTIAL_STRUCTURE",
    "MULTI_MARKET_CONSENSUS",
    "CROSS_MARKET_CONFLICT",
    "BOOK_READING",
    "REPRESENTATIVE_AVAILABLE",
    "DIFFERENT_LINE_STRUCTURE",
    "FINAL_RUN_ALIGNED",
    "FINAL_RUN_COUNTERMOVE",
    "FINAL_RUN_WITHOUT_NET_MOVE",
    "STABLE_MARKET",
    "INSUFFICIENT_TRAJECTORY",
    "PARTIAL_TRAJECTORY",
    "INSUFFICIENT_HISTORICAL_SAMPLE",
    "HISTORICAL_OUTCOME_TIE",
    "HISTORICAL_OUTCOME_READING",
    "CROSS_PERIOD_CONSENSUS",
    "CROSS_PERIOD_DIFFERENCE",
]
InterpretationDirection = Literal[
    "HOME",
    "AWAY",
    "OVER",
    "UNDER",
    "NEUTRAL",
    "MIXED",
    "NONE",
    "DRAW",
    "CHOICE_WEAKENING",
    "CHOICE_STRENGTHENING",
    "NO_MOVEMENT",
    "LINE_UP",
    "LINE_DOWN",
    "LINE_UNCHANGED",
    "SIZE_INCREASE",
    "SIZE_DECREASE",
    "SIZE_UNCHANGED",
    "WIDENING",
    "NARROWING",
    "UNCHANGED",
]
InterpretationComparability = Literal[
    "COMPARABLE", "PARTIAL", "UNAVAILABLE", "INCOMPARABLE_CONTRACT", "INCOMPARABLE_LINE"
]


class InterpretationReading(TypedDict):
    status: Literal["COMPUTED", "BLOCKED", "ERROR"]
    state: InterpretationState
    direction: InterpretationDirection | None
    comparability: InterpretationComparability
    interpretation_code: str
    reason: str | None
    evidence: dict[str, Any]
    summary: str


INTERPRETATION_SUMMARIES = {
    "BOOK_CONSENSUS": "Los bookmakers coinciden en la dirección.",
    "BOOK_DISAGREEMENT": "Los bookmakers indican direcciones opuestas.",
    "NEUTRAL_STRUCTURE": "Las lecturas comparables son neutrales.",
    "PARTIAL_DIRECTIONAL_AGREEMENT": "Una lectura es neutral y otra direccional.",
    "SINGLE_BOOK_READING": "Solo hay una lectura válida de bookmaker.",
    "INCOMPARABLE_CONTRACT": "Los contratos no permiten una lectura conjunta.",
    "MISSING_INPUT": "Faltan inputs para interpretar esta rama.",
    "INVALID_VALUE": "La rama contiene un valor inválido.",
    "AMBIGUOUS_CANDIDATE": "Hay candidatos ambiguos para esta rama.",
    "CALCULATION_ERROR": "El cálculo de esta rama falló.",
    "BACK_LAY_CONSENSUS": "BACK y LAY coinciden en la dirección.",
    "BACK_LAY_DIVERGENCE": "BACK y LAY indican direcciones opuestas.",
    "SINGLE_EXCHANGE_READING": "Solo hay una lectura válida del exchange.",
    "CROSS_SOURCE_CONSENSUS": "Books y exchange coinciden en la dirección.",
    "CROSS_SOURCE_CONFLICT": "Books y exchange indican direcciones opuestas en el mismo contrato.",
    "PARTIAL_STRUCTURE": "La estructura disponible es parcial.",
    "MULTI_MARKET_CONSENSUS": "Los mercados comparables coinciden en la dirección.",
    "CROSS_MARKET_CONFLICT": "Los mercados comparables indican direcciones opuestas.",
    "BOOK_READING": "Lectura individual del bookmaker.",
    "REPRESENTATIVE_AVAILABLE": "El representante contractual está disponible.",
    "DIFFERENT_LINE_STRUCTURE": "Las líneas son distintas; sus direcciones no constituyen un conflicto directo.",
    "FINAL_RUN_ALIGNED": "El último tramo está alineado con el movimiento neto.",
    "FINAL_RUN_COUNTERMOVE": "El último tramo es contrario al movimiento neto.",
    "FINAL_RUN_WITHOUT_NET_MOVE": "Existe movimiento en el último tramo, con cambio neto nulo.",
    "STABLE_MARKET": "La trayectoria disponible es válida y no presenta movimiento.",
    "INSUFFICIENT_TRAJECTORY": "Faltan observaciones o el endpoint para interpretar la trayectoria.",
    "PARTIAL_TRAJECTORY": "Los huecos impiden interpretar el recorrido completo.",
    "INSUFFICIENT_HISTORICAL_SAMPLE": "La muestra histórica no alcanza el mínimo requerido.",
    "HISTORICAL_OUTCOME_TIE": "Hay empate entre las cuentas históricas dominantes.",
    "HISTORICAL_OUTCOME_READING": "La memoria histórica dispone de un resultado dominante.",
    "CROSS_PERIOD_CONSENSUS": "Los periodos presentan la misma dirección; son contratos distintos.",
    "CROSS_PERIOD_DIFFERENCE": "Los periodos presentan direcciones distintas; no es un conflicto sobre un mismo contrato.",
}


def interpretation_reading(
    state: InterpretationState,
    *,
    direction: InterpretationDirection | None = None,
    status: Literal["COMPUTED", "BLOCKED", "ERROR"] = "COMPUTED",
    reason=None,
    comparability: InterpretationComparability = "COMPARABLE",
    evidence=None,
    **fields,
) -> InterpretationReading:
    """Serialize a domain-selected reading; this helper makes no domain decisions."""
    summary = INTERPRETATION_SUMMARIES[state]
    if direction is not None:
        summary += f" Dirección: {direction}."
    return {
        "status": status,
        "state": state,
        "direction": direction,
        "comparability": comparability,
        "interpretation_code": state,
        "reason": reason,
        "evidence": evidence or {},
        "summary": summary,
        **fields,
    }


def interpretation_failure_reason(result, scope, prefix):
    """Reuse existing signal failures for the specified analytical branch."""
    reasons = {
        signal["reason"]
        for signal in result["signals"]
        if signal["status"] != "COMPUTED"
        and signal.get("evidence", {}).get("scope") == scope
        and signal.get("evidence", {}).get("metric", "").startswith(prefix + ".")
    }
    return next(
        (
            reason
            for reason in (
                "CALCULATION_ERROR",
                "AMBIGUOUS_CANDIDATE",
                "INVALID_VALUE",
                "INCOMPATIBLE_CONTRACT",
            )
            if reason in reasons
        ),
        "MISSING_INPUT",
    )


def project_executive_sources(readings):
    """Project existing readings once per source and contract, without deciding."""
    sources = {"books": [], "exchange": [], "book_exchange": []}
    for role, market, reading in readings:
        projected = {
            "market": market,
            **{
                key: value
                for key, value in reading.items()
                if key not in {"evidence", "interpretation_code"}
                and not isinstance(value, (dict, list, tuple, set))
            },
        }
        if projected not in sources[role]:
            sources[role].append(projected)
    return sources


def executive_source_sentences(period, sources):
    """Render domain descriptions, retaining each source's own contract."""
    labels = {"books": "Books", "exchange": "Exchange", "book_exchange": "Books/exchange"}
    return [
        f"{period} {labels[role]} {reading['market']}: {reading['summary']}"
        for role, readings in sources.items()
        for reading in readings
        if reading.get("summary")
        and (reading.get("status") == "COMPUTED" or reading.get("reason") != "MISSING_INPUT")
    ]


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
        if self.status not in SIGNAL_STATUSES:
            raise ValueError(f"unsupported signal status: {self.status!r}")
        if self.status != "COMPUTED" and self.reason not in SIGNAL_REASONS:
            raise ValueError(
                "blocked/failed signals require an explicit registered reason"
            )
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
            "diagnostics": {
                "signals": signal_diagnostics([s.to_dict() for s in self.signals]),
                "details": list(self.diagnostics),
            },
        }
