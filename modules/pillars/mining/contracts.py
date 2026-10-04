"""Persistence-neutral contracts for hierarchical pillar mining output."""

from __future__ import annotations

from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterator

from shared.temporal import utc_now


CANONICAL_STATUSES = frozenset(
    {"SUCCESS", "PARTIAL", "INSUFFICIENT", "ERROR", "SKIPPED"}
)
METRIC_VALUE_TYPES = frozenset({"number", "text", "boolean"})


@dataclass(frozen=True)
class PillarMiningMetric:
    """One scalar, statistically queryable value produced by a mining unit."""

    name: str
    value_type: str
    value: Decimal | str | bool
    group: str | None = None


@dataclass(frozen=True)
class PillarMiningUnit:
    """One evaluable node within a pillar result hierarchy."""

    unit_type: str
    unit_key: str
    producer_status: str
    canonical_status: str
    parent_unit_key: str | None = None
    ordinal: int | None = None
    module_id: str | None = None
    signal_axis: str | None = None
    is_valid: bool | None = None
    score_name: str | None = None
    score: Decimal | None = None
    direction: str | None = None
    strength: str | None = None
    target_minute: int | None = None
    market_group: str | None = None
    market_period: str | None = None
    market_name: str | None = None
    line_value: str | None = None
    choice_name: str | None = None
    bookie_id: int | None = None
    quote_id: int | None = None
    source: str | None = None
    exchange_side: str | None = None
    exchange_level: int | None = None
    dimensions: dict[str, Any] = field(default_factory=dict)
    payload: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    metrics: tuple[PillarMiningMetric, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class PillarMiningRun:
    """Canonical execution of one pillar scope for an event and execution slot."""

    event_id: int
    pillar_id: str
    result_scope: str
    execution_slot: str
    engine_version: str
    payload_schema_version: int
    producer_status: str
    canonical_status: str
    sport: str
    evaluation_minute: int | None = None
    target_minute: int | None = None
    competition_id: int | None = None
    context: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    output_payload: dict[str, Any] = field(default_factory=dict)
    units: tuple[PillarMiningUnit, ...] = field(default_factory=tuple)
    calculated_at: datetime = field(default_factory=utc_now)


def iter_unit_layers(
    units: tuple[PillarMiningUnit, ...],
) -> Iterator[list[PillarMiningUnit]]:
    """Visit parents before children in O(units + links), retaining sibling order."""
    by_key = {}
    children = defaultdict(list)
    for unit in units:
        if unit.unit_key in by_key:
            raise ValueError(f"duplicate mining unit key: {unit.unit_key!r}")
        by_key[unit.unit_key] = unit
        children[unit.parent_unit_key].append(unit)
    for unit in units:
        parent = unit.parent_unit_key
        if parent == unit.unit_key:
            raise ValueError(f"unit {unit.unit_key!r} cannot parent itself")
        if parent is not None and parent not in by_key:
            raise ValueError(f"unknown parent {parent!r} for {unit.unit_key!r}")
    layer = children.pop(None, [])
    visited = 0
    while layer:
        visited += len(layer)
        yield layer
        layer = [child for unit in layer for child in children.pop(unit.unit_key, ())]
    if visited != len(units):
        raise ValueError("cycle detected in mining unit graph")


def validate_mining_run(run: PillarMiningRun) -> None:
    """Reject malformed graphs before they reach persistence infrastructure."""

    if run.event_id <= 0:
        raise ValueError("event_id must be positive")
    for field_name in (
        "pillar_id",
        "result_scope",
        "execution_slot",
        "engine_version",
        "producer_status",
        "sport",
    ):
        if not str(getattr(run, field_name)).strip():
            raise ValueError(f"{field_name} must not be empty")
    if run.payload_schema_version < 1:
        raise ValueError("payload_schema_version must be positive")
    if run.canonical_status not in CANONICAL_STATUSES:
        raise ValueError(f"unsupported canonical status: {run.canonical_status!r}")

    for unit in run.units:
        if not unit.unit_type.strip() or not unit.unit_key.strip():
            raise ValueError("unit_type and unit_key must not be empty")
        if unit.canonical_status not in CANONICAL_STATUSES:
            raise ValueError(
                f"unsupported canonical status for {unit.unit_key!r}: "
                f"{unit.canonical_status!r}"
            )

        metric_names: set[str] = set()
        for metric in unit.metrics:
            if not metric.name.strip():
                raise ValueError("metric name must not be empty")
            if metric.name in metric_names:
                raise ValueError(
                    f"duplicate metric {metric.name!r} in unit {unit.unit_key!r}"
                )
            metric_names.add(metric.name)
            if metric.value_type not in METRIC_VALUE_TYPES:
                raise ValueError(
                    f"unsupported metric type for {metric.name!r}: "
                    f"{metric.value_type!r}"
                )
            if metric.value_type == "number" and not isinstance(metric.value, Decimal):
                raise TypeError(f"numeric metric {metric.name!r} must use Decimal")
            if metric.value_type == "boolean" and not isinstance(metric.value, bool):
                raise TypeError(f"boolean metric {metric.name!r} must use bool")
            if metric.value_type == "text" and not isinstance(metric.value, str):
                raise TypeError(f"text metric {metric.name!r} must use str")

    for _ in iter_unit_layers(run.units):
        pass
