"""Strict serialization helpers owned by the mining boundary."""

from __future__ import annotations

import math
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

from .contracts import PillarMiningMetric


def to_json_value(value: Any) -> Any:
    """Convert supported domain values to deterministic JSON-safe values."""

    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats cannot be persisted as mining JSON")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite Decimals cannot be persisted as mining JSON")
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return to_json_value(value.value)
    if is_dataclass(value) and not isinstance(value, type):
        return to_json_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): to_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return [to_json_value(item) for item in sorted(value, key=str)]

    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return to_json_value(to_dict())
    raise TypeError(f"unsupported mining JSON value: {type(value).__name__}")


def optional_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def scalar_metric(
    name: str,
    value: Any,
    *,
    group: str | None = None,
) -> PillarMiningMetric | None:
    """Build one typed metric; ``None`` means the producer did not emit it."""

    if value is None:
        return None
    if isinstance(value, bool):
        return PillarMiningMetric(name=name, value_type="boolean", value=value, group=group)
    if isinstance(value, str):
        return PillarMiningMetric(name=name, value_type="text", value=value, group=group)
    decimal_value = optional_decimal(value)
    if decimal_value is not None:
        return PillarMiningMetric(
            name=name,
            value_type="number",
            value=decimal_value,
            group=group,
        )
    raise TypeError(f"metric {name!r} is not a supported scalar value")
