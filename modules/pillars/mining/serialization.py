"""Strict serialization helpers owned by the mining boundary."""

from __future__ import annotations

import math
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

from .contracts import PillarMiningMetric


def to_json_value(value: Any, *, copy_containers: bool = True) -> Any:
    """Capture JSON values, or reuse already captured containers during storage."""

    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats cannot be persisted as mining JSON")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite Decimals cannot be persisted as mining JSON")
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return to_json_value(value.value, copy_containers=copy_containers)
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: to_json_value(
                getattr(value, item.name), copy_containers=copy_containers
            )
            for item in fields(value)
        }
    if isinstance(value, dict):
        converted = {
            str(key): to_json_value(item, copy_containers=copy_containers)
            for key, item in value.items()
        }
        if (
            not copy_containers
            and converted.keys() == value.keys()
            and all(item is value[key] for key, item in converted.items())
        ):
            return value
        return converted
    if isinstance(value, (list, tuple)):
        converted = [
            to_json_value(item, copy_containers=copy_containers) for item in value
        ]
        if (
            not copy_containers
            and isinstance(value, list)
            and all(before is after for before, after in zip(value, converted))
        ):
            return value
        return converted
    if isinstance(value, (set, frozenset)):
        return [
            to_json_value(item, copy_containers=copy_containers)
            for item in sorted(value, key=str)
        ]

    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return to_json_value(to_dict(), copy_containers=copy_containers)
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
        return PillarMiningMetric(
            name=name, value_type="boolean", value=value, group=group
        )
    if isinstance(value, str):
        return PillarMiningMetric(
            name=name, value_type="text", value=value, group=group
        )
    decimal_value = optional_decimal(value)
    if decimal_value is not None:
        return PillarMiningMetric(
            name=name,
            value_type="number",
            value=decimal_value,
            group=group,
        )
    raise TypeError(f"metric {name!r} is not a supported scalar value")


def numeric_metrics(value: Any, *, prefix: str = "", group: str = "derived"):
    """Project numeric/boolean fields, retaining their paths inside arrays."""
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            yield from numeric_metrics(item, prefix=path, group=group)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            yield from numeric_metrics(item, prefix=f"{prefix}[{index}]", group=group)
    elif isinstance(value, (int, float, Decimal, bool)):
        yield scalar_metric(prefix, value, group=group)
