"""Serialize selected decimal inputs without inventing absent values."""

from decimal import Decimal


def json_inputs(values: dict[str, Decimal | None]) -> dict[str, float | None]:
    return {
        name: None if value is None else float(value) for name, value in values.items()
    }
