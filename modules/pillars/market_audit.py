"""Serialize selected market inputs and trajectory metadata for both pillars."""

from decimal import Decimal
from typing import Any

from .odds_trajectory_context import OddsTrajectoryContext


def json_inputs(values: dict[str, Decimal | None]) -> dict[str, float | None]:
    return {
        name: None if value is None else float(value) for name, value in values.items()
    }


def build_raw_audit(
    *,
    odds_context: OddsTrajectoryContext | None,
    periods: dict[str, Any],
    inputs: dict[str, float | None],
    input_trace: dict[str, dict[str, Any]],
    extraction_diagnostics: dict[str, Any],
    reason: str | None = None,
) -> dict[str, Any]:
    raw = {
        "inputs": inputs,
        "input_trace": input_trace,
        "periods": periods,
        "extraction_diagnostics": extraction_diagnostics,
        "target_minutes_expected": list(
            getattr(odds_context, "target_minutes_expected", ())
        ),
        "target_minutes_present": list(
            getattr(odds_context, "target_minutes_present", ())
        ),
    }
    if reason is not None:
        raw["reason"] = reason
    return raw
