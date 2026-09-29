"""Raw input audit for P4; deliberately separate from analytical output."""

from __future__ import annotations

from typing import Any

from .models import P4ExtractionResult


def build_raw_audit(extraction: P4ExtractionResult) -> dict[str, Any]:
    inputs: dict[str, Any] = {}
    input_trace: dict[str, Any] = {}
    for series in (*extraction.adaptive_series, *extraction.checkpoint_series):
        if series.value_type != "ODDS_PRICE":
            continue
        inputs[series.series_id] = [float(point.value) for point in series.points]
        input_trace[series.series_id] = {
            **series.market_dict(),
            "POINT_IDS": [point.point_id for point in series.points],
            "TARGET_MINUTES": [point.target_minute for point in series.points],
        }
    return {
        "reason": extraction.reason,
        "nominal_target_as_of": (
            extraction.nominal_target_as_of.isoformat()
            if extraction.nominal_target_as_of is not None
            else None
        ),
        "evaluation_as_of": (
            extraction.evaluation_as_of.isoformat()
            if extraction.evaluation_as_of is not None
            else None
        ),
        "operative_as_of": extraction.operative_as_of.isoformat(),
        "inputs": inputs,
        "input_trace": input_trace,
        "periods": extraction.periods,
        "extraction_diagnostics": {
            "source_series_seen": extraction.source_series_seen,
            "endpoint_series_present": extraction.endpoint_series_present,
            "excluded_future_points": extraction.excluded_future_points,
            "missing_inputs": list(extraction.missing_inputs),
            "missing_endpoint_details": list(extraction.missing_endpoint_details),
            "invalid_inputs": list(extraction.invalid_inputs),
            "ambiguous_inputs": list(extraction.ambiguous_inputs),
            "persistence_provenance": "LEGACY_MIXED_COLLECTED_AT",
            "observation_kind_available": False,
            "ingestion_batch_available": False,
            "main_line_at_capture_available": False,
        },
    }


__all__ = ["build_raw_audit"]
