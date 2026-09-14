"""Build per-series temporal features and deterministic structural readings."""

from __future__ import annotations

from typing import Iterable

from .metrics import build_temporal_features
from .models import P4SeriesInput
from .relations import build_book_exchange_relation_changes
from .signal_models import P4SeriesResult


def _series_result(series: P4SeriesInput) -> P4SeriesResult:
    gap_present = bool(series.missing_target_minutes)
    legs, features, signals = build_temporal_features(
        series.points,
        gap_present=gap_present,
        expected_target_minutes=series.expected_target_minutes,
    )
    if series.value_type == "EXCHANGE_SIZE":
        legs = [{**leg, "VELOCITY_RAW": None} for leg in legs]
        features = {
            **features,
            "VELOCITY_RAW": None,
            "VELOCITY_CHANGES_RAW": [],
        }
        signals = {
            **signals,
            "ACCELERATION_RAW": None,
            "DECELERATION_RAW": None,
        }
    status = (
        "PARTIAL"
        if (
            len(series.points) < 2
            or gap_present
            or not series.operative_endpoint_present
        )
        else "ACTIVE"
    )
    return P4SeriesResult(
        series_id=series.series_id,
        status=status,
        market=series.market_dict(),
        points=tuple(point.to_dict() for point in series.points),
        legs=tuple(legs),
        raw_temporal_features=features,
        structural_signals=signals,
        traceability={
            "BASE_SERIES_ID": series.base_series_id,
            "TRAJECTORY_SOURCE_MODE": (
                "PERSISTED_SNAPSHOTS"
                if series.view == "ADAPTIVE_VIEW"
                else "FIXED_CHECKPOINTS"
            ),
            "EXPECTED_TARGET_MINUTES": list(series.expected_target_minutes),
            "MISSING_TARGET_MINUTES": list(series.missing_target_minutes),
            "DIAGNOSTICS": list(series.diagnostics),
            "CONSTITUENT_SERIES_IDS": list(series.constituent_series_ids),
            "OPERATIVE_ENDPOINT_PRESENT": series.operative_endpoint_present,
            "OBSERVATION_COUNT": len(series.points),
            "LEG_COUNT": len(legs),
        },
    )


def build_trajectory_features(
    series_inputs: Iterable[P4SeriesInput],
    *,
    apply_relations: bool,
) -> tuple[P4SeriesResult, ...]:
    results = [_series_result(series) for series in series_inputs]
    if apply_relations:
        payloads = [result.to_dict() for result in results]
        relation_by_series = build_book_exchange_relation_changes(payloads)
        updated: list[P4SeriesResult] = []
        for result in results:
            signals = dict(result.structural_signals)
            signals["BOOK_EXCHANGE_RELATION_CHANGE_RAW"] = relation_by_series.get(
                result.series_id
            )
            updated.append(
                P4SeriesResult(
                    series_id=result.series_id,
                    market=result.market,
                    points=result.points,
                    legs=result.legs,
                    raw_temporal_features=result.raw_temporal_features,
                    structural_signals=signals,
                    traceability=result.traceability,
                    status=result.status,
                )
            )
        results = updated
    return tuple(results)


__all__ = ["build_trajectory_features"]
