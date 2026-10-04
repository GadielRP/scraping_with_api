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
    if (
        not series.operative_endpoint_present
        and "LINE_CONTRACT_ENDED" in series.diagnostics
    ):
        status = "CONTRACT_ENDED"
    elif not series.operative_endpoint_present or len(series.points) < 2:
        status = "INSUFFICIENT_DATA"
    else:
        status = "ACTIVE"
    if status != "ACTIVE":
        # Keep counts and observation identity, but expose no artificial movement.
        features = {
            key: (
                ([] if isinstance(value, list) else None)
                if key.endswith("_RAW")
                else value
            )
            for key, value in features.items()
        }
        signals = {key: None for key in signals}
    return P4SeriesResult(
        series_id=series.series_id,
        status=status,
        market=series.market_dict(),
        points=series.points,
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
    results = []
    for series in series_inputs:
        try:
            results.append(_series_result(series))
        except Exception as exc:
            results.append(
                P4SeriesResult(
                    series_id=series.series_id,
                    market=series.market_dict(),
                    points=series.points,
                    legs=(),
                    raw_temporal_features={},
                    structural_signals={},
                    status="ERROR",
                    traceability={
                        "OPERATIVE_ENDPOINT_PRESENT": series.operative_endpoint_present,
                        "OBSERVATION_COUNT": len(series.points),
                        "MISSING_TARGET_MINUTES": list(series.missing_target_minutes),
                        "ERROR_CLASS": type(exc).__name__,
                    },
                )
            )
    if apply_relations:
        # Relations need checkpoint values only; never serialize provenance here.
        payloads = [
            {
                "SERIES_ID": result.series_id,
                "MARKET": result.market,
                "POINTS": [
                    {"TARGET_MINUTE": point.target_minute, "VALUE": point.value}
                    for point in result.points
                ],
            }
            for result in results
            if result.status != "ERROR"
        ]
        relation_by_series = build_book_exchange_relation_changes(payloads)
        updated: list[P4SeriesResult] = []
        for result in results:
            signals = dict(result.structural_signals)
            if result.status == "ACTIVE":
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
