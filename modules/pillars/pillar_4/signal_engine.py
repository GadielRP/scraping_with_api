"""Assemble P4 temporal features into one canonical signal profile DTO."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

from .models import P4ExtractionResult
from .semantic_metrics import build_checkpoint_semantic_series
from .signal_models import P4SeriesResult, P4SignalProfile, P4ViewProfile
from .trajectory_engine import build_trajectory_features


ENGINE_VERSION = "p4-signal-profile-v1"


def _view_status(series: Iterable[P4SeriesResult]) -> str:
    primary = [
        item for item in series if item.market.get("VALUE_TYPE") == "ODDS_PRICE"
    ]
    if not primary:
        return "INSUFFICIENT_DATA"
    return "PARTIAL" if any(item.status != "ACTIVE" for item in primary) else "ACTIVE"


def _summary_state(field: str, value: Any) -> str:
    if value is None:
        return "UNAVAILABLE"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, dict):
        if field == "TURNING_STRUCTURE_RAW":
            raw_count = value.get("SIGN_CHANGE_COUNT_RAW")
            if raw_count is None:
                return "UNAVAILABLE"
            count = int(raw_count)
            if count == 0:
                return "NO_TURN"
            if value.get("ZERO_BRIDGED_SIGN_CHANGE"):
                return "PLATEAU"
            return "DIRECT" if count == 1 else "MULTIPLE"
        return str(value.get("STATE") or "AVAILABLE")
    return str(value)


def _domain_summary(series: Iterable[P4SeriesResult]) -> dict[str, Any]:
    fields = (
        "PATH_PATTERN_RAW",
        "NET_DIRECTION_RAW",
        "FINAL_RUN_DIRECTION_RAW",
        "TURNING_STRUCTURE_RAW",
        "CORRECTION_STATE_RAW",
        "OVERSHOOT_RAW",
        "ACCELERATION_RAW",
        "DECELERATION_RAW",
        "BOOK_EXCHANGE_RELATION_CHANGE_RAW",
    )
    result: dict[str, Any] = {}
    all_series = tuple(series)
    for domain in ("SIDE", "TOTALS"):
        domain_series = [
            item
            for item in all_series
            if item.market.get("DOMAIN") == domain
        ]
        grouped: dict[str, dict[str, list[str]]] = {
            field: defaultdict(list) for field in fields
        }
        for item in domain_series:
            for field in fields:
                state = _summary_state(field, item.structural_signals.get(field))
                grouped[field][state].append(item.series_id)
        result[domain] = {
            "SERIES_IDS": [item.series_id for item in domain_series],
            "VIEWS": {
                view: sorted(
                    item.series_id
                    for item in domain_series
                    if item.market.get("VIEW") == view
                )
                for view in ("ADAPTIVE_VIEW", "CHECKPOINT_VIEW")
            },
            "STRUCTURAL_SIGNALS": {
                field: {
                    state: sorted(series_ids)
                    for state, series_ids in sorted(states.items())
                }
                for field, states in grouped.items()
            },
        }
    return result


def build_p4_signal_profile(
    extraction: P4ExtractionResult,
    *,
    debug_mode: bool = False,
) -> P4SignalProfile:
    if not extraction.usable:
        raise ValueError("P4 extraction is not usable")
    adaptive_series = build_trajectory_features(
        extraction.adaptive_series,
        apply_relations=False,
    )
    checkpoint_inputs = (
        *extraction.checkpoint_series,
        *build_checkpoint_semantic_series(extraction.checkpoint_series),
    )
    checkpoint_series = build_trajectory_features(
        checkpoint_inputs,
        apply_relations=True,
    )
    adaptive_view = (
        None
        if not adaptive_series
        else P4ViewProfile(
            source_mode="PERSISTED_SNAPSHOTS",
            status=_view_status(adaptive_series),
            series=adaptive_series,
        )
    )
    checkpoint_view = (
        None
        if not checkpoint_series
        else P4ViewProfile(
            source_mode="FIXED_CHECKPOINTS",
            status=_view_status(checkpoint_series),
            series=checkpoint_series,
        )
    )
    combined = (*adaptive_series, *checkpoint_series)
    primary = [
        item for item in combined if item.market.get("VALUE_TYPE") == "ODDS_PRICE"
    ]
    profile_status = (
        "PARTIAL"
        if extraction.missing_inputs
        or extraction.invalid_inputs
        or extraction.ambiguous_inputs
        or any(item.status != "ACTIVE" for item in primary)
        else "ACTIVE"
    )
    return P4SignalProfile(
        meta={
            "TARGET_MINUTE": extraction.target_minute,
            "OPERATIVE_AS_OF": extraction.operative_as_of.isoformat(),
            "SOURCE_SERIES_SEEN": extraction.source_series_seen,
            "ENDPOINT_SERIES_PRESENT": extraction.endpoint_series_present,
            "TRAJECTORY_PARTIAL": profile_status == "PARTIAL",
        },
        adaptive_view=adaptive_view,
        checkpoint_view=checkpoint_view,
        structural_domain_summary=_domain_summary(combined),
        summary={
            "STATUS": profile_status,
            "ADAPTIVE_SERIES_COUNT": len(adaptive_series),
            "CHECKPOINT_SERIES_COUNT": len(checkpoint_series),
            "PRIMARY_ODDS_SERIES_COUNT": len(primary),
            "EXCLUDED_FUTURE_POINT_COUNT": extraction.excluded_future_points,
        },
        traceability={
            "ENGINE_VERSION": ENGINE_VERSION,
            "CAUSAL_CUTOFF_POLICY": "AVAILABILITY_AT_LTE_NOMINAL_OPERATIVE_AS_OF",
            "AVAILABILITY_FIELD_POLICY": "COLLECTED_AT_ELSE_SOURCE_COLLECTED_AT",
            "PERSISTENCE_PROVENANCE": "LEGACY_MIXED_COLLECTED_AT",
            "LINE_SELECTION_POLICY": "UNIQUE_CONTRACT_PER_CHECKPOINT_OR_AMBIGUOUS",
            "DEBUG_MODE": bool(debug_mode),
        },
    )


__all__ = ["ENGINE_VERSION", "build_p4_signal_profile"]
