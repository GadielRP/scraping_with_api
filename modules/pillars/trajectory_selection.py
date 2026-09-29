"""Shared time-window and target selection policy for pillar odds."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Iterable

from shared.temporal import as_utc

if TYPE_CHECKING:
    from modules.pillars.odds_trajectory_context import OddsTrajectoryContext


# Kept in one policy module so development overrides affect every pillar alike.
HARDCODED_TARGET_MINUTE_BY_FLOW: dict[str, int | None] = {
    "pre_start_signal_profile": None,
}


@dataclass(frozen=True, slots=True)
class TargetMinuteSelection:
    """One target checkpoint selected for every pillar in an event run."""

    target_minute: int | None
    reason: str | None = None
    diagnostics: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SnapshotTargetWindow:
    """Shared target-time window, including the run's evaluation cutoff."""

    target_minute: int
    nominal_at: datetime
    earliest_at: datetime
    latest_at: datetime

    def contains(self, collected_at: datetime | None) -> bool:
        """Whether a snapshot timestamp falls in this common target window."""
        if collected_at is None:
            return False
        instant = as_utc(collected_at, field_name="snapshot collection time")
        return self.earliest_at <= instant <= self.latest_at

    def is_after_cutoff(self, collected_at: datetime | None) -> bool:
        if collected_at is None:
            return False
        instant = as_utc(collected_at, field_name="snapshot collection time")
        return instant > self.latest_at


def snapshot_time_error(
    *,
    collected_at: datetime | None,
) -> str | None:
    """Return why a snapshot cannot be placed on the persisted time axis."""
    return "missing_collection_timestamp" if collected_at is None else None


def effective_snapshot_timestamp(
    *,
    collected_at: datetime,
    source_collected_at: datetime | None,
) -> datetime:
    """Use provider time when plausible; clamp it to the stored snapshot time."""
    collected = as_utc(collected_at, field_name="snapshot collection time")
    if source_collected_at is None:
        return collected
    source = as_utc(source_collected_at, field_name="source snapshot time")
    return min(source, collected)


def build_snapshot_target_window(
    *,
    event_starts_at: datetime,
    target_minute: int,
    evaluation_as_of: datetime | None,
    tolerance_minutes: int,
) -> SnapshotTargetWindow:
    """Build the one shared tolerance and availability window for a target.

    `collected_at` is the persisted snapshot's availability/collection time. Its
    tolerance window is capped at the evaluation instant so a replay cannot use
    snapshots from after the data loaded for that run.
    """
    start = as_utc(event_starts_at, field_name="event start")
    target = int(target_minute)
    tolerance = int(tolerance_minutes)
    if tolerance < 0:
        raise ValueError("tolerance_minutes must be non-negative")

    nominal = start - timedelta(minutes=target)
    earliest = nominal - timedelta(minutes=tolerance)
    latest = nominal + timedelta(minutes=tolerance)
    if target >= 0:
        latest = min(latest, start)
    latest = (
        min(latest, as_utc(evaluation_as_of, field_name="evaluation time"))
        if evaluation_as_of is not None
        else min(latest, nominal)
    )

    return SnapshotTargetWindow(
        target_minute=target,
        nominal_at=nominal,
        earliest_at=earliest,
        latest_at=latest,
    )


def build_snapshot_target_windows(
    *,
    event_starts_at: datetime,
    target_minutes: Iterable[int],
    evaluation_as_of: datetime | None,
    tolerance_minutes: int,
) -> dict[int, SnapshotTargetWindow]:
    """Build a reusable window map once for all snapshots in an event."""
    return {
        int(target): build_snapshot_target_window(
            event_starts_at=event_starts_at,
            target_minute=int(target),
            evaluation_as_of=evaluation_as_of,
            tolerance_minutes=tolerance_minutes,
        )
        for target in target_minutes
    }


def snapshot_target_rank(
    *,
    distance_seconds: float,
    available_at: datetime,
    source_collected_at: datetime | None,
    snapshot_id: int | None,
) -> tuple[float, float, float, int]:
    """Rank candidates identically for projected markets and P4 endpoints."""
    available = as_utc(available_at, field_name="snapshot collection time")
    source_time = effective_snapshot_timestamp(
        collected_at=available,
        source_collected_at=source_collected_at,
    )
    return (
        float(distance_seconds),
        -source_time.timestamp(),
        -available.timestamp(),
        -(int(snapshot_id) if snapshot_id is not None else -1),
    )


def select_target_minute(
    context: OddsTrajectoryContext | None,
    *,
    flow_id: str,
    expected_event_id: int | None = None,
    allowed_target_minutes: Iterable[int] | None = None,
    evaluation_minute: int | None = None,
) -> TargetMinuteSelection:
    """Choose one present, allowed, causal checkpoint for the whole event run."""
    if context is None:
        return TargetMinuteSelection(None, "missing_odds_trajectory_context")
    if not context.available:
        return TargetMinuteSelection(None, "odds_trajectory_unavailable")
    if (
        expected_event_id is not None
        and context.event_id is not None
        and int(context.event_id) != int(expected_event_id)
    ):
        return TargetMinuteSelection(
            None,
            "event_id_mismatch",
            {"trajectory_event_id": context.event_id},
        )

    override = HARDCODED_TARGET_MINUTE_BY_FLOW.get(flow_id)
    if override is not None:
        return TargetMinuteSelection(
            int(override),
            diagnostics={"selection": "hardcoded_override", "flow_id": flow_id},
        )

    allowed = (
        None
        if allowed_target_minutes is None
        else {int(minute) for minute in allowed_target_minutes}
    )
    current_minute = (
        None if evaluation_minute is None else int(evaluation_minute)
    )
    candidates = [
        int(minute)
        for minute in context.target_minutes_present
        if allowed is None or int(minute) in allowed
        if current_minute is None or int(minute) >= current_minute
    ]
    if not candidates:
        return TargetMinuteSelection(
            None,
            "no_target_minutes_present",
            {
                "flow_id": flow_id,
                "target_minutes_present": list(context.target_minutes_present),
                "allowed_target_minutes": sorted(allowed) if allowed is not None else None,
                "evaluation_minute": current_minute,
            },
        )
    return TargetMinuteSelection(
        min(candidates),
        diagnostics={
            "selection": "latest_causal_available",
            "flow_id": flow_id,
            "evaluation_minute": current_minute,
        },
    )


__all__ = [
    "HARDCODED_TARGET_MINUTE_BY_FLOW",
    "SnapshotTargetWindow",
    "TargetMinuteSelection",
    "build_snapshot_target_window",
    "build_snapshot_target_windows",
    "effective_snapshot_timestamp",
    "select_target_minute",
    "snapshot_time_error",
    "snapshot_target_rank",
]
