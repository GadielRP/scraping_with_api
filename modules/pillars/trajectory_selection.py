"""Shared time-window and target selection policy for pillar odds."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Iterable, Mapping, TypeVar

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
    """Use the provider clock, or actual observation time when it is absent."""
    collected = as_utc(collected_at, field_name="snapshot collection time")
    if source_collected_at is None:
        return collected
    source = as_utc(source_collected_at, field_name="source snapshot time")
    return source


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


Snapshot = TypeVar("Snapshot")


def select_checkpoint_snapshots(
    snapshots: Iterable[Snapshot],
    windows: Mapping[int, SnapshotTargetWindow],
) -> dict[int, Snapshot]:
    """Project one instrument's source states, falling back to nearby observations.

    Inputs have already passed their consumer's evaluation availability checks.
    Source ticks advance once over chronological checkpoints. Missing-source
    observations are considered only inside the checkpoint tolerance window.
    """
    sourced, observed = [], []
    for snapshot in snapshots:
        if snapshot.collected_at is None:
            continue
        (sourced if snapshot.source_collected_at is not None else observed).append(
            snapshot
        )
    sourced.sort(
        key=lambda item: (
            item.source_collected_at,
            item.collected_at,
            item.snapshot_id if item.snapshot_id is not None else -1,
        )
    )
    observed.sort(
        key=lambda item: (
            item.collected_at,
            item.snapshot_id if item.snapshot_id is not None else -1,
        )
    )
    selected = {}
    source_index = observation_index = 0
    last_source = None
    for target, window in sorted(windows.items(), key=lambda item: item[1].nominal_at):
        if window.latest_at < window.nominal_at:
            continue  # The checkpoint has not been reached by this evaluation.
        while (
            source_index < len(sourced)
            and sourced[source_index].source_collected_at <= window.nominal_at
        ):
            last_source = sourced[source_index]
            source_index += 1
        if last_source is not None:
            selected[target] = last_source
            continue
        while (
            observation_index < len(observed)
            and observed[observation_index].collected_at < window.earliest_at
        ):
            observation_index += 1
        best = None
        index = observation_index
        while (
            index < len(observed) and observed[index].collected_at <= window.latest_at
        ):
            candidate = observed[index]
            rank = (
                abs(candidate.collected_at - window.nominal_at),
                -candidate.collected_at.timestamp(),
                -(candidate.snapshot_id if candidate.snapshot_id is not None else -1),
            )
            if best is None or rank < best[0]:
                best = (rank, candidate)
            index += 1
        if best is not None:
            selected[target] = best[1]
    return selected


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
    current_minute = None if evaluation_minute is None else int(evaluation_minute)
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
                "allowed_target_minutes": (
                    sorted(allowed) if allowed is not None else None
                ),
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
    "select_checkpoint_snapshots",
]
