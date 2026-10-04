"""Causal snapshot sampling shared by market selection and temporal calculations.

The sampler owns validation, deduplication, checkpoint ranking and the independent
market/availability bounds. It does not decide which bookmaker is required.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from infrastructure.settings import Config
from shared.temporal import as_utc

from .odds_trajectory_context import OddsSnapshotPoint, OddsTrajectoryContext
from .trajectory_selection import (
    SnapshotTargetWindow,
    build_snapshot_target_windows,
    effective_snapshot_timestamp,
    snapshot_target_rank,
    snapshot_time_error,
)

def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _timestamp(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


@dataclass(frozen=True, slots=True)
class TrajectoryPoint:
    point_id: str
    value: Decimal
    effective_at: datetime
    availability_at: datetime
    minutes_before_start: Decimal
    snapshot_id: int | None = None
    quote_id: int | None = None
    collected_at: datetime | None = None
    source_collected_at: datetime | None = None
    source_limit: Decimal | None = None
    exchange_size: Decimal | None = None
    observation_kind: str = "PERSISTED_SNAPSHOT"
    target_minute: int | None = None
    distance_from_target_minutes: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "POINT_ID": self.point_id,
            "VALUE": _number(self.value),
            "EFFECTIVE_AT": _timestamp(self.effective_at),
            "AVAILABILITY_AT": _timestamp(self.availability_at),
            "MINUTES_BEFORE_START": _number(self.minutes_before_start),
            "SNAPSHOT_ID": self.snapshot_id,
            "QUOTE_ID": self.quote_id,
            "COLLECTED_AT": _timestamp(self.collected_at),
            "SOURCE_COLLECTED_AT": _timestamp(self.source_collected_at),
            "SOURCE_LIMIT": _number(self.source_limit),
            "EXCHANGE_SIZE": _number(self.exchange_size),
            "OBSERVATION_KIND": self.observation_kind,
            "TARGET_MINUTE": self.target_minute,
            "DISTANCE_FROM_TARGET_MINUTES": _number(self.distance_from_target_minutes),
        }


@dataclass(frozen=True, slots=True)
class TrajectoryPointValue:
    """Small derived value view sharing all provenance with its source point."""

    original: TrajectoryPoint
    value: Decimal
    value_type: str

    @property
    def point_id(self) -> str:
        return f"{self.original.point_id}_{self.value_type}"

    def __getattr__(self, name):
        return getattr(self.original, name)

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.original.to_dict(),
            "POINT_ID": self.point_id,
            "VALUE": _number(self.value),
        }


def timestamp_value(value: datetime) -> float:
    return as_utc(value, field_name="trajectory timestamp").timestamp()


def minutes_before_start(start: datetime, value: datetime) -> Decimal:
    start_utc = as_utc(start, field_name="event start")
    value_utc = as_utc(value, field_name="trajectory timestamp")
    return Decimal(str((start_utc - value_utc).total_seconds())) / Decimal("60")


def _normalize_snapshot(
    snapshot: OddsSnapshotPoint,
    *,
    event_start: datetime,
    target_window: SnapshotTargetWindow,
    invalid: list[str],
    evaluation_as_of: datetime | None = None,
) -> tuple[TrajectoryPoint | None, bool]:
    """Normalise a raw snapshot into a TrajectoryPoint.

    When ``evaluation_as_of`` is provided, the stored ``collected_at`` must not
    be later than that evaluation boundary, and the
    provider's market timestamp must not be later than the target window.

    ``collected_at`` is the timestamp recorded for the observation. For a
    reconstructed historical key moment, ingestion deliberately records the
    target moment there; it is not necessarily the physical PostgreSQL insert
    time. ``evaluation_as_of`` therefore bounds recorded timestamps, while the
    fact that a row was actually loaded comes from the trajectory query itself.

    When no evaluation boundary is provided, the nominal
    ``target_window.is_after_cutoff(collected_at)`` logic applies.
    """
    collected_at = snapshot.collected_at
    source_at = snapshot.source_collected_at
    availability_at = collected_at
    time_error = snapshot_time_error(
        collected_at=collected_at,
    )
    if time_error is not None:
        invalid.append(f"snapshot:{snapshot.snapshot_id}:{time_error}")
        return None, False
    if evaluation_as_of is not None:
        # Phase 1: recorded observation time must be within the run's boundary.
        # This is not a physical insert-time check for reconstructed moments.
        if availability_at is not None and timestamp_value(
            availability_at
        ) > timestamp_value(evaluation_as_of):
            return None, True
        # Phase 2: prevent market observations after the permitted target window.
        effective_at_pre = effective_snapshot_timestamp(
            collected_at=availability_at,
            source_collected_at=source_at,
        )
        if timestamp_value(effective_at_pre) > timestamp_value(target_window.latest_at):
            return None, True
    elif target_window.is_after_cutoff(availability_at):
        return None, True
    try:
        odds = Decimal(snapshot.odds_value)
    except (InvalidOperation, TypeError, ValueError):
        invalid.append(f"snapshot:{snapshot.snapshot_id}:invalid_odds")
        return None, False
    if not odds.is_finite() or odds <= 1:
        invalid.append(
            f"snapshot:{snapshot.snapshot_id}:non_positive_odds"
        )
        return None, False
    effective_at = effective_snapshot_timestamp(
        collected_at=availability_at,
        source_collected_at=source_at,
    )
    point_id = (
        f"SNAPSHOT_{snapshot.snapshot_id}"
        if snapshot.snapshot_id is not None
        else f"QUOTE_{snapshot.quote_id}_{effective_at.isoformat()}"
    )

    def optional_value(value, name):
        if value is None:
            return None
        if not value.is_finite() or value < 0:
            invalid.append(
                f"snapshot:{snapshot.snapshot_id}:invalid_{name}"
            )
            return None
        return value

    return (
        TrajectoryPoint(
            point_id=point_id,
            value=odds,
            effective_at=effective_at,
            availability_at=availability_at,
            minutes_before_start=minutes_before_start(event_start, effective_at),
            snapshot_id=snapshot.snapshot_id,
            quote_id=snapshot.quote_id,
            collected_at=collected_at,
            source_collected_at=source_at,
            source_limit=optional_value(snapshot.source_limit, "source_limit"),
            exchange_size=optional_value(snapshot.exchange_size, "exchange_size"),
        ),
        False,
    )


def _deduplicate_points(points: Iterable[TrajectoryPoint]) -> list[TrajectoryPoint]:
    selected: dict[tuple[Any, ...], TrajectoryPoint] = {}
    for point in points:
        key = (
            ("snapshot", point.snapshot_id)
            if point.snapshot_id is not None
            else (
                "value",
                point.quote_id,
                timestamp_value(point.effective_at),
                point.value,
            )
        )
        selected[key] = point
    return sorted(
        selected.values(),
        key=lambda point: (
            timestamp_value(point.effective_at),
            timestamp_value(point.availability_at),
            point.snapshot_id if point.snapshot_id is not None else -1,
        ),
    )


def _project_checkpoints(
    points: Iterable[TrajectoryPoint],
    *,
    event_start: datetime,
    target_minutes: Iterable[int],
    target_windows: Mapping[int, SnapshotTargetWindow],
) -> dict[int, TrajectoryPoint]:
    projected: dict[int, TrajectoryPoint] = {}
    candidates = tuple(points)
    for target in target_minutes:
        window = target_windows[target]
        best = None
        for point in candidates:
            if not window.contains(point.availability_at):
                continue
            distance = abs(
                timestamp_value(point.availability_at)
                - timestamp_value(window.nominal_at)
            )
            rank = snapshot_target_rank(
                distance_seconds=distance,
                available_at=point.availability_at,
                source_collected_at=point.source_collected_at,
                snapshot_id=point.snapshot_id,
            )
            if best is None or rank < best[0]:
                best = (rank, point)
        if best is None:
            continue
        selected_rank, selected = best
        selected_distance = selected_rank[0]
        projected[target] = replace(
            selected,
            point_id=f"{selected.point_id}_TARGET_{target}",
            # Select by the recorded collection time, but preserve when this
            # price actually became effective in the market.
            effective_at=selected.effective_at,
            minutes_before_start=minutes_before_start(
                event_start,
                selected.effective_at,
            ),
            observation_kind=(
                "OPERATIVE_ENDPOINT" if target == min(target_minutes) else "CHECKPOINT"
            ),
            target_minute=target,
            distance_from_target_minutes=Decimal(str(selected_distance / 60)),
        )
    return projected


def _adaptive_with_endpoint(
    points: Iterable[TrajectoryPoint],
    endpoint: TrajectoryPoint,
    *,
    target_minute: int,
    event_start: datetime,
    evaluation_as_of: datetime | None = None,
) -> tuple[TrajectoryPoint, ...]:
    operative = replace(
        endpoint,
        point_id=f"{endpoint.point_id}_OPERATIVE_{target_minute}",
        # The endpoint defines the market cutoff through its source/effective
        # timestamp. Its recorded collection time remains a separate axis.
        effective_at=endpoint.effective_at,
        minutes_before_start=minutes_before_start(
            event_start,
            endpoint.effective_at,
        ),
        observation_kind="OPERATIVE_ENDPOINT",
        target_minute=target_minute,
        distance_from_target_minutes=endpoint.distance_from_target_minutes,
    )
    # The operative endpoint sets the market-state boundary. Independently,
    # evaluation_as_of bounds recorded collection timestamps.
    market_cutoff = timestamp_value(operative.effective_at)
    availability_cutoff = (
        timestamp_value(evaluation_as_of)
        if evaluation_as_of is not None
        else float("inf")
    )
    result = [
        point
        for point in points
        if timestamp_value(point.effective_at) <= market_cutoff
        and timestamp_value(point.availability_at) <= availability_cutoff
    ]
    exact_index = next(
        (
            index
            for index, point in enumerate(result)
            if point is endpoint
            or (
                point.snapshot_id is not None
                and point.snapshot_id == endpoint.snapshot_id
            )
            or (
                point.snapshot_id is None
                and endpoint.snapshot_id is None
                and point.quote_id == endpoint.quote_id
                and point.effective_at == endpoint.effective_at
                and point.value == endpoint.value
            )
        ),
        None,
    )
    if exact_index is None:
        result.append(operative)
    else:
        result[exact_index] = operative
    return tuple(
        sorted(
            result,
            key=lambda point: (
                timestamp_value(point.effective_at),
                timestamp_value(point.availability_at),
                point.point_id,
            ),
        )
    )


@dataclass(frozen=True, slots=True)
class TrajectorySample:
    points: tuple[TrajectoryPoint, ...]
    checkpoints: Mapping[int, TrajectoryPoint]
    adaptive: tuple[TrajectoryPoint, ...]
    endpoint: TrajectoryPoint | None
    invalid: tuple[str, ...]
    excluded_future_count: int
    first_after_cutoff_at: datetime | None

    @property
    def has_movement(self) -> bool:
        return self.endpoint is not None and (
            len(self.adaptive) >= 2 or len(self.checkpoints) >= 2
        )


@dataclass(frozen=True, slots=True)
class TrajectorySamplingPolicy:
    event_start: datetime
    evaluation_as_of: datetime | None
    target_minute: int
    windows: Mapping[int, SnapshotTargetWindow]

    @classmethod
    def for_context(
        cls, context: OddsTrajectoryContext, target: int, event_start: datetime
    ) -> TrajectorySamplingPolicy:
        if isinstance(target, bool) or not isinstance(target, int):
            raise TypeError("selected target_minute must be an integer")
        start = as_utc(event_start, field_name="event start")
        evaluation = (
            as_utc(context.evaluation_as_of, field_name="evaluation time")
            if context.evaluation_as_of is not None
            else None
        )
        targets = sorted(
            {int(value) for value in context.target_minutes_expected if int(value) >= target}
            | {target},
            reverse=True,
        )
        windows = build_snapshot_target_windows(
            event_starts_at=start,
            target_minutes=targets,
            evaluation_as_of=evaluation,
            tolerance_minutes=max(
                0, int(Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES)
            ),
        )
        return cls(start, evaluation, target, windows)

    @property
    def target_window(self) -> SnapshotTargetWindow:
        return self.windows[self.target_minute]

    def select(self, snapshots: Iterable[OddsSnapshotPoint]) -> TrajectorySample:
        points: list[TrajectoryPoint] = []
        invalid: list[str] = []
        excluded = 0
        first_future = None
        for snapshot in snapshots:
            point, future = _normalize_snapshot(
                snapshot,
                event_start=self.event_start,
                target_window=self.target_window,
                evaluation_as_of=self.evaluation_as_of,
                invalid=invalid,
            )
            excluded += int(future)
            if future and snapshot.collected_at is not None:
                first_future = (
                    min(first_future, snapshot.collected_at)
                    if first_future is not None
                    else snapshot.collected_at
                )
            if point is not None:
                points.append(point)
        ordered = tuple(_deduplicate_points(points))
        checkpoints = _project_checkpoints(
            ordered,
            event_start=self.event_start,
            target_minutes=tuple(self.windows),
            target_windows=self.windows,
        )
        endpoint = checkpoints.get(self.target_minute)
        adaptive = (
            ordered
            if endpoint is None
            else _adaptive_with_endpoint(
                ordered,
                endpoint,
                target_minute=self.target_minute,
                event_start=self.event_start,
                evaluation_as_of=self.evaluation_as_of,
            )
        )
        return TrajectorySample(
            ordered, checkpoints, adaptive, endpoint,
            tuple(invalid), excluded, first_future,
        )


def select_line_checkpoints(
    observations: Iterable[tuple[object, Mapping[int, TrajectoryPoint]]],
    target_minute: int,
    expected_targets: Iterable[int],
) -> tuple[
    dict[int, tuple[Decimal, tuple[TrajectoryPoint, ...]]],
    dict[int, tuple[Decimal, ...]],
]:
    """Resolve the same unique line at each checkpoint for planning and P4.

    Observations are (line value, projected checkpoints); prices remain separate.
    """
    by_target = defaultdict(lambda: defaultdict(list))
    for value, projected in observations:
        try:
            line = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if not line.is_finite():
            continue
        for target, point in projected.items():
            by_target[target][line].append(point)
    endpoints = by_target.get(target_minute, {})
    operative = next(iter(endpoints)) if len(endpoints) == 1 else None
    selected, ambiguous = {}, {}
    for target in expected_targets:
        candidates = by_target.get(target, {})
        if len(candidates) == 1:
            line = next(iter(candidates))
        elif len(candidates) > 1 and operative in candidates:
            line = operative
        else:
            if candidates:
                ambiguous[target] = tuple(sorted(candidates))
            continue
        selected[target] = (line, tuple(candidates[line]))
    return selected, ambiguous
