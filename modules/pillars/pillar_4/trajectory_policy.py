"""Causal extraction policy for P4 price and line trajectories."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from infrastructure.settings import Config
from modules.pillars.context import EventContext, EventIdentity
from modules.pillars.odds_trajectory_context import (
    OddsSnapshotPoint,
    OddsTrajectoryContext,
)
from modules.pillars.trajectory_selection import (
    SnapshotTargetWindow,
    TargetMinuteSelection,
    build_snapshot_target_window,
    build_snapshot_target_windows,
    effective_snapshot_timestamp,
    snapshot_target_rank,
    snapshot_time_error,
)
from shared.temporal import as_utc

from .models import P4ExtractionResult, P4Point, P4SeriesInput
from .periods import (
    REQUIRED_BOOKIE_IDS,
    SUPPORTED_BOOKIE_IDS,
    bookmaker_role,
    period_key,
    resolve_domain,
)


def _datetime_value(value: datetime) -> float:
    return as_utc(value, field_name="P4 trajectory timestamp").timestamp()


def _minutes_before_start(start: datetime, value: datetime) -> Decimal:
    start_utc = as_utc(start, field_name="event start")
    value_utc = as_utc(value, field_name="P4 trajectory timestamp")
    return Decimal(str((start_utc - value_utc).total_seconds())) / Decimal("60")


def _slug(value: Any) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "NA").strip().upper())
    return token.strip("_") or "NA"


def _decimal_line(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None


def _base_series_id(
    *,
    domain: str,
    market_group: str,
    market_period: str,
    market_name: str,
    line_value: str | None,
    choice_name: str,
    bookie_id: int | None,
    bookie_name: str,
    source: str | None,
    exchange_side: str | None,
    exchange_level: int,
    quote_id: int | None,
) -> str:
    parts = (
        domain,
        market_group,
        market_period,
        market_name,
        line_value or "NO_LINE",
        choice_name,
        bookie_id if bookie_id is not None else bookie_name,
        source or "UNKNOWN_SOURCE",
        exchange_side or "SINGLE",
        exchange_level,
        quote_id if quote_id is not None else "NO_QUOTE",
    )
    return "_".join(_slug(part) for part in parts)


def _series_id(base: str, view: str, value_type: str) -> str:
    return f"{base}_{_slug(view)}_{_slug(value_type)}"


def _normalize_snapshot(
    snapshot: OddsSnapshotPoint,
    *,
    event_start: datetime,
    target_window: SnapshotTargetWindow,
    invalid: list[str],
    series_label: str,
    evaluation_as_of: datetime | None = None,
) -> tuple[P4Point | None, bool]:
    """Normalise a raw snapshot into a P4Point.

    When ``evaluation_as_of`` is provided, the stored ``collected_at`` must not
    be later than that evaluation boundary, and the
    provider's market timestamp must not be later than the target window.

    ``collected_at`` is the timestamp recorded for the observation. For a
    reconstructed historical key moment, ingestion deliberately records the
    target moment there; it is not necessarily the physical PostgreSQL insert
    time. ``evaluation_as_of`` therefore bounds recorded timestamps, while the
    fact that a row was actually loaded comes from the trajectory query itself.

    When ``evaluation_as_of`` is *None* (no simulation) the original
    ``target_window.is_after_cutoff(collected_at)`` logic applies.
    """
    collected_at = snapshot.collected_at
    source_at = snapshot.source_collected_at
    availability_at = collected_at
    time_error = snapshot_time_error(
        collected_at=collected_at,
    )
    if time_error is not None:
        invalid.append(
            f"{series_label}:snapshot:{snapshot.snapshot_id}:{time_error}"
        )
        return None, False
    if evaluation_as_of is not None:
        # Phase 1: recorded observation time must be within the run's boundary.
        # This is not a physical insert-time check for reconstructed moments.
        if (
            availability_at is not None
            and _datetime_value(availability_at) > _datetime_value(evaluation_as_of)
        ):
            return None, True
        # Phase 2: prevent market observations after the permitted target window.
        effective_at_pre = effective_snapshot_timestamp(
            collected_at=availability_at,
            source_collected_at=source_at,
        )
        if _datetime_value(effective_at_pre) > _datetime_value(target_window.latest_at):
            return None, True
    elif target_window.is_after_cutoff(availability_at):
        return None, True
    try:
        odds = Decimal(snapshot.odds_value)
    except (InvalidOperation, TypeError, ValueError):
        invalid.append(f"{series_label}:snapshot:{snapshot.snapshot_id}:invalid_odds")
        return None, False
    if odds <= 0:
        invalid.append(f"{series_label}:snapshot:{snapshot.snapshot_id}:non_positive_odds")
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
    return (
        P4Point(
            point_id=point_id,
            value=odds,
            effective_at=effective_at,
            availability_at=availability_at,
            minutes_before_start=_minutes_before_start(event_start, effective_at),
            snapshot_id=snapshot.snapshot_id,
            quote_id=snapshot.quote_id,
            collected_at=collected_at,
            source_collected_at=source_at,
            source_limit=snapshot.source_limit,
            exchange_size=snapshot.exchange_size,
        ),
        False,
    )


def _deduplicate_points(points: Iterable[P4Point]) -> list[P4Point]:
    selected: dict[tuple[Any, ...], P4Point] = {}
    for point in points:
        key = (
            ("snapshot", point.snapshot_id)
            if point.snapshot_id is not None
            else (
                "value",
                point.quote_id,
                _datetime_value(point.effective_at),
                point.value,
            )
        )
        selected[key] = point
    return sorted(
        selected.values(),
        key=lambda point: (
            _datetime_value(point.effective_at),
            _datetime_value(point.availability_at),
            point.snapshot_id if point.snapshot_id is not None else -1,
        ),
    )


def _project_checkpoints(
    points: Iterable[P4Point],
    *,
    event_start: datetime,
    target_minutes: Iterable[int],
    target_windows: Mapping[int, SnapshotTargetWindow],
) -> dict[int, P4Point]:
    projected: dict[int, P4Point] = {}
    candidates = list(points)
    for target in target_minutes:
        window = target_windows[target]
        eligible: list[tuple[tuple[float, float, float, int], P4Point]] = []
        for point in candidates:
            if not window.contains(point.availability_at):
                continue
            distance = abs(
                _datetime_value(point.availability_at)
                - _datetime_value(window.nominal_at)
            )
            rank = snapshot_target_rank(
                distance_seconds=distance,
                available_at=point.availability_at,
                source_collected_at=point.source_collected_at,
                snapshot_id=point.snapshot_id,
            )
            eligible.append((rank, point))
        if not eligible:
            continue
        selected_rank, selected = min(eligible, key=lambda item: item[0])
        selected_distance = selected_rank[0]
        projected[target] = replace(
            selected,
            point_id=f"{selected.point_id}_TARGET_{target}",
            # Select by the recorded collection time, but preserve when this
            # price actually became effective in the market.
            effective_at=selected.effective_at,
            minutes_before_start=_minutes_before_start(
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
    points: list[P4Point],
    endpoint: P4Point,
    *,
    target_minute: int,
    event_start: datetime,
    evaluation_as_of: datetime | None = None,
) -> tuple[P4Point, ...]:
    operative = replace(
        endpoint,
        point_id=f"{endpoint.point_id}_OPERATIVE_{target_minute}",
        # The endpoint defines the market cutoff through its source/effective
        # timestamp. Its recorded collection time remains a separate axis.
        effective_at=endpoint.effective_at,
        minutes_before_start=_minutes_before_start(
            event_start,
            endpoint.effective_at,
        ),
        observation_kind="OPERATIVE_ENDPOINT",
        target_minute=target_minute,
        distance_from_target_minutes=endpoint.distance_from_target_minutes,
    )
    # The operative endpoint sets the market-state boundary. Independently,
    # evaluation_as_of bounds recorded collection timestamps.
    market_cutoff = _datetime_value(operative.effective_at)
    availability_cutoff = (
        _datetime_value(evaluation_as_of)
        if evaluation_as_of is not None
        else float("inf")
    )
    result = [
        point
        for point in points
        if _datetime_value(point.effective_at) <= market_cutoff
        and _datetime_value(point.availability_at) <= availability_cutoff
    ]
    exact_index = next(
        (
            index
            for index, point in enumerate(result)
            if point.snapshot_id is not None
            and point.snapshot_id == endpoint.snapshot_id
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
                _datetime_value(point.effective_at),
                _datetime_value(point.availability_at),
                point.point_id,
            ),
        )
    )


def _derived_points(points: Iterable[P4Point], value_type: str) -> tuple[P4Point, ...]:
    derived: list[P4Point] = []
    for point in points:
        value: Decimal | None
        if value_type == "IMPLIED_PROBABILITY_RAW":
            value = Decimal("1") / point.value
        elif value_type == "SOURCE_LIMIT":
            value = point.source_limit
        elif value_type == "EXCHANGE_SIZE":
            value = point.exchange_size
        else:
            value = point.value
        if value is None:
            continue
        derived.append(
            replace(
                point,
                point_id=f"{point.point_id}_{value_type}",
                value=Decimal(value),
            )
        )
    return tuple(derived)


def _append_value_series(
    destination: list[P4SeriesInput],
    *,
    base: str,
    view: str,
    values: tuple[P4Point, ...],
    identity: dict[str, Any],
    expected: tuple[int, ...] = (),
    missing: tuple[int, ...] = (),
    diagnostics: tuple[str, ...] = (),
) -> None:
    for value_type in (
        "ODDS_PRICE",
        "IMPLIED_PROBABILITY_RAW",
        "SOURCE_LIMIT",
        "EXCHANGE_SIZE",
    ):
        points = _derived_points(values, value_type)
        if not points:
            continue
        endpoint_present = any(
            point.observation_kind == "OPERATIVE_ENDPOINT" for point in points
        )
        destination.append(
            P4SeriesInput(
                series_id=_series_id(base, view, value_type),
                base_series_id=base,
                view=view,
                value_type=value_type,
                points=points,
                expected_target_minutes=expected,
                missing_target_minutes=missing,
                diagnostics=diagnostics,
                operative_endpoint_present=endpoint_present,
                **identity,
            )
        )


def _line_series(
    line_observations: list[dict[str, Any]],
    *,
    event_start: datetime,
    target_minute: int,
    expected_targets: tuple[int, ...],
    ambiguous: list[str],
    issue_bookie_ids: set[int],
) -> list[P4SeriesInput]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for observation in line_observations:
        key = (
            observation["domain"],
            observation["market_id"],
            observation["market_group"],
            observation["market_period"],
            observation["market_name"],
            observation["bookie_id"],
            observation["bookie_name"],
            observation["source"],
            observation["exchange_side"],
            observation["exchange_level"],
        )
        groups[key].append(observation)

    result: list[P4SeriesInput] = []
    for key, related in sorted(groups.items(), key=lambda item: str(item[0])):
        # Determine the unique operative line at the target_minute first.
        # This is used to disambiguate historical checkpoints where an old
        # line contract overlaps with the new (operative) one.
        operative_line_candidates: dict[Decimal, list[P4Point]] = defaultdict(list)
        for observation in related:
            line = _decimal_line(observation["line_value"])
            if line is None:
                continue
            point = observation["projected"].get(target_minute)
            if point is not None:
                operative_line_candidates[line].append(point)
        operative_line: Decimal | None = (
            next(iter(operative_line_candidates))
            if len(operative_line_candidates) == 1
            else None
        )
        points: list[P4Point] = []
        missing: list[int] = []
        for target in expected_targets:
            line_candidates: dict[Decimal, list[P4Point]] = defaultdict(list)
            for observation in related:
                line = _decimal_line(observation["line_value"])
                if line is None:
                    continue
                point = observation["projected"].get(target)
                if point is not None:
                    line_candidates[line].append(point)
            if len(line_candidates) != 1:
                # If a unique operative line was identified, use it to select
                # among historical checkpoint candidates (line transition case).
                if len(line_candidates) > 1 and operative_line is not None and operative_line in line_candidates:
                    # Disambiguate: use the operative line's checkpoint projection.
                    # No ambiguity flag — this is an expected line transition.
                    pass
                else:
                    missing.append(target)
                    if len(line_candidates) > 1:
                        label = "|".join(str(part) for part in key)
                        ambiguous.append(
                            f"LINE_SELECTION:{label}:TARGET:{target}:"
                            f"{','.join(str(line) for line in sorted(line_candidates))}"
                        )
                        if key[5] is not None:
                            issue_bookie_ids.add(int(key[5]))
                    continue
                # Override line_candidates to just the operative line's data
                line_candidates = {operative_line: line_candidates[operative_line]}
            # fall through: len(line_candidates) == 1
            line, source_points = next(iter(line_candidates.items()))
            availability = max(
                source_points,
                key=lambda point: _datetime_value(point.availability_at),
            ).availability_at
            effective = availability
            points.append(
                P4Point(
                    point_id=f"LINE_TARGET_{target}",
                    value=line,
                    effective_at=effective,
                    availability_at=availability,
                    minutes_before_start=_minutes_before_start(event_start, effective),
                    observation_kind=(
                        "OPERATIVE_ENDPOINT" if target == target_minute else "CHECKPOINT"
                    ),
                    target_minute=target,
                    distance_from_target_minutes=abs(
                        _minutes_before_start(event_start, availability)
                        - Decimal(target)
                    ),
                )
            )
        if not any(point.target_minute == target_minute for point in points):
            continue
        (
            domain,
            market_id,
            market_group,
            market_period,
            market_name,
            bookie_id,
            bookie_name,
            source,
            exchange_side,
            exchange_level,
        ) = key
        base = _base_series_id(
            domain=domain,
            market_group=market_group,
            market_period=market_period,
            market_name=market_name,
            line_value="LINE_SELECTION",
            choice_name="MARKET_LINE",
            bookie_id=bookie_id,
            bookie_name=bookie_name,
            source=source,
            exchange_side=exchange_side,
            exchange_level=exchange_level,
            quote_id=None,
        )
        result.append(
            P4SeriesInput(
                series_id=_series_id(base, "CHECKPOINT_VIEW", "LINE"),
                base_series_id=base,
                domain=domain,
                view="CHECKPOINT_VIEW",
                value_type="LINE",
                market_id=market_id,
                market_group=market_group,
                market_period=market_period,
                market_name=market_name,
                line_value=None,
                line_value_key="LINE_SELECTION",
                choice_name="MARKET_LINE",
                choice_id=None,
                main_line=True,
                bookie_id=bookie_id,
                bookie_name=bookie_name,
                source=source,
                exchange_side=exchange_side,
                exchange_level=exchange_level,
                quote_id=None,
                points=tuple(sorted(points, key=lambda point: _datetime_value(point.effective_at))),
                expected_target_minutes=expected_targets,
                missing_target_minutes=tuple(missing),
                diagnostics=("LINE_SELECTION_FROM_UNIQUE_CHECKPOINT_CONTRACT",),
                operative_endpoint_present=True,
            )
        )
    return result


def _period_diagnostics(
    adaptive: Iterable[P4SeriesInput],
    checkpoint: Iterable[P4SeriesInput],
    *,
    missing_required_sources: set[int],
    required_issue_sources: set[int],
) -> dict[str, Any]:
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for series in (*tuple(adaptive), *tuple(checkpoint)):
        key = (series.domain, period_key(series.market_period))
        bucket = buckets.setdefault(
            key,
            {
                "DOMAIN": series.domain,
                "MARKET_PERIOD": period_key(series.market_period),
                "ADAPTIVE_SERIES_COUNT": 0,
                "CHECKPOINT_SERIES_COUNT": 0,
                "REQUIRED_SERIES_COUNT": 0,
                "OPTIONAL_SERIES_COUNT": 0,
                "PARTIAL_SERIES_COUNT": 0,
                "PARTIAL_REQUIRED_SERIES_COUNT": 0,
                "PARTIAL_OPTIONAL_SERIES_COUNT": 0,
            },
        )
        bucket[f"{series.view.replace('_VIEW', '')}_SERIES_COUNT"] += 1
        role = bookmaker_role(series.bookie_id)
        if role == "REQUIRED":
            bucket["REQUIRED_SERIES_COUNT"] += 1
        elif role == "OPTIONAL":
            bucket["OPTIONAL_SERIES_COUNT"] += 1
        is_partial = (
            len(series.points) < 2
            or bool(series.missing_target_minutes)
            or not series.operative_endpoint_present
        )
        if is_partial:
            bucket["PARTIAL_SERIES_COUNT"] += 1
            if role == "REQUIRED":
                bucket["PARTIAL_REQUIRED_SERIES_COUNT"] += 1
            elif role == "OPTIONAL":
                bucket["PARTIAL_OPTIONAL_SERIES_COUNT"] += 1
    result: dict[str, Any] = {}
    for (domain, period), details in sorted(buckets.items()):
        count = details["ADAPTIVE_SERIES_COUNT"] + details["CHECKPOINT_SERIES_COUNT"]
        required_count = details["REQUIRED_SERIES_COUNT"]
        details["status"] = (
            "INCOMPLETE"
            if required_count == 0
            else "PARTIAL"
            if missing_required_sources
            or required_issue_sources
            or details["PARTIAL_REQUIRED_SERIES_COUNT"]
            else "COMPLETE"
        )
        details["all_sources_status"] = (
            "INCOMPLETE"
            if count == 0
            else "PARTIAL"
            if details["PARTIAL_SERIES_COUNT"]
            else "COMPLETE"
        )
        result[f"{domain}_{period}"] = details
    return result


def extract_p4_trajectory_inputs(
    event_context: EventIdentity | EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None,
    target_selection: TargetMinuteSelection,
) -> P4ExtractionResult:
    """Extract trajectories for the event-wide selected checkpoint."""
    context = odds_trajectory_context
    observed_as_of = (
        context.evaluation_as_of
        if context is not None
        else None
    )
    target = target_selection.target_minute
    if target is None:
        return P4ExtractionResult(
            event_id=int(event_context.event_id),
            target_minute=None,
            operative_as_of=None,
            evaluation_as_of=observed_as_of,
            missing_inputs=("OPERATIVE_TARGET:UNSELECTED",),
            reason=target_selection.reason or "target_minute_not_selected",
        )
    if isinstance(target, bool) or not isinstance(target, int):
        raise TypeError("selected target_minute must be an integer")
    start = as_utc(event_context.starts_at, field_name="event start")
    if observed_as_of is not None:
        observed_as_of = as_utc(observed_as_of, field_name="evaluation time")
    tolerance = max(0, int(Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES))
    target_window = build_snapshot_target_window(
        event_starts_at=start,
        target_minute=target,
        evaluation_as_of=observed_as_of,
        tolerance_minutes=tolerance,
    )
    nominal_target_as_of = target_window.nominal_at
    operative_as_of = target_window.latest_at
    if context is None or not context.available or not context.markets:
        return P4ExtractionResult(
            event_id=int(event_context.event_id),
            target_minute=target,
            operative_as_of=operative_as_of,
            nominal_target_as_of=nominal_target_as_of,
            evaluation_as_of=observed_as_of,
            missing_inputs=(f"OPERATIVE_TARGET:{target}",),
            reason="odds_trajectory_unavailable",
        )

    expected_targets = tuple(
        sorted(
            {int(value) for value in context.target_minutes_expected if int(value) >= target}
            | {target},
            reverse=True,
        )
    )
    checkpoint_windows = build_snapshot_target_windows(
        event_starts_at=start,
        target_minutes=expected_targets,
        evaluation_as_of=observed_as_of,
        tolerance_minutes=tolerance,
    )
    adaptive: list[P4SeriesInput] = []
    checkpoints: list[P4SeriesInput] = []
    missing: list[str] = []
    missing_endpoint_details: list[dict[str, Any]] = []
    invalid: list[str] = []
    ambiguous: list[str] = []
    excluded_future = 0
    source_series_seen = 0
    endpoint_series_present = 0
    observed_bookie_ids: set[int] = set()
    issue_bookie_ids: set[int] = set()
    line_observations: list[dict[str, Any]] = []

    for market_group, periods in sorted(context.markets.items()):
        for market_period, names in sorted(periods.items()):
            for market_name, line_values in sorted(names.items()):
                domain = resolve_domain(market_group, market_name)
                if domain is None:
                    continue
                bookie_choices_with_endpoint: set[tuple[int, str, str | None, int, str]] = set()
                for _, m_line in line_values.items():
                    for _, b_obj in m_line.bookies.items():
                        if b_obj.bookie_id not in SUPPORTED_BOOKIE_IDS:
                            continue
                        for _, c_obj in b_obj.choices.items():
                            if c_obj.main_line is False:
                                continue
                            norm_pts: list[P4Point] = []
                            for sn in c_obj.snapshots:
                                pt, _ = _normalize_snapshot(
                                    sn,
                                    event_start=start,
                                    target_window=target_window,
                                    invalid=[],
                                    series_label="",
                                    evaluation_as_of=observed_as_of,
                                )
                                if pt is not None:
                                    norm_pts.append(pt)
                            p_pts = _project_checkpoints(
                                _deduplicate_points(norm_pts),
                                event_start=start,
                                target_minutes=expected_targets,
                                target_windows=checkpoint_windows,
                            )
                            if target in p_pts:
                                bookie_choices_with_endpoint.add(
                                    (
                                        int(b_obj.bookie_id),
                                        str(b_obj.source or ""),
                                        b_obj.exchange_side,
                                        int(b_obj.exchange_level or 0),
                                        str(c_obj.choice_name),
                                    )
                                )
                for line_value_key, market_line in sorted(line_values.items()):
                    line_value = market_line.line_value
                    if line_value_key == "__default__":
                        line_value = None
                    for _, bookie in sorted(market_line.bookies.items()):
                        if bookie.bookie_id not in SUPPORTED_BOOKIE_IDS:
                            continue
                        for _, choice in sorted(bookie.choices.items()):
                            if choice.main_line is False:
                                continue
                            source_series_seen += 1
                            observed_bookie_ids.add(int(bookie.bookie_id))
                            base = _base_series_id(
                                domain=domain,
                                market_group=market_group,
                                market_period=market_period,
                                market_name=market_name,
                                line_value=line_value,
                                choice_name=choice.choice_name,
                                bookie_id=bookie.bookie_id,
                                bookie_name=bookie.bookie_name,
                                source=bookie.source,
                                exchange_side=bookie.exchange_side,
                                exchange_level=bookie.exchange_level,
                                quote_id=choice.quote_id,
                            )
                            normalized: list[P4Point] = []
                            future_availability: list[datetime] = []
                            for snapshot in choice.snapshots:
                                invalid_count = len(invalid)
                                point, future = _normalize_snapshot(
                                    snapshot,
                                    event_start=start,
                                    target_window=target_window,
                                    invalid=invalid,
                                    series_label=base,
                                    evaluation_as_of=observed_as_of,
                                )
                                if len(invalid) > invalid_count:
                                    issue_bookie_ids.add(int(bookie.bookie_id))
                                excluded_future += int(future)
                                if future:
                                    available_at = snapshot.collected_at
                                    if available_at is not None:
                                        future_availability.append(available_at)
                                if point is not None:
                                    normalized.append(point)
                            safe_points = _deduplicate_points(normalized)
                            projected = _project_checkpoints(
                                safe_points,
                                event_start=start,
                                target_minutes=expected_targets,
                                target_windows=checkpoint_windows,
                            )
                            if line_value is not None and projected:
                                line_observations.append(
                                    {
                                        "domain": domain,
                                        "market_id": market_line.market_id,
                                        "market_group": market_group,
                                        "market_period": market_period,
                                        "market_name": market_name,
                                        "bookie_id": bookie.bookie_id,
                                        "bookie_name": bookie.bookie_name,
                                        "source": bookie.source,
                                        "exchange_side": bookie.exchange_side,
                                        "exchange_level": bookie.exchange_level,
                                        "line_value": line_value,
                                        "projected": projected,
                                    }
                                )
                            endpoint = projected.get(target)
                            identity = {
                                "domain": domain,
                                "market_id": market_line.market_id,
                                "market_group": market_group,
                                "market_period": market_period,
                                "market_name": market_name,
                                "line_value": line_value,
                                "line_value_key": line_value_key,
                                "choice_name": choice.choice_name,
                                "choice_id": choice.choice_id,
                                "main_line": choice.main_line,
                                "bookie_id": bookie.bookie_id,
                                "bookie_name": bookie.bookie_name,
                                "source": bookie.source,
                                "exchange_side": bookie.exchange_side,
                                "exchange_level": bookie.exchange_level,
                                "quote_id": choice.quote_id,
                            }
                            if endpoint is None:
                                bookie_choice_key = (
                                    int(bookie.bookie_id),
                                    str(bookie.source or ""),
                                    bookie.exchange_side,
                                    int(bookie.exchange_level or 0),
                                    str(choice.choice_name),
                                )
                                has_transitioned_line = (
                                    bookie_choice_key in bookie_choices_with_endpoint
                                )
                                if has_transitioned_line:
                                    adaptive_points = tuple(safe_points)
                                    adaptive_diagnostics = (
                                        "LEGACY_MIXED_TIMESTAMP_PROVENANCE",
                                        "LINE_CONTRACT_ENDED",
                                    )
                                else:
                                    missing.append(f"{base}:OPERATIVE_TARGET:{target}")
                                    issue_bookie_ids.add(int(bookie.bookie_id))
                                    missing_endpoint_details.append(
                                        {
                                            "market_group": market_group,
                                            "market_period": market_period,
                                            "market_name": market_name,
                                            "line_value": line_value,
                                            "choice_name": choice.choice_name,
                                            "bookie_name": bookie.bookie_name,
                                            "exchange_side": bookie.exchange_side,
                                            "last_available_at": (
                                                max(
                                                    (point.availability_at for point in safe_points),
                                                    key=_datetime_value,
                                                ).isoformat()
                                                if safe_points
                                                else None
                                            ),
                                            "first_after_cutoff_at": (
                                                min(
                                                    future_availability,
                                                    key=_datetime_value,
                                                ).isoformat()
                                                if future_availability
                                                else None
                                            ),
                                        }
                                    )
                                    adaptive_points = tuple(safe_points)
                                    adaptive_diagnostics = (
                                        "LEGACY_MIXED_TIMESTAMP_PROVENANCE",
                                        "OPERATIVE_ENDPOINT_MISSING",
                                    )
                            else:
                                endpoint_series_present += 1
                                adaptive_points = _adaptive_with_endpoint(
                                    safe_points,
                                    endpoint,
                                    target_minute=target,
                                    event_start=start,
                                    evaluation_as_of=observed_as_of,
                                )
                                adaptive_diagnostics = (
                                    "LEGACY_MIXED_TIMESTAMP_PROVENANCE",
                                )
                            checkpoint_points = tuple(
                                projected[minute]
                                for minute in expected_targets
                                if minute in projected
                            )
                            missing_targets = tuple(
                                minute for minute in expected_targets if minute not in projected
                            )
                            _append_value_series(
                                adaptive,
                                base=base,
                                view="ADAPTIVE_VIEW",
                                values=adaptive_points,
                                identity=identity,
                                diagnostics=adaptive_diagnostics,
                            )
                            _append_value_series(
                                checkpoints,
                                base=base,
                                view="CHECKPOINT_VIEW",
                                values=checkpoint_points,
                                identity=identity,
                                expected=expected_targets,
                                missing=missing_targets,
                            )

    checkpoints.extend(
        _line_series(
            line_observations,
            event_start=start,
            target_minute=target,
            expected_targets=expected_targets,
            ambiguous=ambiguous,
            issue_bookie_ids=issue_bookie_ids,
        )
    )
    missing_required_sources = set(REQUIRED_BOOKIE_IDS) - observed_bookie_ids
    required_issue_sources = set(REQUIRED_BOOKIE_IDS) & issue_bookie_ids
    periods = _period_diagnostics(
        adaptive,
        checkpoints,
        missing_required_sources=missing_required_sources,
        required_issue_sources=required_issue_sources,
    )
    reason = None if endpoint_series_present else "operative_target_unavailable"
    return P4ExtractionResult(
        event_id=int(event_context.event_id),
        target_minute=target,
        operative_as_of=operative_as_of,
        nominal_target_as_of=nominal_target_as_of,
        evaluation_as_of=observed_as_of,
        adaptive_series=tuple(adaptive),
        checkpoint_series=tuple(checkpoints),
        periods=periods,
        missing_inputs=tuple(sorted(set(missing))),
        missing_endpoint_details=tuple(missing_endpoint_details),
        invalid_inputs=tuple(sorted(set(invalid))),
        ambiguous_inputs=tuple(sorted(set(ambiguous))),
        excluded_future_points=excluded_future,
        source_series_seen=source_series_seen,
        endpoint_series_present=endpoint_series_present,
        observed_bookie_ids=tuple(sorted(observed_bookie_ids)),
        issue_bookie_ids=tuple(sorted(issue_bookie_ids)),
        reason=reason,
    )


__all__ = ["extract_p4_trajectory_inputs"]
