"""Causal extraction policy for P4 price and line trajectories."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from infrastructure.settings import Config
from modules.pillars.context import EventContext
from modules.pillars.odds_trajectory_context import (
    OddsSnapshotPoint,
    OddsTrajectoryContext,
)
from shared.temporal import as_utc

from .models import P4ExtractionResult, P4Point, P4SeriesInput
from .periods import SUPPORTED_BOOKIE_IDS, period_key, resolve_domain


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
    choice_group: str | None,
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
        choice_group or "NO_LINE",
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
    operative_as_of: datetime,
    invalid: list[str],
    series_label: str,
) -> tuple[P4Point | None, bool]:
    collected_at = snapshot.collected_at
    source_at = snapshot.source_collected_at
    availability_at = collected_at or source_at
    if availability_at is None:
        invalid.append(f"{series_label}:snapshot:{snapshot.snapshot_id}:missing_timestamp")
        return None, False
    if (
        collected_at is not None
        and source_at is not None
        and _datetime_value(source_at) > _datetime_value(collected_at)
    ):
        invalid.append(
            f"{series_label}:snapshot:{snapshot.snapshot_id}:source_after_collection"
        )
        return None, False
    if _datetime_value(availability_at) > _datetime_value(operative_as_of):
        return None, True
    try:
        odds = Decimal(snapshot.odds_value)
    except (InvalidOperation, TypeError, ValueError):
        invalid.append(f"{series_label}:snapshot:{snapshot.snapshot_id}:invalid_odds")
        return None, False
    if odds <= 0:
        invalid.append(f"{series_label}:snapshot:{snapshot.snapshot_id}:non_positive_odds")
        return None, False
    effective_at = source_at or availability_at
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
    tolerance_minutes: int,
) -> dict[int, P4Point]:
    projected: dict[int, P4Point] = {}
    tolerance_seconds = max(0, int(tolerance_minutes)) * 60
    candidates = list(points)
    for target in target_minutes:
        nominal = event_start - timedelta(minutes=target)
        eligible: list[tuple[float, float, int, P4Point]] = []
        for point in candidates:
            distance = abs(_datetime_value(point.availability_at) - _datetime_value(nominal))
            if distance > tolerance_seconds:
                continue
            eligible.append(
                (
                    distance,
                    -_datetime_value(point.source_collected_at or point.availability_at),
                    -(point.snapshot_id if point.snapshot_id is not None else -1),
                    point,
                )
            )
        if not eligible:
            continue
        selected = min(eligible, key=lambda item: item[:3])[3]
        projected[target] = replace(
            selected,
            point_id=f"{selected.point_id}_TARGET_{target}",
            effective_at=selected.availability_at,
            minutes_before_start=_minutes_before_start(
                event_start,
                selected.availability_at,
            ),
            observation_kind=(
                "OPERATIVE_ENDPOINT" if target == min(target_minutes) else "CHECKPOINT"
            ),
            target_minute=target,
            distance_from_target_minutes=Decimal(str(distance / 60)),
        )
    return projected


def _adaptive_with_endpoint(
    points: list[P4Point],
    endpoint: P4Point,
    *,
    target_minute: int,
    event_start: datetime,
) -> tuple[P4Point, ...]:
    result = list(points)
    exact_index = next(
        (
            index
            for index, point in enumerate(result)
            if point.snapshot_id is not None
            and point.snapshot_id == endpoint.snapshot_id
            and _datetime_value(point.effective_at)
            == _datetime_value(endpoint.availability_at)
        ),
        None,
    )
    operative = replace(
        endpoint,
        point_id=f"{endpoint.point_id}_OPERATIVE_{target_minute}",
        effective_at=endpoint.availability_at,
        minutes_before_start=_minutes_before_start(
            event_start,
            endpoint.availability_at,
        ),
        observation_kind="OPERATIVE_ENDPOINT",
        target_minute=target_minute,
        distance_from_target_minutes=endpoint.distance_from_target_minutes,
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
        points: list[P4Point] = []
        missing: list[int] = []
        for target in expected_targets:
            line_candidates: dict[Decimal, list[P4Point]] = defaultdict(list)
            for observation in related:
                line = _decimal_line(observation["choice_group"])
                if line is None:
                    continue
                point = observation["projected"].get(target)
                if point is not None:
                    line_candidates[line].append(point)
            if len(line_candidates) != 1:
                missing.append(target)
                if len(line_candidates) > 1:
                    label = "|".join(str(part) for part in key)
                    ambiguous.append(
                        f"LINE_SELECTION:{label}:TARGET:{target}:"
                        f"{','.join(str(line) for line in sorted(line_candidates))}"
                    )
                continue
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
            choice_group="LINE_SELECTION",
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
                choice_group=None,
                choice_group_key="LINE_SELECTION",
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
                "PARTIAL_SERIES_COUNT": 0,
            },
        )
        bucket[f"{series.view.replace('_VIEW', '')}_SERIES_COUNT"] += 1
        if len(series.points) < 2 or series.missing_target_minutes:
            bucket["PARTIAL_SERIES_COUNT"] += 1
    result: dict[str, Any] = {}
    for (domain, period), details in sorted(buckets.items()):
        count = details["ADAPTIVE_SERIES_COUNT"] + details["CHECKPOINT_SERIES_COUNT"]
        details["status"] = (
            "INCOMPLETE"
            if count == 0
            else "PARTIAL"
            if details["PARTIAL_SERIES_COUNT"]
            else "COMPLETE"
        )
        result[f"{domain}_{period}"] = details
    return result


def extract_p4_trajectory_inputs(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None,
    *,
    target_minute: int,
) -> P4ExtractionResult:
    """Extract causal, endpoint-complete trajectories from the shared context."""
    if isinstance(target_minute, bool) or not isinstance(target_minute, int):
        raise TypeError("target_minute must be an integer")
    target = target_minute
    start = event_context.starts_at
    operative_as_of = start - timedelta(minutes=target)
    context = odds_trajectory_context
    if context is None or not context.available or not context.markets:
        return P4ExtractionResult(
            event_id=int(event_context.event_id),
            target_minute=target,
            operative_as_of=operative_as_of,
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
    tolerance = int(Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES)
    adaptive: list[P4SeriesInput] = []
    checkpoints: list[P4SeriesInput] = []
    missing: list[str] = []
    invalid: list[str] = []
    ambiguous: list[str] = []
    excluded_future = 0
    source_series_seen = 0
    endpoint_series_present = 0
    line_observations: list[dict[str, Any]] = []

    for market_group, periods in sorted(context.markets.items()):
        for market_period, names in sorted(periods.items()):
            for market_name, choice_groups in sorted(names.items()):
                domain = resolve_domain(market_group, market_name)
                if domain is None:
                    continue
                for choice_group_key, market_line in sorted(choice_groups.items()):
                    choice_group = market_line.choice_group
                    if choice_group_key == "__default__":
                        choice_group = None
                    for _, bookie in sorted(market_line.bookies.items()):
                        if bookie.bookie_id not in SUPPORTED_BOOKIE_IDS:
                            continue
                        for _, choice in sorted(bookie.choices.items()):
                            if choice.main_line is False:
                                continue
                            source_series_seen += 1
                            base = _base_series_id(
                                domain=domain,
                                market_group=market_group,
                                market_period=market_period,
                                market_name=market_name,
                                choice_group=choice_group,
                                choice_name=choice.choice_name,
                                bookie_id=bookie.bookie_id,
                                bookie_name=bookie.bookie_name,
                                source=bookie.source,
                                exchange_side=bookie.exchange_side,
                                exchange_level=bookie.exchange_level,
                                quote_id=choice.quote_id,
                            )
                            normalized: list[P4Point] = []
                            for snapshot in choice.snapshots:
                                point, future = _normalize_snapshot(
                                    snapshot,
                                    event_start=start,
                                    operative_as_of=operative_as_of,
                                    invalid=invalid,
                                    series_label=base,
                                )
                                excluded_future += int(future)
                                if point is not None:
                                    normalized.append(point)
                            safe_points = _deduplicate_points(normalized)
                            projected = _project_checkpoints(
                                safe_points,
                                event_start=start,
                                target_minutes=expected_targets,
                                tolerance_minutes=tolerance,
                            )
                            if choice_group is not None and projected:
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
                                        "choice_group": choice_group,
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
                                "choice_group": choice_group,
                                "choice_group_key": choice_group_key,
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
                                missing.append(f"{base}:OPERATIVE_TARGET:{target}")
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
        )
    )
    periods = _period_diagnostics(adaptive, checkpoints)
    reason = None if endpoint_series_present else "operative_target_unavailable"
    return P4ExtractionResult(
        event_id=int(event_context.event_id),
        target_minute=target,
        operative_as_of=operative_as_of,
        adaptive_series=tuple(adaptive),
        checkpoint_series=tuple(checkpoints),
        periods=periods,
        missing_inputs=tuple(sorted(set(missing))),
        invalid_inputs=tuple(sorted(set(invalid))),
        ambiguous_inputs=tuple(sorted(set(ambiguous))),
        excluded_future_points=excluded_future,
        source_series_seen=source_series_seen,
        endpoint_series_present=endpoint_series_present,
        reason=reason,
    )


__all__ = ["extract_p4_trajectory_inputs"]
