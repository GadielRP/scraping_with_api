"""Causal extraction policy for P4 price and line trajectories."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable

from modules.pillars.context import EventContext, EventIdentity
from modules.pillars.odds_trajectory_context import (
    OddsTrajectoryContext,
)
from modules.pillars.trajectory_selection import TargetMinuteSelection
from shared.temporal import as_utc

from .models import P4ExtractionResult, P4SeriesInput
from modules.pillars.trajectory_sampling import (
    TrajectoryPoint, TrajectoryPointValue, TrajectorySamplingPolicy,
    select_line_checkpoints, timestamp_value, minutes_before_start,
)
from .periods import (
    SUPPORTED_BOOKIE_IDS,
    resolve_domain,
)


def _slug(value: Any) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "NA").strip().upper())
    return token.strip("_") or "NA"


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


def _derived_points(points: Iterable[TrajectoryPoint], value_type: str) -> tuple[TrajectoryPoint, ...]:
    if value_type == "ODDS_PRICE":
        return tuple(points)
    derived: list[TrajectoryPoint] = []
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
        if not Decimal(value).is_finite() or (
            value_type in ("SOURCE_LIMIT", "EXCHANGE_SIZE") and value < 0
        ):
            continue
        derived.append(TrajectoryPointValue(point, Decimal(value), value_type))
    return tuple(derived)


def _append_value_series(
    destination: list[P4SeriesInput],
    *,
    base: str,
    view: str,
    values: tuple[TrajectoryPoint, ...],
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
            observation["market_group"],
            observation["market_period"],
            observation["bookie_id"],
            observation["source"],
            observation["exchange_side"],
            observation["exchange_level"],
        )
        groups[key].append(observation)

    result: list[P4SeriesInput] = []
    for key, related in sorted(groups.items(), key=lambda item: str(item[0])):
        selected, conflicts = select_line_checkpoints(
            ((item["line_value"], item["projected"]) for item in related),
            target_minute, expected_targets,
        )
        label = "|".join(str(part) for part in key)
        ambiguous.extend(
            f"LINE_SELECTION:{label}:TARGET:{target}:{','.join(map(str, lines))}"
            for target, lines in conflicts.items()
        )
        points: list[TrajectoryPoint] = []
        missing = [target for target in expected_targets if target not in selected]
        for target, (line, source_points) in selected.items():
            anchor = max(
                source_points,
                key=lambda point: timestamp_value(point.availability_at),
            )
            availability = anchor.availability_at
            effective = availability
            points.append(
                TrajectoryPoint(
                    point_id=f"LINE_TARGET_{target}",
                    value=line,
                    effective_at=effective,
                    availability_at=availability,
                    minutes_before_start=minutes_before_start(event_start, effective),
                    snapshot_id=anchor.snapshot_id,
                    quote_id=anchor.quote_id,
                    collected_at=anchor.collected_at,
                    source_collected_at=anchor.source_collected_at,
                    observation_kind=(
                        "OPERATIVE_ENDPOINT"
                        if target == target_minute
                        else "CHECKPOINT"
                    ),
                    target_minute=target,
                    distance_from_target_minutes=abs(
                        minutes_before_start(event_start, availability)
                        - Decimal(target)
                    ),
                )
            )
        if not any(point.target_minute == target_minute for point in points):
            continue
        (
            domain,
            market_group,
            market_period,
            bookie_id,
            source,
            exchange_side,
            exchange_level,
        ) = key
        market_ids = {item["market_id"] for item in related}
        market_id = next(iter(market_ids)) if len(market_ids) == 1 else None
        market_name = f"{market_group} {market_period}"
        bookie_name = related[0]["bookie_name"]
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
                points=tuple(
                    sorted(
                        points, key=lambda point: timestamp_value(point.effective_at)
                    )
                ),
                expected_target_minutes=expected_targets,
                missing_target_minutes=tuple(missing),
                diagnostics=("LINE_SELECTION_FROM_UNIQUE_CHECKPOINT_CONTRACT",),
                operative_endpoint_present=True,
            )
        )
    return result


def extract_p4_trajectory_inputs(
    event_context: EventIdentity | EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None,
    target_selection: TargetMinuteSelection,
) -> P4ExtractionResult:
    """Extract trajectories for the event-wide selected checkpoint."""
    context = odds_trajectory_context
    observed_as_of = context.evaluation_as_of if context is not None else None
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
    sampling = TrajectorySamplingPolicy.for_context(
        context or OddsTrajectoryContext(False, event_context.event_id, [], [], []),
        target, start,
    )
    target_window = sampling.target_window
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

    expected_targets = tuple(sampling.windows)
    adaptive: list[P4SeriesInput] = []
    checkpoints: list[P4SeriesInput] = []
    missing: list[str] = []
    missing_endpoint_details: list[dict[str, Any]] = []
    invalid: list[str] = []
    ambiguous: list[str] = []
    excluded_future = 0
    source_series_seen = 0
    endpoint_series_present = 0
    line_observations: list[dict[str, Any]] = []

    for market_group, periods in sorted(context.markets.items()):
        for market_period, names in sorted(periods.items()):
            for market_name, line_values in sorted(names.items()):
                domain = resolve_domain(market_group, market_name)
                if domain is None:
                    continue
                bookie_choices_with_endpoint: set[
                    tuple[int, str, str | None, int, str]
                ] = set()
                prepared = {}
                for _, m_line in line_values.items():
                    for _, b_obj in m_line.bookies.items():
                        if b_obj.bookie_id not in SUPPORTED_BOOKIE_IDS:
                            continue
                        for _, c_obj in b_obj.choices.items():
                            if c_obj.main_line is False:
                                continue
                            sample = sampling.select(c_obj.snapshots)
                            prepared[(id(m_line), id(b_obj), id(c_obj))] = sample
                            if sample.endpoint is not None:
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
                            sample = prepared.pop((id(market_line), id(bookie), id(choice)))
                            safe_points, projected = sample.points, sample.checkpoints
                            invalid.extend(f"{base}:{issue}" for issue in sample.invalid)
                            excluded_future += sample.excluded_future_count
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
                                    series_diagnostics = (
                                        "RECORDED_COLLECTION_AND_SOURCE_TIME",
                                        "LINE_CONTRACT_ENDED",
                                    )
                                else:
                                    missing.append(f"{base}:OPERATIVE_TARGET:{target}")
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
                                                    (
                                                        point.availability_at
                                                        for point in safe_points
                                                    ),
                                                    key=timestamp_value,
                                                ).isoformat()
                                                if safe_points
                                                else None
                                            ),
                                            "first_after_cutoff_at": (
                                                sample.first_after_cutoff_at.isoformat()
                                                if sample.first_after_cutoff_at else None
                                            ),
                                        }
                                    )
                                    series_diagnostics = (
                                        "RECORDED_COLLECTION_AND_SOURCE_TIME",
                                        "OPERATIVE_ENDPOINT_MISSING",
                                    )
                            else:
                                endpoint_series_present += 1
                                series_diagnostics = (
                                    "RECORDED_COLLECTION_AND_SOURCE_TIME",
                                )
                            checkpoint_points = tuple(
                                projected[minute]
                                for minute in expected_targets
                                if minute in projected
                            )
                            missing_targets = tuple(
                                minute
                                for minute in expected_targets
                                if minute not in projected
                            )
                            _append_value_series(
                                adaptive,
                                base=base,
                                view="ADAPTIVE_VIEW",
                                values=sample.adaptive,
                                identity=identity,
                                diagnostics=series_diagnostics,
                            )
                            _append_value_series(
                                checkpoints,
                                base=base,
                                view="CHECKPOINT_VIEW",
                                values=checkpoint_points,
                                identity=identity,
                                expected=expected_targets,
                                missing=missing_targets,
                                diagnostics=series_diagnostics,
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
    reason = None if endpoint_series_present else "operative_target_unavailable"
    return P4ExtractionResult(
        event_id=int(event_context.event_id),
        target_minute=target,
        operative_as_of=operative_as_of,
        nominal_target_as_of=nominal_target_as_of,
        evaluation_as_of=observed_as_of,
        adaptive_series=tuple(adaptive),
        checkpoint_series=tuple(checkpoints),
        missing_inputs=tuple(sorted(set(missing))),
        missing_endpoint_details=tuple(missing_endpoint_details),
        invalid_inputs=tuple(sorted(set(invalid))),
        ambiguous_inputs=tuple(sorted(set(ambiguous))),
        excluded_future_points=excluded_future,
        source_series_seen=source_series_seen,
        endpoint_series_present=endpoint_series_present,
        reason=reason,
    )


__all__ = ["extract_p4_trajectory_inputs"]
