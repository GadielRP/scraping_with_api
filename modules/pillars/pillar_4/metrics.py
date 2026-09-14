"""Pure, threshold-free temporal calculations used by every P4 series."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Sequence

from .models import P4Point


ZERO = Decimal("0")
ONE = Decimal("1")
SIXTY = Decimal("60")


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _sign(value: Decimal | None) -> int | None:
    if value is None:
        return None
    return 1 if value > ZERO else -1 if value < ZERO else 0


def _direction(value: Decimal | None) -> str | None:
    sign = _sign(value)
    if sign is None:
        return None
    return "POSITIVE" if sign > 0 else "NEGATIVE" if sign < 0 else "ZERO"


def _path_pattern(deltas: Sequence[Decimal]) -> str:
    non_zero = [sign for value in deltas if (sign := _sign(value))]
    if not non_zero:
        return "NO_MOVEMENT"
    changes = sum(left != right for left, right in zip(non_zero, non_zero[1:]))
    if changes == 0:
        return "UNIDIRECTIONAL"
    if changes == 1:
        return "REVERSAL"
    return "MULTI_REVERSAL"


def build_legs(
    points: Sequence[P4Point],
    *,
    expected_target_minutes: Sequence[int] = (),
) -> list[dict[str, Any]]:
    target_positions = {
        minute: position for position, minute in enumerate(expected_target_minutes)
    }
    legs: list[dict[str, Any]] = []
    for ordinal, (start, end) in enumerate(zip(points, points[1:]), start=1):
        delta = end.value - start.value
        elapsed = Decimal(str((end.effective_at - start.effective_at).total_seconds())) / SIXTY
        contiguous = True
        if expected_target_minutes:
            start_position = target_positions.get(start.target_minute)
            end_position = target_positions.get(end.target_minute)
            contiguous = (
                start_position is not None
                and end_position is not None
                and end_position == start_position + 1
            )
        velocity = None if elapsed <= ZERO or not contiguous else delta / elapsed
        legs.append(
            {
                "LEG_ID": f"{start.point_id}__{end.point_id}",
                "ORDINAL": ordinal,
                "FROM_POINT_ID": start.point_id,
                "TO_POINT_ID": end.point_id,
                "FROM_TARGET_MINUTE": start.target_minute,
                "TO_TARGET_MINUTE": end.target_minute,
                "START_AT": start.effective_at.isoformat(),
                "END_AT": end.effective_at.isoformat(),
                "ELAPSED_MINUTES_ACTUAL": _number(elapsed),
                "DELTA_RAW": _number(delta),
                "ABS_DELTA_RAW": _number(abs(delta)),
                "DIRECTION_BY_SIGN": _sign(delta),
                "DIRECTION_RAW": _direction(delta),
                "VELOCITY_RAW": _number(velocity),
                "CONTIGUOUS_RAW": contiguous,
                "MOVE_SHARE_RAW": None,
                "_DELTA": delta,
                "_ABS_DELTA": abs(delta),
                "_VELOCITY": velocity,
                "_START_POINT": start,
                "_END_POINT": end,
            }
        )
    return legs


def _continuous_segments(legs: Sequence[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    segments: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for leg in legs:
        if not leg["CONTIGUOUS_RAW"]:
            if current:
                segments.append(current)
                current = []
            continue
        current.append(leg)
    if current:
        segments.append(current)
    return segments


def _directional_runs(
    segments: Sequence[Sequence[dict[str, Any]]],
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    run_number = 0
    for segment_index, segment in enumerate(segments, start=1):
        current_sign: int | None = None
        current_legs: list[dict[str, Any]] = []

        def finish() -> None:
            nonlocal current_sign, current_legs, run_number
            if current_sign is None or not current_legs:
                return
            run_number += 1
            delta = sum((leg["_DELTA"] for leg in current_legs), ZERO)
            result.append(
                {
                    "RUN_ID": f"SEGMENT_{segment_index}_RUN_{run_number}",
                    "SEGMENT_ID": segment_index,
                    "DIRECTION_BY_SIGN": current_sign,
                    "DIRECTION": "POSITIVE" if current_sign > 0 else "NEGATIVE",
                    "DELTA_RAW": _number(delta),
                    "ABS_DELTA_RAW": _number(abs(delta)),
                    "LEG_IDS": [leg["LEG_ID"] for leg in current_legs],
                    "START_POINT_ID": current_legs[0]["FROM_POINT_ID"],
                    "END_POINT_ID": current_legs[-1]["TO_POINT_ID"],
                    "START_AT": current_legs[0]["START_AT"],
                    "END_AT": current_legs[-1]["END_AT"],
                }
            )
            current_sign = None
            current_legs = []

        for leg in segment:
            sign = int(leg["DIRECTION_BY_SIGN"])
            if sign == 0:
                if current_sign is not None:
                    current_legs.append(leg)
                continue
            if current_sign is None or current_sign == sign:
                current_sign = sign
                current_legs.append(leg)
                continue
            finish()
            current_sign = sign
            current_legs = [leg]
        finish()
    return result


def _turning_structure(
    segments: Sequence[Sequence[dict[str, Any]]],
) -> dict[str, Any]:
    zones: list[dict[str, Any]] = []
    zero_bridged = False
    for segment_index, segment in enumerate(segments, start=1):
        previous_sign: int | None = None
        previous_leg: dict[str, Any] | None = None
        zero_legs: list[dict[str, Any]] = []
        for leg in segment:
            sign = int(leg["DIRECTION_BY_SIGN"])
            if sign == 0:
                if previous_sign is not None:
                    zero_legs.append(leg)
                continue
            if previous_sign is not None and sign != previous_sign and previous_leg:
                plateau = bool(zero_legs)
                zero_bridged = zero_bridged or plateau
                zones.append(
                    {
                        "TYPE": "PEAK" if previous_sign > 0 else "TROUGH",
                        "STRUCTURE": "PLATEAU" if plateau else "POINT",
                        "SEGMENT_ID": segment_index,
                        "FROM_LEG_ID": previous_leg["LEG_ID"],
                        "TO_LEG_ID": leg["LEG_ID"],
                        "OBSERVATION_ID": None if plateau else previous_leg["TO_POINT_ID"],
                        "START_OBSERVATION_ID": previous_leg["TO_POINT_ID"] if plateau else None,
                        "END_OBSERVATION_ID": leg["FROM_POINT_ID"] if plateau else None,
                        "PLATEAU_LEG_IDS": [item["LEG_ID"] for item in zero_legs],
                    }
                )
            previous_sign = sign
            previous_leg = leg
            zero_legs = []
    return {
        "SIGN_CHANGE_COUNT_RAW": len(zones),
        "ZERO_BRIDGED_SIGN_CHANGE": zero_bridged,
        "ZONES": zones,
    }


def _correction(
    runs: Sequence[dict[str, Any]],
    *,
    complete_path: bool,
) -> dict[str, Any]:
    base = {
        "DIRECTIONAL_RUNS": list(runs),
        "INITIAL_RUN": None,
        "CORRECTION_RUN": None,
        "LATER_PHASES": [],
        "CORRECTION_RATIO_RAW": None,
        "MOVE_RETENTION_RAW": None,
        "OVERSHOOT_RAW": None,
        "OVERSHOOT_MAGNITUDE_RAW": None,
    }
    if not complete_path:
        return {**base, "STATE": None, "UNAVAILABLE_REASON": "NON_CONTIGUOUS_GAP"}
    if not runs:
        return {**base, "STATE": "NO_DIRECTIONAL_RUN", "UNAVAILABLE_REASON": None}
    initial = runs[0]
    correction = next(
        (
            run
            for run in runs[1:]
            if run["DIRECTION_BY_SIGN"] != initial["DIRECTION_BY_SIGN"]
        ),
        None,
    )
    if correction is None:
        return {
            **base,
            "STATE": "NO_CORRECTION",
            "UNAVAILABLE_REASON": None,
            "INITIAL_RUN": initial,
            "LATER_PHASES": list(runs[1:]),
        }
    initial_abs = Decimal(str(initial["ABS_DELTA_RAW"]))
    correction_abs = Decimal(str(correction["ABS_DELTA_RAW"]))
    ratio = None if initial_abs == ZERO else correction_abs / initial_abs
    overshoot = None if ratio is None else ratio > ONE
    retention = None if ratio is None else max(ZERO, ONE - ratio)
    correction_index = list(runs).index(correction)
    return {
        **base,
        "STATE": "OVERSHOOT" if overshoot else "CORRECTION",
        "UNAVAILABLE_REASON": None,
        "INITIAL_RUN": initial,
        "CORRECTION_RUN": correction,
        "LATER_PHASES": list(runs[correction_index + 1 :]),
        "CORRECTION_RATIO_RAW": _number(ratio),
        "MOVE_RETENTION_RAW": _number(retention),
        "OVERSHOOT_RAW": overshoot,
        "OVERSHOOT_MAGNITUDE_RAW": (
            None
            if overshoot is None
            else _number(correction_abs - initial_abs) if overshoot else 0.0
        ),
    }


def _velocity_changes(
    segments: Sequence[Sequence[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], bool | None, bool | None]:
    changes: list[dict[str, Any]] = []
    for segment_index, segment in enumerate(segments, start=1):
        moving = [leg for leg in segment if leg["_VELOCITY"] is not None]
        for previous, current in zip(moving, moving[1:]):
            previous_velocity = previous["_VELOCITY"]
            current_velocity = current["_VELOCITY"]
            assert previous_velocity is not None and current_velocity is not None
            velocity_change = current_velocity - previous_velocity
            absolute_change = abs(current_velocity) - abs(previous_velocity)
            same_direction = (
                previous["DIRECTION_BY_SIGN"] == current["DIRECTION_BY_SIGN"]
                and previous["DIRECTION_BY_SIGN"] != 0
            )
            changes.append(
                {
                    "SEGMENT_ID": segment_index,
                    "FROM_LEG_ID": previous["LEG_ID"],
                    "TO_LEG_ID": current["LEG_ID"],
                    "VELOCITY_CHANGE_RAW": _number(velocity_change),
                    "ABS_VELOCITY_CHANGE_RAW": _number(absolute_change),
                    "ACCELERATION_RAW": same_direction and absolute_change > ZERO,
                    "DECELERATION_RAW": same_direction and absolute_change < ZERO,
                    "REVERSAL_RAW": (
                        previous["DIRECTION_BY_SIGN"]
                        * current["DIRECTION_BY_SIGN"]
                        < 0
                    ),
                }
            )
    if not changes:
        return [], None, None
    return (
        changes,
        any(item["ACCELERATION_RAW"] for item in changes),
        any(item["DECELERATION_RAW"] for item in changes),
    )


def _window_name(end_point: P4Point, operative_target: int) -> str | None:
    position = (
        Decimal(end_point.target_minute)
        if end_point.target_minute is not None
        else end_point.minutes_before_start
    )
    target = Decimal(operative_target)
    if Decimal("120") <= position <= Decimal("360"):
        return "EARLY_WINDOW"
    if Decimal("30") <= position < Decimal("120"):
        return "DEVELOPMENT_WINDOW"
    if target <= position < Decimal("30"):
        return "LATE_WINDOW"
    return None


def _window_features(
    legs: Sequence[dict[str, Any]],
    *,
    total_path: Decimal,
    operative_target: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    buckets: dict[str, list[dict[str, Any]]] = {
        "EARLY_WINDOW": [],
        "DEVELOPMENT_WINDOW": [],
        "LATE_WINDOW": [],
    }
    for leg in legs:
        if not leg["CONTIGUOUS_RAW"]:
            continue
        name = _window_name(leg["_END_POINT"], operative_target)
        if name:
            buckets[name].append(leg)
    windows: list[dict[str, Any]] = []
    for name, selected in buckets.items():
        if not selected:
            continue
        deltas = [leg["_DELTA"] for leg in selected]
        net = sum(deltas, ZERO)
        path = sum((leg["_ABS_DELTA"] for leg in selected), ZERO)
        point_ids = [selected[0]["FROM_POINT_ID"]] + [leg["TO_POINT_ID"] for leg in selected]
        elapsed = Decimal(
            str(
                (
                    selected[-1]["_END_POINT"].effective_at
                    - selected[0]["_START_POINT"].effective_at
                ).total_seconds()
            )
        ) / SIXTY
        windows.append(
            {
                "WINDOW": name,
                "WINDOW_START_VALUE": _number(selected[0]["_START_POINT"].value),
                "WINDOW_END_VALUE": _number(selected[-1]["_END_POINT"].value),
                "WINDOW_NET_MOVE_RAW": _number(net),
                "WINDOW_PATH_LENGTH_RAW": _number(path),
                "WINDOW_POINT_COUNT": len(set(point_ids)),
                "WINDOW_LEG_COUNT": len(selected),
                "WINDOW_SIGN_CHANGE_COUNT_RAW": _turning_structure([selected])[
                    "SIGN_CHANGE_COUNT_RAW"
                ],
                "WINDOW_START_TIMESTAMP": selected[0]["START_AT"],
                "WINDOW_END_TIMESTAMP": selected[-1]["END_AT"],
                "WINDOW_ELAPSED_MINUTES": _number(elapsed),
                "WINDOW_NET_DIRECTION_RAW": _direction(net),
                "WINDOW_PATTERN_RAW": _path_pattern(deltas),
                "MOVE_SHARE_BY_WINDOW": None if total_path == ZERO else _number(path / total_path),
                "LEG_IDS": [leg["LEG_ID"] for leg in selected],
            }
        )
    if not windows:
        return [], []
    maximum = max(Decimal(str(item["WINDOW_PATH_LENGTH_RAW"])) for item in windows)
    dominant = [
        item["WINDOW"]
        for item in windows
        if Decimal(str(item["WINDOW_PATH_LENGTH_RAW"])) == maximum
    ]
    return windows, dominant


def build_temporal_features(
    points: Sequence[P4Point],
    *,
    gap_present: bool = False,
    expected_target_minutes: Sequence[int] = (),
    operative_target_minute: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    legs = build_legs(points, expected_target_minutes=expected_target_minutes)
    public_legs = [
        {key: value for key, value in leg.items() if not key.startswith("_")}
        for leg in legs
    ]
    if not legs:
        features = {
            "OBSERVATION_COUNT": len(points),
            "LEG_COUNT": 0,
            "SIGN_SEQUENCE_RAW": [],
            "NET_START_OBSERVATION": None,
            "NET_END_OBSERVATION": None,
            "NET_MOVE_RAW": None,
            "PATH_LENGTH_RAW": None,
            "PATH_EFFICIENCY_RAW": None,
            "NO_MOVEMENT_RAW": None,
            "ELAPSED_MINUTES_ACTUAL": None,
            "VELOCITY_RAW": None,
            "DIRECTIONAL_RUNS": [],
            "VELOCITY_CHANGES_RAW": [],
            "GAP_PRESENT_RAW": gap_present,
            "WINDOWS": [],
            "DOMINANT_SEGMENTS_RAW": [],
            "DOMINANT_WINDOWS_RAW": [],
        }
        signals = {
            "PATH_PATTERN_RAW": None,
            "NET_DIRECTION_RAW": None,
            "FINAL_RUN_DIRECTION_RAW": None,
            "TURNING_STRUCTURE_RAW": {
                "SIGN_CHANGE_COUNT_RAW": None,
                "ZERO_BRIDGED_SIGN_CHANGE": None,
                "ZONES": [],
            },
            "CORRECTION_STATE_RAW": None,
            "CORRECTION_RAW": None,
            "OVERSHOOT_RAW": None,
            "ACCELERATION_RAW": None,
            "DECELERATION_RAW": None,
            "BOOK_EXCHANGE_RELATION_CHANGE_RAW": None,
        }
        return public_legs, features, signals

    segments = _continuous_segments(legs)
    path_legs = [leg for segment in segments for leg in segment]
    complete_path = len(path_legs) == len(legs)
    net = points[-1].value - points[0].value
    path = sum((leg["_ABS_DELTA"] for leg in path_legs), ZERO)
    elapsed = Decimal(
        str((points[-1].effective_at - points[0].effective_at).total_seconds())
    ) / SIXTY
    for leg in legs:
        if leg["CONTIGUOUS_RAW"] and path > ZERO:
            leg["MOVE_SHARE_RAW"] = _number(leg["_ABS_DELTA"] / path)
    public_legs = [
        {key: value for key, value in leg.items() if not key.startswith("_")}
        for leg in legs
    ]
    runs = _directional_runs(segments)
    turning = _turning_structure(segments)
    if not complete_path:
        turning = {
            **turning,
            "OBSERVED_CONTIGUOUS_SIGN_CHANGE_COUNT_RAW": turning[
                "SIGN_CHANGE_COUNT_RAW"
            ],
            "SIGN_CHANGE_COUNT_RAW": None,
            "UNAVAILABLE_REASON": "NON_CONTIGUOUS_GAP",
        }
    correction = _correction(runs, complete_path=complete_path)
    velocity_changes, acceleration, deceleration = _velocity_changes(segments)
    target = operative_target_minute
    if target is None:
        target = next(
            (
                int(point.target_minute)
                for point in reversed(points)
                if point.target_minute is not None
            ),
            5,
        )
    windows, dominant_windows = _window_features(
        legs,
        total_path=path,
        operative_target=target,
    )
    if path_legs:
        maximum = max(leg["_ABS_DELTA"] for leg in path_legs)
        dominant_segments = [leg["LEG_ID"] for leg in path_legs if leg["_ABS_DELTA"] == maximum]
    else:
        dominant_segments = []
    pattern = _path_pattern([leg["_DELTA"] for leg in path_legs]) if complete_path else None
    features = {
        "OBSERVATION_COUNT": len(points),
        "LEG_COUNT": len(legs),
        "SIGN_SEQUENCE_RAW": [leg["DIRECTION_BY_SIGN"] for leg in legs],
        "NET_START_OBSERVATION": points[0].point_id,
        "NET_END_OBSERVATION": points[-1].point_id,
        "NET_START_TARGET": points[0].target_minute,
        "NET_END_TARGET": points[-1].target_minute,
        "NET_MOVE_RAW": _number(net),
        "PATH_LENGTH_RAW": _number(path),
        "PATH_EFFICIENCY_RAW": (
            None if not complete_path or path == ZERO else _number(abs(net) / path)
        ),
        "NO_MOVEMENT_RAW": complete_path and path == ZERO,
        "ELAPSED_MINUTES_ACTUAL": _number(elapsed),
        "VELOCITY_RAW": None if not complete_path or elapsed <= ZERO else _number(net / elapsed),
        "DIRECTIONAL_RUNS": runs,
        "VELOCITY_CHANGES_RAW": velocity_changes,
        "GAP_PRESENT_RAW": gap_present or not complete_path,
        "WINDOW_ASSIGNMENT_POLICY": "LEG_ASSIGNED_BY_END_OBSERVATION",
        "WINDOWS": windows,
        "DOMINANT_SEGMENTS_RAW": dominant_segments,
        "DOMINANT_WINDOWS_RAW": dominant_windows,
    }
    signals = {
        "PATH_PATTERN_RAW": pattern,
        "PATH_UNAVAILABLE_REASON": None if complete_path else "NON_CONTIGUOUS_GAP",
        "NET_DIRECTION_RAW": _direction(net),
        "FINAL_RUN_DIRECTION_RAW": runs[-1]["DIRECTION"] if runs else None,
        "TURNING_STRUCTURE_RAW": turning,
        "CORRECTION_STATE_RAW": correction["STATE"],
        "CORRECTION_RAW": correction,
        "OVERSHOOT_RAW": correction["OVERSHOOT_RAW"],
        "ACCELERATION_RAW": acceleration,
        "DECELERATION_RAW": deceleration,
        "BOOK_EXCHANGE_RELATION_CHANGE_RAW": None,
    }
    return public_legs, features, signals


__all__ = ["build_legs", "build_temporal_features"]
