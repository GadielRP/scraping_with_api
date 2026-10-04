from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from infrastructure.settings import Config
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.pillar_4.metrics import build_temporal_features
from modules.pillars.trajectory_sampling import TrajectoryPoint
from modules.pillars.pillar_4.run_pillar_4 import calculate_pillar_4
from modules.pillars.trajectory_selection import (
    TargetMinuteSelection,
    select_target_minute,
)
from shared.temporal import NaiveDateTimeError

KICKOFF = datetime(2026, 9, 12, 18, 0, tzinfo=timezone.utc)


def _event(target: int = 5):
    return SimpleNamespace(
        event_id=4404,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=target,
        starts_at=KICKOFF,
        context_status="normalized",
        competition=SimpleNamespace(competition_id=99, display_name="League"),
    )


def _row(
    *,
    minute: int,
    odds: str,
    choice: str = "Over",
    choice_id: int = 11,
    quote_id: int = 101,
    snapshot_id: int | None = None,
    line: str | None = "2.5",
    collected_at: datetime | None = None,
    source_collected_at: datetime | None = None,
    bookie_id: int = 302,
    bookie_name: str = "Pinnacle",
    source: str = "oddspapi",
    exchange_side: str | None = None,
    exchange_level: int = 0,
    market_group: str = "Over/Under",
    market_name: str = "Over/Under Full Time",
    market_period: str = "Full Time",
    main_line: bool | None = True,
) -> dict:
    observed_at = collected_at or (KICKOFF - timedelta(minutes=minute))
    source_at = source_collected_at or observed_at
    return {
        "event_id": 4404,
        "market_id": 70,
        "market_name": market_name,
        "market_group": market_group,
        "market_period": market_period,
        "line_value": line,
        "bookie_id": bookie_id,
        "bookie_name": bookie_name,
        "choice_id": choice_id,
        "choice_name": choice,
        "main_line": main_line,
        "quote_id": quote_id,
        "source": source,
        "exchange_side": exchange_side,
        "exchange_level": exchange_level,
        "initial_odds": "2.00",
        "odds_value": odds,
        "snapshot_id": (
            snapshot_id if snapshot_id is not None else quote_id * 1000 + minute
        ),
        "source_collected_at": source_at,
        "collected_at": observed_at,
        "observed_minutes_before_start": minute,
        "trajectory_minutes_before_start": Decimal(minute),
        "source_limit": Decimal("100"),
    }


def _complete_required_book_rows() -> list[dict]:
    rows = []
    for bookie_id, bookie_name, id_offset in (
        (302, "Pinnacle Sports", 0),
        (3, "bet365", 100),
    ):
        for choice, choice_id, quote_id, odds_by_minute in (
            ("Over", 11, 101, {120: "2.05", 30: "1.95", 5: "1.85"}),
            ("Under", 12, 102, {120: "1.80", 30: "1.90", 5: "2.00"}),
        ):
            rows.extend(
                _row(
                    minute=minute,
                    odds=odds,
                    choice=choice,
                    choice_id=choice_id + id_offset,
                    quote_id=quote_id + id_offset,
                    bookie_id=bookie_id,
                    bookie_name=bookie_name,
                )
                for minute, odds in odds_by_minute.items()
            )
    return rows


def _context(rows: list[dict], target: int = 5):
    return build_odds_trajectory_context(
        rows,
        target_minutes_expected=[120, 30, 5, 1, 0, -5],
        tolerance_minutes=0,
        evaluation_minute=target,
    )


@pytest.fixture(autouse=True)
def _zero_tolerance(monkeypatch):
    monkeypatch.setattr(Config, "PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES", 0)


def _run_p4(
    event,
    context,
    *,
    target_minute,
    evaluation_as_of=None,
    debug_mode=False,
):
    if evaluation_as_of is not None:
        context = replace(context, evaluation_as_of=evaluation_as_of)
    return calculate_pillar_4(
        event,
        context,
        TargetMinuteSelection(target_minute=target_minute),
        debug_mode=debug_mode,
    )


def test_p4_context_rejects_naive_snapshot_timestamps() -> None:
    rows = [
        _row(
            minute=minute,
            odds=odds,
            collected_at=datetime(2026, 9, 12, hour, minute_of_hour),
            source_collected_at=datetime(2026, 9, 12, hour, minute_of_hour),
        )
        for minute, odds, hour, minute_of_hour in (
            (120, "2.05", 10, 0),
            (30, "1.95", 11, 30),
            (5, "1.85", 11, 55),
        )
    ]

    with pytest.raises(NaiveDateTimeError):
        _context(rows, 5)


def test_zero_plateau_between_opposite_legs_is_a_reversal_turning_zone() -> None:
    def point(index: int, value: str) -> TrajectoryPoint:
        timestamp = KICKOFF + timedelta(minutes=index)
        return TrajectoryPoint(
            point_id=str(index),
            value=Decimal(value),
            effective_at=timestamp,
            availability_at=timestamp,
            minutes_before_start=Decimal(-index),
        )

    _, _, signals = build_temporal_features(
        [point(0, "1"), point(1, "2"), point(2, "2"), point(3, "1")]
    )

    assert signals["PATH_PATTERN_RAW"] == "REVERSAL"
    assert signals["TURNING_STRUCTURE_RAW"]["ZERO_BRIDGED_SIGN_CHANGE"] is True
    assert signals["TURNING_STRUCTURE_RAW"]["ZONES"][0]["STRUCTURE"] == "PLATEAU"


def test_checkpoint_gap_preserves_net_but_does_not_invent_path() -> None:
    rows = [
        _row(minute=120, odds="2.10"),
        _row(minute=5, odds="1.90"),
    ]

    result = _run_p4(
        _event(5),
        _context(rows, 5),
        target_minute=5,
    )

    series = next(
        item
        for item in result["analysis"].values()
        if item["market"]["VALUE_TYPE"] == "ODDS_PRICE"
        and item["market"]["VIEW"] == "CHECKPOINT_VIEW"
    )
    features = series["raw_temporal_features"]
    signals = series["structural_signals"]
    assert series["legs"][0]["CONTIGUOUS_RAW"] is False
    assert features["GAP_PRESENT_RAW"] is True
    assert features["NET_MOVE_RAW"] == pytest.approx(-0.20)
    assert features["PATH_LENGTH_RAW"] == 0.0
    assert features["PATH_EFFICIENCY_RAW"] is None
    assert features["VELOCITY_RAW"] is None
    assert signals["PATH_PATTERN_RAW"] is None
    assert signals["PATH_UNAVAILABLE_REASON"] == "NON_CONTIGUOUS_GAP"
    assert signals["TURNING_STRUCTURE_RAW"]["SIGN_CHANGE_COUNT_RAW"] is None


def test_temporal_math_preserves_runs_overshoot_velocity_and_dominant_ties() -> None:
    def point(index: int, value: str, minute: int) -> TrajectoryPoint:
        timestamp = KICKOFF - timedelta(minutes=minute)
        return TrajectoryPoint(
            point_id=str(index),
            value=Decimal(value),
            effective_at=timestamp,
            availability_at=timestamp,
            minutes_before_start=Decimal(minute),
        )

    legs, features, signals = build_temporal_features(
        [
            point(0, "1.00", 120),
            point(1, "1.10", 90),
            point(2, "1.20", 60),
            point(3, "0.95", 5),
        ],
        operative_target_minute=5,
    )

    correction = signals["CORRECTION_RAW"]
    assert features["SIGN_SEQUENCE_RAW"] == [1, 1, -1]
    assert signals["PATH_PATTERN_RAW"] == "REVERSAL"
    assert correction["CORRECTION_RATIO_RAW"] == pytest.approx(1.25)
    assert correction["MOVE_RETENTION_RAW"] == 0.0
    assert correction["OVERSHOOT_RAW"] is True
    assert correction["OVERSHOOT_MAGNITUDE_RAW"] == pytest.approx(0.05)
    assert legs[0]["VELOCITY_RAW"] == pytest.approx(0.10 / 30)
    assert features["DOMINANT_SEGMENTS_RAW"] == [legs[2]["LEG_ID"]]


def test_missing_early_anchor_preserves_contiguous_later_path() -> None:
    rows = [
        _row(minute=120, odds="2.10"),
        _row(minute=30, odds="2.00"),
        _row(minute=5, odds="1.90"),
    ]
    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=[360, 120, 30, 5],
        tolerance_minutes=0,
        evaluation_minute=5,
    )

    result = _run_p4(_event(5), context, target_minute=5)

    series = next(
        item
        for item in result["analysis"].values()
        if item["market"]["VALUE_TYPE"] == "ODDS_PRICE"
        and item["market"]["VIEW"] == "CHECKPOINT_VIEW"
    )
    assert series["status"] == "ACTIVE"
    assert all(leg["CONTIGUOUS_RAW"] for leg in series["legs"])
    assert series["raw_temporal_features"]["PATH_LENGTH_RAW"] == pytest.approx(0.20)
    assert series["structural_signals"]["PATH_PATTERN_RAW"] == "UNIDIRECTIONAL"


def test_exact_zero_and_sub_threshold_moves_are_not_reclassified() -> None:
    def points(values: tuple[str, ...]) -> list[TrajectoryPoint]:
        return [
            TrajectoryPoint(
                point_id=str(index),
                value=Decimal(value),
                effective_at=KICKOFF + timedelta(minutes=index),
                availability_at=KICKOFF + timedelta(minutes=index),
                minutes_before_start=Decimal(-index),
            )
            for index, value in enumerate(values)
        ]

    small_legs, _, small_signals = build_temporal_features(points(("1", "1.019")))
    _, flat_features, flat_signals = build_temporal_features(points(("1", "1")))

    assert small_legs[0]["DELTA_RAW"] == pytest.approx(0.019)
    assert small_legs[0]["DIRECTION_BY_SIGN"] == 1
    assert small_signals["NET_DIRECTION_RAW"] == "POSITIVE"
    assert flat_features["NO_MOVEMENT_RAW"] is True
    assert flat_features["PATH_EFFICIENCY_RAW"] is None
    assert flat_signals["PATH_PATTERN_RAW"] == "NO_MOVEMENT"
    assert flat_signals["OVERSHOOT_RAW"] is None
