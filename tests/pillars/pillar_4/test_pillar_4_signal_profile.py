from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from infrastructure.settings import Config
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.pillar_4.metrics import build_temporal_features
from modules.pillars.pillar_4.models import P4Point
from modules.pillars.pillar_4.run_pillar_4 import calculate_pillar_4


KICKOFF = datetime(2026, 9, 12, 18, 0)


def _event(target: int = 5):
    return SimpleNamespace(
        event_id=4404,
        sport="Football",
        participants_label="Home vs Away",
        minutes_until_start=target,
        start_time_utc=KICKOFF,
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
        "choice_group": line,
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
        "snapshot_id": snapshot_id if snapshot_id is not None else quote_id * 1000 + minute,
        "source_collected_at": source_at,
        "collected_at": observed_at,
        "observed_minutes_before_start": minute,
        "trajectory_minutes_before_start": Decimal(minute),
        "source_limit": Decimal("100"),
    }


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


def test_profile_uses_snapshots_and_excludes_every_point_after_dynamic_target() -> None:
    rows = []
    for choice, choice_id, quote_id, values in (
        ("Over", 11, 101, {120: "2.05", 30: "1.95", 5: "1.85", 1: "1.80", 0: "1.78", -5: "1.70"}),
        ("Under", 12, 102, {120: "1.80", 30: "1.90", 5: "2.00", 1: "2.05", 0: "2.08", -5: "2.20"}),
    ):
        rows.extend(
            _row(
                minute=minute,
                odds=odds,
                choice=choice,
                choice_id=choice_id,
                quote_id=quote_id,
            )
            for minute, odds in values.items()
        )

    result = calculate_pillar_4(
        _event(5),
        _context(rows, 5),
        target_minute=5,
    )

    assert result["P4_STATUS"] == "ACTIVE"
    assert result["P4_TARGET_MINUTE"] == 5
    assert result["pillar_id"] == "pillar_4_temporal_market_drift"
    assert result["raw"]["extraction_diagnostics"]["excluded_future_points"] == 6
    profile = result["P4_SIGNAL_PROFILE"]
    assert profile["META"]["OPERATIVE_AS_OF"] == (KICKOFF - timedelta(minutes=5)).isoformat()
    assert profile["TRACEABILITY"]["CAUSAL_CUTOFF_POLICY"].startswith("AVAILABILITY_AT")
    assert result["modules"][0]["P4_SIGNAL_PROFILE"] == profile

    over_checkpoint = next(
        series
        for series in profile["CHECKPOINT_VIEW"]["SERIES"]
        if series["MARKET"]["VALUE_TYPE"] == "ODDS_PRICE"
        and series["MARKET"]["CHOICE_NAME"] == "Over"
    )
    assert [point["TARGET_MINUTE"] for point in over_checkpoint["POINTS"]] == [120, 30, 5]
    assert [point["VALUE"] for point in over_checkpoint["POINTS"]] == [2.05, 1.95, 1.85]
    assert over_checkpoint["STRUCTURAL_SIGNALS"]["NET_DIRECTION_RAW"] == "NEGATIVE"
    assert profile["STRUCTURAL_DOMAIN_SUMMARY"]["TOTALS"]["SERIES_IDS"]


def test_debug_logging_reports_inputs_formulas_signals_and_lineage(caplog) -> None:
    caplog.set_level(logging.INFO)
    rows = [
        _row(minute=120, odds="2.05"),
        _row(minute=30, odds="1.95"),
        _row(minute=5, odds="1.85"),
    ]

    calculate_pillar_4(
        _event(5),
        _context(rows, 5),
        target_minute=5,
        debug_mode=True,
    )

    assert "P4 DEBUG | extraction | event_id=4404 | target_minute=5" in caplog.text
    assert "P4 DEBUG | extraction counts | source_series_seen=1" in caplog.text
    assert "P4 EXTRACTION | event_id=4404 | target_minute=5" in caplog.text
    assert "P4 DEBUG | input assignment | view=ADAPTIVE_VIEW" in caplog.text
    assert "P4 DEBUG | input lineage | series_id=" in caplog.text
    assert "P4 FORMULA |" in caplog.text
    assert ".DELTA_RAW | formula=end.VALUE - start.VALUE" in caplog.text
    assert ".NET_MOVE_RAW | formula=final.VALUE - initial.VALUE" in caplog.text
    assert "P4 SIGNAL | ADAPTIVE_VIEW | source_mode=PERSISTED_SNAPSHOTS" in caplog.text
    assert "P4 SIGNAL | CHECKPOINT_VIEW | source_mode=FIXED_CHECKPOINTS" in caplog.text
    assert "field=PATH_PATTERN_RAW" in caplog.text
    assert "P4 SIGNAL | STRUCTURAL_DOMAIN_SUMMARY.TOTALS" in caplog.text
    assert "P4 SIGNAL | SUMMARY | value=" in caplog.text


def test_detailed_debug_logging_is_silent_when_debug_mode_is_false(caplog) -> None:
    caplog.set_level(logging.INFO)

    calculate_pillar_4(
        _event(5),
        _context([_row(minute=5, odds="1.85")], 5),
        target_minute=5,
        debug_mode=False,
    )

    assert "P4 DEBUG |" not in caplog.text
    assert "P4 FORMULA |" not in caplog.text
    assert "P4 SIGNAL |" not in caplog.text


def test_exact_operational_target_is_required_without_fallback() -> None:
    rows = [_row(minute=30, odds="1.90")]

    result = calculate_pillar_4(
        _event(5),
        _context(rows, 5),
        target_minute=5,
    )

    assert result["P4_STATUS"] == "INSUFFICIENT_DATA"
    assert result["P4_TARGET_MINUTE"] == 5
    assert result["P4_SIGNAL_PROFILE"] is None
    assert result["modules"] == []
    assert any("OPERATIVE_TARGET:5" in item for item in result["MISSING_INPUTS"])


def test_target_minute_is_modular_and_not_hardcoded_to_five() -> None:
    rows = [
        _row(minute=120, odds="2.10"),
        _row(minute=30, odds="2.00"),
        _row(minute=5, odds="1.80"),
    ]

    result = calculate_pillar_4(
        _event(30),
        _context(rows, 30),
        target_minute=30,
    )

    assert result["P4_TARGET_MINUTE"] == 30
    assert result["P4_SIGNAL_PROFILE"] is not None
    series = next(
        item
        for item in result["P4_SIGNAL_PROFILE"]["CHECKPOINT_VIEW"]["SERIES"]
        if item["MARKET"]["VALUE_TYPE"] == "ODDS_PRICE"
    )
    assert [point["TARGET_MINUTE"] for point in series["POINTS"]] == [120, 30]
    assert result["raw"]["extraction_diagnostics"]["excluded_future_points"] == 1


def test_future_snapshot_with_old_provider_timestamp_is_still_excluded() -> None:
    rows = [
        _row(minute=30, odds="2.00"),
        _row(minute=5, odds="1.90"),
        _row(
            minute=1,
            odds="1.50",
            collected_at=KICKOFF - timedelta(minutes=1),
            source_collected_at=KICKOFF - timedelta(minutes=20),
        ),
    ]

    result = calculate_pillar_4(
        _event(5),
        _context(rows, 5),
        target_minute=5,
    )

    adaptive = next(
        item
        for item in result["P4_SIGNAL_PROFILE"]["ADAPTIVE_VIEW"]["SERIES"]
        if item["MARKET"]["VALUE_TYPE"] == "ODDS_PRICE"
    )
    assert 1.50 not in [point["VALUE"] for point in adaptive["POINTS"]]
    assert result["raw"]["extraction_diagnostics"]["excluded_future_points"] == 1


def test_endpoint_only_series_is_partial_and_not_no_movement() -> None:
    result = calculate_pillar_4(
        _event(5),
        _context([_row(minute=5, odds="1.90")], 5),
        target_minute=5,
    )

    assert result["P4_STATUS"] == "PARTIAL"
    series = next(
        item
        for item in result["P4_SIGNAL_PROFILE"]["ADAPTIVE_VIEW"]["SERIES"]
        if item["MARKET"]["VALUE_TYPE"] == "ODDS_PRICE"
    )
    assert series["STATUS"] == "PARTIAL"
    assert series["LEGS"] == []
    assert series["RAW_TEMPORAL_FEATURES"]["NET_MOVE_RAW"] is None
    assert series["STRUCTURAL_SIGNALS"]["PATH_PATTERN_RAW"] is None


def test_line_series_can_cross_contracts_without_crossing_price_series() -> None:
    rows = [
        _row(minute=30, odds="1.90", line="2.5", quote_id=201),
        _row(minute=5, odds="1.85", line="3.0", quote_id=202),
    ]

    result = calculate_pillar_4(
        _event(5),
        _context(rows, 5),
        target_minute=5,
    )

    line_series = next(
        item
        for item in result["P4_SIGNAL_PROFILE"]["CHECKPOINT_VIEW"]["SERIES"]
        if item["MARKET"]["VALUE_TYPE"] == "LINE"
    )
    assert [point["VALUE"] for point in line_series["POINTS"]] == [2.5, 3.0]
    assert line_series["RAW_TEMPORAL_FEATURES"]["NET_MOVE_RAW"] == 0.5
    price_series = [
        item
        for item in result["P4_SIGNAL_PROFILE"]["CHECKPOINT_VIEW"]["SERIES"]
        if item["MARKET"]["VALUE_TYPE"] == "ODDS_PRICE"
    ]
    assert {item["MARKET"]["CHOICE_GROUP"] for item in price_series} == {
        "2.5",
        "3.0",
    }
    assert all(item["LEGS"] == [] for item in price_series)
    old_contract = next(
        item for item in price_series if item["MARKET"]["CHOICE_GROUP"] == "2.5"
    )
    assert old_contract["STATUS"] == "PARTIAL"
    assert old_contract["TRACEABILITY"]["OPERATIVE_ENDPOINT_PRESENT"] is False


def test_ambiguous_line_at_same_checkpoint_is_diagnosed_without_selection() -> None:
    rows = [
        _row(minute=5, odds="1.85", line="2.5", quote_id=201),
        _row(minute=5, odds="1.95", line="3.0", quote_id=202),
    ]

    result = calculate_pillar_4(
        _event(5),
        _context(rows, 5),
        target_minute=5,
    )

    assert result["P4_STATUS"] == "PARTIAL"
    assert any("LINE_SELECTION" in item for item in result["AMBIGUOUS_INPUTS"])
    assert not any(
        item["MARKET"]["VALUE_TYPE"] == "LINE"
        for item in result["P4_SIGNAL_PROFILE"]["CHECKPOINT_VIEW"]["SERIES"]
    )


def test_zero_plateau_between_opposite_legs_is_a_reversal_turning_zone() -> None:
    def point(index: int, value: str) -> P4Point:
        timestamp = KICKOFF + timedelta(minutes=index)
        return P4Point(
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

    result = calculate_pillar_4(
        _event(5),
        _context(rows, 5),
        target_minute=5,
    )

    series = next(
        item
        for item in result["P4_SIGNAL_PROFILE"]["CHECKPOINT_VIEW"]["SERIES"]
        if item["MARKET"]["VALUE_TYPE"] == "ODDS_PRICE"
    )
    features = series["RAW_TEMPORAL_FEATURES"]
    signals = series["STRUCTURAL_SIGNALS"]
    assert series["LEGS"][0]["CONTIGUOUS_RAW"] is False
    assert features["GAP_PRESENT_RAW"] is True
    assert features["NET_MOVE_RAW"] == pytest.approx(-0.20)
    assert features["PATH_LENGTH_RAW"] == 0.0
    assert features["PATH_EFFICIENCY_RAW"] is None
    assert features["VELOCITY_RAW"] is None
    assert signals["PATH_PATTERN_RAW"] is None
    assert signals["PATH_UNAVAILABLE_REASON"] == "NON_CONTIGUOUS_GAP"
    assert signals["TURNING_STRUCTURE_RAW"]["SIGN_CHANGE_COUNT_RAW"] is None


def test_checkpoint_builds_p2_p3_compatible_semantic_series_and_relations() -> None:
    rows: list[dict] = []
    sources = (
        (
            302,
            "Pinnacle",
            None,
            "oddspapi",
            {"Home": ("2.00", "1.90", "1.80"), "Away": ("3.00", "3.10", "3.20")},
        ),
        (
            3,
            "bet365",
            None,
            "oddspapi",
            {"Home": ("2.10", "2.00", "1.90"), "Away": ("2.90", "3.00", "3.10")},
        ),
        (
            4,
            "Betfair Exchange",
            "back",
            "sofascore",
            {"Home": ("2.02", "1.92", "1.82"), "Away": ("3.02", "3.12", "3.22")},
        ),
        (
            4,
            "Betfair Exchange",
            "lay",
            "sofascore",
            {"Home": ("2.06", "1.96", "1.86"), "Away": ("3.08", "3.18", "3.28")},
        ),
    )
    quote_id = 300
    for bookie_id, bookie_name, exchange_side, source, choices in sources:
        for choice_id, (choice, values) in enumerate(choices.items(), start=1):
            quote_id += 1
            for minute, value in zip((120, 30, 5), values):
                rows.append(
                    _row(
                        minute=minute,
                        odds=value,
                        choice=choice,
                        choice_id=choice_id,
                        quote_id=quote_id,
                        line=None,
                        bookie_id=bookie_id,
                        bookie_name=bookie_name,
                        source=source,
                        exchange_side=exchange_side,
                        market_group="1X2",
                        market_name="1X2 Full Time",
                    )
                )

    result = calculate_pillar_4(_event(5), _context(rows, 5), target_minute=5)

    checkpoint = result["P4_SIGNAL_PROFILE"]["CHECKPOINT_VIEW"]["SERIES"]
    value_types = {series["MARKET"]["VALUE_TYPE"] for series in checkpoint}
    assert {
        "SIDE_EDGE",
        "BOOK_REP_EDGE",
        "BOOK_INTERNAL_GAP",
        "EXCHANGE_REP_EDGE",
        "EXCHANGE_INTERNAL_GAP",
        "BOOK_EXCHANGE_GAP",
        "BACK_LAY_RELATIVE_SPREAD",
    } <= value_types
    semantic = next(
        series for series in checkpoint if series["MARKET"]["VALUE_TYPE"] == "SIDE_EDGE"
    )
    assert semantic["TRACEABILITY"]["CONSTITUENT_SERIES_IDS"]
    assert any(
        series["STRUCTURAL_SIGNALS"]["BOOK_EXCHANGE_RELATION_CHANGE_RAW"]
        is not None
        for series in checkpoint
    )


def test_temporal_math_preserves_runs_overshoot_velocity_and_dominant_ties() -> None:
    def point(index: int, value: str, minute: int) -> P4Point:
        timestamp = KICKOFF - timedelta(minutes=minute)
        return P4Point(
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

    result = calculate_pillar_4(_event(5), context, target_minute=5)

    series = next(
        item
        for item in result["P4_SIGNAL_PROFILE"]["CHECKPOINT_VIEW"]["SERIES"]
        if item["MARKET"]["VALUE_TYPE"] == "ODDS_PRICE"
    )
    assert series["STATUS"] == "PARTIAL"
    assert all(leg["CONTIGUOUS_RAW"] for leg in series["LEGS"])
    assert series["RAW_TEMPORAL_FEATURES"]["PATH_LENGTH_RAW"] == pytest.approx(0.20)
    assert series["STRUCTURAL_SIGNALS"]["PATH_PATTERN_RAW"] == "UNIDIRECTIONAL"


def test_exact_zero_and_sub_threshold_moves_are_not_reclassified() -> None:
    def points(values: tuple[str, ...]) -> list[P4Point]:
        return [
            P4Point(
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


def test_non_main_line_contract_is_not_admitted_to_p4_scope() -> None:
    rows = [
        _row(minute=5, odds="1.90", line="2.5", quote_id=401, main_line=True),
        _row(minute=5, odds="2.10", line="3.0", quote_id=402, main_line=False),
    ]

    result = calculate_pillar_4(_event(5), _context(rows, 5), target_minute=5)

    prices = [
        series
        for series in result["P4_SIGNAL_PROFILE"]["CHECKPOINT_VIEW"]["SERIES"]
        if series["MARKET"]["VALUE_TYPE"] == "ODDS_PRICE"
    ]
    assert [series["MARKET"]["CHOICE_GROUP"] for series in prices] == ["2.5"]


def test_target_minute_rejects_coercion_that_could_move_the_causal_boundary() -> None:
    with pytest.raises(TypeError):
        calculate_pillar_4(_event(5), _context([], 5), target_minute=5.5)  # type: ignore[arg-type]
