from __future__ import annotations

from modules.pillars.odds_trajectory_context import build_odds_trajectory_context


def _make_rows() -> list[dict[str, object]]:
    return [
        {
            "event_id": 1,
            "market_id": 10,
            "market_name": "1X2 Full Time",
            "market_group": "1X2",
            "market_period": "Full Time",
            "choice_group": None,
            "bookie_id": 1,
            "bookie_name": "SofaScore",
            "choice_id": 101,
            "choice_name": "1",
            "initial_odds": "1.900",
            "odds_value": "1.850",
            "snapshot_id": 1001,
            "collected_at": "2026-01-01T10:00:00",
            "minutes_before_start": 0,
            "target_minute": 0,
            "distance_from_target": 0,
        },
        {
            "event_id": 1,
            "market_id": 11,
            "market_name": "1X2 Full Time",
            "market_group": "1X2",
            "market_period": "Full Time",
            "choice_group": None,
            "bookie_id": 2,
            "bookie_name": "Pinnacle",
            "choice_id": 201,
            "choice_name": "1",
            "initial_odds": "1.910",
            "odds_value": "1.860",
            "snapshot_id": 2001,
            "collected_at": "2026-01-01T10:00:00",
            "minutes_before_start": 0,
            "target_minute": 0,
            "distance_from_target": 0,
        },
    ]


def test_filter_by_bookie_ids_keeps_only_requested_bookie() -> None:
    context = build_odds_trajectory_context(_make_rows(), target_minutes_expected=[0])

    filtered = context.filter_by_bookie_ids({1})

    assert filtered.available is True
    assert filtered.target_minutes_present == [0]
    assert filtered.missing_target_minutes == []

    original_bookies = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"].bookies
    )
    filtered_bookies = (
        filtered.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"].bookies
    )

    assert set(original_bookies.keys()) == {"SofaScore", "Pinnacle"}
    assert set(filtered_bookies.keys()) == {"SofaScore"}
    assert "Pinnacle" not in filtered_bookies
    assert set(original_bookies.keys()) == {"SofaScore", "Pinnacle"}


def test_filter_by_bookie_ids_returns_unavailable_context_when_no_bookie_matches() -> None:
    context = build_odds_trajectory_context(_make_rows(), target_minutes_expected=[0])

    filtered = context.filter_by_bookie_ids({999})

    assert filtered.available is False
    assert filtered.markets == {}
    assert filtered.target_minutes_present == []
    assert filtered.missing_target_minutes == [0]


def test_market_group_and_period_filters_still_preserve_shape_and_availability() -> None:
    rows = _make_rows() + [
        {
            "event_id": 1,
            "market_id": 12,
            "market_name": "1X2 Full Time",
            "market_group": "1X2",
            "market_period": "1st half",
            "choice_group": None,
            "bookie_id": 1,
            "bookie_name": "SofaScore",
            "choice_id": 102,
            "choice_name": "1",
            "initial_odds": "2.010",
            "odds_value": "1.970",
            "snapshot_id": 3001,
            "collected_at": "2026-01-01T10:00:00",
            "minutes_before_start": 0,
            "target_minute": 0,
            "distance_from_target": 0,
        },
        {
            "event_id": 1,
            "market_id": 13,
            "market_name": "Over/Under Full Time",
            "market_group": "totals",
            "market_period": "Full Time",
            "choice_group": None,
            "bookie_id": 1,
            "bookie_name": "SofaScore",
            "choice_id": 103,
            "choice_name": "over",
            "initial_odds": "1.750",
            "odds_value": "1.720",
            "snapshot_id": 4001,
            "collected_at": "2026-01-01T10:00:00",
            "minutes_before_start": 0,
            "target_minute": 0,
            "distance_from_target": 0,
        },
    ]

    context = build_odds_trajectory_context(rows, target_minutes_expected=[0])

    filtered = context.filter_by_market_groups({"1X2"}).filter_by_market_period({"Full Time"})

    assert filtered.available is True
    assert set(filtered.markets.keys()) == {"1X2"}
    assert set(filtered.markets["1X2"].keys()) == {"Full Time"}
    assert set(filtered.target_minutes_present) == {0}
    assert filtered.missing_target_minutes == []
