from __future__ import annotations

import pytest
from decimal import Decimal
from datetime import datetime, timezone

from modules.pillars.odds_trajectory_context import (
    BookieOddsTrajectory,
    ChoiceOddsTrajectory,
    MarketLineOddsTrajectory,
    OddsPointMeta,
    OddsTrajectoryContext,
)


def _make_meta(minute: int) -> OddsPointMeta:
    return OddsPointMeta(
        snapshot_id=minute + 1000,
        collected_at=datetime(2026, 6, 2, 12, 0, tzinfo=timezone.utc),
        minutes_before_start=minute,
        target_minute=minute,
        distance_from_target=0,
    )


def _make_choice(
    *,
    odds_values: dict[int, float],
    choice_name: str = "Home",
) -> ChoiceOddsTrajectory:
    choice_odds = {min_val: Decimal(str(val)) for min_val, val in odds_values.items()}
    meta_by_minute = {min_val: _make_meta(min_val) for min_val in odds_values}

    return ChoiceOddsTrajectory(
        choice_name=choice_name,
        choice_id=1,
        initial_odds=Decimal("2.00"),
        odds_values=choice_odds,
        meta_by_minute=meta_by_minute,
    )


def _make_market_line(
    choice: ChoiceOddsTrajectory,
    *,
    market_group: str,
    market_period: str = "Full-time",
    market_name: str = "Match Result",
    choice_group: str | None = None,
    bookie_name: str = "SofaScore",
) -> MarketLineOddsTrajectory:
    bookie = BookieOddsTrajectory(
        bookie_id=1,
        bookie_name=bookie_name,
        choices={choice.choice_name: choice},
    )
    return MarketLineOddsTrajectory(
        market_id=1,
        market_name=market_name,
        market_group=market_group,
        market_period=market_period,
        choice_group=choice_group,
        bookies={bookie_name: bookie},
    )


def test_filter_by_market_groups_keeps_only_allowed() -> None:
    # Set up some test markets:
    # 1. 1X2 group
    choice_1x2 = _make_choice(odds_values={120: 1.95, 30: 1.90})
    line_1x2 = _make_market_line(choice_1x2, market_group="1X2")

    # 2. Home/Away group
    choice_ha = _make_choice(odds_values={30: 1.85, 5: 1.80})
    line_ha = _make_market_line(choice_ha, market_group="Home/Away")

    # 3. totals group (should be filtered out)
    choice_totals = _make_choice(odds_values={120: 2.10, 0: 2.05})
    line_totals = _make_market_line(choice_totals, market_group="totals")

    # Construct complete markets dictionary
    markets = {
        "1X2": {"Full-time": {"Match Result": {"__default__": line_1x2}}},
        "Home/Away": {"Full-time": {"Match Result": {"__default__": line_ha}}},
        "totals": {"Full-time": {"Match Result": {"__default__": line_totals}}},
    }

    context = OddsTrajectoryContext(
        available=True,
        event_id=42,
        target_minutes_expected=[120, 30, 5, 0],
        target_minutes_present=[120, 30, 5, 0],
        missing_target_minutes=[],
        markets=markets,
    )

    # Filter context
    filtered = context.filter_by_market_groups(["1X2", "Home/Away"])

    # Assertions
    assert filtered.available is True
    assert filtered.event_id == 42
    assert "1X2" in filtered.markets
    assert "Home/Away" in filtered.markets
    assert "totals" not in filtered.markets

    # target_minutes_present and missing_target_minutes should be recalculated
    # Remaining markets have present minutes:
    # - 1X2: 120, 30
    # - Home/Away: 30, 5
    # Combined present minutes: {120, 30, 5}
    # Expected: [120, 30, 5, 0]
    assert filtered.target_minutes_present == [120, 30, 5]
    assert filtered.missing_target_minutes == [0]


def test_filter_by_market_groups_returns_unavailable_when_no_matches() -> None:
    choice_totals = _make_choice(odds_values={120: 2.10, 0: 2.05})
    line_totals = _make_market_line(choice_totals, market_group="totals")

    markets = {
        "totals": {"Full-time": {"Match Result": {"__default__": line_totals}}},
    }

    context = OddsTrajectoryContext(
        available=True,
        event_id=42,
        target_minutes_expected=[120, 30, 5, 0],
        target_minutes_present=[120, 0],
        missing_target_minutes=[30, 5],
        markets=markets,
    )

    # Filter context for non-existing groups
    filtered = context.filter_by_market_groups(["1X2", "Home/Away"])

    # Assertions
    assert filtered.available is False
    assert filtered.event_id == 42
    assert not filtered.markets
    assert filtered.target_minutes_present == []
    assert filtered.missing_target_minutes == [120, 30, 5, 0]


def test_filter_by_market_period_keeps_only_allowed() -> None:
    # Set up some test markets:
    # 1. 1X2 Full-time
    choice_ft = _make_choice(odds_values={120: 1.95, 30: 1.90})
    line_ft = _make_market_line(choice_ft, market_group="1X2", market_period="Full-time")

    # 2. 1X2 1st half
    choice_fh = _make_choice(odds_values={30: 1.85, 5: 1.80})
    line_fh = _make_market_line(choice_fh, market_group="1X2", market_period="1st half")

    # Construct markets dictionary
    markets = {
        "1X2": {
            "Full-time": {"Match Result": {"__default__": line_ft}},
            "1st half": {"Match Result": {"__default__": line_fh}},
        }
    }

    context = OddsTrajectoryContext(
        available=True,
        event_id=42,
        target_minutes_expected=[120, 30, 5, 0],
        target_minutes_present=[120, 30, 5],
        missing_target_minutes=[0],
        markets=markets,
    )

    # Filter context (default is "Full-time")
    filtered = context.filter_by_market_period()

    # Assertions
    assert filtered.available is True
    assert filtered.event_id == 42
    assert "1X2" in filtered.markets
    assert "Full-time" in filtered.markets["1X2"]
    assert "1st half" not in filtered.markets["1X2"]

    # target_minutes_present and missing_target_minutes should be recalculated
    # Remaining markets have present minutes:
    # - Full-time: 120, 30
    # Combined present minutes: {120, 30}
    # Expected: [120, 30, 5, 0]
    assert filtered.target_minutes_present == [120, 30]
    assert filtered.missing_target_minutes == [5, 0]


def test_filter_by_market_period_returns_unavailable_when_no_matches() -> None:
    choice_fh = _make_choice(odds_values={30: 1.85, 5: 1.80})
    line_fh = _make_market_line(choice_fh, market_group="1X2", market_period="1st half")

    markets = {
        "1X2": {
            "1st half": {"Match Result": {"__default__": line_fh}},
        }
    }

    context = OddsTrajectoryContext(
        available=True,
        event_id=42,
        target_minutes_expected=[120, 30, 5, 0],
        target_minutes_present=[30, 5],
        missing_target_minutes=[120, 0],
        markets=markets,
    )

    # Filter context for "Full-time" (which is not in markets)
    filtered = context.filter_by_market_period()

    # Assertions
    assert filtered.available is False
    assert filtered.event_id == 42
    assert not filtered.markets
    assert filtered.target_minutes_present == []
    assert filtered.missing_target_minutes == [120, 30, 5, 0]

