from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.market_snapshot_extractor import (
    QuotePoint,
    QuoteTrace,
    TargetMinuteSelection,
)
from modules.pillars.odds_trajectory_context import (
    BookieOddsTrajectory,
    ChoiceOddsTrajectory,
    MarketLineOddsTrajectory,
    OddsPointMeta,
    OddsTrajectoryContext,
)
from modules.pillars.pillar_5.periods import (
    B365_AWAY_1X2_FULL_TIME_ODDS_PRICE,
    B365_DRAW_1X2_FULL_TIME_ODDS_PRICE,
    B365_HOME_1X2_FULL_TIME_ODDS_PRICE,
    BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE,
    BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE,
    BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE,
    BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE,
    BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE,
    BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE,
    BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE,
    PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE,
    PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE,
    PIN_HOME_1X2_FULL_TIME_ODDS_PRICE,
    SOFA_AWAY_1X2_FULL_TIME_ODDS_PRICE,
    SOFA_DRAW_1X2_FULL_TIME_ODDS_PRICE,
    SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE,
)
from modules.pillars.pillar_5.run_pillar_5 import ENGINE_VERSION, calculate_pillar_5
from modules.pillars.pillar_5.snapshot_policy import extract_p5_market_snapshot


def _make_meta(minute: int, exchange_size: Decimal | None = None) -> OddsPointMeta:
    return OddsPointMeta(
        snapshot_id=minute + 1000,
        collected_at=datetime(2026, 6, 2, 12, 0, tzinfo=timezone.utc),
        minutes_before_start=minute,
        target_minute=minute,
        distance_from_target=0,
        exchange_size=exchange_size,
    )


def _make_choice(
    choice_name: str,
    odds: Decimal,
    minute: int = 0,
    exchange_size: Decimal | None = None,
) -> ChoiceOddsTrajectory:
    return ChoiceOddsTrajectory(
        choice_name=choice_name,
        choice_id=1,
        initial_odds=odds,
        odds_values={minute: odds},
        meta_by_minute={minute: _make_meta(minute, exchange_size=exchange_size)},
    )


def _build_event_context(minutes_until_start: int = 0) -> EventContext:
    home = ParticipantContext(
        participant_id=1,
        source="test",
        source_participant_id=101,
        name="Celta Vigo",
        slug="celta-vigo",
        short_name="CEL",
        source_status="normalized",
    )
    away = ParticipantContext(
        participant_id=2,
        source="test",
        source_participant_id=202,
        name="Levante UD",
        slug="levante-ud",
        short_name="LEV",
        source_status="normalized",
    )
    competition = CompetitionContext(
        competition_id=11,
        source="test",
        source_tournament_id=11,
        source_unique_tournament_id=11,
        canonical_name="La Liga",
        display_name="La Liga",
        slug="la-liga",
        unique_slug="la-liga",
        category_id=1,
        category_name="Spain",
        number_of_teams=20,
        number_of_teams_source="db_cache",
        total_regular_season_games=38,
        standings_grouping="league",
        league_config_source="test",
        has_standings_source_endpoint=True,
        source_status="normalized",
    )
    return EventContext(
        event_id=999,
        custom_id="event-999",
        sport="Football",
        season_id=2025,
        season_name="2025",
        season_year=2025,
        starts_at=datetime(2025, 5, 15, tzinfo=timezone.utc),
        minutes_until_start=minutes_until_start,
        discovery_source="test",
        home=home,
        away=away,
        competition=competition,
        participants_label="Celta Vigo vs Levante UD",
        context_status="normalized",
    )


def test_p5_extraction_sofascore_1x2():
    """Verify that SofaScore 1X2 Full Time quotes are extracted into SOFA_* variables."""
    minute = 0
    choices = {
        "1": _make_choice("1", Decimal("1.850"), minute=minute),
        "x": _make_choice("x", Decimal("3.400"), minute=minute),
        "2": _make_choice("2", Decimal("4.200"), minute=minute),
    }
    bookie = BookieOddsTrajectory(
        bookie_id=1,
        bookie_name="SofaScore",
        choices=choices,
    )
    market_line = MarketLineOddsTrajectory(
        market_id=1,
        market_name="1X2 Full Time",
        market_group="1X2",
        market_period="Full Time",
        line_value="Full Time",
        bookies={"1:sofascore:single:0": bookie},
    )
    context = OddsTrajectoryContext(
        available=True,
        event_id=999,
        target_minutes_expected=[minute],
        target_minutes_present=[minute],
        missing_target_minutes=[],
        markets={"1X2": {"Full Time": {"1X2 Full Time": {"__default__": market_line}}}},
    )

    event_context = _build_event_context(minutes_until_start=minute)
    target_selection = TargetMinuteSelection(target_minute=minute)

    result = calculate_pillar_5(
        event_context,
        context,
        target_selection=target_selection,
        debug_mode=True,
    )

    assert result["pillar_id"] == "pillar_5"
    assert result["engine_version"] == ENGINE_VERSION
    assert result["P5_TARGET_MINUTE"] == minute
    assert result["P5_STATUS"] in {"ACTIVE", "PARTIAL"}
    assert result["status"] in {"ACTIVE", "PARTIAL"}

    inputs = result["raw"]["inputs"]
    assert inputs[SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE] == 1.85
    assert inputs[SOFA_DRAW_1X2_FULL_TIME_ODDS_PRICE] == 3.40
    assert inputs[SOFA_AWAY_1X2_FULL_TIME_ODDS_PRICE] == 4.20

    # Traces must be present for SofaScore
    traces = result["raw"]["traces"]
    assert SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE in traces
    assert traces[SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE]["bookie_name"] == "SofaScore"
    assert traces[SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE]["target_minute"] == minute


def test_p5_extraction_pinnacle_bet365_and_exchange():
    """Verify multi-bookmaker extraction including Betfair Exchange with back and lay."""
    minute = 30
    pin_choices = {
        "1": _make_choice("1", Decimal("2.100"), minute=minute),
        "x": _make_choice("x", Decimal("3.250"), minute=minute),
        "2": _make_choice("2", Decimal("3.600"), minute=minute),
    }
    b365_choices = {
        "1": _make_choice("1", Decimal("2.050"), minute=minute),
        "x": _make_choice("x", Decimal("3.300"), minute=minute),
        "2": _make_choice("2", Decimal("3.500"), minute=minute),
    }
    bf_back_choices = {
        "1": _make_choice("1", Decimal("2.120"), minute=minute, exchange_size=Decimal("1500.0")),
        "x": _make_choice("x", Decimal("3.350"), minute=minute, exchange_size=Decimal("800.0")),
        "2": _make_choice("2", Decimal("3.650"), minute=minute, exchange_size=Decimal("1200.0")),
    }
    bf_lay_choices = {
        "1": _make_choice("1", Decimal("2.140"), minute=minute, exchange_size=Decimal("2000.0")),
        "x": _make_choice("x", Decimal("3.400"), minute=minute, exchange_size=Decimal("950.0")),
        "2": _make_choice("2", Decimal("3.700"), minute=minute, exchange_size=Decimal("1400.0")),
    }

    bookies = {
        "302:pinnacle:single:0": BookieOddsTrajectory(
            bookie_id=302, bookie_name="Pinnacle Sports", choices=pin_choices
        ),
        "3:bet365:single:0": BookieOddsTrajectory(
            bookie_id=3, bookie_name="bet365", choices=b365_choices
        ),
        "4:betfair:back:0": BookieOddsTrajectory(
            bookie_id=4,
            bookie_name="Betfair Exchange",
            exchange_side="back",
            exchange_level=0,
            choices=bf_back_choices,
        ),
        "4:betfair:lay:0": BookieOddsTrajectory(
            bookie_id=4,
            bookie_name="Betfair Exchange",
            exchange_side="lay",
            exchange_level=0,
            choices=bf_lay_choices,
        ),
    }
    market_line = MarketLineOddsTrajectory(
        market_id=1,
        market_name="1X2 Full Time",
        market_group="1X2",
        market_period="Full Time",
        line_value="Full Time",
        bookies=bookies,
    )
    context = OddsTrajectoryContext(
        available=True,
        event_id=999,
        target_minutes_expected=[minute],
        target_minutes_present=[minute],
        missing_target_minutes=[],
        markets={"1X2": {"Full Time": {"1X2 Full Time": {"__default__": market_line}}}},
    )

    event_context = _build_event_context(minutes_until_start=minute)
    target_selection = TargetMinuteSelection(target_minute=minute)

    result = calculate_pillar_5(
        event_context,
        context,
        target_selection=target_selection,
    )

    assert result["P5_STATUS"] in {"ACTIVE", "PARTIAL"}
    inputs = result["raw"]["inputs"]
    assert inputs[PIN_HOME_1X2_FULL_TIME_ODDS_PRICE] == 2.10
    assert inputs[PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE] == 3.25
    assert inputs[PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE] == 3.60

    assert inputs[B365_HOME_1X2_FULL_TIME_ODDS_PRICE] == 2.05
    assert inputs[B365_DRAW_1X2_FULL_TIME_ODDS_PRICE] == 3.30
    assert inputs[B365_AWAY_1X2_FULL_TIME_ODDS_PRICE] == 3.50

    assert inputs[BF_HOME_BACK_1X2_FULL_TIME_ODDS_PRICE] == 2.12
    assert inputs[BF_HOME_BACK_1X2_FULL_TIME_EXCHANGE_SIZE] == 1500.0
    assert inputs[BF_HOME_LAY_1X2_FULL_TIME_ODDS_PRICE] == 2.14
    assert inputs[BF_DRAW_BACK_1X2_FULL_TIME_ODDS_PRICE] == 3.35
    assert inputs[BF_DRAW_LAY_1X2_FULL_TIME_ODDS_PRICE] == 3.40
    assert inputs[BF_AWAY_BACK_1X2_FULL_TIME_ODDS_PRICE] == 3.65
    assert inputs[BF_AWAY_LAY_1X2_FULL_TIME_ODDS_PRICE] == 3.70


def test_p5_home_away_two_way():
    """Verify 2-way Home/Away Full Time market without Draw."""
    minute = 30
    choices = {
        "1": _make_choice("1", Decimal("1.750"), minute=minute),
        "2": _make_choice("2", Decimal("2.150"), minute=minute),
    }
    bookie = BookieOddsTrajectory(
        bookie_id=302,
        bookie_name="Pinnacle Sports",
        choices=choices,
    )
    market_line = MarketLineOddsTrajectory(
        market_id=1,
        market_name="Home/Away Full Time",
        market_group="Home/Away",
        market_period="Full Time",
        line_value="Full Time",
        bookies={"302:pinnacle:single:0": bookie},
    )
    context = OddsTrajectoryContext(
        available=True,
        event_id=999,
        target_minutes_expected=[minute],
        target_minutes_present=[minute],
        missing_target_minutes=[],
        markets={"Home/Away": {"Full Time": {"Home/Away Full Time": {"__default__": market_line}}}},
    )

    event_context = _build_event_context(minutes_until_start=minute)
    result = calculate_pillar_5(
        event_context,
        context,
        target_selection=TargetMinuteSelection(target_minute=minute),
    )

    assert result["P5_STATUS"] in {"ACTIVE", "PARTIAL"}
    inputs = result["raw"]["inputs"]
    assert inputs[PIN_HOME_1X2_FULL_TIME_ODDS_PRICE] == 1.75
    assert inputs[PIN_DRAW_1X2_FULL_TIME_ODDS_PRICE] is None
    assert inputs[PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE] == 2.15


def test_p5_auto_target_minute_resolution():
    """Verify that calculate_pillar_5 automatically resolves target_minute when target_selection is None."""
    minute = 30
    choices = {
        "1": _make_choice("1", Decimal("1.900"), minute=minute),
        "2": _make_choice("2", Decimal("2.000"), minute=minute),
    }
    bookie = BookieOddsTrajectory(
        bookie_id=1,
        bookie_name="SofaScore",
        choices=choices,
    )
    market_line = MarketLineOddsTrajectory(
        market_id=1,
        market_name="1X2 Full Time",
        market_group="1X2",
        market_period="Full Time",
        line_value="Full Time",
        bookies={"1:sofascore:single:0": bookie},
    )
    context = OddsTrajectoryContext(
        available=True,
        event_id=999,
        target_minutes_expected=[minute],
        target_minutes_present=[minute],
        missing_target_minutes=[],
        markets={"1X2": {"Full Time": {"1X2 Full Time": {"__default__": market_line}}}},
    )

    event_context = _build_event_context(minutes_until_start=minute)
    result = calculate_pillar_5(event_context, context, target_selection=None)

    assert result["P5_TARGET_MINUTE"] == minute
    assert result["P5_STATUS"] in {"ACTIVE", "PARTIAL"}
    assert result["raw"]["inputs"][SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE] == 1.90


def test_p5_missing_trajectory_insufficient_data():
    """Verify that missing or unavailable trajectory returns INSUFFICIENT_DATA."""
    event_context = _build_event_context()
    context = OddsTrajectoryContext(
        available=False,
        event_id=999,
        target_minutes_expected=[0],
        target_minutes_present=[],
        missing_target_minutes=[0],
        markets={},
    )

    result = calculate_pillar_5(event_context, context, target_selection=None)
    assert result["P5_STATUS"] == "INSUFFICIENT_DATA"
    assert result["P5_TARGET_MINUTE"] is None
    assert result["raw"]["inputs"][SOFA_HOME_1X2_FULL_TIME_ODDS_PRICE] is None
