from __future__ import annotations

import pytest
from decimal import Decimal
from datetime import datetime, timezone
from unittest.mock import patch

from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.odds_trajectory_context import (
    BookieOddsTrajectory,
    ChoiceOddsTrajectory,
    MarketLineOddsTrajectory,
    OddsPointMeta,
    OddsTrajectoryContext,
)
from modules.pillars.pillar_5.run_pillar_5 import calculate_pillar_5
from modules.pillars.pillar_5.exact_price_memory_engine.exact_price_memory_engine import (
    calculate_p5_exact_price_memory_engine,
)
from modules.pillars.pillar_5.exact_price_memory_engine.historical_samples import (
    ExactPriceMemorySample,
)


def _make_meta(minute: int) -> OddsPointMeta:
    return OddsPointMeta(
        snapshot_id=minute + 1000,
        collected_at=datetime(2026, 6, 2, 12, 0, tzinfo=timezone.utc),
        minutes_before_start=minute,
        target_minute=minute,
        distance_from_target=0,
    )


def _make_choice(choice_name: str, odds: Decimal) -> ChoiceOddsTrajectory:
    return ChoiceOddsTrajectory(
        choice_name=choice_name,
        choice_id=1,
        initial_odds=odds,
        odds_values={0: odds},
        meta_by_minute={0: _make_meta(0)},
    )


def _build_event_context() -> EventContext:
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
        minutes_until_start=90,
        discovery_source="test",
        home=home,
        away=away,
        competition=competition,
        participants_label="Celta Vigo vs Levante UD",
        context_status="normalized",
    )


def _build_odds_trajectory_context(
    *,
    market_group: str,
    home_odds: float,
    away_odds: float,
    draw_odds: float | None = None,
    bookie_name: str = "SofaScore",
) -> OddsTrajectoryContext:
    choices = {
        "1": _make_choice("1", Decimal(str(home_odds))),
        "2": _make_choice("2", Decimal(str(away_odds))),
    }
    if draw_odds is not None:
        choices["x"] = _make_choice("x", Decimal(str(draw_odds)))

    bookie = BookieOddsTrajectory(
        bookie_id=1,
        bookie_name=bookie_name,
        choices=choices,
    )
    
    market_line = MarketLineOddsTrajectory(
        market_id=1,
        market_name="Match Result",
        market_group=market_group,
        market_period="Full-time",
        choice_group="Full time",
        bookies={bookie_name: bookie},
    )

    markets = {
        market_group: {
            "Full-time": {
                "Match Result": {
                    "__default__": market_line
                }
            }
        }
    }

    return OddsTrajectoryContext(
        available=True,
        event_id=999,
        target_minutes_expected=[120, 30, 5, 0],
        target_minutes_present=[0],
        missing_target_minutes=[120, 30, 5],
        markets=markets,
    )


@patch("modules.pillars.pillar_5.exact_price_memory_engine.exact_price_memory_engine.get_exact_price_memory_sample")
def test_pillar_5_2way_vegas_example(mock_get_sample) -> None:
    # Setup mock matches
    mock_matches = [
        {
            "event_id": 1000 + i,
            "sport": "Football",
            "home_team": f"HomeTeam {i}",
            "away_team": f"AwayTeam {i}",
            "home_score": 1 if i < 5 else 2,
            "away_score": 2 if i < 5 else 1,
            "start_time": "2026-06-01",
            "one_final": 1.71,
            "x_final": None,
            "two_final": 2.15,
            "var_shape": False,
            "winner_side": "2" if i < 5 else "1",
        }
        for i in range(7)
    ]
    mock_get_sample.return_value = ExactPriceMemorySample(
        sample_size=7,
        wins_home=2,
        wins_draw=0,
        wins_away=5,
        rows=[
            {"winner_side": "1", "wins_count": 2},
            {"winner_side": "2", "wins_count": 5},
        ],
        historical_matches=mock_matches,
    )

    event_context = _build_event_context()
    trajectory_context = _build_odds_trajectory_context(
        market_group="Home/Away",
        home_odds=1.71,
        away_odds=2.15,
    )

    result = calculate_pillar_5(event_context, trajectory_context, debug_mode=True)

    assert result["P5_VALID"] is True
    assert result["P5_STATUS"] == "ACTIVE"
    assert result["P5_DIRECTION"] == "AWAY"
    assert result["P5"] == pytest.approx(0.30)
    assert result["P5_STRENGTH"] == "MODERATE"
    assert result["raw"]["exact_price_memory_engine"]["historical_matches"] == mock_matches

    # Verify mock call parameters
    mock_get_sample.assert_called_once_with(
        event_id=999,
        sport="Football",
        current_home_odds=Decimal("1.71"),
        current_away_odds=Decimal("2.15"),
        current_draw_odds=None,
        debug_mode=True,
    )


@patch("modules.pillars.pillar_5.exact_price_memory_engine.exact_price_memory_engine.get_exact_price_memory_sample")
def test_pillar_5_1x2_example(mock_get_sample) -> None:
    mock_matches = [
        {
            "event_id": 1000 + i,
            "sport": "Football",
            "home_team": f"HomeTeam {i}",
            "away_team": f"AwayTeam {i}",
            "home_score": 2 if i < 4 else (1 if i < 7 else 1),
            "away_score": 1 if i < 4 else (1 if i < 7 else 2),
            "start_time": "2026-06-01",
            "one_final": 2.10,
            "x_final": 3.25,
            "two_final": 3.40,
            "var_shape": True,
            "winner_side": "1" if i < 4 else ("X" if i < 7 else "2"),
        }
        for i in range(10)
    ]
    mock_get_sample.return_value = ExactPriceMemorySample(
        sample_size=10,
        wins_home=4,
        wins_draw=3,
        wins_away=3,
        rows=[
            {"winner_side": "1", "wins_count": 4},
            {"winner_side": "X", "wins_count": 3},
            {"winner_side": "2", "wins_count": 3},
        ],
        historical_matches=mock_matches,
    )

    event_context = _build_event_context()
    trajectory_context = _build_odds_trajectory_context(
        market_group="1X2",
        home_odds=2.10,
        draw_odds=3.25,
        away_odds=3.40,
    )

    result = calculate_pillar_5(event_context, trajectory_context, debug_mode=True)

    assert result["P5_VALID"] is True
    assert result["P5_STATUS"] == "ACTIVE"
    assert result["P5_DIRECTION"] == "HOME"
    assert result["P5"] == pytest.approx(0.00)
    assert result["P5_STRENGTH"] == "NONE"
    assert result["raw"]["exact_price_memory_engine"]["historical_matches"] == mock_matches

    mock_get_sample.assert_called_once_with(
        event_id=999,
        sport="Football",
        current_home_odds=Decimal("2.10"),
        current_away_odds=Decimal("3.40"),
        current_draw_odds=Decimal("3.25"),
        debug_mode=True,
    )


@patch("modules.pillars.pillar_5.exact_price_memory_engine.exact_price_memory_engine.get_exact_price_memory_sample")
def test_pillar_5_insufficient_sample(mock_get_sample) -> None:
    mock_get_sample.return_value = ExactPriceMemorySample(
        sample_size=2,
        wins_home=1,
        wins_draw=0,
        wins_away=1,
        rows=[
            {"winner_side": "1", "wins_count": 1},
            {"winner_side": "2", "wins_count": 1},
        ],
        historical_matches=[
            {
                "event_id": 1001,
                "sport": "Football",
                "home_team": "Team A",
                "away_team": "Team B",
                "home_score": 2,
                "away_score": 1,
                "start_time": "2026-06-01",
                "one_final": 1.80,
                "x_final": None,
                "two_final": 2.00,
                "var_shape": False,
                "winner_side": "1",
            },
            {
                "event_id": 1002,
                "sport": "Football",
                "home_team": "Team C",
                "away_team": "Team D",
                "home_score": 1,
                "away_score": 2,
                "start_time": "2026-06-02",
                "one_final": 1.80,
                "x_final": None,
                "two_final": 2.00,
                "var_shape": False,
                "winner_side": "2",
            }
        ]
    )

    event_context = _build_event_context()
    trajectory_context = _build_odds_trajectory_context(
        market_group="Home/Away",
        home_odds=1.80,
        away_odds=2.00,
    )

    result = calculate_pillar_5(event_context, trajectory_context, debug_mode=True)

    assert result["P5_VALID"] is False
    assert result["P5_STATUS"] == "INSUFFICIENT_DATA"
    assert result["P5_DIRECTION"] == "NONE"
    assert result["P5"] == 0.0
    assert result["P5_STRENGTH"] == "NONE"
    # Under insufficient result, it returns empty list of matches as per blueprint or _build_insufficient_result
    assert result["raw"]["exact_price_memory_engine"]["historical_matches"] == []


@patch("modules.pillars.pillar_5.exact_price_memory_engine.exact_price_memory_engine.get_exact_price_memory_sample")
def test_pillar_5_tie_handling(mock_get_sample) -> None:
    mock_matches = [
        {
            "event_id": 1000 + i,
            "sport": "Football",
            "home_team": f"HomeTeam {i}",
            "away_team": f"AwayTeam {i}",
            "home_score": 2 if i < 4 else 1,
            "away_score": 1 if i < 4 else 2,
            "start_time": "2026-06-01",
            "one_final": 1.90,
            "x_final": None,
            "two_final": 1.90,
            "var_shape": False,
            "winner_side": "1" if i < 4 else "2",
        }
        for i in range(8)
    ]
    mock_get_sample.return_value = ExactPriceMemorySample(
        sample_size=8,
        wins_home=4,
        wins_draw=0,
        wins_away=4,
        rows=[
            {"winner_side": "1", "wins_count": 4},
            {"winner_side": "2", "wins_count": 4},
        ],
        historical_matches=mock_matches,
    )

    event_context = _build_event_context()
    trajectory_context = _build_odds_trajectory_context(
        market_group="Home/Away",
        home_odds=1.90,
        away_odds=1.90,
    )

    result = calculate_pillar_5(event_context, trajectory_context, debug_mode=True)

    assert result["P5_VALID"] is True
    assert result["P5_STATUS"] == "ACTIVE"
    assert result["P5_DIRECTION"] == "NONE"
    assert result["P5"] == 0.0
    assert result["P5_STRENGTH"] == "NONE"
    assert result["raw"]["exact_price_memory_engine"]["historical_matches"] == mock_matches
