from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.pillar_1_team_structure.module_8.opponent_expectation_engine import (
    calculate_opponent_expectation_engine,
)


def _build_event_context(number_of_teams):
    home = ParticipantContext(
        participant_id=1,
        source="test",
        source_participant_id=101,
        name="Celta",
        slug="celta",
        short_name="CEL",
        source_status="normalized",
    )
    away = ParticipantContext(
        participant_id=2,
        source="test",
        source_participant_id=202,
        name="Levante",
        slug="levante",
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
        number_of_teams=number_of_teams,
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
        participants_label="Celta vs Levante",
        context_status="normalized",
    )


def test_opponent_expectation_engine_with_invalid_matches(caplog):
    # Setup some home results: 34 valid, 1 invalid (missing opponent_rank)
    home_results = []
    # 34 valid matches
    for i in range(34):
        home_results.append({
            "opponent_name": f"Opponent {i}",
            "opponent_ranking": 10,
            "team_result_code": 1,
            "net_score": 1.0,
            "startTimestamp": 1000 + i,
        })
    # 1 invalid match (missing opponent_ranking/standing)
    home_results.append({
        "opponent_name": "Invalid Opponent Home",
        "team_result_code": 1,
        "net_score": 1.0,
        "startTimestamp": 2000,
    })

    # Setup some away results: 34 valid, 1 invalid (missing team_result_code and net_score)
    away_results = []
    for i in range(34):
        away_results.append({
            "opponent_name": f"Opponent {i}",
            "opponent_ranking": 5,
            "team_result_code": 0,
            "net_score": 0.0,
            "startTimestamp": 3000 + i,
        })
    # 1 invalid match
    away_results.append({
        "opponent_name": "Invalid Opponent Away",
        "opponent_ranking": 5,
        "startTimestamp": 4000,
    })

    streak_analysis = SimpleNamespace(
        event_id=999,
        participants="Celta vs Levante",
        home_team_name="Celta",
        away_team_name="Levante",
        home_team_results=home_results,
        away_team_results=away_results,
    )

    with caplog.at_level(logging.INFO):
        result = calculate_opponent_expectation_engine(
            streak_analysis, _build_event_context(20), debug_mode=True
        )

    # 1. Assertions on status and counts
    assert result.raw["m8_status"] == "ACTIVE"
    assert result.raw["home_context"]["valid_match_count"] == 34
    assert result.raw["home_context"]["invalid_match_count"] == 1
    assert result.raw["away_context"]["valid_match_count"] == 34
    assert result.raw["away_context"]["invalid_match_count"] == 1

    # 2. Check content of invalid_games
    home_invalid = result.raw["home_context"]["invalid_games"]
    assert len(home_invalid) == 1
    assert home_invalid[0]["index"] == 34
    assert home_invalid[0]["opponent"] == "Invalid Opponent Home"
    assert home_invalid[0]["startTimestamp"] == 2000
    assert home_invalid[0]["missing"] == ["opponent_rank"]

    away_invalid = result.raw["away_context"]["invalid_games"]
    assert len(away_invalid) == 1
    assert away_invalid[0]["index"] == 34
    assert away_invalid[0]["opponent"] == "Invalid Opponent Away"
    assert away_invalid[0]["startTimestamp"] == 4000
    assert sorted(away_invalid[0]["missing"]) == ["game_gd", "team_result_code"]

    # 3. Check optional debug logs
    log_text = caplog.text
    assert "[HOME_INVALID_GAMES]" in log_text
    assert "[AWAY_INVALID_GAMES]" in log_text
    assert "Invalid Opponent Home" in log_text
    assert "Invalid Opponent Away" in log_text
