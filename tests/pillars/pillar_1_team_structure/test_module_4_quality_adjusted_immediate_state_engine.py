from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.pillar_1_team_structure.module_4.quality_adjusted_immediate_state_engine import (
    calculate_quality_adjusted_immediate_state_engine,
)


def _build_event_context(
    *,
    number_of_teams: int = 20,
    home_name: str = "Celta Vigo",
    away_name: str = "Levante UD",
) -> EventContext:
    home = ParticipantContext(
        participant_id=1,
        source="test",
        source_participant_id=101,
        name=home_name,
        slug="celta-vigo",
        short_name="CEL",
        source_status="normalized",
    )
    away = ParticipantContext(
        participant_id=2,
        source="test",
        source_participant_id=202,
        name=away_name,
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
        participants_label=f"{home_name} vs {away_name}",
        context_status="normalized",
    )


def _match(
    *,
    event_id: int,
    start_timestamp: int,
    opponent_name: str,
    opponent_ranking=None,
    team_result=None,
    team_score=None,
    opponent_score=None,
):
    match = {
        "event_id": event_id,
        "startTimestamp": start_timestamp,
        "opponent_name": opponent_name,
        "opponent_ranking": opponent_ranking,
        "team_result": team_result,
        "team_score": team_score,
        "opponent_score": opponent_score,
    }
    return match


def _build_streak_analysis(
    *,
    home_team_results,
    away_team_results,
    current_standings=None,
    home_team_name: str = "Celta Vigo",
    away_team_name: str = "Levante UD",
    event_id: int = 999,
):
    return SimpleNamespace(
        event_id=event_id,
        participants=f"{home_team_name} vs {away_team_name}",
        home_team_name=home_team_name,
        away_team_name=away_team_name,
        home_team_results=home_team_results,
        away_team_results=away_team_results,
        current_standings=current_standings,
    )


def test_m4_celta_levante_blueprint_exact_values():
    streak_analysis = _build_streak_analysis(
        current_standings={str(index): {} for index in range(20)},
        home_team_results=[
            _match(
                event_id=101,
                start_timestamp=500,
                opponent_name="Atletico Madrid",
                opponent_ranking=4,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
            _match(
                event_id=102,
                start_timestamp=400,
                opponent_name="Elche",
                opponent_ranking=14,
                team_result="W",
                team_score=3,
                opponent_score=1,
            ),
            _match(
                event_id=103,
                start_timestamp=300,
                opponent_name="Villarreal",
                opponent_ranking=3,
                team_result="L",
                team_score=1,
                opponent_score=2,
            ),
            _match(
                event_id=104,
                start_timestamp=200,
                opponent_name="FC Barcelona",
                opponent_ranking=1,
                team_result="L",
                team_score=0,
                opponent_score=1,
            ),
            _match(
                event_id=105,
                start_timestamp=100,
                opponent_name="Real Oviedo",
                opponent_ranking=20,
                team_result="L",
                team_score=0,
                opponent_score=3,
            ),
        ],
        away_team_results=[
            _match(
                event_id=201,
                start_timestamp=600,
                opponent_name="Osasuna",
                opponent_ranking=10,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
            _match(
                event_id=202,
                start_timestamp=500,
                opponent_name="Villarreal",
                opponent_ranking=3,
                team_result="L",
                team_score=0,
                opponent_score=4,
            ),
            _match(
                event_id=203,
                start_timestamp=400,
                opponent_name="Espanyol",
                opponent_ranking=14,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=204,
                start_timestamp=300,
                opponent_name="Sevilla",
                opponent_ranking=17,
                team_result="W",
                team_score=2,
                opponent_score=0,
            ),
            _match(
                event_id=205,
                start_timestamp=200,
                opponent_name="Getafe",
                opponent_ranking=8,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
        ],
    )

    result = calculate_quality_adjusted_immediate_state_engine(streak_analysis, _build_event_context())

    assert result.module_id == "M4"
    assert result.module_name == "Quality-Adjusted Immediate State Engine"
    assert result.raw["engine_profile"] == "quality_adjusted_immediate_state_v1"
    assert result.raw["n_liga"] == 20
    assert result.raw["n_liga_source"] == "streak_analysis.current_standings.len"
    assert result.raw["m4_status"] == "ACTIVE"
    assert result.raw["m4_status_reason"] == "active"
    assert result.bias == "AWAY"
    assert result.strength == "HIGH"
    assert result.value == pytest.approx(-0.3333333333333333, abs=1e-3)
    assert result.raw["home_adjusted_result"] == pytest.approx(-0.736842105263, abs=1e-3)
    assert result.raw["away_adjusted_result"] == pytest.approx(0.421052631579, abs=1e-3)
    assert result.raw["home_adjusted_gd"] == pytest.approx(-0.421052631579, abs=1e-3)
    assert result.raw["away_adjusted_gd"] == pytest.approx(-2.105263157895, abs=1e-3)
    assert result.raw["result_score"] == pytest.approx(-1.0, abs=1e-3)
    assert result.raw["gd_score"] == pytest.approx(0.666666666667, abs=1e-3)
    assert result.raw["m4_edge"] == pytest.approx(-0.333333333333, abs=1e-3)
    assert result.raw["m4_abs_edge"] == pytest.approx(0.333333333333, abs=1e-3)
    assert result.raw["m4_bias"] == "AWAY"
    assert result.raw["m4_strength"] == "HIGH"
    assert len(result.components) == 2
    assert [component.name for component in result.components] == ["RESULT_SCORE", "GD_SCORE"]
    assert result.raw["home_valid_matches"] == 5
    assert result.raw["away_valid_matches"] == 5
    assert result.raw["home_window"][0]["opponent_name"] == "Atletico Madrid"
    assert result.raw["away_window"][0]["opponent_name"] == "Osasuna"


def test_m4_insufficient_data_when_less_than_3_valid_matches():
    streak_analysis = _build_streak_analysis(
        home_team_results=[
            _match(
                event_id=301,
                start_timestamp=300,
                opponent_name="Opponent A",
                opponent_ranking=4,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
            _match(
                event_id=302,
                start_timestamp=200,
                opponent_name="Opponent B",
                opponent_ranking=10,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
        ],
        away_team_results=[
            _match(
                event_id=401,
                start_timestamp=500,
                opponent_name="Opponent C",
                opponent_ranking=8,
                team_result="W",
                team_score=2,
                opponent_score=1,
            ),
            _match(
                event_id=402,
                start_timestamp=400,
                opponent_name="Opponent D",
                opponent_ranking=6,
                team_result="L",
                team_score=0,
                opponent_score=1,
            ),
            _match(
                event_id=403,
                start_timestamp=300,
                opponent_name="Opponent E",
                opponent_ranking=3,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
            _match(
                event_id=404,
                start_timestamp=200,
                opponent_name="Opponent F",
                opponent_ranking=11,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=405,
                start_timestamp=100,
                opponent_name="Opponent G",
                opponent_ranking=14,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
        ],
    )

    result = calculate_quality_adjusted_immediate_state_engine(streak_analysis, _build_event_context())

    assert result.value == 0.0
    assert result.bias == "NEUTRAL"
    assert result.strength == "IGNORE"
    assert result.components == []
    assert result.raw["m4_status"] == "INSUFFICIENT_DATA"
    assert result.raw["m4_status_reason"] == "not_enough_valid_matches"
    assert result.raw["home_valid_matches"] == 2
    assert result.raw["away_valid_matches"] == 5


def test_m4_degraded_when_between_3_and_4_valid_matches():
    streak_analysis = _build_streak_analysis(
        home_team_results=[
            _match(
                event_id=501,
                start_timestamp=400,
                opponent_name="Opponent A",
                opponent_ranking=4,
                team_result="W",
                team_score=2,
                opponent_score=0,
            ),
            _match(
                event_id=502,
                start_timestamp=300,
                opponent_name="Opponent B",
                opponent_ranking=10,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
            _match(
                event_id=503,
                start_timestamp=200,
                opponent_name="Opponent C",
                opponent_ranking=12,
                team_result="D",
                team_score=1,
                opponent_score=1,
            ),
            _match(
                event_id=504,
                start_timestamp=100,
                opponent_name="Opponent D",
                opponent_ranking=18,
                team_result="L",
                team_score=0,
                opponent_score=1,
            ),
        ],
        away_team_results=[
            _match(
                event_id=601,
                start_timestamp=500,
                opponent_name="Opponent E",
                opponent_ranking=5,
                team_result="L",
                team_score=0,
                opponent_score=2,
            ),
            _match(
                event_id=602,
                start_timestamp=400,
                opponent_name="Opponent F",
                opponent_ranking=7,
                team_result="D",
                team_score=1,
                opponent_score=1,
            ),
            _match(
                event_id=603,
                start_timestamp=300,
                opponent_name="Opponent G",
                opponent_ranking=9,
                team_result="W",
                team_score=2,
                opponent_score=0,
            ),
            _match(
                event_id=604,
                start_timestamp=200,
                opponent_name="Opponent H",
                opponent_ranking=11,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
            _match(
                event_id=605,
                start_timestamp=100,
                opponent_name="Opponent I",
                opponent_ranking=13,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
        ],
    )

    result = calculate_quality_adjusted_immediate_state_engine(streak_analysis, _build_event_context())

    assert result.raw["m4_status"] == "DEGRADED"
    assert result.raw["m4_status_reason"] == "partial_window"
    assert result.raw["home_valid_matches"] == 4
    assert result.raw["away_valid_matches"] == 5
    assert result.components
    assert result.value == pytest.approx(result.raw["m4_edge"], abs=1e-12)


def test_m4_ignores_invalid_matches_and_records_reasons():
    streak_analysis = _build_streak_analysis(
        home_team_results=[
            _match(
                event_id=701,
                start_timestamp=500,
                opponent_name="Opponent A",
                opponent_ranking=4,
                team_result="W",
                team_score=2,
                opponent_score=0,
            ),
            _match(
                event_id=702,
                start_timestamp=400,
                opponent_name="Opponent B",
                opponent_ranking=None,
                team_result="W",
                team_score=1,
                opponent_score=0,
            ),
            _match(
                event_id=703,
                start_timestamp=300,
                opponent_name="Opponent C",
                opponent_ranking=6,
                team_result="DRAW",
                team_score=1,
                opponent_score=1,
            ),
            _match(
                event_id=704,
                start_timestamp=200,
                opponent_name="Opponent D",
                opponent_ranking=10,
                team_result="X",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=705,
                start_timestamp=100,
                opponent_name="Opponent E",
                opponent_ranking=12,
                team_result="L",
                team_score=None,
                opponent_score=1,
            ),
        ],
        away_team_results=[
            _match(
                event_id=801,
                start_timestamp=500,
                opponent_name="Opponent F",
                opponent_ranking=3,
                team_result="W",
                team_score=2,
                opponent_score=0,
            ),
            _match(
                event_id=802,
                start_timestamp=400,
                opponent_name="Opponent G",
                opponent_ranking=5,
                team_result="L",
                team_score=0,
                opponent_score=1,
            ),
            _match(
                event_id=803,
                start_timestamp=300,
                opponent_name="Opponent H",
                opponent_ranking=7,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=804,
                start_timestamp=200,
                opponent_name="Opponent I",
                opponent_ranking=9,
                team_result="WIN",
                team_score=1,
                opponent_score=0,
            ),
            _match(
                event_id=805,
                start_timestamp=100,
                opponent_name="Opponent J",
                opponent_ranking=11,
                team_result="L",
                team_score="invalid",
                opponent_score=1,
            ),
        ],
    )

    result = calculate_quality_adjusted_immediate_state_engine(streak_analysis, _build_event_context())

    home_reasons = {entry["reason"] for entry in result.raw["invalid_home_matches"]}
    away_reasons = {entry["reason"] for entry in result.raw["invalid_away_matches"]}

    assert result.raw["home_valid_matches"] == 3
    assert result.raw["away_valid_matches"] == 4
    assert result.raw["m4_status"] == "DEGRADED"
    assert home_reasons == {
        "missing_opponent_ranking",
        "missing_team_score",
    }
    assert away_reasons == {"missing_team_score"}
    assert result.raw["home_window"]
    assert result.raw["away_window"]
    assert len(result.components) == 2


def test_m4_zero_denominator_relative_score():
    streak_analysis = _build_streak_analysis(
        home_team_results=[
            _match(
                event_id=901,
                start_timestamp=500,
                opponent_name="Opponent A",
                opponent_ranking=1,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=902,
                start_timestamp=400,
                opponent_name="Opponent B",
                opponent_ranking=2,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=903,
                start_timestamp=300,
                opponent_name="Opponent C",
                opponent_ranking=3,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=904,
                start_timestamp=200,
                opponent_name="Opponent D",
                opponent_ranking=4,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=905,
                start_timestamp=100,
                opponent_name="Opponent E",
                opponent_ranking=5,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
        ],
        away_team_results=[
            _match(
                event_id=1001,
                start_timestamp=500,
                opponent_name="Opponent F",
                opponent_ranking=1,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=1002,
                start_timestamp=400,
                opponent_name="Opponent G",
                opponent_ranking=2,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=1003,
                start_timestamp=300,
                opponent_name="Opponent H",
                opponent_ranking=3,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=1004,
                start_timestamp=200,
                opponent_name="Opponent I",
                opponent_ranking=4,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
            _match(
                event_id=1005,
                start_timestamp=100,
                opponent_name="Opponent J",
                opponent_ranking=5,
                team_result="D",
                team_score=0,
                opponent_score=0,
            ),
        ],
    )

    result = calculate_quality_adjusted_immediate_state_engine(streak_analysis, _build_event_context())

    assert result.raw["m4_status"] == "ACTIVE"
    assert result.raw["result_score"] == 0.0
    assert result.raw["gd_score"] == 0.0
    assert result.raw["m4_edge"] == 0.0
    assert result.bias == "NEUTRAL"
    assert result.strength == "IGNORE"
