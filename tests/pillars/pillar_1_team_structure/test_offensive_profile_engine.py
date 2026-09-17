from __future__ import annotations

from types import SimpleNamespace

import pytest

from modules.pillars.pillar_1_team_structure.module_2.offensive_profile_engine import (
    calculate_performance_profile,
)


def _build_event_context(home_name: str = "Celta Vigo", away_name: str = "Levante UD"):
    return SimpleNamespace(
        participants_label=f"{home_name} vs {away_name}",
        home=SimpleNamespace(name=home_name),
        away=SimpleNamespace(name=away_name),
    )


def _build_streak_analysis(home_results, away_results):
    return SimpleNamespace(
        event_id=14083613,
        participants="Celta Vigo vs Levante UD",
        home_team_name="Celta Vigo",
        away_team_name="Levante UD",
        home_team_results=home_results,
        away_team_results=away_results,
    )


def test_performance_profile_blueprint_golden_case():
    home_series = [
        1, 3, 1, 0, 0,
        3, 3, 1, 1, 2,
        2, 2, 1, 0, 1,
        3, 1, 4, 0, 2,
        2, 0, 1, 2, 2,
        3, 1, 1, 1, 1,
        1, 1, 1, 1, 0,
    ]
    away_series = [
        3, 1, 0, 2, 1,
        0, 4, 1, 1, 2,
        0, 0, 0, 2, 0,
        3, 0, 1, 3, 1,
        0, 0, 1, 1, 1,
        1, 0, 2, 1, 1,
        4, 2, 0, 2, 1,
    ]
    streak_analysis = _build_streak_analysis(
        [{"team_score": value} for value in home_series],
        [{"team_score": value} for value in away_series],
    )

    result = calculate_performance_profile(streak_analysis, _build_event_context())

    assert result.event_id == 14083613
    assert result.participants == "Celta Vigo vs Levante UD"
    assert result.value == pytest.approx(0.019711178300125948, abs=1e-12)
    assert result.bias == "HOME"
    assert result.strength == "IGNORE"
    assert result.raw["m2_bias_label"] == "SLIGHT_HOME"
    assert result.raw["m2_status"] == "ACTIVE"
    assert result.raw["m2_status_reason"] == "active"
    assert result.raw["home_gp"] == 35
    assert result.raw["away_gp"] == 35
    assert sum(result.raw["home_game_gf"]) == 49
    assert sum(result.raw["away_game_gf"]) == 42
    assert result.raw["std_gf_home"] == pytest.approx(1.019803902719, abs=1e-12)
    assert result.raw["std_gf_away"] == pytest.approx(1.141427677454, abs=1e-12)
    assert result.raw["scoring_consistency_edge"] == pytest.approx(0.121623774735, abs=1e-12)
    assert result.raw["offensive_ceiling_home"] == pytest.approx(3.2, abs=1e-12)
    assert result.raw["offensive_ceiling_away"] == pytest.approx(3.4, abs=1e-12)
    assert result.raw["offensive_ceiling_edge"] == pytest.approx(-0.2, abs=1e-12)
    assert result.raw["blanks_home"] == 6
    assert result.raw["blanks_away"] == 11
    assert result.raw["blank_rate_edge"] == pytest.approx(0.142857142857, abs=1e-12)
    assert result.raw["explosion_games_home"] == 13
    assert result.raw["explosion_games_away"] == 11
    assert result.raw["explosion_frequency_edge"] == pytest.approx(0.057142857143, abs=1e-12)
    assert result.raw["m2_edge_raw"] == pytest.approx(0.019711178300125948, abs=1e-12)
    assert result.raw["m2_edge"] == pytest.approx(0.019711178300125948, abs=1e-12)
    assert result.raw["m2_abs_edge"] == pytest.approx(0.019711178300125948, abs=1e-12)
    assert len(result.components) == 4
    assert [component.name for component in result.components] == [
        "SCORING_CONSISTENCY_EDGE",
        "OFFENSIVE_CEILING_EDGE",
        "BLANK_RATE_EDGE",
        "EXPLOSION_FREQUENCY_EDGE",
    ]


def test_performance_profile_degrades_when_one_series_is_short():
    streak_analysis = _build_streak_analysis(
        [{"team_score": 2}, {"team_score": 1}, {"team_score": 0}, {"team_score": 3}],
        [{"team_score": 1}, {"team_score": 2}, {"team_score": 1}, {"team_score": 0}, {"team_score": 3}],
    )

    result = calculate_performance_profile(streak_analysis, _build_event_context())

    assert result.raw["m2_status"] == "DEGRADED"
    assert result.raw["m2_status_reason"] == "low_sample_size"
    assert result.raw["home_gp"] == 4
    assert result.raw["away_gp"] == 5
    assert result.components


def test_performance_profile_returns_insufficient_data_when_a_series_is_missing():
    streak_analysis = _build_streak_analysis(
        [],
        [{"team_score": 1}, {"team_score": 2}, {"team_score": 0}, {"team_score": 3}, {"team_score": 1}],
    )

    result = calculate_performance_profile(streak_analysis, _build_event_context())

    assert result.value == 0.0
    assert result.bias == "NEUTRAL"
    assert result.strength == "IGNORE"
    assert result.components == []
    assert result.raw["m2_status"] == "INSUFFICIENT_DATA"
    assert result.raw["m2_status_reason"] == "missing_game_gf_series"


def test_performance_profile_accepts_numeric_strings_but_rejects_bools():
    streak_analysis = _build_streak_analysis(
        [
            {"team_score": "2"},
            {"score_for": 0},
            {"goals_for": True},
            {"gf": " 3.5 "},
            {"team_score": False},
        ],
        [
            {"team_score": "1"},
            {"score_for": "0"},
            {"goals_for": "2"},
            {"gf": False},
            {"team_score": "4"},
        ],
    )

    result = calculate_performance_profile(streak_analysis, _build_event_context())

    assert result.raw["home_game_gf"] == [2.0, 0.0, 3.5]
    assert result.raw["away_game_gf"] == [1.0, 0.0, 2.0, 4.0]
    assert result.raw["home_gp"] == 3
    assert result.raw["away_gp"] == 4
    assert 0.0 in result.raw["home_game_gf"]
    assert 0.0 in result.raw["away_game_gf"]
    assert result.raw["m2_status"] == "DEGRADED"
