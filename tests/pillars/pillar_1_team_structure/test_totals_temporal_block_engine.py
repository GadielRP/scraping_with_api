from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from modules.pillars.pillar_1_team_structure.totals import (
    TOTALS_TEMPORAL_WINDOW_CONFIG,
    TOTALS_TEMPORAL_WINDOW_NAMES,
    _resolve_totals_temporal_window_config,
    _resolve_window_size_from_ratio,
    calculate_p1_totals,
)
from modules.pillars.pillar_1_team_structure.totals.totals import _is_active_signal


def _make_event_context(total_regular_season_games: int | None) -> SimpleNamespace:
    competition = SimpleNamespace(
        total_regular_season_games=total_regular_season_games,
        number_of_teams=20,
        standings_response=None,
    )
    return SimpleNamespace(
        home=SimpleNamespace(name="Home FC"),
        away=SimpleNamespace(name="Away FC"),
        competition=competition,
        participants_label="Home FC vs Away FC",
    )


def _make_streak_analysis(
    home_results: list[dict],
    away_results: list[dict],
    *,
    league_totals_context: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        event_id=101,
        home_team_name="Home FC",
        away_team_name="Away FC",
        participants="Home FC vs Away FC",
        home_team_results=home_results,
        away_team_results=away_results,
        league_totals_context=league_totals_context,
        current_standings=None,
        standings_response=None,
        standings_source=None,
        home_team_current_standing=None,
        away_team_current_standing=None,
        home_team_standing=None,
        away_team_standing=None,
    )


def _result_series(count: int, *, gf: float, ga: float) -> list[dict]:
    return [{"gf": gf, "ga": ga} for _ in range(count)]


def test_window_size_rounding_is_half_up_not_bankers_rounding():
    assert _resolve_window_size_from_ratio(38, 0.15) == 6
    assert _resolve_window_size_from_ratio(38, 0.35) == 13
    assert _resolve_window_size_from_ratio(38, 0.60) == 23
    assert _resolve_window_size_from_ratio(38, 1.0) == 38
    assert _resolve_window_size_from_ratio(5, 0.50) == 3


def test_temporal_config_prefers_event_context_season_length():
    config = _resolve_totals_temporal_window_config(
        _make_event_context(38),
    )

    assert config["resolution_status"] == "RESOLVED"
    assert config["n_season"] == 38
    assert config["n_season_source"] == "event_context.competition.total_regular_season_games"
    assert config["resolved_window_sizes"] == {
        "TOTALS_SHORT": 6,
        "TOTALS_RECENT": 13,
        "TOTALS_MID": 23,
        "TOTALS_FULL": 38,
    }
    assert config["window_config"]["TOTALS_FULL"]["target_window"] == 38
    assert config["window_config"]["TOTALS_SHORT"]["ratio"] == pytest.approx(0.15)
    assert config["window_config"]["TOTALS_RECENT"]["weight"] == pytest.approx(0.35)
    assert tuple(config["ratios"].keys()) == TOTALS_TEMPORAL_WINDOW_NAMES
    assert tuple(config["weights"].keys()) == TOTALS_TEMPORAL_WINDOW_NAMES
    assert tuple(name for name, _, _ in TOTALS_TEMPORAL_WINDOW_CONFIG) == TOTALS_TEMPORAL_WINDOW_NAMES


def test_temporal_config_does_not_resolve_without_total_regular_season_games():
    config = _resolve_totals_temporal_window_config(
        _make_event_context(None),
    )

    assert config["resolution_status"] == "INSUFFICIENT_DATA"
    assert config["abort_reason"] == "missing_total_regular_season_games"
    assert config["n_season"] is None
    assert config["n_season_source"] is None
    assert config["resolved_window_sizes"] == {}


def test_active_signal_helper_matches_epsilons_near_threshold():
    assert _is_active_signal(0.04999999999999984) is True
    assert _is_active_signal(0.049999999999) is False
    assert _is_active_signal(None) is False


def test_calculate_p1_totals_uses_resolved_season_length_for_confidence_total():
    home_results = _result_series(35, gf=2, ga=1)
    away_results = _result_series(35, gf=1, ga=2)
    output = calculate_p1_totals(
        _make_streak_analysis(home_results, away_results),
        _make_event_context(38),
        debug_mode=False,
    )

    assert output.TEMPORAL_CONFIG["n_season"] == 38
    assert output.confidence_total == pytest.approx(35 / 38, abs=1e-12)
    assert output.TEMPORAL_CONFIG["resolved_window_sizes"]["TOTALS_FULL"] == 38
    assert output.TEMPORAL_CONFIG["resolution_status"] == "RESOLVED"


def test_calculate_p1_totals_aborts_without_total_season_length(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO)

    output = calculate_p1_totals(
        _make_streak_analysis([], []),
        _make_event_context(None),
        debug_mode=True,
    )

    assert output.status == "INSUFFICIENT_DATA"
    assert output.status_reason == "missing_total_regular_season_games"
    assert output.P1_TOTALS_SCORE == 0.0
    assert output.P1_TOTALS_DIRECTION == "NEUTRAL_PROFILE"
    assert output.P1_TOTALS_STRENGTH == "NONE"
    assert output.active_layers == []
    assert output.ignored_layers == []
    assert output.confidence_total == 0.0
    assert output.TEMPORAL_CONFIG["resolution_status"] == "INSUFFICIENT_DATA"
    assert output.TEMPORAL_CONFIG["abort_reason"] == "missing_total_regular_season_games"
    assert any(
        "[P1_TOTALS][TEMPORAL_CONFIG][ABORT] reason=missing_total_regular_season_games" in record.message
        for record in caplog.records
    )
