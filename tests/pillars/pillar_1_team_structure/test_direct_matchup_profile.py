from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.pillar_1_team_structure.module_3.direct_matchup_profile import (
    calculate_direct_matchup_profile,
)


def _build_event_context():
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


def _match(home_score, away_score, start_ts=1000):
    return {
        "home_score": home_score,
        "away_score": away_score,
        "startTimestamp": start_ts,
    }


def test_single_h2h_home_win_is_active_but_sample_discounted():
    streak_analysis = SimpleNamespace(
        event_id=999,
        participants="Celta Vigo vs Levante UD",
        home_team_name="Celta Vigo",
        away_team_name="Levante UD",
        h2h_matchup_matches=[_match(2, 1)],
        h2h_matchup_matches_analyzed=1,
    )

    result = calculate_direct_matchup_profile(streak_analysis, _build_event_context())

    assert result.raw["m3_status"] == "ACTIVE"
    assert result.value == pytest.approx(0.16, rel=1e-12, abs=1e-12)
    assert result.bias == "HOME"
    assert result.strength == "MEDIUM"
    assert result.raw["m3_raw_edge"] == pytest.approx(0.8, rel=1e-12, abs=1e-12)
    assert result.raw["m3_sample_factor"] == pytest.approx(0.2, rel=1e-12, abs=1e-12)
    assert result.raw["m3_sample_confidence"] == "VERY LOW"
    assert result.raw["m3_edge"] == pytest.approx(0.16, rel=1e-12, abs=1e-12)
    assert result.raw["m3_raw_strength"] == "EXTREME"
    assert len(result.components) == 2
    assert result.raw["parsed_h2h"][0]["home_goals"] == 2
    assert result.raw["parsed_h2h"][0]["away_goals"] == 1


def test_no_h2h_returns_inactive_neutral():
    streak_analysis = SimpleNamespace(
        event_id=999,
        participants="Celta Vigo vs Levante UD",
        home_team_name="Celta Vigo",
        away_team_name="Levante UD",
        h2h_matchup_matches=[],
        h2h_matchup_matches_analyzed=0,
    )

    result = calculate_direct_matchup_profile(streak_analysis, _build_event_context())

    assert result.value == 0.0
    assert result.bias == "NEUTRAL"
    assert result.strength == "IGNORE"
    assert result.raw["m3_status"] == "INACTIVE"
    assert result.raw["m3_status_reason"] == "NO_VALID_H2H_SAMPLE"
    assert result.raw["m3_edge"] == 0.0
    assert result.raw["m3_raw_edge"] == 0.0
    assert result.raw["m3_sample_factor"] == 0.0
    assert result.raw["m3_sample_confidence"] == "NONE"


def test_draw_counts_as_half_result():
    streak_analysis = SimpleNamespace(
        event_id=999,
        participants="Celta Vigo vs Levante UD",
        home_team_name="Celta Vigo",
        away_team_name="Levante UD",
        h2h_matchup_matches=[_match(1, 1)],
        h2h_matchup_matches_analyzed=1,
    )

    result = calculate_direct_matchup_profile(streak_analysis, _build_event_context())

    assert result.raw["h2h_home_wins"] == 0
    assert result.raw["h2h_away_wins"] == 0
    assert result.raw["h2h_draws"] == 1
    assert result.raw["win_rate_home"] == pytest.approx(0.5, rel=1e-12, abs=1e-12)
    assert result.raw["win_rate_away"] == pytest.approx(0.5, rel=1e-12, abs=1e-12)
    assert result.raw["win_matchup_edge_raw"] == pytest.approx(0.0, rel=1e-12, abs=1e-12)
    assert result.raw["goal_matchup_edge_raw"] == pytest.approx(0.0, rel=1e-12, abs=1e-12)
    assert result.raw["m3_edge"] == pytest.approx(0.0, rel=1e-12, abs=1e-12)


def test_away_h2h_advantage_returns_negative_edge():
    streak_analysis = SimpleNamespace(
        event_id=999,
        participants="Celta Vigo vs Levante UD",
        home_team_name="Celta Vigo",
        away_team_name="Levante UD",
        h2h_matchup_matches=[_match(1, 2)],
        h2h_matchup_matches_analyzed=1,
    )

    result = calculate_direct_matchup_profile(streak_analysis, _build_event_context())

    assert result.raw["win_matchup_edge_raw"] == pytest.approx(-1.0, rel=1e-12, abs=1e-12)
    assert result.raw["goal_matchup_edge_raw"] == pytest.approx(-0.3333333333333333, rel=1e-12, abs=1e-12)
    assert result.raw["m3_raw_edge"] == pytest.approx(-0.8, rel=1e-12, abs=1e-12)
    assert result.raw["m3_sample_factor"] == pytest.approx(0.2, rel=1e-12, abs=1e-12)
    assert result.raw["m3_edge"] == pytest.approx(-0.16, rel=1e-12, abs=1e-12)
    assert result.bias == "AWAY"


def test_five_matches_sample_factor_caps_at_one():
    streak_analysis = SimpleNamespace(
        event_id=999,
        participants="Celta Vigo vs Levante UD",
        home_team_name="Celta Vigo",
        away_team_name="Levante UD",
        h2h_matchup_matches=[_match(2, 1, 1000 + i) for i in range(5)],
        h2h_matchup_matches_analyzed=5,
    )

    result = calculate_direct_matchup_profile(streak_analysis, _build_event_context())

    assert result.raw["m3_sample_factor"] == pytest.approx(1.0, rel=1e-12, abs=1e-12)
    assert result.raw["m3_sample_confidence"] == "VERY HIGH"


def test_more_than_five_matches_sample_factor_caps_at_one():
    streak_analysis = SimpleNamespace(
        event_id=999,
        participants="Celta Vigo vs Levante UD",
        home_team_name="Celta Vigo",
        away_team_name="Levante UD",
        h2h_matchup_matches=[_match(2, 1, 1000 + i) for i in range(6)],
        h2h_matchup_matches_analyzed=6,
    )

    result = calculate_direct_matchup_profile(streak_analysis, _build_event_context())

    assert result.raw["m3_sample_factor"] == pytest.approx(1.0, rel=1e-12, abs=1e-12)
    assert result.raw["m3_sample_confidence"] == "VERY HIGH"
