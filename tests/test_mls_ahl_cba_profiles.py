from dataclasses import replace
from types import SimpleNamespace

import pytest

from modules.alerts.matchup_streak_analysis.standings_engine import (
    _build_team_standing_payload, _create_team_stats, _finalize_team_stats,
)
from modules.competition.competition_profiles import get_competition_profile
from modules.pillars.pillar_1_team_structure.competition_context_resolver import resolve_p1_competition_context
from tests.pillars.pillar_1_team_structure.test_module_5_late_season import _event, _calculate_m5


def _resolved_event(cid):
    event = _event()
    event.competition = replace(event.competition, competition_id=cid)
    event.competition = resolve_p1_competition_context(event.to_identity(), event.competition)
    return event


def test_current_profiles_have_sizes_duration_scoring_and_explicit_objectives():
    for cid, teams, games, maximum in ((76, 30, 34, 3), (554, 32, 72, 2), (174, 20, 46, 1)):
        profile = get_competition_profile(cid)
        assert (profile.number_of_teams, profile.total_regular_season_games) == (teams, games)
        assert profile.scoring.maximum_per_game == maximum
        assert profile.objectives is not None
    for cid in (76, 554):
        assert all(rule.objective_type != "DIVISION_RETENTION" for rule in get_competition_profile(cid).objectives)
    retention = next(rule for rule in get_competition_profile(174).objectives if rule.objective_type == "DIVISION_RETENTION")
    assert (retention.rank_start, retention.rank_end, retention.reference_rank) == (19, 20, 18)
    assert retention.qualification_path == "RELEGATION_PLAYOFF"
    assert not get_competition_profile(174).standings.verified


@pytest.mark.parametrize("division,size,places", [
    ("Atlantic", 7, 5), ("North", 8, 6), ("Central", 7, 5), ("Pacific", 10, 7),
])
def test_ahl_current_division_quotas(division, size, places):
    rule = next(rule for rule in get_competition_profile(554).objectives if rule.ranking_group == division)
    assert (rule.group_size, rule.rank_start, rule.rank_end, rule.reference_rank) == (size, 1, places, places)


def test_ahl_payload_exposes_division_and_scoring_preserves_overtime_loss_point():
    profile = get_competition_profile(554)
    stats = _create_team_stats("North")
    stats.update(wins=2, ot_so_wins=1, losses=1, ot_losses=1, games_played=3)
    _finalize_team_stats(stats, profile)
    payload = _build_team_standing_payload(stats, {"position": 1}, profile, "North")
    assert payload["division"] == "North"
    assert payload["points"] == 5


def test_ahl_sixth_qualifies_in_north_but_not_atlantic():
    table = {}
    for division, size in (("Atlantic", 7), ("North", 8), ("Central", 7), ("Pacific", 10)):
        for rank in range(1, size + 1):
            name = ("Valencia" if division == "North" and rank == 6 else
                    "Real Sociedad" if division == "Atlantic" and rank == 6 else f"{division}{rank}")
            table[name] = dict(rank=rank, points=100 - rank * 3, games_played=65, division=division)
    result = _calculate_m5(SimpleNamespace(current_standings=table), _resolved_event(554))
    assert result.raw["m5_status"] == "ACTIVE"
    assert result.raw["home"]["objective_rule"]["ranking_group"] == "North"
    assert result.raw["home"]["points_cutoff"] == 79
    assert result.raw["away"]["objective_type"] == "NO_APPLICABLE_OBJECTIVE"
    assert result.bias == "HOME"
    for row in table.values():
        row.pop("division")
    result = _calculate_m5(SimpleNamespace(current_standings=table), _resolved_event(554))
    assert result.raw["m5_status_reason"] == "MISSING_RANKING_GROUP"


def test_mls_direct_and_wild_card_match_use_the_correct_conference():
    table = {}
    for conference in ("Eastern", "Western"):
        for rank in range(1, 16):
            name = ("Valencia" if conference == "Eastern" and rank == 7 else
                    "Real Sociedad" if conference == "Western" and rank == 8 else f"{conference}{rank}")
            table[name] = dict(rank=rank, points=100-rank*2, games_played=30, conference=conference)
    result = _calculate_m5(SimpleNamespace(current_standings=table), _resolved_event(76))
    assert result.raw["m5_status"] == "ACTIVE"
    assert result.raw["home"]["objective_rule"]["target"] == "ROUND_ONE_BEST_OF_THREE"
    assert result.raw["away"]["objective_rule"]["target"] == "WILD_CARD_MATCH"


def test_cba_declared_retention_does_not_bypass_percentage_calculation_limit():
    table = {name: dict(rank=rank, points=10, games_played=40)
             for name, rank in (("Valencia", 19), ("Real Sociedad", 20))}
    result = _calculate_m5(SimpleNamespace(current_standings=table), _resolved_event(174))
    assert result.raw["m5_status_reason"] == "UNSUPPORTED_RANKING_METRIC"
