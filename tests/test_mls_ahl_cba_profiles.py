from types import SimpleNamespace

from modules.standings.calculator import (
    _build_team_standing_payload, _create_team_stats, _finalize_team_stats,
)
from tests.competition_fixtures import profile as get_competition_profile, with_identity
from modules.pillars.pillar_1_team_structure.competition_context_resolver import resolve_p1_competition_context
from tests.pillars.pillar_1_team_structure.test_module_5_late_season import _event, _calculate_m5


def _resolved_event(cid):
    event = _event()
    event.competition = with_identity(event.competition, cid)
    event.competition = resolve_p1_competition_context(event.to_identity(), event.competition)
    return event


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
    assert result.raw["home"]["objective_rule"]["rank_selection"]["ranking_group"] == "North"
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
    assert result.raw["home"]["objective_rule"]["qualification_destination"]["destination_phase"] == "PLAYOFFS"
    assert result.raw["away"]["objective_rule"]["qualification_destination"]["destination_phase"] == "PLAY_IN"


def test_cba_classification_points_make_its_declared_retention_computable():
    # CBA ranks by classification points (2 per win, 1 per ordinary loss), a metric M5 supports.
    table = {name: dict(rank=rank, points=10, games_played=40)
             for name, rank in (("Valencia", 19), ("Real Sociedad", 20))}
    result = _calculate_m5(SimpleNamespace(current_standings=table), _resolved_event(174))
    assert result.raw["m5_status"] == "ACTIVE"
