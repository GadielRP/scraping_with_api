"""Observed policy gaps: league-specific ordering, H2H and unavailable inputs."""

import json
from dataclasses import asdict

import pytest

from modules.alerts.matchup_streak_analysis.constants import get_team_group
from modules.alerts.matchup_streak_analysis.standings_engine import _apply_game_result, _create_team_stats, _finalize_team_stats
from modules.alerts.matchup_streak_analysis.standings_rules import rank_standings
from modules.competition.competition_profiles import COMPETITION_PROFILES, get_competition_profile
from modules.competition.tracked_competitions import TRACKED_COMPETITION_IDS


def _stats(**values):
    stats = _create_team_stats(None)
    stats.update(points=30, wins=8, games_played=16, goal_diff=0, goals_for=20)
    stats.update(values)
    return stats


def _game(home, away, scored, conceded, subtype="REG"):
    return dict(home_team=home, away_team=away, home_score=scored, away_score=conceded,
                winner="1" if scored > conceded else "2" if conceded > scored else "X",
                result_subtype=subtype)


def _rank(cid, a=None, b=None, games=(), final=False):
    return rank_standings(
        {"A": a or _stats(), "B": b or _stats()}.items(), get_competition_profile(cid),
        list(games), season_complete=final,
    )


def _rank_games(cid, names, games):
    profile = get_competition_profile(cid)
    stats = {team: _create_team_stats(None) for game in games for team in (game["home_team"], game["away_team"])}
    for game in games:
        _apply_game_result(stats, **game)
    for row in stats.values():
        _finalize_team_stats(row, profile)
    ordered, positions = rank_standings([(name, stats[name]) for name in names], profile, games)
    assert set(positions) == {name for name, _ in ordered} == set(names)
    return [name for name, _ in ordered], positions


def test_every_tracked_competition_has_a_serializable_sporting_profile():
    assert TRACKED_COMPETITION_IDS <= COMPETITION_PROFILES.keys()
    for cid in TRACKED_COMPETITION_IDS:
        profile = get_competition_profile(cid)
        assert profile.standings.source_urls
        json.dumps(asdict(profile))
    assert get_competition_profile(76).grouping_method == "mls_conference"
    assert get_competition_profile(554).grouping_method == "ahl_division"
    assert get_competition_profile(554).total_regular_season_games == 72
    assert not get_competition_profile(174).standings.verified


@pytest.mark.parametrize("cid", [167, 88])
def test_original_complete_and_incomplete_football_h2h_behavior_is_preserved(cid):
    games = [_game("A", "B", 1, 0), _game("B", "A", 0, 1)]
    a, b = _stats(goal_diff=-4), _stats(goal_diff=10)
    ordered, _ = _rank(cid, a, b, games)
    assert [name for name, _ in ordered] == ["A", "B"]
    ordered, _ = _rank(cid, a, b, games[:1])
    assert [name for name, _ in ordered] == ["B", "A"]


def test_brazil_wins_precede_global_goal_difference():
    ordered, positions = _rank(318, _stats(wins=9, goal_diff=-4), _stats(wins=8, goal_diff=10))
    assert ordered[0][0] == "A"
    assert positions["A"]["position"] == 1


def test_portugal_uses_h2h_points_even_before_the_return_fixture():
    ordered, _ = _rank(261, _stats(goal_diff=-4), _stats(goal_diff=10), [_game("A", "B", 1, 0)])
    assert ordered[0][0] == "A"


def test_ligue_1_uses_complete_h2h_before_total_goals_scored():
    ordered, _ = _rank(172, _stats(goals_for=20), _stats(goals_for=30),
                       [_game("A", "B", 1, 0), _game("B", "A", 0, 1)])
    assert ordered[0][0] == "A"


def test_bundesliga_incomplete_direct_games_do_not_allow_away_goal_fallback():
    _, positions = _rank(171, _stats(away_goals=5), _stats(away_goals=9), [_game("A", "B", 1, 0)])
    assert positions["A"]["position"] == positions["B"]["position"] == 1
    assert positions["A"]["ranking_status"] == "SHARED_POSITION"


def test_saudi_uses_different_interim_and_final_orders():
    a, b = _stats(goal_diff=-4), _stats(goal_diff=10)
    games = [_game("A", "B", 1, 0), _game("B", "A", 0, 1)]
    assert _rank(50, a, b, games)[0][0][0] == "B"
    assert _rank(50, a, b, games, final=True)[0][0][0] == "A"


def test_eredivisie_interim_table_prefers_fewer_games_before_goal_difference():
    a, b = _stats(games_played=15, goal_diff=-4), _stats(games_played=16, goal_diff=10)
    assert _rank(317, a, b)[0][0][0] == "A"
    assert _rank(317, a, b, final=True)[0][0][0] == "B"


def test_shl_goal_difference_precedes_regulation_wins():
    ordered, _ = _rank(180, _stats(regulation_wins=2, goal_diff=10), _stats(regulation_wins=8, goal_diff=-4))
    assert ordered[0][0] == "A"


def test_nhl_row_precedes_total_wins():
    ordered, _ = _rank(5196, _stats(regulation_overtime_wins=8, wins=8),
                       _stats(regulation_overtime_wins=7, wins=10))
    assert ordered[0][0] == "A"


def test_nhl_excludes_first_game_at_the_venue_with_the_extra_meeting():
    a = _stats(wins=8, regulation_overtime_wins=8)
    b = _stats(wins=8, regulation_overtime_wins=8)
    games = [_game("A", "B", 2, 0), _game("A", "B", 0, 2), _game("B", "A", 2, 0)]
    ordered, _ = _rank(5196, a, b, games)
    assert ordered[0][0] == "B"


def test_euroleague_never_uses_overtime_totals_as_regulation_score_differential():
    games = [_game("A", "B", 90, 88, "OT"), _game("B", "A", 85, 80)]
    _, positions = _rank(429, games=games)
    assert positions["A"]["position"] is None
    assert positions["A"]["ranking_reason"] == "MISSING_REGULATION_SCORES"


def test_euroleague_can_resolve_h2h_wins_without_overtime_scoring_details():
    games = [_game("A", "B", 90, 88, "OT"), _game("B", "A", 80, 85)]
    ordered, positions = _rank(429, games=games)
    assert ordered[0][0] == "A"
    assert positions["A"]["position"] == 1


def test_missing_discipline_does_not_authorize_a_wins_or_alphabetical_fallback():
    _, positions = _rank(167, games=[_game("A", "B", 0, 0), _game("B", "A", 0, 0)])
    assert positions["A"]["position"] is positions["B"]["position"] is None
    assert positions["A"]["ranking_reason"] == "MISSING_DISCIPLINARY_RECORD"


@pytest.mark.parametrize("cid", [176, 129, 5153, 174])
def test_unsupported_complex_ties_remain_unresolved_instead_of_using_global_score_difference(cid):
    _, positions = _rank(cid, _stats(goal_diff=40), _stats(goal_diff=-40))
    assert positions["A"]["position"] is positions["B"]["position"] is None
    assert positions["A"]["ranking_status"] == "UNRESOLVED"


@pytest.mark.parametrize("cid", [176, 129])
def test_two_team_h2h_can_resolve_percentage_ties_before_unimplemented_steps(cid):
    ordered, positions = _rank(cid, games=[_game("A", "B", 2, 1)])
    assert ordered[0][0] == "A"
    assert positions["A"]["position"] == 1
    assert set(positions) == {name for name, _ in ordered} == {"A", "B"}
    json.dumps(positions)


def test_nba_multiteam_tie_checks_division_winner_before_h2h():
    boston, milwaukee, miami = "Boston Celtics", "Milwaukee Bucks", "Miami Heat"
    games = [_game(milwaukee, boston, 101, 100), _game(miami, boston, 101, 100),
             _game(boston, "Atlanta Hawks", 101, 100), _game(boston, "Atlanta Hawks", 101, 100),
             _game("Detroit Pistons", milwaukee, 101, 100), _game("Orlando Magic", miami, 101, 100)]
    ordered, positions = _rank_games(176, [boston, milwaukee, miami], games)
    # All three are .500. Boston leads its division, while Detroit and Orlando
    # lead the divisions of the two clubs which beat Boston directly.
    assert ordered[0] == boston
    assert positions[boston]["position"] == 1


def test_nfl_same_division_tie_uses_division_record_after_split_h2h():
    buffalo, miami = "Buffalo Bills", "Miami Dolphins"
    games = [_game(buffalo, miami, 21, 20), _game(miami, buffalo, 60, 0),
             _game(buffalo, "New England Patriots", 21, 20), _game("Chicago Bears", buffalo, 21, 20),
             _game("New England Patriots", miami, 21, 20), _game(miami, "Green Bay Packers", 60, 0)]
    ordered, _ = _rank_games(5153, [buffalo, miami], games)
    assert ordered == [buffalo, miami]


def test_nfl_multidivision_tie_eliminates_lower_division_club_before_sweep():
    baltimore, buffalo, miami, houston = "Baltimore Ravens", "Buffalo Bills", "Miami Dolphins", "Houston Texans"
    games = [_game(baltimore, buffalo, 21, 20), _game(baltimore, houston, 21, 20),
             _game(miami, baltimore, 21, 20), _game(buffalo, miami, 21, 20),
             _game(buffalo, houston, 21, 20), _game(miami, "Chicago Bears", 21, 20)]
    games += [_game(houston, opponent, 21, 20) for opponent in
              ("Arizona Cardinals", "Los Angeles Rams", "San Francisco 49ers", "Seattle Seahawks")]
    ordered, _ = _rank_games(5153, [baltimore, buffalo, miami, houston], games)
    # All four have a .667 record. Miami cannot enter the initial cross-division
    # comparison because Buffalo wins the division comparison against Miami.
    assert ordered == [baltimore, buffalo, miami, houston]


def test_mlb_two_team_tie_uses_intradivision_record_after_split_h2h():
    yankees, boston = "New York Yankees", "Boston Red Sox"
    games = [_game(yankees, boston, 2, 1), _game(boston, yankees, 8, 0),
             _game(yankees, "Baltimore Orioles", 2, 1), _game("Chicago Cubs", yankees, 2, 1),
             _game("Baltimore Orioles", boston, 2, 1), _game(boston, "New York Mets", 8, 0)]
    ordered, _ = _rank_games(129, [yankees, boston], games)
    assert ordered == [yankees, boston]


def test_euroleague_partial_tie_recomputes_the_mini_table_for_remaining_clubs():
    games = [_game("A", "B", 1, 0), _game("B", "A", 0, 1),
             _game("A", "C", 100, 0), _game("C", "A", 0, 100),
             _game("B", "C", 1, 0), _game("C", "B", 10, 0)]
    for opponent in ("D", "E", "F"):
        games += [_game(opponent, "A", 1, 0), _game("B", opponent, 1, 0), _game("C", opponent, 1, 0)]
    ordered, _ = _rank_games(429, ["A", "B", "C"], games)
    # Equal overall wins/GP. A wins the first mini-table. C then wins the B/C
    # comparison, although B's differential in the original A/B/C table is better.
    assert ordered == ["A", "C", "B"]


@pytest.mark.parametrize("cid", [429, 180, 525])
def test_auxiliary_percentage_dropped_by_the_first_refactor_is_restored(cid):
    stats = _create_team_stats(None)
    stats.update(wins=1, games_played=2)
    _finalize_team_stats(stats, get_competition_profile(cid))
    assert stats["pct"] == 0.5


@pytest.mark.parametrize("method,team,group", [
    ("argentina_zones", "Boca Juniors", "Group A"),
    ("argentina_zones", "River Plate", "Group B"),
    ("mls_conference", "Inter Miami CF", "Eastern"),
    ("mls_conference", "San Diego FC", "Western"),
    ("ahl_division", "Hamilton Hammers", "North"),
    ("ahl_division", "Rockford Icehogs", "Central"),
])
def test_published_group_memberships_match_stored_event_names(method, team, group):
    assert get_team_group(team, method) == group
