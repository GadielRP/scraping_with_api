"""Observed policy gaps: league-specific ordering, H2H and unavailable inputs."""

import json
from dataclasses import asdict

import pytest

from modules.alerts.matchup_streak_analysis.constants import get_team_group
from modules.alerts.matchup_streak_analysis.standings_engine import _apply_game_result, _create_team_stats, _finalize_team_stats
from modules.alerts.matchup_streak_analysis.standings_rules import rank_standings
from modules.competition.competition_profiles import COMPETITION_PROFILES
from tests.competition_fixtures import profile as get_competition_profile, IDENTITIES, with_identity
from modules.competition.tracked_competitions import TRACKED_COMPETITION_IDENTITIES


def _stats(**values):
    stats = _create_team_stats(None)
    stats.update(points=30, wins=8, games_played=16, goal_diff=0, goals_for=20)
    stats.update(values)
    return stats


def _game(home, away, scored, conceded, subtype="REG"):
    return dict(home_team=home, away_team=away, home_score=scored, away_score=conceded,
                winner="1" if scored > conceded else "2" if conceded > scored else "X",
                result_subtype=subtype)


def _rank(cid, a=None, b=None, games=(), standings_policy="closing_projection"):
    return rank_standings(
        {"A": a or _stats(), "B": b or _stats()}.items(), get_competition_profile(cid), list(games),
        standings_policy=standings_policy,
    )


def _rank_games(cid, names, games, standings_policy="closing_projection"):
    profile = get_competition_profile(cid)
    stats = {team: _create_team_stats(None) for game in games for team in (game["home_team"], game["away_team"])}
    for game in games:
        _apply_game_result(stats, **game)
    for row in stats.values():
        _finalize_team_stats(row, profile)
    ordered, positions = rank_standings([(name, stats[name]) for name in names], profile, games, standings_policy=standings_policy)
    assert set(positions) == {name for name, _ in ordered} == set(names)
    return [name for name, _ in ordered], positions


def test_every_tracked_competition_has_a_serializable_sporting_profile():
    assert TRACKED_COMPETITION_IDENTITIES <= COMPETITION_PROFILES.keys()
    for cid in TRACKED_COMPETITION_IDENTITIES:
        profile = get_competition_profile(cid)
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


def test_saudi_orders_by_goal_difference_before_any_head_to_head():
    a, b = _stats(goal_diff=-4), _stats(goal_diff=10)
    games = [_game("A", "B", 1, 0), _game("B", "A", 0, 1)]
    assert _rank(50, a, b, games, standings_policy="in_season")[0][0][0] == "B"


@pytest.mark.parametrize("first", [2, 10])
def test_premier_league_tie_shares_a_position_without_a_sporting_boundary(first):
    rows = {f"T{i:02}": _stats(points=100 - 4 * i) for i in range(1, 21)}
    points = rows.pop(f"T{first:02}")["points"]
    rows.pop(f"T{first + 1:02}")
    rows.update({team: _stats(points=points) for team in ("A", "B")})
    games = [_game("A", "B", 2, 1), _game("B", "A", 0, 0)]
    _, positions = rank_standings(rows.items(), get_competition_profile(168), games, standings_policy="closing_projection")
    assert positions["A"]["position"] == positions["B"]["position"] == first
    assert positions["A"]["ranking_status"] == positions["B"]["ranking_status"] == "SHARED_POSITION"


@pytest.mark.parametrize("first", [1, 4, 17])
def test_premier_league_h2h_decides_title_qualification_and_relegation(first):
    rows = {f"T{i:02}": _stats(points=100 - 4 * i) for i in range(1, 21)}
    points = rows.pop(f"T{first:02}")["points"]
    rows.pop(f"T{first + 1:02}")
    rows.update({team: _stats(points=points) for team in ("A", "B")})
    games = [_game("A", "B", 2, 1), _game("B", "A", 0, 0)]
    _, positions = rank_standings(rows.items(), get_competition_profile(168), games, standings_policy="closing_projection")
    assert (positions["A"]["position"], positions["B"]["position"]) == (first, first + 1)


@pytest.mark.parametrize("cid", [328, 2192])
def test_mexico_uses_away_goals_when_away_match_counts_are_equal(cid):
    games = [_game("A", "B", 2, 0), _game("B", "A", 3, 1)]
    ordered, positions = _rank(cid, _stats(games_played=2), _stats(games_played=2), games)
    assert ordered[0][0] == "A"  # One away goal against zero, at one visit each.
    assert positions["A"]["ranking_status"] == "RESOLVED"


def test_mexico_does_not_compare_unequal_away_counts_without_the_matchday_cutoff():
    games = [_game("A", "B", 2, 0), _game("B", "A", 3, 1),
             _game("X", "A", 0, 1), _game("B", "X", 1, 0)]
    # Both clubs have 4 GF and +1 GD after three games. A has played two
    # away games, B only one: raw away totals (2 vs 0) are not comparable.
    _, positions = _rank(328, _stats(games_played=3, goals_for=4, goal_diff=1),
                         _stats(games_played=3, goals_for=4, goal_diff=1), games)
    assert positions["A"]["ranking_reason"] == "MISSING_BALANCED_AWAY_GOALS"
    assert positions["A"]["position"] is positions["B"]["position"] is None


@pytest.mark.parametrize("cid", [328, 2192])
def test_mexico_fair_play_precedes_quotient_and_never_uses_h2h(cid):
    games = [_game("A", "B", 2, 0), _game("B", "A", 0, 0)]
    # Equal overall GD/GF and zero away goals, despite A winning H2H.
    _, positions = _rank(cid, _stats(games_played=2), _stats(games_played=2), games)
    assert positions["A"]["ranking_reason"] == "MISSING_FAIR_PLAY_POINTS"
    assert positions["A"]["position"] is positions["B"]["position"] is None


def test_liiga_h2h_regulation_wins_follow_h2h_points_before_h2h_goal_difference():
    games = [_game("A", "B", 2, 0), _game("B", "A", 2, 1, "OT"),
             _game("A", "B", 1, 2, "OT"), _game("B", "A", 2, 1, "OT")]
    # Six H2H points each; A has one regulation win and B none, even though
    # B has the better H2H goal difference and more total H2H wins.
    ordered, positions = _rank(525, games=games)
    assert ordered[0][0] == "A"
    assert positions["A"]["ranking_status"] == "RESOLVED"


def test_eredivisie_interim_table_prefers_fewer_games_before_goal_difference():
    a, b = _stats(games_played=15, goal_diff=-4), _stats(games_played=16, goal_diff=10)
    assert _rank(317, a, b, standings_policy="in_season")[0][0][0] == "A"
    assert _rank(317, a, b)[0][0][0] == "B"  # the closing projection drops the games-played criterion
    a, b = _stats(goal_diff=-4), _stats(goal_diff=10)
    assert _rank(317, a, b)[0][0][0] == "B"


def test_premier_h2h_decides_a_boundary_tie_once_both_legs_are_played():
    # Level on points, goal difference and goals scored, tied across the title boundary.
    both_legs = [_game("A", "B", 0, 1), _game("B", "A", 2, 0)]  # B wins both meetings
    ordered, positions = _rank(168, games=both_legs)
    assert [name for name, _ in ordered] == ["B", "A"]
    assert {p["ranking_status"] for p in positions.values()} == {"RESOLVED"}

    # With a single meeting the head-to-head cannot apply yet: the position is shared.
    _, positions = _rank(168, games=both_legs[:1])
    assert {p["ranking_status"] for p in positions.values()} == {"SHARED_POSITION"}


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
    assert positions["A"]["ranking_reason"] == "MISSING_FAIR_PLAY_POINTS"


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


@pytest.mark.parametrize(("played", "ratio", "policy"), [
    (320, 640 / 760, "in_season"),
    (323, 646 / 760, "closing_projection"),
    (350, 700 / 760, "closing_projection"),
])
def test_standings_policy_switches_at_85_percent_inclusive(played, ratio, policy):
    from modules.alerts.matchup_streak_analysis.standings_rules import select_standings_policy

    team_games = played * 2  # each match counts for both participants
    games = [team_games // 20 + (i < team_games % 20) for i in range(20)]
    assert select_standings_policy(games, number_of_teams=20, games_per_team=38) == (ratio, policy)


@pytest.mark.parametrize(("games", "teams", "per_team"), [
    ([10] * 20, None, 38), ([10] * 20, 20, None), ([10] * 20, 20, 0), ([10] * 21, 20, 38), ([39] + [10] * 19, 20, 38),
])
def test_unknown_or_incompatible_format_keeps_in_season_policy(games, teams, per_team):
    from modules.alerts.matchup_streak_analysis.standings_rules import select_standings_policy

    assert select_standings_policy(games, number_of_teams=teams, games_per_team=per_team) == (None, "in_season")


def _rows(*names, **values):
    return {name: _stats(**values) for name in names}


def _positions(cid, rows, games=(), standings_policy="closing_projection"):
    _, positions = rank_standings(rows.items(), get_competition_profile(cid), list(games), standings_policy=standings_policy)
    return positions


def test_missing_meeting_hands_the_tied_clubs_to_the_in_season_order_and_records_it():
    # Level on points and goal difference with one meeting only: the LaLiga closing head-to-head cannot
    # apply, so the in-season order (goal difference) takes over for that subgroup alone.
    rows = {"A": _stats(goal_diff=5), "B": _stats(goal_diff=5), "C": _stats(goal_diff=0, points=20)}
    positions = _positions(167, rows, [_game("A", "B", 1, 0)])
    assert positions["A"]["ranking_policy"] == positions["B"]["ranking_policy"] == "in_season"
    assert positions["A"]["ranking_policy_reason"] == "INCOMPLETE_HEAD_TO_HEAD"
    assert positions["A"]["ranking_status"] == "SHARED_POSITION"
    assert "ranking_policy" not in positions["C"]
    assert positions["C"]["position"] == 3  # earlier separations are preserved


def test_complete_meetings_keep_the_closing_policy_without_fallback_fields():
    rows = _rows("A", "B", fair_play_points=0, standalone_yellow_cards=0,
                 second_yellow_dismissals=0, direct_red_cards=0)
    positions = _positions(167, rows, [_game("A", "B", 1, 0), _game("B", "A", 0, 0)])
    assert "ranking_policy" not in positions["A"]


def test_serie_a_orders_candidates_by_the_mini_table_but_leaves_title_places_pending():
    rows = _rows("A", "B", goal_diff=0)
    games = [_game("A", "B", 1, 0), _game("B", "A", 0, 0)]
    ordered, positions = rank_standings(rows.items(), get_competition_profile(88), games,
                                        standings_policy="closing_projection")
    assert [name for name, _ in ordered] == ["A", "B"]
    assert (positions["A"]["ranking_order"], positions["B"]["ranking_order"]) == (1, 2)
    assert {p["position"] for p in positions.values()} == {None}
    assert {p["ranking_reason"] for p in positions.values()} == {"OFFICIAL_RESOLUTION_PENDING"}


def test_serie_a_tie_away_from_the_title_and_relegation_lines_is_decided_by_the_mini_table():
    rows = {f"T{i:02}": _stats(points=100 - 4 * i) for i in range(1, 21)}
    points = rows.pop("T08")["points"]
    rows.pop("T09")
    rows.update({team: _stats(points=points) for team in ("A", "B")})
    positions = _positions(88, rows, [_game("A", "B", 1, 0), _game("B", "A", 0, 0)])
    assert (positions["A"]["position"], positions["B"]["position"]) == (8, 9)


def test_serie_a_in_season_uses_the_published_table_without_head_to_head():
    ordered, positions = _rank(88, _stats(goal_diff=-4), _stats(goal_diff=10),
                               [_game("A", "B", 1, 0), _game("B", "A", 0, 1)], standings_policy="in_season")
    assert ordered[0][0] == "B"
    assert positions["B"]["position"] == 1


def test_saudi_closing_compares_the_whole_mini_table_before_reapplying_it():
    # A, B and C are level. A leads the three-club table (6 points); B and C have 5 points but C has the
    # better goal difference there, so the complete block orders C ahead of B at once. Reapplying after
    # the points alone would restart with only B and C, whose own meetings are level.
    games = [_game("A", "B", 1, 0), _game("A", "C", 1, 0), _game("B", "C", 3, 3), _game("C", "B", 0, 0),
             _game("B", "A", 2, 1), _game("C", "A", 5, 0)]
    rows = {team: _stats(goal_diff=0, goals_for=20) for team in "ABC"}
    ordered, positions = rank_standings(rows.items(), get_competition_profile(50), games, standings_policy="closing_projection")
    assert [name for name, _ in ordered] == ["A", "C", "B"]
    assert {p["ranking_status"] for p in positions.values()} == {"RESOLVED"}


def test_eredivisie_alphabetical_order_ranks_a_prefix_before_the_longer_name():
    rows = {"Ajax": _stats(), "Ajax Amsterdam": _stats()}
    ordered, positions = rank_standings(rows.items(), get_competition_profile(317), [], standings_policy="in_season")
    assert [name for name, _ in ordered] == ["Ajax", "Ajax Amsterdam"]
    assert positions["Ajax"]["position"] == 1


def test_eredivisie_closing_ends_in_a_pending_playoff_after_complete_head_to_head():
    games = [_game("A", "B", 1, 1), _game("B", "A", 1, 1)]
    _, positions = _rank(317, games=games)
    assert positions["A"]["ranking_reason"] == "PLAYOFF_MATCH_PENDING"


def test_liga_portugal_closing_needs_both_legs_and_falls_back_to_the_in_season_head_to_head():
    one_leg = [_game("A", "B", 1, 0)]
    ordered, positions = _rank(261, games=one_leg)
    assert ordered[0][0] == "A"
    assert positions["A"]["ranking_policy_reason"] == "INCOMPLETE_HEAD_TO_HEAD"
    ordered, positions = _rank(261, games=one_leg, standings_policy="in_season")
    assert ordered[0][0] == "A" and "ranking_policy" not in positions["A"]


def test_bundesliga_closing_leaves_a_play_off_after_complete_head_to_head():
    games = [_game("A", "B", 1, 1), _game("B", "A", 1, 1)]
    assert _rank(171, games=games)[1]["A"]["ranking_reason"] == "PLAYOFF_MATCH_PENDING"
    assert _rank(171, games=games, standings_policy="in_season")[1]["A"]["ranking_status"] == "SHARED_POSITION"


def test_liga_mx_in_season_uses_the_published_table_and_closing_continues_after_goal_difference():
    a, b = _stats(goal_diff=3, goals_for=10), _stats(goal_diff=3, goals_for=20)
    _, in_season = _rank(328, a, b, standings_policy="in_season")
    assert {p["ranking_status"] for p in in_season.values()} == {"SHARED_POSITION"}
    ordered, _ = _rank(328, a, b)
    assert ordered[0][0] == "B"


def test_mls_closing_orders_by_discipline_per_game_without_head_to_head():
    cards = dict(standalone_yellow_cards=0, second_yellow_dismissals=0, direct_red_cards=0)
    clean, rough = _stats(**cards), _stats(**{**cards, "standalone_yellow_cards": 4})
    ordered, _ = _rank(76, rough, clean, [_game("A", "B", 3, 0), _game("B", "A", 0, 0)])
    assert ordered[0][0] == "B"  # B has fewer discipline points per game; the A head-to-head win is ignored
    _, positions = _rank(76, rough, clean, standings_policy="in_season")
    assert {p["ranking_status"] for p in positions.values()} == {"SHARED_POSITION"}


def test_pfl_and_cba_leave_ties_pending_instead_of_inventing_a_tiebreak():
    for cid in (5151, 174):
        for policy in ("in_season", "closing_projection"):
            _, positions = _rank(cid, standings_policy=policy)
            assert positions["A"]["ranking_status"] == "UNRESOLVED"


def test_cba_scores_two_points_per_win_and_one_per_ordinary_loss():
    profile = get_competition_profile(174)
    assert profile.standings.primary_metric == "points"
    assert profile.scoring.points({"wins": 3, "losses": 2}) == 8


def test_wnba_closing_reapplies_head_to_head_then_waits_for_the_record_against_winning_teams():
    games = [_game("A", "B", 2, 1), _game("B", "A", 3, 2)]
    _, in_season = _rank(145, games=games, standings_policy="in_season")
    assert in_season["A"]["ranking_status"] == "SHARED_POSITION"
    _, closing = _rank(145, games=games)
    assert closing["A"]["ranking_reason"] == "MISSING_FINAL_RECORD_VS_WINNING_TEAMS"


def test_nba_in_season_resolves_only_two_team_head_to_head_and_keeps_legitimate_ties():
    ordered, _ = _rank(176, games=[_game("A", "B", 2, 1)], standings_policy="in_season")
    assert ordered[0][0] == "A"
    _, tied = _rank(176, standings_policy="in_season")
    assert {p["ranking_status"] for p in tied.values()} == {"SHARED_POSITION"}


@pytest.mark.parametrize("cid", [129, 5153])
def test_mlb_and_nfl_in_season_keep_exhausted_ties_shared_while_closing_waits_for_the_next_step(cid):
    clubs = ("New York Yankees", "Boston Red Sox") if cid == 129 else ("Buffalo Bills", "Miami Dolphins")
    in_season = _positions(cid, _rows(*clubs), standings_policy="in_season")
    assert {p["ranking_status"] for p in in_season.values()} == {"SHARED_POSITION"}
    closing = _positions(cid, _rows(*clubs))
    assert {p["ranking_status"] for p in closing.values()} == {"UNRESOLVED"}


def test_ahl_in_season_leaves_the_provisional_tiebreak_pending():
    _, in_season = _rank(554, standings_policy="in_season")
    assert in_season["A"]["ranking_reason"] == "AHL_IN_SEASON_TIEBREAK_UNCONFIRMED"


def test_ahl_closing_breaks_a_final_tie_by_points_percentage_against_the_own_conference():
    east, other_east, west = "Hershey Bears", "Charlotte Checkers", "Texas Stars"
    # Equal in every earlier step; only the record against the own conference differs.
    games = [_game(east, other_east, 2, 1), _game("Iowa Wild", west, 2, 1)]
    stats = _stats(wins=1, regulation_wins=1, regulation_overtime_wins=1, games_played=1, goal_diff=0)
    rows = {east: dict(stats), west: dict(stats)}
    ordered, positions = rank_standings(rows.items(), get_competition_profile(554), games,
                                        standings_policy="closing_projection")
    assert [name for name, _ in ordered] == [east, west]
    assert positions[east]["ranking_status"] == "RESOLVED"
