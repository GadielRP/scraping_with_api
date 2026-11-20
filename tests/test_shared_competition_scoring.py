from dataclasses import replace

from modules.standings.calculator import (
    _apply_game_result, _create_team_stats, _finalize_team_stats,
)
from modules.standings.ranking import build_head_to_head_stats
from modules.competition.competitive_rules import ScoringRules
from tests.pillars.pillar_1_team_structure.test_module_5_late_season import (
    _analysis, _calculate_m5, _event,
)


def test_one_scoring_change_reaches_standings_tiebreaks_and_m5():
    event = _event()
    profile = replace(event.competition.competitive_rules, scoring=ScoringRules(5, 5, draw=2))
    event.competition = replace(event.competition, competitive_rules=profile)
    stats = {name: _create_team_stats(None) for name in ("A", "B")}
    records = [
        {"home_team": "A", "away_team": "B", "home_score": 2, "away_score": 0},
        {"home_team": "A", "away_team": "B", "home_score": 1, "away_score": 1},
    ]
    for record, winner in zip(records, ("1", "X")):
        _apply_game_result(stats, **record, winner=winner)
    for team in stats.values():
        _finalize_team_stats(team, profile)
    mini_table = build_head_to_head_stats(("A", "B"), records, profile.scoring)
    assert stats["A"]["points"] == mini_table["A"]["points"] == 7
    assert stats["B"]["points"] == mini_table["B"]["points"] == 2
    result = _calculate_m5(_analysis(), event)
    assert result.raw["max_points_remaining"] == {"home": 15, "away": 15}

