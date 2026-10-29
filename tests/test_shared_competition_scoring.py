from dataclasses import replace
import importlib.util
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, text

from modules.alerts.matchup_streak_analysis.standings_engine import (
    _apply_game_result, _create_team_stats, _finalize_team_stats,
)
from modules.alerts.matchup_streak_analysis.standings_rules import build_head_to_head_stats
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
    assert result.raw["max_points_remaining"] == 15


def test_column_rename_preserves_data_and_can_be_reversed():
    path = Path(__file__).parents[1] / "infrastructure/persistence/alembic/versions/20261029_01_competition_config_source.py"
    spec = importlib.util.spec_from_file_location("rename_competition_source", path)
    revision = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(revision)
    engine = create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE competitions (league_config_source TEXT)"))
            connection.execute(text("INSERT INTO competitions VALUES ('competition_rules')"))
            with Operations.context(MigrationContext.configure(connection)):
                revision.upgrade()
                assert connection.execute(text("SELECT competition_config_source FROM competitions")).scalar_one() == "competition_rules"
                assert {c["name"] for c in inspect(connection).get_columns("competitions")} == {"competition_config_source"}
                revision.downgrade()
                assert connection.execute(text("SELECT league_config_source FROM competitions")).scalar_one() == "competition_rules"
    finally:
        engine.dispose()
