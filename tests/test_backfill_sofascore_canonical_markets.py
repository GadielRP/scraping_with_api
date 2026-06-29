from __future__ import annotations

from types import SimpleNamespace

import pytest

import scripts.maintenance.backfill_sofascore_canonical_markets as script
from infrastructure.persistence.repositories.market_mapping_repository import (
    CANONICAL_MARKET_TYPE_SEEDS,
    MarketMappingRepository,
)


class _FakeContextManager:
    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        return self._connection

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def execute(self, *args, **kwargs):
        raise AssertionError("execute() should not be called in this test")


class _FakeEngine:
    def __init__(self, dialect_name: str = "postgresql"):
        self.dialect = SimpleNamespace(name=dialect_name)

    def begin(self):
        return _FakeContextManager(_FakeConnection())

    def connect(self):
        return _FakeContextManager(_FakeConnection())


class _FakeInspector:
    def __init__(self, tables: set[str], columns: dict[str, set[str]]):
        self._tables = tables
        self._columns = columns

    def has_table(self, table_name: str) -> bool:
        return table_name in self._tables

    def get_columns(self, table_name: str):
        return [{"name": name} for name in sorted(self._columns.get(table_name, set()))]


def test_validate_required_schema_detects_missing_columns(monkeypatch):
    fake_inspector = _FakeInspector(
        {"markets", "events", "canonical_market_types"},
        {
            "markets": {
                "market_id",
                "event_id",
                "bookie_id",
                "market_name",
                "market_period",
                "choice_group",
                "is_live",
            },
            "events": {"id"},
            "canonical_market_types": {
                "canonical_market_key",
                "canonical_market_name",
                "canonical_market_group",
                "canonical_market_period",
                "requires_choice_group",
                "enabled_for_trajectory",
            },
        },
    )
    monkeypatch.setattr(script, "inspect", lambda connection: fake_inspector)

    with pytest.raises(RuntimeError, match="events missing columns: sport"):
        script.validate_required_schema(object())


def test_detect_projected_conflicts_coalesces_null_choice_group():
    conflicts = script.detect_projected_conflicts(
        [
            {
                "market_id": 1,
                "event_id": 11,
                "bookie_id": 1,
                "market_name": "Total",
                "market_period": "Full-time",
                "choice_group": None,
                "is_live": False,
            },
            {
                "market_id": 2,
                "event_id": 11,
                "bookie_id": 1,
                "market_name": "Total",
                "market_period": "Full-time",
                "choice_group": "",
                "is_live": False,
            },
        ]
    )

    assert conflicts == [
        {
            "event_id": 11,
            "bookie_id": 1,
            "projected_market_name": "Total",
            "projected_market_period": "Full-time",
            "choice_group": "",
            "is_live": False,
            "market_ids": [1, 2],
        }
    ]


def test_run_migrations_flag_controls_schema_migration_call(monkeypatch):
    calls = {"migrations": 0}

    def fake_check_and_migrate_schema():
        calls["migrations"] += 1
        return True

    fake_db_manager = SimpleNamespace(
        engine=_FakeEngine(),
        check_and_migrate_schema=fake_check_and_migrate_schema,
    )
    monkeypatch.setattr(script, "db_manager", fake_db_manager)
    monkeypatch.setattr(script, "validate_required_schema", lambda connection: {})
    monkeypatch.setattr(script, "_preflight", lambda connection, rules: [{"canonical_market_key": "x"}])
    monkeypatch.setattr(
        script,
        "_analyze_in_event_batches",
        lambda connection, rules, params, batch_size: (
            {
                "rules": [
                    {
                        "rule_id": "rule_1",
                        "candidate_markets": 1,
                        "events": 1,
                        "sports": 1,
                        "already_in_target": 0,
                        "requires_update": 1,
                        "eligible_markets": 1,
                        "risky_markets": 0,
                        "skipped_missing_choice_group": 0,
                        "markets_with_unexpected_choice_names": 0,
                        "sample_unexpected_choice_names": [],
                        "choice_group_null": 0,
                        "requires_choice_group": False,
                        "sport_names": ["Football"],
                    }
                ],
                "by_sport": [
                    {
                        "sport": "Football",
                        "candidate_markets": 1,
                        "events": 1,
                        "eligible_markets": 1,
                        "skipped_missing_choice_group": 0,
                    }
                ],
            },
            {"total_conflicts": 0, "rows": []},
            [(1, 1)],
            {"min_event_id": 1, "max_event_id": 1, "candidate_events": 1, "candidate_markets": 1},
        ),
    )
    monkeypatch.setattr(script, "_post_update_validation", lambda connection, params: None)
    monkeypatch.setattr(script, "_load_csv_summary", lambda path, label: None)

    rc, summary = script.run(script.build_parser().parse_args(["--dry-run"]))
    assert rc == 0
    assert summary["run_migrations"] is False
    assert calls["migrations"] == 0

    rc, summary = script.run(script.build_parser().parse_args(["--dry-run", "--run-migrations"]))
    assert rc == 0
    assert summary["run_migrations"] is True
    assert calls["migrations"] == 1


def test_build_rules_values_sql_exposes_required_columns():
    rules = script.validate_rule_definitions(script.RULES)
    sql, params = script.build_rules_values_sql(rules)

    assert "expected_choice_names" not in sql
    assert "canonical_market_key" in sql
    assert "requires_choice_group" in sql
    assert params


def test_new_canonical_market_types_are_seeded_and_disabled_for_trajectory():
    expected_keys = {
        "1x2_1st_quarter",
        "full_time_including_overtime",
        "game_total_full_time",
        "total_points_full_time",
        "match_goals_full_time",
        "1st_period_goals_1st_period",
        "cards_in_match_full_time",
        "corners_2_way_full_time",
        "total_games_won_extra_time",
        "point_spread_full_time",
        "first_team_to_score_full_time",
        "next_goal_full_time",
        "tie_break_in_match_extra_time",
        "first_set_winner_1st_set",
        "current_set_winner_current_set",
    }

    assert expected_keys.issubset(CANONICAL_MARKET_TYPE_SEEDS)
    for key in expected_keys:
        assert CANONICAL_MARKET_TYPE_SEEDS[key]["enabled_for_trajectory"] is False


def test_resolve_canonical_key_from_catalog_market_supports_new_specific_variants():
    cases = [
        (
            {
                "marketName": "1st quarter winner",
                "marketType": "Home/Away",
                "period": "1st quarter",
                "outcomes": [{"outcomeName": "1"}, {"outcomeName": "X"}, {"outcomeName": "2"}],
            },
            "1x2_1st_quarter",
        ),
        (
            {
                "marketName": "Game total",
                "marketType": "Over/Under",
                "period": "Match",
                "outcomes": [{"outcomeName": "Over"}, {"outcomeName": "Under"}],
            },
            "game_total_full_time",
        ),
        (
            {
                "marketName": "First set winner",
                "marketType": "Home/Away",
                "period": "1st set",
                "outcomes": [{"outcomeName": "1"}, {"outcomeName": "2"}],
            },
            "first_set_winner_1st_set",
        ),
    ]

    for item, expected_key in cases:
        canonical_key, reason = MarketMappingRepository.resolve_canonical_key_from_catalog_market(item)
        assert canonical_key == expected_key
        assert reason.startswith("matched_")
