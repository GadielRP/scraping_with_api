from __future__ import annotations

from sqlalchemy import create_engine

from infrastructure.persistence.migrations.market_choice_snapshot_slim import (
    LEGACY_INDEXES,
    REDUNDANT_COLUMNS,
    SCRIPT_MANAGED_DEPENDENT_VIEWS,
    SLIM_COLUMNS,
    MarketChoiceSnapshotSlimMigrator,
)
from infrastructure.persistence.migrations.market_choice_snapshot_slim_postflight import (
    REQUIRED_EVENT_READERS,
)
from scripts.maintenance import migrate_market_choice_snapshots_slim as cli
from app import initialize as app_initialize


def _expanded_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE market_choice_quotes (quote_id INTEGER PRIMARY KEY, choice_id INTEGER NOT NULL)"
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE market_choice_snapshots (
                snapshot_id INTEGER PRIMARY KEY,
                choice_id INTEGER NOT NULL,
                odds_value NUMERIC NOT NULL,
                collected_at TIMESTAMP NOT NULL,
                source TEXT,
                source_collected_at TIMESTAMP,
                source_market_id TEXT,
                source_outcome_id TEXT,
                bookmaker_outcome_id TEXT,
                main_line BOOLEAN,
                source_limit NUMERIC,
                exchange_side TEXT,
                exchange_level INTEGER,
                exchange_size NUMERIC,
                quote_id INTEGER REFERENCES market_choice_quotes(quote_id)
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX idx_market_choice_snapshots_quote_collected "
            "ON market_choice_snapshots (quote_id, collected_at DESC, snapshot_id DESC)"
        )
        connection.exec_driver_sql(
            "INSERT INTO market_choice_quotes VALUES (10, 1)"
        )
        connection.exec_driver_sql(
            "INSERT INTO market_choice_snapshots "
            "(snapshot_id, choice_id, quote_id, odds_value, collected_at) "
            "VALUES (100, 1, 10, 2.25, '2026-08-12 12:00:00')"
        )
    return engine


def _slim_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE market_choice_quotes (quote_id INTEGER PRIMARY KEY, choice_id INTEGER NOT NULL)"
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE market_choice_snapshots (
                snapshot_id INTEGER PRIMARY KEY,
                quote_id INTEGER NOT NULL REFERENCES market_choice_quotes(quote_id),
                odds_value NUMERIC NOT NULL,
                collected_at TIMESTAMP NOT NULL,
                source_collected_at TIMESTAMP,
                source_limit NUMERIC,
                exchange_size NUMERIC
            )
            """
        )
        connection.exec_driver_sql(
            "CREATE INDEX idx_market_choice_snapshots_quote_collected "
            "ON market_choice_snapshots (quote_id, collected_at DESC, snapshot_id DESC)"
        )
    return engine


def test_expanded_schema_is_ready_only_with_complete_quote_lineage():
    audit = MarketChoiceSnapshotSlimMigrator.audit(_expanded_engine())

    assert audit.schema_state == "expanded"
    assert audit.ready_to_migrate is True
    assert audit.null_quote_count == 0
    assert audit.orphan_quote_count == 0
    assert audit.choice_quote_mismatch_count == 0
    assert audit.metrics.row_count == 1


def test_slim_schema_has_exact_contract_and_is_idempotent():
    audit = MarketChoiceSnapshotSlimMigrator.audit(_slim_engine())

    assert audit.is_slim is True
    assert set(audit.columns) == set(SLIM_COLUMNS)
    assert audit.quote_id_nullable is False


def test_unexpected_column_blocks_even_an_otherwise_expanded_schema():
    engine = _expanded_engine()
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE market_choice_snapshots ADD COLUMN surprise TEXT"
        )

    audit = MarketChoiceSnapshotSlimMigrator.audit(engine)

    assert audit.schema_state == "expanded"
    assert audit.ready_to_migrate is False
    assert "unexpected_column:surprise" in audit.blockers


def test_postgresql_plan_is_exact_and_never_uses_cascade():
    statements = MarketChoiceSnapshotSlimMigrator.postgresql_ddl_statements()
    rendered = "\n".join(statements)

    assert "CASCADE" not in rendered.upper()
    assert "ALTER COLUMN quote_id SET NOT NULL" in rendered
    assert all(column in rendered for column in REDUNDANT_COLUMNS)
    assert all(index_name in rendered for index_name in LEGACY_INDEXES)


def test_direct_server_upgrade_allows_only_views_rebuilt_by_phase6():
    assert SCRIPT_MANAGED_DEPENDENT_VIEWS == {
        "public.v_dual_process_event_odds",
        "public.v_pre_start_odds_trajectory",
    }


def test_cli_requires_explicit_destructive_confirmation(monkeypatch):
    monkeypatch.setattr(cli.Config, "validate_odds_read_settings", lambda: None)

    assert cli.main(["--commit"]) == 3
    assert cli.main(["--compact"]) == 3


def test_cli_commit_refreshes_mv_before_reader_postflight(monkeypatch):
    calls = []
    slim = MarketChoiceSnapshotSlimMigrator.audit(_slim_engine())
    monkeypatch.setattr(cli.Config, "validate_odds_read_settings", lambda: None)
    monkeypatch.setattr(
        cli.MarketChoiceSnapshotSlimMigrator,
        "audit",
        lambda _engine: slim,
    )
    monkeypatch.setattr(
        cli.MarketChoiceSnapshotSlimMigrator,
        "apply_postgresql",
        lambda *_args, **_kwargs: (slim, slim),
    )
    monkeypatch.setattr(
        cli,
        "create_or_replace_odds_read_views",
        lambda _engine: calls.append("views"),
    )
    monkeypatch.setattr(
        cli,
        "refresh_materialized_views",
        lambda _engine: calls.append("refresh_mv"),
    )

    class _Postflight:
        ok = True

        @staticmethod
        def to_dict():
            return {"ok": True}

    monkeypatch.setattr(
        cli.MarketChoiceSnapshotSlimPostflight,
        "run",
        lambda *_args, **_kwargs: calls.append("postflight") or _Postflight(),
    )

    assert cli.main(["--commit", "--confirm-destructive"]) == 0
    assert calls == ["views", "refresh_mv", "postflight"]


def test_postflight_covers_all_snapshot_dependent_reader_contracts():
    assert set(REQUIRED_EVENT_READERS) == {
        "v_dual_process_event_odds",
        "event_all_odds",
        "v_pre_start_odds_trajectory",
        "mv_alert_events",
    }


def test_application_startup_is_fail_closed_on_schema_error(monkeypatch):
    view_calls = []
    monkeypatch.setattr(
        app_initialize.Config,
        "validate_odds_read_settings",
        lambda: None,
    )
    monkeypatch.setattr(app_initialize.db_manager, "test_connection", lambda: True)
    monkeypatch.setattr(app_initialize.db_manager, "create_tables", lambda: None)
    monkeypatch.setattr(
        app_initialize.db_manager,
        "check_and_migrate_schema",
        lambda: False,
    )
    monkeypatch.setattr(
        app_initialize,
        "create_or_replace_views",
        lambda _engine: view_calls.append("views"),
    )
    monkeypatch.setattr(
        app_initialize,
        "create_or_replace_materialized_views",
        lambda _engine: view_calls.append("mv"),
    )

    assert app_initialize.initialize_system() is False
    assert view_calls == []
