from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.migrations.market_choice_price_state_drop import (
    IDENTITY_COLUMNS,
    LEGACY_PRICE_COLUMNS,
    SCRIPT_MANAGED_DEPENDENT_VIEWS,
    MarketChoicePriceStateMigrator,
)
from infrastructure.persistence.models import MarketChoice
from scripts.maintenance import migrate_market_choice_price_state as cli


def _engine(*, legacy: bool):
    engine = create_engine("sqlite:///:memory:")
    legacy_columns = (
        ", initial_odds NUMERIC, current_odds NUMERIC, change INTEGER"
        if legacy
        else ""
    )
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE market_choices ("
            "choice_id INTEGER PRIMARY KEY, "
            "market_id INTEGER NOT NULL, "
            f"choice_name TEXT NOT NULL{legacy_columns})"
        )
        values = "(10, 20, '1', 1.9, 2.1, 1)" if legacy else "(10, 20, '1')"
        connection.exec_driver_sql(
            f"INSERT INTO market_choices VALUES {values}"
        )
    return engine


def test_market_choice_orm_is_identity_only():
    assert tuple(MarketChoice.__table__.columns.keys()) == IDENTITY_COLUMNS
    assert not (set(LEGACY_PRICE_COLUMNS) & set(MarketChoice.__mapper__.attrs.keys()))


def test_expanded_schema_is_ready_and_slim_schema_is_idempotent():
    expanded = MarketChoicePriceStateMigrator.audit(_engine(legacy=True))
    slim = MarketChoicePriceStateMigrator.audit(_engine(legacy=False))

    assert expanded.schema_state == "expanded"
    assert expanded.ready_to_migrate is True
    assert expanded.metrics.row_count == 1
    assert slim.schema_state == "slim"
    assert slim.is_slim is True


def test_partial_legacy_schema_is_blocked():
    engine = _engine(legacy=False)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "ALTER TABLE market_choices ADD COLUMN initial_odds NUMERIC"
        )

    audit = MarketChoicePriceStateMigrator.audit(engine)

    assert audit.schema_state == "invalid"
    assert "partial_legacy_price_schema" in audit.blockers


def test_view_dependency_is_reported_before_destructive_ddl():
    engine = _engine(legacy=True)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE VIEW legacy_choice_prices AS "
            "SELECT choice_id, initial_odds FROM market_choices"
        )

    audit = MarketChoicePriceStateMigrator.audit(engine)

    assert audit.ready_to_migrate is False
    assert "main.legacy_choice_prices:initial_odds" in audit.dependent_columns


def test_cli_can_replace_only_the_known_canonical_dependent_views():
    assert SCRIPT_MANAGED_DEPENDENT_VIEWS == {
        "public.v_dual_process_event_odds",
        "public.v_pre_start_odds_trajectory",
    }


def test_postgresql_plan_is_exact_and_never_uses_cascade():
    statement = MarketChoicePriceStateMigrator.postgresql_ddl_statement()

    assert "CASCADE" not in statement.upper()
    assert statement.startswith("ALTER TABLE public.market_choices")
    assert all(column in statement for column in LEGACY_PRICE_COLUMNS)


def test_compact_runs_vacuum_full_in_autocommit():
    calls = []

    class _Connection:
        def execution_options(self, **options):
            calls.append(("options", options))
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def exec_driver_sql(self, statement):
            calls.append(("sql", statement))

    engine = SimpleNamespace(
        dialect=SimpleNamespace(name="postgresql"),
        connect=lambda: _Connection(),
    )

    MarketChoicePriceStateMigrator.compact_postgresql(engine)

    assert calls == [
        ("options", {"isolation_level": "AUTOCOMMIT"}),
        ("sql", "VACUUM (FULL, ANALYZE) public.market_choices"),
    ]


def test_compact_rejects_non_postgresql_engines():
    with pytest.raises(ValueError, match="PostgreSQL"):
        MarketChoicePriceStateMigrator.compact_postgresql(
            _engine(legacy=False)
        )


def test_cli_requires_explicit_destructive_confirmation(monkeypatch):
    monkeypatch.setattr(cli.Config, "validate_odds_read_settings", lambda: None)

    assert cli.main(["--commit"]) == 3
    assert cli.main(["--compact"]) == 3


def test_cli_dry_run_accepts_ready_and_already_slim_schemas(monkeypatch):
    monkeypatch.setattr(cli.Config, "validate_odds_read_settings", lambda: None)
    audits = iter(
        (
            MarketChoicePriceStateMigrator.audit(_engine(legacy=True)),
            MarketChoicePriceStateMigrator.audit(_engine(legacy=False)),
        )
    )
    monkeypatch.setattr(
        cli.MarketChoicePriceStateMigrator,
        "audit",
        lambda _engine: next(audits),
    )
    class _SnapshotAudit:
        is_slim = True
        blockers = ()
        schema_state = "slim"

        @staticmethod
        def to_dict():
            return {"is_slim": True}

    monkeypatch.setattr(
        cli.MarketChoiceSnapshotSlimMigrator,
        "audit",
        lambda _engine: _SnapshotAudit(),
    )

    assert cli.main([]) == 0
    assert cli.main([]) == 0


def test_cli_commit_rebuilds_readers_around_the_ddl(monkeypatch):
    calls = []
    expanded = MarketChoicePriceStateMigrator.audit(_engine(legacy=True))
    slim = MarketChoicePriceStateMigrator.audit(_engine(legacy=False))

    class _SnapshotAudit:
        is_slim = True
        blockers = ()
        schema_state = "slim"

        @staticmethod
        def to_dict():
            return {"is_slim": True}

    class _Postflight:
        ok = True

        @staticmethod
        def to_dict():
            return {"ok": True}

    monkeypatch.setattr(cli.Config, "validate_odds_read_settings", lambda: None)
    monkeypatch.setattr(
        cli.MarketChoiceSnapshotSlimMigrator,
        "audit",
        lambda _engine: _SnapshotAudit(),
    )
    audits = iter((expanded, slim))
    monkeypatch.setattr(
        cli.MarketChoicePriceStateMigrator,
        "audit",
        lambda _engine: next(audits),
    )
    monkeypatch.setattr(
        cli,
        "create_or_replace_odds_read_views",
        lambda _engine: calls.append("views"),
    )
    monkeypatch.setattr(
        cli.MarketChoicePriceStateMigrator,
        "apply_postgresql",
        lambda *_args, **_kwargs: calls.append("ddl") or (expanded, slim),
    )
    monkeypatch.setattr(
        cli.MarketChoicePriceStateMigrator,
        "compact_postgresql",
        lambda _engine: calls.append("compact"),
    )
    monkeypatch.setattr(
        cli,
        "refresh_materialized_views",
        lambda _engine: calls.append("refresh"),
    )
    monkeypatch.setattr(
        cli.MarketChoiceSnapshotSlimPostflight,
        "run",
        lambda *_args, **_kwargs: calls.append("postflight") or _Postflight(),
    )

    assert cli.main(
        ["--commit", "--confirm-destructive", "--compact"]
    ) == 0
    assert calls == ["views", "ddl", "compact", "refresh", "postflight"]


def test_startup_schema_validation_is_fail_closed(tmp_path):
    expanded = DatabaseManager(f"sqlite:///{tmp_path / 'expanded.db'}")
    with expanded.engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE market_choices ("
            "choice_id INTEGER PRIMARY KEY, market_id INTEGER NOT NULL, "
            "choice_name TEXT NOT NULL, initial_odds NUMERIC, "
            "current_odds NUMERIC, change INTEGER)"
        )

    try:
        expanded._validate_market_choice_price_state_schema()
    except RuntimeError as exc:
        assert "migrate_market_choice_price_state" in str(exc)
    else:
        raise AssertionError("expanded schema must block application startup")

    slim = DatabaseManager(f"sqlite:///{tmp_path / 'slim.db'}")
    with slim.engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE market_choices ("
            "choice_id INTEGER PRIMARY KEY, market_id INTEGER NOT NULL, "
            "choice_name TEXT NOT NULL)"
        )
    slim._validate_market_choice_price_state_schema()
