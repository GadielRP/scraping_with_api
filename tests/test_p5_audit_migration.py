"""Exercise additive migration lifecycle and PostgreSQL deployment DDL."""

import importlib.util
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, text


def migration():
    path = (
        Path(__file__).parents[1]
        / "infrastructure/persistence/alembic/versions/20261002_01_p5_auditable_samples.py"
    )
    spec = importlib.util.spec_from_file_location("p5_audit_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_additive_upgrade_and_downgrade_preserve_existing_runs():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE pillar_mining_runs(id INTEGER PRIMARY KEY)")
        )
        connection.execute(text("INSERT INTO pillar_mining_runs VALUES (17)"))
        module = migration()
        module.op = Operations(MigrationContext.configure(connection))
        module.upgrade()
        assert set(inspect(connection).get_table_names()) == {
            "pillar_mining_runs",
            "p5_memory_samples",
            "p5_memory_sample_members",
        }
        assert (
            inspect(connection).get_foreign_keys("p5_memory_samples")[0]["options"][
                "ondelete"
            ]
            == "CASCADE"
        )
        module.downgrade()
        assert inspect(connection).get_table_names() == ["pillar_mining_runs"]
        assert (
            connection.execute(text("SELECT id FROM pillar_mining_runs")).scalar_one()
            == 17
        )
    engine.dispose()


def test_postgresql_migration_has_typed_timestamps_json_and_cascades():
    output = StringIO()
    module = migration()
    module.op = Operations(
        MigrationContext.configure(
            dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output}
        )
    )
    module.upgrade()
    ddl = output.getvalue()
    assert "TIMESTAMP WITH TIME ZONE" in ddl
    assert "JSONB" in ddl
    assert ddl.count("ON DELETE CASCADE") == 2
    assert "ix_p5_sample_page" in ddl


def _orm_audit_tables():
    from infrastructure.persistence.models import P5MemorySample, P5MemorySampleMember

    metadata = sa.MetaData()
    runs = sa.Table(
        "pillar_mining_runs", metadata, sa.Column("id", sa.Integer, primary_key=True)
    )
    header = P5MemorySample.__table__.to_metadata(metadata)
    members = P5MemorySampleMember.__table__.to_metadata(metadata)
    return runs, header, members


def test_upgrade_adopts_orm_tables_preserving_data_and_can_repeat():
    engine = create_engine("sqlite://")
    runs, header, members = _orm_audit_tables()
    with engine.begin() as connection:
        runs.metadata.create_all(connection)
        connection.execute(runs.insert().values(id=17))
        instant = datetime(2026, 10, 1, tzinfo=timezone.utc)
        connection.execute(
            header.insert().values(
                sample_id="frozen-sample",
                run_id=17,
                query={"key": "original"},
                cutoff=instant,
                policy_version="market-evaluation-v1",
                sample_size=1,
                wins_home=1,
                wins_draw=0,
                wins_away=0,
            )
        )
        connection.execute(
            members.insert().values(
                sample_id="frozen-sample",
                event_id=42,
                starts_at=instant,
                odds_home=2,
                odds_away=3,
                home_score=1,
                away_score=0,
                winner_side="HOME",
                outcome="HOME",
            )
        )
        before_header = connection.execute(header.select()).mappings().all()
        before_members = connection.execute(members.select()).mappings().all()
        module = migration()
        module.op = Operations(MigrationContext.configure(connection))
        module.upgrade()
        module.upgrade()
        assert connection.execute(header.select()).mappings().all() == before_header
        assert connection.execute(members.select()).mappings().all() == before_members
    engine.dispose()


@pytest.mark.parametrize(
    "existing_tables",
    [
        ("p5_memory_samples",),
        ("p5_memory_sample_members",),
        ("p5_memory_samples", "p5_memory_sample_members"),
    ],
)
def test_upgrade_completes_partial_creation_and_missing_indexes(existing_tables):
    engine = create_engine("sqlite://")
    runs, header, members = _orm_audit_tables()
    with engine.begin() as connection:
        runs.create(connection)
        for table in (header, members):
            if table.name in existing_tables:
                table.create(connection)
                for index in table.indexes:
                    index.drop(connection)
        module = migration()
        module.op = Operations(MigrationContext.configure(connection))
        module.upgrade()
        inspector = inspect(connection)
        assert inspector.has_table(header.name) and inspector.has_table(members.name)
        assert inspector.get_indexes(header.name)[0]["column_names"] == ["run_id"]
        assert inspector.get_indexes(members.name)[0]["column_names"] == [
            "sample_id", "starts_at", "event_id"
        ]
    engine.dispose()


@pytest.mark.parametrize(
    "drift", ["type", "nullable", "primary_key", "foreign_key", "column", "index"]
)
def test_upgrade_rejects_incompatible_existing_tables_before_ddl(drift):
    engine = create_engine("sqlite://")
    runs, header, members = _orm_audit_tables()
    if drift == "type":
        header.c.sample_size.type = sa.String(20)
    elif drift == "nullable":
        header.c.sample_size.nullable = True
    elif drift == "primary_key":
        header.c.run_id.primary_key = True
        header.append_constraint(sa.PrimaryKeyConstraint("sample_id", "run_id"))
        header.c.run_id.nullable = False
    elif drift == "foreign_key":
        next(iter(header.foreign_key_constraints)).ondelete = "RESTRICT"
    elif drift == "column":
        header.append_column(sa.Column("unexpected", sa.Integer))
    elif drift == "index":
        header.indexes.clear()
        sa.Index("ix_p5_memory_samples_run_id", header.c.sample_size)
    with engine.begin() as connection:
        runs.create(connection)
        header.create(connection)
        module = migration()
        module.op = Operations(MigrationContext.configure(connection))
        with pytest.raises(RuntimeError, match="Cannot adopt existing p5_memory_samples"):
            module.upgrade()
        assert not inspect(connection).has_table(members.name)
        assert inspect(connection).has_table(header.name)
    engine.dispose()
