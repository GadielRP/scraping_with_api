"""The startup gate must reject missing or stale migration state."""

from unittest.mock import Mock, patch

from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text

from infrastructure.persistence.schema_version import verify_schema_at_head


def test_schema_gate_requires_current_revision() -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE canonical_market_types (market_type_id INTEGER, requires_line_value BOOLEAN)"))
        connection.execute(text("CREATE TABLE market_source_mappings (market_type_id INTEGER)"))
        connection.execute(text("CREATE TABLE markets (market_type_id INTEGER, line_value NUMERIC)"))

    assert not verify_schema_at_head(engine)

    with engine.begin() as connection:
        connection.execute(text("INSERT INTO alembic_version VALUES ('20260919_01')"))
    assert not verify_schema_at_head(engine)

    head = ScriptDirectory.from_config(AlembicConfig("alembic.ini")).get_current_head()
    with engine.begin() as connection:
        connection.execute(text("UPDATE alembic_version SET version_num = :head"), {"head": head})
    assert verify_schema_at_head(engine)


def test_schema_gate_fails_closed_when_alembic_scripts_cannot_be_read() -> None:
    engine = create_engine("sqlite://")
    with patch(
        "infrastructure.persistence.schema_version.ScriptDirectory.from_config",
        side_effect=FileNotFoundError("missing migration scripts"),
    ):
        assert not verify_schema_at_head(engine)


def test_postgres_startup_checks_schema_without_creating_objects() -> None:
    from app import initialize

    manager = Mock()
    manager.engine.dialect.name = "postgresql"
    manager.test_connection.return_value = True
    with patch.object(initialize, "db_manager", manager), patch.object(
        initialize, "verify_schema_at_head", return_value=True
    ) as verify:
        assert initialize.initialize_system()

    verify.assert_called_once_with(manager.engine)
    manager.create_tables.assert_not_called()
