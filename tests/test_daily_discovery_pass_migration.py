"""Keep existing progress and queried dates when renaming passes."""

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text


def test_upgrade_and_downgrade_preserve_progress():
    path = Path(__file__).parents[1] / "infrastructure/persistence/alembic/versions/20261007_01_daily_discovery_passes.py"
    spec = spec_from_file_location("daily_pass_migration", path)
    migration = module_from_spec(spec)
    spec.loader.exec_module(migration)
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text(
            "CREATE TABLE daily_discovery_log (id INTEGER PRIMARY KEY, date TEXT, "
            "run_slot TEXT, sport TEXT, status TEXT, attempts INTEGER, "
            "UNIQUE(date, run_slot, sport))"
        ))
        connection.execute(text(
            "INSERT INTO daily_discovery_log VALUES "
            "(1995, '2026-10-06', 'next_utc_day', 'football', 'completed', 1), "
            "(2019, '2026-10-06', 'current_utc_day', 'football', 'failed', 2)"
        ))
        before = connection.execute(text("SELECT * FROM daily_discovery_log ORDER BY id")).all()
        migration.op = Operations(MigrationContext.configure(connection))
        migration.upgrade()
        rows = connection.execute(text("SELECT * FROM daily_discovery_log ORDER BY id")).all()
        assert rows == [
            (1995, "2026-10-06", "anticipada", "football", "completed", 1),
            (2019, "2026-10-06", "actualizacion", "football", "failed", 2),
        ]
        migration.downgrade()
        assert connection.execute(text("SELECT * FROM daily_discovery_log ORDER BY id")).all() == before
    engine.dispose()
