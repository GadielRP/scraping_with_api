"""Migrate canonical event instants to timezone-aware PostgreSQL columns."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import inspect, text


logger = logging.getLogger(__name__)

# This view belongs to an older basketball read model. It is absent from the
# current application, has no database dependants, and is intentionally not
# recreated after the event timestamp migration.
RETIRED_EVENT_VIEWS = (
    "basketball_results_season_year",
)


@dataclass(frozen=True)
class TimestampMigration:
    table_name: str
    column_name: str
    primary_key: str
    legacy_timezone: str
    verification_table: str
    drops_event_views: bool = False


EVENT_TIMESTAMP_MIGRATIONS = (
    TimestampMigration(
        table_name="events",
        column_name="start_time_utc",
        primary_key="id",
        legacy_timezone="America/Mexico_City",
        verification_table="expected_event_start_epochs",
        drops_event_views=True,
    ),
    TimestampMigration(
        table_name="event_source_resolution_queue",
        column_name="source_start_time_utc",
        primary_key="queue_id",
        legacy_timezone="UTC",
        verification_table="expected_queue_start_epochs",
    ),
)


def _validated_timezone_literal(timezone_name: str) -> str:
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown IANA timezone: {timezone_name}") from exc
    return timezone_name.replace("'", "''")


def _column_metadata(engine, table_name: str, column_name: str):
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return None
    return next(
        (
            column
            for column in inspector.get_columns(table_name)
            if column["name"] == column_name
        ),
        None,
    )


def _is_timezone_aware(column_metadata) -> bool:
    return bool(getattr(column_metadata.get("type"), "timezone", False))


def _drop_event_read_models(connection) -> None:
    """Drop read models that depend on ``events.start_time_utc``.

    Startup recreates the managed read models after schema migrations complete.
    Retired views are dropped without CASCADE so an unknown dependency stops
    the migration instead of being silently destroyed.
    """
    for view_name in RETIRED_EVENT_VIEWS:
        connection.exec_driver_sql(f"DROP VIEW IF EXISTS {view_name};")

    connection.exec_driver_sql("DROP MATERIALIZED VIEW IF EXISTS mv_alert_events;")
    connection.exec_driver_sql("DROP VIEW IF EXISTS basketball_results CASCADE;")
    connection.exec_driver_sql("DROP VIEW IF EXISTS season_events_with_results CASCADE;")
    connection.exec_driver_sql("DROP VIEW IF EXISTS event_all_odds CASCADE;")
    connection.exec_driver_sql("DROP VIEW IF EXISTS v_dual_process_event_odds CASCADE;")
    connection.exec_driver_sql("DROP VIEW IF EXISTS v_pre_start_odds_trajectory CASCADE;")


def _migrate_column(engine, migration: TimestampMigration) -> bool:
    metadata = _column_metadata(
        engine,
        migration.table_name,
        migration.column_name,
    )
    if metadata is None:
        logger.debug(
            "Skipping timezone migration for missing %s.%s",
            migration.table_name,
            migration.column_name,
        )
        return False
    if _is_timezone_aware(metadata):
        logger.debug(
            "%s.%s is already timezone-aware",
            migration.table_name,
            migration.column_name,
        )
        return False

    timezone_literal = _validated_timezone_literal(migration.legacy_timezone)
    expected_table = migration.verification_table

    with engine.begin() as connection:
        connection.exec_driver_sql("SET LOCAL TIME ZONE 'UTC';")
        if migration.drops_event_views:
            _drop_event_read_models(connection)

        connection.exec_driver_sql(
            f"""
            CREATE TEMPORARY TABLE {expected_table} ON COMMIT DROP AS
            SELECT
                {migration.primary_key} AS id,
                EXTRACT(EPOCH FROM (
                    {migration.column_name} AT TIME ZONE '{timezone_literal}'
                )) AS expected_epoch
            FROM {migration.table_name};
            """
        )

        row_count = connection.execute(
            text(f"SELECT COUNT(*) FROM {expected_table}")
        ).scalar_one()
        logger.info(
            "Migrating %s row(s) in %s.%s from %s-naive to timestamptz",
            row_count,
            migration.table_name,
            migration.column_name,
            migration.legacy_timezone,
        )

        connection.exec_driver_sql(
            f"""
            ALTER TABLE {migration.table_name}
            ALTER COLUMN {migration.column_name}
            TYPE TIMESTAMP WITH TIME ZONE
            USING {migration.column_name} AT TIME ZONE '{timezone_literal}';
            """
        )

        mismatch_count = connection.execute(
            text(
                f"""
                SELECT COUNT(*)
                FROM {migration.table_name} migrated
                JOIN {expected_table} expected
                  ON expected.id = migrated.{migration.primary_key}
                WHERE expected.expected_epoch IS DISTINCT FROM
                      EXTRACT(EPOCH FROM migrated.{migration.column_name});
                """
            )
        ).scalar_one()
        if mismatch_count:
            raise RuntimeError(
                f"Timezone migration changed {mismatch_count} unexpected epoch(s) "
                f"in {migration.table_name}.{migration.column_name}"
            )

    logger.info(
        "Migrated %s.%s from %s-naive to timestamptz",
        migration.table_name,
        migration.column_name,
        migration.legacy_timezone,
    )
    return True


def migrate_event_timezones(engine) -> list[str]:
    """Apply all event-time migrations required by the current schema."""
    if engine.dialect.name != "postgresql":
        logger.debug("Skipping PostgreSQL timezone migration on %s", engine.dialect.name)
        return []

    migrated: list[str] = []
    for migration in EVENT_TIMESTAMP_MIGRATIONS:
        if _migrate_column(engine, migration):
            migrated.append(f"{migration.table_name}.{migration.column_name}")
    return migrated


__all__ = [
    "EVENT_TIMESTAMP_MIGRATIONS",
    "RETIRED_EVENT_VIEWS",
    "TimestampMigration",
    "migrate_event_timezones",
]
