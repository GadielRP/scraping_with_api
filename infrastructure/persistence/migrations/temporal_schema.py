"""Migrate every persisted absolute instant to the canonical UTC contract.

Historical ``timestamp without time zone`` values in this database represent
``America/Mexico_City`` wall-clock time.  The migration makes that provenance
explicit, converts each instant to PostgreSQL ``timestamptz``, and verifies
the expected epoch row by row before committing.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import inspect, text


logger = logging.getLogger(__name__)

LEGACY_DATABASE_TIMEZONE = "America/Mexico_City"


@dataclass(frozen=True)
class ColumnRename:
    table_name: str
    old_name: str
    new_name: str


@dataclass(frozen=True)
class InstantColumnMigration:
    table_name: str
    column_name: str
    primary_key: str
    legacy_timezone: str = LEGACY_DATABASE_TIMEZONE


COLUMN_RENAMES = (
    ColumnRename("events", "start_time_utc", "starts_at"),
    ColumnRename(
        "event_source_resolution_queue",
        "source_start_time_utc",
        "source_starts_at",
    ),
)


INSTANT_COLUMN_MIGRATIONS = (
    InstantColumnMigration("participants", "created_at", "participant_id"),
    InstantColumnMigration("participants", "updated_at", "participant_id"),
    InstantColumnMigration("competitions", "created_at", "competition_id"),
    InstantColumnMigration("competitions", "updated_at", "competition_id"),
    InstantColumnMigration("events", "starts_at", "id"),
    InstantColumnMigration("events", "created_at", "id"),
    InstantColumnMigration("events", "updated_at", "id"),
    InstantColumnMigration("event_source_mappings", "created_at", "mapping_id"),
    InstantColumnMigration("event_source_mappings", "updated_at", "mapping_id"),
    InstantColumnMigration(
        "event_source_resolution_queue", "source_starts_at", "queue_id"
    ),
    InstantColumnMigration(
        "event_source_resolution_queue", "first_seen_at", "queue_id"
    ),
    InstantColumnMigration(
        "event_source_resolution_queue", "last_attempted_at", "queue_id"
    ),
    InstantColumnMigration(
        "event_source_resolution_queue", "updated_at", "queue_id"
    ),
    InstantColumnMigration("event_observations", "created_at", "observation_id"),
    InstantColumnMigration("event_observations", "updated_at", "observation_id"),
    InstantColumnMigration("pillar_mining_runs", "calculated_at", "id"),
    InstantColumnMigration("pillar_mining_runs", "created_at", "id"),
    InstantColumnMigration("pillar_mining_runs", "updated_at", "id"),
    InstantColumnMigration("pillar_mining_units", "created_at", "id"),
    InstantColumnMigration("pillar_mining_units", "updated_at", "id"),
    InstantColumnMigration("pillar_mining_metric_values", "created_at", "id"),
    InstantColumnMigration("pillar_mining_metric_values", "updated_at", "id"),
    InstantColumnMigration("bookie_source_mappings", "created_at", "mapping_id"),
    InstantColumnMigration("bookie_source_mappings", "updated_at", "mapping_id"),
    InstantColumnMigration(
        "canonical_market_types", "created_at", "canonical_market_key"
    ),
    InstantColumnMigration(
        "canonical_market_types", "updated_at", "canonical_market_key"
    ),
    InstantColumnMigration("market_source_mappings", "created_at", "mapping_id"),
    InstantColumnMigration("market_source_mappings", "updated_at", "mapping_id"),
    InstantColumnMigration(
        "market_outcome_source_mappings", "created_at", "outcome_mapping_id"
    ),
    InstantColumnMigration(
        "market_outcome_source_mappings", "updated_at", "outcome_mapping_id"
    ),
    InstantColumnMigration("source_catalog_syncs", "imported_at", "sync_id"),
    InstantColumnMigration("source_catalog_syncs", "created_at", "sync_id"),
    InstantColumnMigration("markets", "collected_at", "market_id"),
    InstantColumnMigration("market_choice_snapshots", "collected_at", "snapshot_id"),
    InstantColumnMigration(
        "market_choice_snapshots", "source_collected_at", "snapshot_id"
    ),
    InstantColumnMigration("market_choice_quotes", "initial_captured_at", "quote_id"),
    InstantColumnMigration("market_choice_quotes", "current_updated_at", "quote_id"),
    InstantColumnMigration("market_choice_quotes", "created_at", "quote_id"),
    InstantColumnMigration("market_choice_quotes", "updated_at", "quote_id"),
    InstantColumnMigration("oddsportal_league_cache", "cached_date", "season_id"),
    InstantColumnMigration("oddsportal_league_cache", "created_at", "season_id"),
    InstantColumnMigration("daily_discovery_log", "last_attempt_at", "id"),
    InstantColumnMigration("daily_discovery_log", "created_at", "id"),
    InstantColumnMigration("oddspapi_fixture_discovery_runs", "started_at", "id"),
    InstantColumnMigration("oddspapi_fixture_discovery_runs", "heartbeat_at", "id"),
    InstantColumnMigration("oddspapi_fixture_discovery_runs", "finished_at", "id"),
    InstantColumnMigration(
        "oddspapi_api_key_usage", "subscription_valid_from", "key_fingerprint"
    ),
    InstantColumnMigration(
        "oddspapi_api_key_usage", "subscription_valid_until", "key_fingerprint"
    ),
    InstantColumnMigration(
        "oddspapi_api_key_usage", "account_refreshed_at", "key_fingerprint"
    ),
    InstantColumnMigration(
        "oddspapi_api_key_usage", "last_error_at", "key_fingerprint"
    ),
    InstantColumnMigration(
        "oddspapi_api_key_usage", "updated_at", "key_fingerprint"
    ),
    InstantColumnMigration(
        "oddspapi_mainline_outcome_cache", "captured_at", "cache_id"
    ),
    InstantColumnMigration("event_migration_status", "completed_at", "migration_key"),
)


RETIRED_READ_MODELS = ("basketball_results_season_year",)
MANAGED_VIEWS = (
    "v_pre_start_odds_trajectory",
    "v_market_choice_trajectory",
    "v_dual_process_event_odds",
    "event_all_odds",
    "basketball_results",
    "season_events_with_results",
)
MANAGED_MATERIALIZED_VIEWS = ("mv_alert_events",)


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _timezone_literal(value: str) -> str:
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown IANA timezone: {value}") from exc
    return value.replace("'", "''")


def _column_metadata(connection, table_name: str, column_name: str):
    inspector = inspect(connection)
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
    return bool(getattr(column_metadata["type"], "timezone", False))


def _naive_timestamp_columns(connection) -> list[str]:
    rows = connection.execute(
        text(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND data_type = 'timestamp without time zone'
            ORDER BY table_name, ordinal_position
            """
        )
    ).all()
    return [f"{table_name}.{column_name}" for table_name, column_name in rows]


def _has_pending_work(engine) -> bool:
    with engine.connect() as connection:
        for rename in COLUMN_RENAMES:
            old = _column_metadata(connection, rename.table_name, rename.old_name)
            new = _column_metadata(connection, rename.table_name, rename.new_name)
            if old is not None and new is None:
                return True
        for migration in INSTANT_COLUMN_MIGRATIONS:
            metadata = _column_metadata(
                connection, migration.table_name, migration.column_name
            )
            if metadata is not None and not _is_timezone_aware(metadata):
                return True
        if _naive_timestamp_columns(connection):
            return True
    return False


def _drop_read_models(connection) -> None:
    for name in RETIRED_READ_MODELS:
        connection.exec_driver_sql(
            f"DROP VIEW IF EXISTS {_identifier(name)};"
        )
    for name in MANAGED_MATERIALIZED_VIEWS:
        connection.exec_driver_sql(
            f"DROP MATERIALIZED VIEW IF EXISTS {_identifier(name)} CASCADE;"
        )
    for name in MANAGED_VIEWS:
        connection.exec_driver_sql(
            f"DROP VIEW IF EXISTS {_identifier(name)} CASCADE;"
        )


def _rename_columns(connection, applied: list[str]) -> None:
    for rename in COLUMN_RENAMES:
        table = _identifier(rename.table_name)
        old = _identifier(rename.old_name)
        new = _identifier(rename.new_name)
        old_metadata = _column_metadata(connection, table, old)
        new_metadata = _column_metadata(connection, table, new)
        if old_metadata is None:
            continue
        if new_metadata is not None:
            raise RuntimeError(
                f"Cannot rename {table}.{old}: {table}.{new} already exists"
            )
        connection.exec_driver_sql(
            f"ALTER TABLE {table} RENAME COLUMN {old} TO {new};"
        )
        applied.append(f"renamed:{table}.{old}->{new}")


def _migrate_column(
    connection,
    migration: InstantColumnMigration,
    *,
    sequence: int,
) -> bool:
    table = _identifier(migration.table_name)
    column = _identifier(migration.column_name)
    primary_key = _identifier(migration.primary_key)
    metadata = _column_metadata(connection, table, column)
    if metadata is None or _is_timezone_aware(metadata):
        return False

    timezone_literal = _timezone_literal(migration.legacy_timezone)
    expected_table = f"expected_instant_epochs_{sequence}"
    connection.exec_driver_sql(
        f"""
        CREATE TEMPORARY TABLE {expected_table} ON COMMIT DROP AS
        SELECT
            {primary_key} AS row_id,
            EXTRACT(EPOCH FROM (
                {column} AT TIME ZONE '{timezone_literal}'
            )) AS expected_epoch
        FROM {table};
        """
    )
    row_count = connection.execute(
        text(f"SELECT COUNT(*) FROM {expected_table}")
    ).scalar_one()
    logger.info(
        "Migrating %s row(s) in %s.%s from %s-naive to timestamptz",
        row_count,
        table,
        column,
        migration.legacy_timezone,
    )
    connection.exec_driver_sql(
        f"""
        ALTER TABLE {table}
        ALTER COLUMN {column}
        TYPE TIMESTAMP WITH TIME ZONE
        USING {column} AT TIME ZONE '{timezone_literal}';
        """
    )
    mismatch_count = connection.execute(
        text(
            f"""
            SELECT COUNT(*)
            FROM {table} migrated
            JOIN {expected_table} expected
              ON expected.row_id = migrated.{primary_key}
            WHERE expected.expected_epoch IS DISTINCT FROM
                  EXTRACT(EPOCH FROM migrated.{column});
            """
        )
    ).scalar_one()
    if mismatch_count:
        raise RuntimeError(
            f"Timezone migration changed {mismatch_count} unexpected epoch(s) "
            f"in {table}.{column}"
        )
    return True


def migrate_temporal_schema(engine) -> list[str]:
    """Rename temporal columns and migrate every declared instant atomically."""
    if engine.dialect.name != "postgresql":
        logger.debug("Skipping PostgreSQL temporal migration on %s", engine.dialect.name)
        return []
    if not _has_pending_work(engine):
        return []

    applied: list[str] = []
    with engine.begin() as connection:
        connection.exec_driver_sql("SET LOCAL TIME ZONE 'UTC';")
        _drop_read_models(connection)
        _rename_columns(connection, applied)
        for sequence, migration in enumerate(INSTANT_COLUMN_MIGRATIONS, start=1):
            if _migrate_column(connection, migration, sequence=sequence):
                applied.append(
                    f"converted:{migration.table_name}.{migration.column_name}"
                )
        remaining_naive = _naive_timestamp_columns(connection)
        if remaining_naive:
            raise RuntimeError(
                "Temporal migration is incomplete; declare how to migrate these "
                "timestamp-without-time-zone columns: " + ", ".join(remaining_naive)
            )
    return applied


__all__ = [
    "COLUMN_RENAMES",
    "INSTANT_COLUMN_MIGRATIONS",
    "LEGACY_DATABASE_TIMEZONE",
    "ColumnRename",
    "InstantColumnMigration",
    "migrate_temporal_schema",
]
