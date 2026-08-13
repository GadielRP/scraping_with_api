"""Fail-closed Phase 6 migration for ``market_choice_snapshots``.

This module owns schema inspection and the destructive PostgreSQL DDL. It does
not build application views or make quote-classification decisions.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Optional

from sqlalchemy import bindparam, inspect, text


TABLE_NAME = "market_choice_snapshots"
SLIM_COLUMNS = (
    "snapshot_id",
    "quote_id",
    "odds_value",
    "collected_at",
    "source_collected_at",
    "source_limit",
    "exchange_size",
)
REDUNDANT_COLUMNS = (
    "choice_id",
    "source",
    "source_market_id",
    "source_outcome_id",
    "bookmaker_outcome_id",
    "main_line",
    "exchange_side",
    "exchange_level",
)
LEGACY_INDEXES = (
    "idx_choice_collected",
    "idx_market_choice_snapshots_choice_collected_desc",
    "idx_market_choice_snapshots_source",
    "idx_market_choice_snapshots_source_collected",
    "idx_market_choice_snapshots_source_market",
)
QUOTE_INDEX = "idx_market_choice_snapshots_quote_collected"
# These canonical views may still be their pre-Phase-5 definitions when a
# server upgrades directly. The CLI replaces both with quote-aware SQL before
# taking the table lock and applying the destructive DDL. Any other dependency
# remains a blocker.
SCRIPT_MANAGED_DEPENDENT_VIEWS = frozenset(
    {
        "public.v_dual_process_event_odds",
        "public.v_pre_start_odds_trajectory",
    }
)


@dataclass(frozen=True, slots=True)
class SnapshotTableMetrics:
    row_count: int
    min_snapshot_id: Optional[int]
    max_snapshot_id: Optional[int]
    payload_checksum: str
    heap_bytes: Optional[int] = None
    index_bytes: Optional[int] = None
    total_bytes: Optional[int] = None


@dataclass(frozen=True, slots=True)
class SnapshotSlimAudit:
    schema_state: str
    columns: tuple[str, ...]
    quote_id_nullable: Optional[bool]
    null_quote_count: int
    orphan_quote_count: int
    choice_quote_mismatch_count: int
    dependent_columns: tuple[str, ...]
    indexes: tuple[str, ...]
    has_quote_foreign_key: bool
    metrics: SnapshotTableMetrics
    blockers: tuple[str, ...]

    @property
    def ready_to_migrate(self) -> bool:
        return self.schema_state == "expanded" and not self.blockers

    @property
    def is_slim(self) -> bool:
        return self.schema_state == "slim" and not self.blockers

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["ready_to_migrate"] = self.ready_to_migrate
        payload["is_slim"] = self.is_slim
        return payload


class MarketChoiceSnapshotSlimMigrator:
    """Audit and apply the one-way Phase 6 snapshot schema migration."""

    @classmethod
    def audit(cls, engine) -> SnapshotSlimAudit:
        with engine.connect() as connection:
            return cls._audit_connection(connection)

    @classmethod
    def _audit_connection(cls, connection) -> SnapshotSlimAudit:
        inspector = inspect(connection)
        if TABLE_NAME not in inspector.get_table_names():
            empty_metrics = SnapshotTableMetrics(0, None, None, "")
            return SnapshotSlimAudit(
                schema_state="missing",
                columns=(),
                quote_id_nullable=None,
                null_quote_count=0,
                orphan_quote_count=0,
                choice_quote_mismatch_count=0,
                dependent_columns=(),
                indexes=(),
                has_quote_foreign_key=False,
                metrics=empty_metrics,
                blockers=(f"missing_table:{TABLE_NAME}",),
            )

        column_rows = inspector.get_columns(TABLE_NAME)
        columns = tuple(item["name"] for item in column_rows)
        column_set = set(columns)
        slim_set = set(SLIM_COLUMNS)
        redundant_set = set(REDUNDANT_COLUMNS)
        if column_set == slim_set:
            schema_state = "slim"
        elif slim_set.issubset(column_set) and redundant_set.issubset(column_set):
            schema_state = "expanded"
        else:
            schema_state = "invalid"
        unexpected_columns = sorted(column_set - slim_set - redundant_set)

        quote_column = next(
            (item for item in column_rows if item["name"] == "quote_id"), None
        )
        quote_id_nullable = (
            bool(quote_column.get("nullable")) if quote_column is not None else None
        )
        indexes = tuple(
            sorted(
                item.get("name")
                for item in inspector.get_indexes(TABLE_NAME)
                if item.get("name")
            )
        )
        has_quote_foreign_key = any(
            item.get("referred_table") == "market_choice_quotes"
            and item.get("constrained_columns") == ["quote_id"]
            for item in inspector.get_foreign_keys(TABLE_NAME)
        )

        null_quote_count = 0
        orphan_quote_count = 0
        mismatch_count = 0
        if "quote_id" in column_set:
            null_quote_count = int(
                connection.execute(
                    text(
                        "SELECT COUNT(*) FROM market_choice_snapshots "
                        "WHERE quote_id IS NULL"
                    )
                ).scalar()
                or 0
            )
            orphan_quote_count = int(
                connection.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM market_choice_snapshots mcs
                        LEFT JOIN market_choice_quotes mcq
                          ON mcq.quote_id = mcs.quote_id
                        WHERE mcs.quote_id IS NOT NULL
                          AND mcq.quote_id IS NULL
                        """
                    )
                ).scalar()
                or 0
            )
        if {"choice_id", "quote_id"}.issubset(column_set):
            mismatch_count = int(
                connection.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM market_choice_snapshots mcs
                        JOIN market_choice_quotes mcq
                          ON mcq.quote_id = mcs.quote_id
                        WHERE mcs.choice_id <> mcq.choice_id
                        """
                    )
                ).scalar()
                or 0
            )

        dependent_columns = cls._dependent_redundant_columns(
            connection, column_set
        )
        metrics = cls._measure(connection, column_set)
        blockers: list[str] = []
        if schema_state == "invalid":
            missing = sorted(slim_set - column_set)
            blockers.extend(f"missing_column:{item}" for item in missing)
            if not missing and not unexpected_columns:
                blockers.append("partial_expanded_schema")
        blockers.extend(
            f"unexpected_column:{item}" for item in unexpected_columns
        )
        if null_quote_count:
            blockers.append(f"null_quote_id:{null_quote_count}")
        if orphan_quote_count:
            blockers.append(f"orphan_quote_id:{orphan_quote_count}")
        if mismatch_count:
            blockers.append(f"choice_quote_mismatch:{mismatch_count}")
        blockers.extend(
            f"dependent_column:{item}"
            for item in dependent_columns
            if item.rsplit(":", 1)[0] not in SCRIPT_MANAGED_DEPENDENT_VIEWS
        )
        if QUOTE_INDEX not in indexes:
            blockers.append(f"missing_index:{QUOTE_INDEX}")
        if not has_quote_foreign_key:
            blockers.append("missing_quote_foreign_key")
        if schema_state == "slim" and quote_id_nullable:
            blockers.append("quote_id_still_nullable")
        if schema_state == "slim":
            blockers.extend(
                f"legacy_index_still_present:{item}"
                for item in LEGACY_INDEXES
                if item in indexes
            )

        return SnapshotSlimAudit(
            schema_state=schema_state,
            columns=columns,
            quote_id_nullable=quote_id_nullable,
            null_quote_count=null_quote_count,
            orphan_quote_count=orphan_quote_count,
            choice_quote_mismatch_count=mismatch_count,
            dependent_columns=dependent_columns,
            indexes=indexes,
            has_quote_foreign_key=has_quote_foreign_key,
            metrics=metrics,
            blockers=tuple(blockers),
        )

    @staticmethod
    def _dependent_redundant_columns(
        connection, column_set: set[str]
    ) -> tuple[str, ...]:
        redundant = sorted(set(REDUNDANT_COLUMNS) & column_set)
        if not redundant:
            return ()
        if connection.dialect.name == "postgresql":
            statement = text(
                    """
                    SELECT DISTINCT
                        concat(vu.view_schema, '.', vu.view_name, ':', vu.column_name)
                    FROM information_schema.view_column_usage vu
                    WHERE vu.table_schema = 'public'
                      AND vu.table_name = 'market_choice_snapshots'
                      AND vu.column_name IN :columns
                    ORDER BY 1
                    """
                ).bindparams(bindparam("columns", expanding=True))
            rows = connection.execute(
                statement,
                {"columns": redundant},
            ).scalars()
            return tuple(str(item) for item in rows)

        if connection.dialect.name == "sqlite":
            view_rows = connection.execute(
                text("SELECT name, sql FROM sqlite_master WHERE type = 'view'")
            ).all()
            dependencies = []
            for view_name, definition in view_rows:
                normalized = str(definition or "").casefold()
                if TABLE_NAME not in normalized:
                    continue
                for column in redundant:
                    if column.casefold() in normalized:
                        dependencies.append(f"main.{view_name}:{column}")
            return tuple(sorted(set(dependencies)))
        return ()

    @classmethod
    def _measure(
        cls, connection, column_set: set[str]
    ) -> SnapshotTableMetrics:
        row = connection.execute(
            text(
                "SELECT COUNT(*), MIN(snapshot_id), MAX(snapshot_id) "
                "FROM market_choice_snapshots"
            )
        ).one()
        checksum = cls._payload_checksum(connection, column_set)
        heap_bytes = index_bytes = total_bytes = None
        if connection.dialect.name == "postgresql":
            heap_bytes, index_bytes, total_bytes = connection.execute(
                text(
                    """
                    SELECT
                        pg_relation_size('public.market_choice_snapshots'),
                        pg_indexes_size('public.market_choice_snapshots'),
                        pg_total_relation_size('public.market_choice_snapshots')
                    """
                )
            ).one()
        return SnapshotTableMetrics(
            row_count=int(row[0] or 0),
            min_snapshot_id=int(row[1]) if row[1] is not None else None,
            max_snapshot_id=int(row[2]) if row[2] is not None else None,
            payload_checksum=checksum,
            heap_bytes=int(heap_bytes) if heap_bytes is not None else None,
            index_bytes=int(index_bytes) if index_bytes is not None else None,
            total_bytes=int(total_bytes) if total_bytes is not None else None,
        )

    @staticmethod
    def _payload_checksum(connection, column_set: set[str]) -> str:
        payload_columns = [item for item in SLIM_COLUMNS if item in column_set]
        if not payload_columns:
            return ""
        if connection.dialect.name == "postgresql":
            expressions = ", ".join(
                f"COALESCE({column}::text, '<NULL>')" for column in payload_columns
            )
            return str(
                connection.execute(
                    text(
                        f"""
                        WITH row_hashes AS (
                            SELECT hashtextextended(
                                concat_ws(E'\\x1f', {expressions}), 0
                            ) AS row_hash
                            FROM market_choice_snapshots
                        )
                        SELECT md5(
                            concat_ws('|',
                                COUNT(*),
                                COALESCE(SUM(row_hash::numeric), 0),
                                COALESCE(bit_xor(row_hash), 0)
                            )
                        )
                        FROM row_hashes
                        """
                    )
                ).scalar()
            )

        digest = hashlib.sha256()
        rendered = ", ".join(payload_columns)
        for row in connection.execute(
            text(
                f"SELECT {rendered} FROM market_choice_snapshots "
                "ORDER BY snapshot_id"
            )
        ):
            digest.update(repr(tuple(row)).encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest()

    @classmethod
    def apply_postgresql(
        cls,
        engine,
        *,
        lock_timeout_ms: int = 5_000,
        statement_timeout_ms: int = 120_000,
    ) -> tuple[SnapshotSlimAudit, SnapshotSlimAudit]:
        if engine.dialect.name != "postgresql":
            raise ValueError("Phase 6 destructive DDL currently requires PostgreSQL")
        if lock_timeout_ms <= 0 or statement_timeout_ms <= 0:
            raise ValueError("Migration timeouts must be positive")

        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"SET LOCAL lock_timeout = '{int(lock_timeout_ms)}ms'"
            )
            connection.exec_driver_sql(
                f"SET LOCAL statement_timeout = '{int(statement_timeout_ms)}ms'"
            )
            connection.exec_driver_sql(
                "LOCK TABLE public.market_choice_snapshots "
                "IN ACCESS EXCLUSIVE MODE"
            )
            before = cls._audit_connection(connection)
            if before.is_slim:
                return before, before
            if not before.ready_to_migrate:
                raise RuntimeError(
                    "Snapshot slim migration blocked: "
                    + ", ".join(before.blockers or (before.schema_state,))
                )

            for statement in cls.postgresql_ddl_statements():
                connection.exec_driver_sql(statement)

            after = cls._audit_connection(connection)
            if not after.is_slim:
                raise RuntimeError(
                    "Snapshot slim postflight failed: "
                    + ", ".join(after.blockers or (after.schema_state,))
                )
            before_payload = (
                before.metrics.row_count,
                before.metrics.min_snapshot_id,
                before.metrics.max_snapshot_id,
                before.metrics.payload_checksum,
            )
            after_payload = (
                after.metrics.row_count,
                after.metrics.min_snapshot_id,
                after.metrics.max_snapshot_id,
                after.metrics.payload_checksum,
            )
            if before_payload != after_payload:
                raise RuntimeError("Snapshot slim payload changed during migration")
            return before, after

    @staticmethod
    def postgresql_ddl_statements() -> tuple[str, ...]:
        drop_columns = ", ".join(
            f'DROP COLUMN "{column}"' for column in REDUNDANT_COLUMNS
        )
        return (
            "ALTER TABLE public.market_choice_snapshots "
            "ALTER COLUMN quote_id SET NOT NULL",
            *tuple(
                f'DROP INDEX IF EXISTS public."{index_name}"'
                for index_name in LEGACY_INDEXES
            ),
            "ALTER TABLE public.market_choice_snapshots " + drop_columns,
        )

    @staticmethod
    def compact_postgresql(engine) -> None:
        if engine.dialect.name != "postgresql":
            raise ValueError("VACUUM FULL is only supported here for PostgreSQL")
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.exec_driver_sql(
                "VACUUM (FULL, ANALYZE) public.market_choice_snapshots"
            )

__all__ = [
    "LEGACY_INDEXES",
    "MarketChoiceSnapshotSlimMigrator",
    "REDUNDANT_COLUMNS",
    "SCRIPT_MANAGED_DEPENDENT_VIEWS",
    "SLIM_COLUMNS",
    "SnapshotSlimAudit",
    "SnapshotTableMetrics",
]
