"""Fail-closed Phase 7 migration for ``market_choices``.

``MarketChoice`` owns outcome identity only. Price state belongs to
``MarketChoiceQuote``. This module audits that boundary and owns the one-way
PostgreSQL DDL that removes the frozen price mirror.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Optional

from sqlalchemy import bindparam, inspect, text


TABLE_NAME = "market_choices"
IDENTITY_COLUMNS = ("choice_id", "market_id", "choice_name")
LEGACY_PRICE_COLUMNS = ("initial_odds", "current_odds", "change")
SCRIPT_MANAGED_DEPENDENT_VIEWS = frozenset(
    {
        "public.v_dual_process_event_odds",
        "public.v_pre_start_odds_trajectory",
    }
)


@dataclass(frozen=True, slots=True)
class MarketChoiceMetrics:
    row_count: int
    min_choice_id: Optional[int]
    max_choice_id: Optional[int]
    identity_checksum: str
    heap_bytes: Optional[int] = None
    index_bytes: Optional[int] = None
    total_bytes: Optional[int] = None


@dataclass(frozen=True, slots=True)
class MarketChoicePriceStateAudit:
    schema_state: str
    columns: tuple[str, ...]
    dependent_columns: tuple[str, ...]
    metrics: MarketChoiceMetrics
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


class MarketChoicePriceStateMigrator:
    """Audit and apply the one-way Phase 7 price-state removal."""

    @classmethod
    def audit(cls, engine) -> MarketChoicePriceStateAudit:
        with engine.connect() as connection:
            return cls._audit_connection(connection)

    @classmethod
    def _audit_connection(cls, connection) -> MarketChoicePriceStateAudit:
        inspector = inspect(connection)
        if TABLE_NAME not in inspector.get_table_names():
            return MarketChoicePriceStateAudit(
                schema_state="missing",
                columns=(),
                dependent_columns=(),
                metrics=MarketChoiceMetrics(0, None, None, ""),
                blockers=(f"missing_table:{TABLE_NAME}",),
            )

        columns = tuple(
            item["name"] for item in inspector.get_columns(TABLE_NAME)
        )
        column_set = set(columns)
        identity_set = set(IDENTITY_COLUMNS)
        legacy_set = set(LEGACY_PRICE_COLUMNS)
        unexpected = sorted(column_set - identity_set - legacy_set)

        if column_set == identity_set:
            schema_state = "slim"
        elif column_set == identity_set | legacy_set:
            schema_state = "expanded"
        else:
            schema_state = "invalid"

        dependent_columns = cls._dependent_legacy_columns(
            connection, column_set
        )
        blockers: list[str] = []
        if schema_state == "invalid":
            blockers.extend(
                f"missing_identity_column:{item}"
                for item in sorted(identity_set - column_set)
            )
            present_legacy = legacy_set & column_set
            if present_legacy and present_legacy != legacy_set:
                blockers.append("partial_legacy_price_schema")
        blockers.extend(f"unexpected_column:{item}" for item in unexpected)
        blockers.extend(
            f"dependent_column:{item}"
            for item in dependent_columns
            if item.rsplit(":", 1)[0]
            not in SCRIPT_MANAGED_DEPENDENT_VIEWS
        )

        return MarketChoicePriceStateAudit(
            schema_state=schema_state,
            columns=columns,
            dependent_columns=dependent_columns,
            metrics=cls._measure(connection),
            blockers=tuple(blockers),
        )

    @staticmethod
    def _dependent_legacy_columns(
        connection, column_set: set[str]
    ) -> tuple[str, ...]:
        legacy = sorted(set(LEGACY_PRICE_COLUMNS) & column_set)
        if not legacy:
            return ()
        if connection.dialect.name == "postgresql":
            statement = text(
                """
                SELECT DISTINCT
                    concat(vu.view_schema, '.', vu.view_name, ':', vu.column_name)
                FROM information_schema.view_column_usage vu
                WHERE vu.table_schema = 'public'
                  AND vu.table_name = 'market_choices'
                  AND vu.column_name IN :columns
                ORDER BY 1
                """
            ).bindparams(bindparam("columns", expanding=True))
            return tuple(
                str(item)
                for item in connection.execute(
                    statement, {"columns": legacy}
                ).scalars()
            )
        if connection.dialect.name == "sqlite":
            dependencies = []
            rows = connection.execute(
                text("SELECT name, sql FROM sqlite_master WHERE type = 'view'")
            ).all()
            for view_name, definition in rows:
                normalized = str(definition or "").casefold()
                if TABLE_NAME not in normalized:
                    continue
                for column in legacy:
                    if column.casefold() in normalized:
                        dependencies.append(f"main.{view_name}:{column}")
            return tuple(sorted(set(dependencies)))
        return ()

    @classmethod
    def _measure(cls, connection) -> MarketChoiceMetrics:
        row = connection.execute(
            text(
                "SELECT COUNT(*), MIN(choice_id), MAX(choice_id) "
                "FROM market_choices"
            )
        ).one()
        heap_bytes = index_bytes = total_bytes = None
        if connection.dialect.name == "postgresql":
            heap_bytes, index_bytes, total_bytes = connection.execute(
                text(
                    """
                    SELECT
                        pg_relation_size('public.market_choices'),
                        pg_indexes_size('public.market_choices'),
                        pg_total_relation_size('public.market_choices')
                    """
                )
            ).one()
        return MarketChoiceMetrics(
            row_count=int(row[0] or 0),
            min_choice_id=int(row[1]) if row[1] is not None else None,
            max_choice_id=int(row[2]) if row[2] is not None else None,
            identity_checksum=cls._identity_checksum(connection),
            heap_bytes=int(heap_bytes) if heap_bytes is not None else None,
            index_bytes=int(index_bytes) if index_bytes is not None else None,
            total_bytes=int(total_bytes) if total_bytes is not None else None,
        )

    @staticmethod
    def _identity_checksum(connection) -> str:
        if connection.dialect.name == "postgresql":
            return str(
                connection.execute(
                    text(
                        """
                        WITH row_hashes AS (
                            SELECT hashtextextended(
                                concat_ws(E'\\x1f', choice_id::text,
                                          market_id::text, choice_name),
                                0
                            ) AS row_hash
                            FROM market_choices
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
        rows = connection.execute(
            text(
                "SELECT choice_id, market_id, choice_name "
                "FROM market_choices ORDER BY choice_id"
            )
        )
        for row in rows:
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
    ) -> tuple[MarketChoicePriceStateAudit, MarketChoicePriceStateAudit]:
        if engine.dialect.name != "postgresql":
            raise ValueError("Phase 7 destructive DDL requires PostgreSQL")
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
                "LOCK TABLE public.market_choices IN ACCESS EXCLUSIVE MODE"
            )
            before = cls._audit_connection(connection)
            if before.is_slim:
                return before, before
            if not before.ready_to_migrate:
                raise RuntimeError(
                    "MarketChoice price-state migration blocked: "
                    + ", ".join(before.blockers or (before.schema_state,))
                )

            connection.exec_driver_sql(cls.postgresql_ddl_statement())

            after = cls._audit_connection(connection)
            if not after.is_slim:
                raise RuntimeError(
                    "MarketChoice price-state postflight failed: "
                    + ", ".join(after.blockers or (after.schema_state,))
                )
            before_identity = (
                before.metrics.row_count,
                before.metrics.min_choice_id,
                before.metrics.max_choice_id,
                before.metrics.identity_checksum,
            )
            after_identity = (
                after.metrics.row_count,
                after.metrics.min_choice_id,
                after.metrics.max_choice_id,
                after.metrics.identity_checksum,
            )
            if before_identity != after_identity:
                raise RuntimeError(
                    "MarketChoice identity changed during migration"
                )
            return before, after

    @staticmethod
    def postgresql_ddl_statement() -> str:
        clauses = ", ".join(
            f'DROP COLUMN "{column}"' for column in LEGACY_PRICE_COLUMNS
        )
        return f"ALTER TABLE public.market_choices {clauses}"

    @staticmethod
    def compact_postgresql(engine) -> None:
        """Physically rewrite and analyze ``market_choices`` after the DROP.

        VACUUM FULL cannot run inside the DDL transaction. The CLI invokes
        this method only after a successful/idempotent migration and verifies
        the identity payload again once compaction finishes.
        """
        if engine.dialect.name != "postgresql":
            raise ValueError("VACUUM FULL is only supported here for PostgreSQL")
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            connection.exec_driver_sql(
                "VACUUM (FULL, ANALYZE) public.market_choices"
            )


__all__ = [
    "IDENTITY_COLUMNS",
    "LEGACY_PRICE_COLUMNS",
    "MarketChoiceMetrics",
    "MarketChoicePriceStateAudit",
    "MarketChoicePriceStateMigrator",
    "SCRIPT_MANAGED_DEPENDENT_VIEWS",
]
