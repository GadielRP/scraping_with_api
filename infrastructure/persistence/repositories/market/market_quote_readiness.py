"""Read-only readiness audit for the Phase 5 quote-reader cutover."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Iterable, Optional, Tuple

from sqlalchemy import bindparam, inspect, text


@dataclass(frozen=True, slots=True)
class MarketQuoteReadinessIssue:
    code: str
    count: int
    sample_ids: Tuple[int, ...] = ()
    detail: Optional[str] = None


@dataclass(frozen=True, slots=True)
class MarketQuoteReadinessReport:
    ready: bool
    event_scope: Tuple[int, ...]
    issues: Tuple[MarketQuoteReadinessIssue, ...] = ()
    schema_errors: Tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {
            "ready": self.ready,
            "event_scope": list(self.event_scope),
            "issues": [asdict(item) for item in self.issues],
            "schema_errors": list(self.schema_errors),
        }


class MarketQuoteReadinessAuditor:
    REQUIRED_COLUMNS = {
        "markets": {"market_id", "event_id"},
        "market_choices": {
            "choice_id",
            "market_id",
            "choice_name",
        },
        "market_choice_quotes": {
            "quote_id",
            "choice_id",
            "source",
            "exchange_side",
            "exchange_level",
        },
        "market_choice_snapshots": {"snapshot_id", "quote_id"},
    }

    @staticmethod
    def schema_preflight(engine) -> tuple[str, ...]:
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        errors = [
            f"missing_table:{table}"
            for table in sorted(set(MarketQuoteReadinessAuditor.REQUIRED_COLUMNS) - tables)
        ]
        for table, required_columns in MarketQuoteReadinessAuditor.REQUIRED_COLUMNS.items():
            if table not in tables:
                continue
            columns = {item["name"] for item in inspector.get_columns(table)}
            for column in sorted(required_columns - columns):
                errors.append(f"missing_column:{table}.{column}")
        return tuple(errors)

    @staticmethod
    def _scope_sql(event_ids: tuple[int, ...], alias: str = "m") -> tuple[str, dict, list]:
        if not event_ids:
            return "", {}, []
        return (
            f" AND {alias}.event_id IN :event_ids",
            {"event_ids": list(event_ids)},
            [bindparam("event_ids", expanding=True)],
        )

    @staticmethod
    def _issue_from_query(
        session,
        *,
        code: str,
        sql: str,
        params: dict,
        bind_params: list,
        detail: Optional[str] = None,
    ) -> Optional[MarketQuoteReadinessIssue]:
        statement = text(sql)
        if bind_params:
            statement = statement.bindparams(*bind_params)
        rows = session.execute(statement, params).mappings().all()
        if not rows:
            return None
        count = (
            max(int(row["total_count"]) for row in rows)
            if "total_count" in rows[0]
            else sum(int(row["row_count"]) for row in rows)
        )
        samples = tuple(
            int(row["sample_id"])
            for row in rows[:20]
            if row.get("sample_id") is not None
        )
        return MarketQuoteReadinessIssue(code, count, samples, detail)

    @staticmethod
    def audit(session, *, event_ids: Optional[Iterable[int]] = None) -> MarketQuoteReadinessReport:
        scope = tuple(sorted({int(item) for item in (event_ids or [])}))
        schema_errors = MarketQuoteReadinessAuditor.schema_preflight(session.get_bind())
        if schema_errors:
            return MarketQuoteReadinessReport(False, scope, schema_errors=schema_errors)

        scope_sql, params, binds = MarketQuoteReadinessAuditor._scope_sql(scope)
        snapshot_columns = {
            item["name"]
            for item in inspect(session.get_bind()).get_columns(
                "market_choice_snapshots"
            )
        }
        issues = []
        # A NULL quote has no path to an event.  This lineage invariant is
        # therefore deliberately global even when the rest of the audit is
        # scoped to reference events.
        unlinked_issue = MarketQuoteReadinessAuditor._issue_from_query(
            session,
            code="unlinked_snapshot",
            sql="""
                SELECT mcs.snapshot_id AS sample_id, 1 AS row_count,
                       COUNT(*) OVER () AS total_count
                FROM market_choice_snapshots mcs
                WHERE mcs.quote_id IS NULL
                ORDER BY mcs.snapshot_id
                LIMIT 20
            """,
            params={},
            bind_params=[],
            detail="Global invariant: an unlinked row cannot be event-scoped.",
        )
        if unlinked_issue:
            issues.append(unlinked_issue)

        queries = [
            (
                "invalid_quote_side_or_level",
                f"""
                SELECT mcq.quote_id AS sample_id, 1 AS row_count,
                       COUNT(*) OVER () AS total_count
                FROM market_choice_quotes mcq
                JOIN market_choices mc ON mc.choice_id = mcq.choice_id
                JOIN markets m ON m.market_id = mc.market_id
                WHERE (
                    (mcq.exchange_side IS NOT NULL AND mcq.exchange_side NOT IN ('back', 'lay'))
                    OR mcq.exchange_level < 0
                ) {scope_sql}
                ORDER BY mcq.quote_id
                LIMIT 20
                """,
                None,
            ),
            (
                "invalid_quote_source",
                f"""
                SELECT mcq.quote_id AS sample_id, 1 AS row_count,
                       COUNT(*) OVER () AS total_count
                FROM market_choice_quotes mcq
                JOIN market_choices mc ON mc.choice_id = mcq.choice_id
                JOIN markets m ON m.market_id = mc.market_id
                WHERE TRIM(COALESCE(mcq.source, '')) = '' {scope_sql}
                ORDER BY mcq.quote_id
                LIMIT 20
                """,
                None,
            ),
        ]
        # This consistency check only exists during the expanded pre-Phase 6
        # schema.  After the DROP, quote_id is the sole lineage identity.
        if "choice_id" in snapshot_columns:
            queries.insert(
                0,
                (
                    "snapshot_quote_choice_mismatch",
                    f"""
                    SELECT mcs.snapshot_id AS sample_id, 1 AS row_count,
                           COUNT(*) OVER () AS total_count
                    FROM market_choice_snapshots mcs
                    JOIN market_choice_quotes mcq ON mcq.quote_id = mcs.quote_id
                    JOIN market_choices mc ON mc.choice_id = mcq.choice_id
                    JOIN markets m ON m.market_id = mc.market_id
                    WHERE mcs.choice_id <> mcq.choice_id {scope_sql}
                    ORDER BY mcs.snapshot_id
                    LIMIT 20
                    """,
                    None,
                ),
            )
        if not scope:
            queries.append(
                (
                    "orphan_quote",
                    """
                    SELECT mcq.quote_id AS sample_id, 1 AS row_count,
                           COUNT(*) OVER () AS total_count
                    FROM market_choice_quotes mcq
                    LEFT JOIN market_choices mc ON mc.choice_id = mcq.choice_id
                    LEFT JOIN markets m ON m.market_id = mc.market_id
                    WHERE mc.choice_id IS NULL OR m.market_id IS NULL
                    ORDER BY mcq.quote_id
                    LIMIT 20
                    """,
                    None,
                )
            )
        for code, sql, detail in queries:
            issue = MarketQuoteReadinessAuditor._issue_from_query(
                session,
                code=code,
                sql=sql,
                params=params,
                bind_params=binds,
                detail=detail,
            )
            if issue:
                issues.append(issue)

        duplicate_scope_sql, duplicate_params, duplicate_binds = (
            MarketQuoteReadinessAuditor._scope_sql(scope)
        )
        duplicate_issue = MarketQuoteReadinessAuditor._issue_from_query(
            session,
            code="duplicate_quote_identity",
            sql=f"""
                SELECT MIN(mcq.quote_id) AS sample_id,
                       COUNT(*) AS row_count,
                       SUM(COUNT(*)) OVER () AS total_count
                FROM market_choice_quotes mcq
                JOIN market_choices mc ON mc.choice_id = mcq.choice_id
                JOIN markets m ON m.market_id = mc.market_id
                WHERE 1=1 {duplicate_scope_sql}
                GROUP BY mcq.choice_id, mcq.source,
                         COALESCE(mcq.exchange_side, ''), mcq.exchange_level
                HAVING COUNT(*) > 1
                ORDER BY MIN(mcq.quote_id)
                LIMIT 20
            """,
            params=duplicate_params,
            bind_params=duplicate_binds,
        )
        if duplicate_issue:
            issues.append(duplicate_issue)
        return MarketQuoteReadinessReport(not issues, scope, tuple(issues))


__all__ = [
    "MarketQuoteReadinessAuditor",
    "MarketQuoteReadinessIssue",
    "MarketQuoteReadinessReport",
]
