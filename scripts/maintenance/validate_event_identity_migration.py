#!/usr/bin/env python3
"""Validate the canonical event identity migration."""

from __future__ import annotations

import logging
import sys

from sqlalchemy import text

from infrastructure.persistence.database import db_manager

logger = logging.getLogger(__name__)


def _scalar(session, sql: str) -> int:
    return int(session.execute(text(sql)).scalar() or 0)


def validate_event_identity_migration() -> int:
    critical_failures: list[str] = []
    dialect_name = db_manager.engine.dialect.name

    with db_manager.get_session() as session:
        total_events = _scalar(session, "SELECT COUNT(*) FROM events")
        total_sofascore_mappings = _scalar(
            session,
            "SELECT COUNT(*) FROM event_source_mappings WHERE lower(source) = 'sofascore'",
        )
        events_without_sofascore_mapping = _scalar(
            session,
            """
            SELECT COUNT(*)
            FROM events e
            LEFT JOIN event_source_mappings esm
                ON esm.event_id = e.id
               AND lower(esm.source) = 'sofascore'
            WHERE esm.mapping_id IS NULL
            """,
        )
        orphan_results = _scalar(
            session,
            """
            SELECT COUNT(*)
            FROM results r
            LEFT JOIN events e ON r.event_id = e.id
            WHERE e.id IS NULL
            """,
        )
        orphan_markets = _scalar(
            session,
            """
            SELECT COUNT(*)
            FROM markets m
            LEFT JOIN events e ON m.event_id = e.id
            WHERE e.id IS NULL
            """,
        )
        orphan_event_observations = _scalar(
            session,
            """
            SELECT COUNT(*)
            FROM event_observations eo
            LEFT JOIN events e ON eo.event_id = e.id
            WHERE e.id IS NULL
            """,
        )
        orphan_prediction_logs = _scalar(
            session,
            """
            SELECT COUNT(*)
            FROM prediction_logs pl
            LEFT JOIN events e ON pl.event_id = e.id
            WHERE e.id IS NULL
            """,
        )
        orphan_event_source_mappings = _scalar(
            session,
            """
            SELECT COUNT(*)
            FROM event_source_mappings esm
            LEFT JOIN events e ON esm.event_id = e.id
            WHERE e.id IS NULL
            """,
        )
        duplicate_mappings = _scalar(
            session,
            """
            SELECT COUNT(*)
            FROM (
                SELECT source, source_event_id
                FROM event_source_mappings
                GROUP BY source, source_event_id
                HAVING COUNT(*) > 1
            ) duplicates
            """,
        )
        events_id_default_is_sequence = True
        if dialect_name == "postgresql":
            events_id_default = session.execute(text("""
                SELECT column_default
                FROM information_schema.columns
                WHERE table_name = 'events'
                  AND column_name = 'id'
            """)).scalar()
            events_id_default_is_sequence = bool(events_id_default and "nextval" in str(events_id_default))

        print(f"total events: {total_events}")
        print(f"total sofascore mappings: {total_sofascore_mappings}")
        print(f"events without sofascore mapping: {events_without_sofascore_mapping}")
        print(f"orphan results: {orphan_results}")
        print(f"orphan markets: {orphan_markets}")
        print(f"orphan event_observations: {orphan_event_observations}")
        print(f"orphan prediction_logs: {orphan_prediction_logs}")
        print(f"orphan event_source_mappings: {orphan_event_source_mappings}")
        print(f"duplicate mappings: {duplicate_mappings}")
        print(f"events.id sequence default: {events_id_default_is_sequence}")
        print("sample mappings old source_event_id -> canonical event_id:")

        sample_rows = session.execute(text(
            """
            SELECT esm.source_event_id, esm.event_id
            FROM event_source_mappings esm
            ORDER BY esm.mapping_id
            LIMIT 10
            """
        )).fetchall()
        for source_event_id, event_id in sample_rows:
            print(f"  {source_event_id} -> {event_id}")

        if events_without_sofascore_mapping:
            critical_failures.append("events without sofascore mapping")
        if orphan_results:
            critical_failures.append("orphan results")
        if orphan_markets:
            critical_failures.append("orphan markets")
        if orphan_event_observations:
            critical_failures.append("orphan event_observations")
        if orphan_prediction_logs:
            critical_failures.append("orphan prediction_logs")
        if orphan_event_source_mappings:
            critical_failures.append("orphan event_source_mappings")
        if duplicate_mappings:
            critical_failures.append("duplicate mappings")
        if dialect_name == "postgresql" and not events_id_default_is_sequence:
            critical_failures.append("events.id sequence default missing")

        if critical_failures:
            logger.error("Event identity validation failed: %s", ", ".join(critical_failures))
            return 1

        logger.info("Event identity validation passed")
        return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    return validate_event_identity_migration()


if __name__ == "__main__":
    raise SystemExit(main())
