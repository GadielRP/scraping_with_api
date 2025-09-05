#!/usr/bin/env python3
"""
SQLite -> PostgreSQL migration utility for SofaScore Odds System

Usage examples:

1) With Dockerized PostgreSQL running locally (recommended for Windows):
   - Start Postgres:
     docker run --name sofascore-pg -e POSTGRES_PASSWORD=your_pg_password -e POSTGRES_DB=sofascore_odds -p 5432:5432 -d postgres:15

   - Migrate (default SQLite file in current dir):
     python migrate_sqlite_to_postgres.py \
       --sqlite sqlite:///sofascore_odds.db \
       --postgres postgresql://postgres:your_pg_password@localhost:5432/sofascore_odds

2) With a custom SQLite path and a dedicated PG user/database:
     python migrate_sqlite_to_postgres.py \
       --sqlite sqlite:///C:/path/to/sofascore_odds.db \
       --postgres postgresql://sofascore:your_secure_password@localhost:5432/sofascore_odds

The script:
- creates destination tables if they don't exist
- copies rows table-by-table preserving primary keys
- resets PostgreSQL sequences for autoincrement columns
- prints row counts before/after
"""

from __future__ import annotations

import argparse
import logging
from typing import Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Reuse your ORM models
from models import Base, Event, EventOdds, OddsSnapshot, Result, AlertLog


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate SQLite data to PostgreSQL")
    parser.add_argument(
        "--sqlite",
        default="sqlite:///sofascore_odds.db",
        help="SQLite SQLAlchemy URL. Example: sqlite:///sofascore_odds.db",
    )
    parser.add_argument(
        "--postgres",
        required=True,
        help=(
            "PostgreSQL SQLAlchemy URL. Example: postgresql://user:pass@localhost:5432/sofascore_odds"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Insert in batches of this size",
    )
    return parser.parse_args()


def create_sessions(sqlite_url: str, postgres_url: str):
    # Source: SQLite
    src_engine = create_engine(sqlite_url, echo=False, pool_pre_ping=True)
    SrcSession = sessionmaker(bind=src_engine, autoflush=False, expire_on_commit=False)

    # Destination: PostgreSQL
    dst_engine = create_engine(postgres_url, echo=False, pool_pre_ping=True)
    DstSession = sessionmaker(bind=dst_engine, autoflush=False, expire_on_commit=False)

    # Create tables in destination if missing
    Base.metadata.create_all(bind=dst_engine)

    return SrcSession, DstSession, src_engine, dst_engine


def count_rows(session, model) -> int:
    return session.query(model).count()


def copy_table(session_src, session_dst, model, batch_size: int = 1000) -> Tuple[int, int]:
    """Copy all rows from src to dst for a given ORM model, preserving PKs."""
    total_src = session_src.query(model).count()
    copied = 0

    logging.info(f"Copying {model.__tablename__}: {total_src} rows")

    # Stream in chunks to keep memory bounded
    offset = 0
    while offset < total_src:
        rows = (
            session_src.query(model)
            .order_by(*model.__table__.primary_key.columns)
            .offset(offset)
            .limit(batch_size)
            .all()
        )
        if not rows:
            break

        # Create detached copies with same PKs
        new_objs = []
        for r in rows:
            data = {}
            for col in model.__table__.columns:
                data[col.name] = getattr(r, col.name)
            new_objs.append(model(**data))

        session_dst.bulk_save_objects(new_objs, return_defaults=False)
        session_dst.commit()
        copied += len(new_objs)
        offset += batch_size

        logging.info(f"  Copied {copied}/{total_src} rows...")

    total_dst = session_dst.query(model).count()
    logging.info(f"Done {model.__tablename__}: src={total_src}, dst={total_dst}")
    return total_src, total_dst


def reset_pg_sequences(dst_engine) -> None:
    """Reset sequences for tables with autoincrement PKs (PostgreSQL)."""
    with dst_engine.begin() as conn:
        # OddsSnapshot.snapshot_id
        conn.execute(
            text(
                """
                DO $$
                BEGIN
                  IF EXISTS (SELECT 1 FROM information_schema.sequences WHERE sequence_name = 'odds_snapshot_snapshot_id_seq') THEN
                    PERFORM setval('odds_snapshot_snapshot_id_seq', COALESCE((SELECT MAX(snapshot_id) FROM odds_snapshot), 0));
                  END IF;
                END
                $$;
                """
            )
        )

        # AlertLog.id
        conn.execute(
            text(
                """
                DO $$
                BEGIN
                  IF EXISTS (SELECT 1 FROM information_schema.sequences WHERE sequence_name = 'alerts_log_id_seq') THEN
                    PERFORM setval('alerts_log_id_seq', COALESCE((SELECT MAX(id) FROM alerts_log), 0));
                  END IF;
                END
                $$;
                """
            )
        )

    logging.info("PostgreSQL sequences reset (if present)")


def migrate(sqlite_url: str, postgres_url: str, batch_size: int = 1000) -> None:
    SrcSession, DstSession, src_engine, dst_engine = create_sessions(sqlite_url, postgres_url)
    src = SrcSession()
    dst = DstSession()

    try:
        # Show pre-counts
        logging.info("Source row counts (SQLite):")
        logging.info(
            "  events=%s, event_odds=%s, odds_snapshot=%s, results=%s, alerts_log=%s",
            count_rows(src, Event),
            count_rows(src, EventOdds),
            count_rows(src, OddsSnapshot),
            count_rows(src, Result),
            count_rows(src, AlertLog),
        )

        logging.info("Destination row counts (PostgreSQL) BEFORE:")
        logging.info(
            "  events=%s, event_odds=%s, odds_snapshot=%s, results=%s, alerts_log=%s",
            count_rows(dst, Event),
            count_rows(dst, EventOdds),
            count_rows(dst, OddsSnapshot),
            count_rows(dst, Result),
            count_rows(dst, AlertLog),
        )

        # Copy in FK-safe order
        copy_table(src, dst, Event, batch_size)
        copy_table(src, dst, EventOdds, batch_size)
        copy_table(src, dst, OddsSnapshot, batch_size)
        copy_table(src, dst, Result, batch_size)
        copy_table(src, dst, AlertLog, batch_size)

        # Reset sequences for autoincrement PKs
        reset_pg_sequences(dst_engine)

        logging.info("Destination row counts (PostgreSQL) AFTER:")
        logging.info(
            "  events=%s, event_odds=%s, odds_snapshot=%s, results=%s, alerts_log=%s",
            count_rows(dst, Event),
            count_rows(dst, EventOdds),
            count_rows(dst, OddsSnapshot),
            count_rows(dst, Result),
            count_rows(dst, AlertLog),
        )

        logging.info("✅ Migration completed successfully")

    except Exception as e:
        logging.exception(f"Migration failed: {e}")
        raise
    finally:
        src.close()
        dst.close()


if __name__ == "__main__":
    setup_logging()
    args = parse_args()
    migrate(args.sqlite, args.postgres, args.batch_size)


