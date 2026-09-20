"""Database engine and session lifecycle."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event as sqlalchemy_event, text
from sqlalchemy.orm import Session, sessionmaker

from infrastructure.persistence import models as _registered_models  # noqa: F401
from infrastructure.persistence.orm_base import Base
from infrastructure.settings import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = database_url or Config.DATABASE_URL
        connect_args = (
            {} if self.database_url.startswith("sqlite")
            else {"connect_timeout": Config.DB_CONNECT_TIMEOUT}
        )
        self.engine = create_engine(
            self.database_url,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args=connect_args,
        )
        if self.engine.dialect.name == "postgresql":
            @sqlalchemy_event.listens_for(self.engine, "connect")
            def _set_utc_session_timezone(dbapi_connection, _connection_record):
                cursor = dbapi_connection.cursor()
                try:
                    cursor.execute("SET TIME ZONE 'UTC'")
                finally:
                    cursor.close()
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
            bind=self.engine,
        )
        logger.info(
            "Database engine created for: %s",
            self.engine.url.render_as_string(hide_password=True),
        )

    def create_tables(self) -> None:
        """Create tables for SQLite development and isolated tests only."""
        if self.engine.dialect.name != "sqlite":
            raise RuntimeError("PostgreSQL schema changes must use Alembic")
        Base.metadata.create_all(bind=self.engine)

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            logger.exception("Database session failed")
            raise
        finally:
            session.close()

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True
        except Exception:
            logger.exception("Database connection test failed")
            return False


db_manager = DatabaseManager()
