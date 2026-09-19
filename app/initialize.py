import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    create_or_replace_materialized_views,
    create_or_replace_views,
)
from infrastructure.settings import Config


def initialize_system() -> bool:
    """Initialize the database and reporting views."""
    logger = logging.getLogger(__name__)

    try:
        Config.validate_odds_read_settings()

        if not db_manager.test_connection():
            logger.error("Database connection failed")
            return False

        if Config.AUTO_CREATE_SCHEMA:
            db_manager.create_tables()

        if Config.REQUIRE_ALEMBIC_SCHEMA and not db_manager.verify_schema_at_head():
            logger.error(
                "Schema validation failed; application startup is blocked. "
                "Apply migrations with `alembic upgrade head` before starting "
                "the application."
            )
            return False

        if not Config.REQUIRE_ALEMBIC_SCHEMA:
            logger.warning(
                "Alembic schema verification is disabled; this mode is intended "
                "only for SQLite/local development."
            )

        create_or_replace_views(db_manager.engine)
        create_or_replace_materialized_views(db_manager.engine)

        logger.info("System initialized successfully")
        return True
    except Exception as exc:
        logger.error(f"Failed to initialize system: {exc}")
        return False


__all__ = ["initialize_system"]

