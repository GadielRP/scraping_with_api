import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.schema_version import verify_schema_at_head
from infrastructure.settings import Config


def initialize_system() -> bool:
    """Check the deployed schema before starting application work."""
    logger = logging.getLogger(__name__)

    try:
        Config.validate_odds_read_settings()

        if not db_manager.test_connection():
            logger.error("Database connection failed")
            return False

        is_postgresql = db_manager.engine.dialect.name == "postgresql"
        if Config.AUTO_CREATE_SCHEMA and not is_postgresql:
            db_manager.create_tables()

        if (is_postgresql or Config.REQUIRE_ALEMBIC_SCHEMA) and not verify_schema_at_head(
            db_manager.engine
        ):
            logger.error(
                "Schema validation failed; application startup is blocked. "
                "Apply migrations with `alembic upgrade head` before starting "
                "the application."
            )
            return False

        logger.info("System initialized successfully")
        return True
    except Exception as exc:
        logger.error(f"Failed to initialize system: {exc}")
        return False


__all__ = ["initialize_system"]

