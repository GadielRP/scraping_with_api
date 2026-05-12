import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    create_or_replace_materialized_views,
    create_or_replace_views,
)


def initialize_system() -> bool:
    """Initialize the database and reporting views."""
    logger = logging.getLogger(__name__)

    try:
        if not db_manager.test_connection():
            logger.error("Database connection failed")
            return False

        db_manager.create_tables()

        if not db_manager.check_and_migrate_schema():
            logger.warning("Schema migration check failed, but continuing...")

        create_or_replace_views(db_manager.engine)
        create_or_replace_materialized_views(db_manager.engine)

        logger.info("System initialized successfully")
        return True
    except Exception as exc:
        logger.error(f"Failed to initialize system: {exc}")
        return False


__all__ = ["initialize_system"]

