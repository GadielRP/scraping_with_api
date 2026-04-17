import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import refresh_materialized_views


def refresh_alert_data():
    """Refresh materialized views for alert processing."""
    logger = logging.getLogger(__name__)
    logger.info("Refreshing alert materialized views...")

    try:
        refresh_materialized_views(db_manager.engine)
        logger.info("Alert data refreshed successfully")
    except Exception as exc:
        logger.error(f"Error refreshing alert data: {exc}")

