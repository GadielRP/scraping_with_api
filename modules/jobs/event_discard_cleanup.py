"""One bounded cleanup batch per daily discovery heartbeat; no provider requests."""
import logging
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.event_discard_repository import EventDiscardRepository
from modules.events.discards.settings import DiscardSettings

logger = logging.getLogger(__name__)


def run_event_discard_cleanup():
    settings = DiscardSettings.current()
    if not settings.cleanup_enabled:
        return 0
    try:
        with db_manager.get_session() as session:
            deleted = EventDiscardRepository.cleanup(session, settings)
        logger.info('Discard memory cleanup deleted=%s retention_days=%s', deleted, settings.retention_days)
        return deleted
    except Exception:
        logger.exception('Discard memory cleanup failed; retained rows still block ingestion')
        return 0
