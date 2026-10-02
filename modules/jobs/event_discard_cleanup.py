"""One bounded cleanup batch per daily discovery heartbeat; no provider requests."""
import logging
from time import monotonic
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.event_discard_repository import EventDiscardRepository
from modules.events.discards.settings import DiscardSettings

logger = logging.getLogger(__name__)


def run_event_discard_cleanup():
    settings = DiscardSettings.current()
    if not settings.cleanup_enabled:
        logger.info('Discard memory cleanup skipped: cleanup_enabled=false')
        return 0
    started = monotonic()
    try:
        with db_manager.get_session() as session:
            deleted = EventDiscardRepository.cleanup(session, settings)
        logger.info('Discard memory cleanup deleted=%s retention_days=%s batch_limit=%s duration_s=%.3f',
                    deleted, settings.retention_days, settings.cleanup_batch_size, monotonic() - started)
        return deleted
    except Exception:
        logger.exception('Discard memory cleanup failed; retained rows still block ingestion')
        return 0
