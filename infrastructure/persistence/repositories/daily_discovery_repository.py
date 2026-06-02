import logging
from typing import List
from datetime import timedelta

from infrastructure.persistence.models import DailyDiscoveryLog
from infrastructure.persistence.database import db_manager
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class DailyDiscoveryRepository:
    """Repository for managing the Daily Discovery retry queue/logs."""

    @staticmethod
    def initialize_sports_for_slot(date_str: str, run_slot: str, sports: List[str]) -> bool:
        """
        Creates 'pending' entries for all given sports for a specific date/slot if they don't exist.
        """
        normalized_run_slot = (run_slot or "AM").strip().upper()
        try:
            with db_manager.get_session() as session:
                for sport in sports:
                    existing = session.query(DailyDiscoveryLog).filter(
                        DailyDiscoveryLog.date == date_str,
                        DailyDiscoveryLog.run_slot == normalized_run_slot,
                        DailyDiscoveryLog.sport == sport
                    ).first()

                    if not existing:
                        new_log = DailyDiscoveryLog(
                            date=date_str,
                            run_slot=normalized_run_slot,
                            sport=sport,
                            status='pending'
                        )
                        session.add(new_log)
                logger.info(
                    "Initialized DailyDiscoveryLog for date %s, slot %s and sports %s",
                    date_str,
                    normalized_run_slot,
                    sports,
                )
                return True
        except Exception as e:
            logger.error(
                "Error initializing DailyDiscoveryLog for %s slot %s: %s",
                date_str,
                normalized_run_slot,
                e,
            )
            return False

    @staticmethod
    def get_pending_sports(date_str: str, run_slot: str) -> List[str]:
        """
        Returns a list of sports that are 'pending' or 'failed' for the given date/slot.
        """
        normalized_run_slot = (run_slot or "AM").strip().upper()
        try:
            with db_manager.get_session() as session:
                pending_logs = session.query(DailyDiscoveryLog).filter(
                    DailyDiscoveryLog.date == date_str,
                    DailyDiscoveryLog.run_slot == normalized_run_slot,
                    DailyDiscoveryLog.status != 'completed'
                ).order_by(DailyDiscoveryLog.id).all()

                return [log.sport for log in pending_logs]
        except Exception as e:
            logger.error(
                "Error getting pending sports for %s slot %s: %s",
                date_str,
                normalized_run_slot,
                e,
            )
            return []

    @staticmethod
    def update_sport_status(date_str: str, run_slot: str, sport: str, status: str) -> bool:
        """
        Updates the status of a specific sport for a date/slot, increments attempts.
        """
        normalized_run_slot = (run_slot or "AM").strip().upper()
        try:
            with db_manager.get_session() as session:
                log = session.query(DailyDiscoveryLog).filter(
                    DailyDiscoveryLog.date == date_str,
                    DailyDiscoveryLog.run_slot == normalized_run_slot,
                    DailyDiscoveryLog.sport == sport
                ).first()
                if log:
                    log.status = status
                    log.attempts = (log.attempts or 0) + 1
                    log.last_attempt_at = get_local_now()
                    logger.info(
                        "Updated DailyDiscoveryLog %s - %s - %s to %s",
                        date_str,
                        normalized_run_slot,
                        sport,
                        status,
                    )
                    return True
                return False
        except Exception as e:
            logger.error(
                "Error updating DailyDiscoveryLog for %s - %s - %s: %s",
                date_str,
                normalized_run_slot,
                sport,
                e,
            )
            return False

    @staticmethod
    def cleanup_old_logs(days_to_keep: int) -> int:
        """
        Deletes daily discovery logs older than `days_to_keep` days.
        """
        if days_to_keep < 0:
            return 0

        try:
            with db_manager.get_session() as session:
                cutoff_date = (get_local_now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
                deleted = session.query(DailyDiscoveryLog).filter(
                    DailyDiscoveryLog.date < cutoff_date
                ).delete()

                if deleted > 0:
                    logger.info(f"🧹 Cleaned up {deleted} DailyDiscoveryLog entries older than {cutoff_date}")
                return deleted
        except Exception as e:
            logger.error(f"Error cleaning up DailyDiscoveryLogs: {e}")
            return 0
