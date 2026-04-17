import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta

from infrastructure.persistence.models import DailyDiscoveryLog
from infrastructure.persistence.database import db_manager
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class DailyDiscoveryRepository:
    """Repository for managing the Daily Discovery retry queue/logs."""

    @staticmethod
    def initialize_sports_for_date(date_str: str, sports: List[str]) -> bool:
        """
        Creates 'pending' entries for all given sports for a specific date if they don't exist.
        """
        try:
            with db_manager.get_session() as session:
                for sport in sports:
                    existing = session.query(DailyDiscoveryLog).filter(
                        DailyDiscoveryLog.date == date_str,
                        DailyDiscoveryLog.sport == sport
                    ).first()

                    if not existing:
                        new_log = DailyDiscoveryLog(
                            date=date_str,
                            sport=sport,
                            status='pending'
                        )
                        session.add(new_log)
                logger.info(f"Initialized DailyDiscoveryLog for date {date_str} and sports {sports}")
                return True
        except Exception as e:
            logger.error(f"Error initializing DailyDiscoveryLog for {date_str}: {e}")
            return False

    @staticmethod
    def get_pending_sports(date_str: str) -> List[str]:
        """
        Returns a list of sports that are 'pending' or 'failed' for the given date.
        """
        try:
            with db_manager.get_session() as session:
                pending_logs = session.query(DailyDiscoveryLog).filter(
                    DailyDiscoveryLog.date == date_str,
                    DailyDiscoveryLog.status != 'completed'
                ).all()

                return [log.sport for log in pending_logs]
        except Exception as e:
            logger.error(f"Error getting pending sports for {date_str}: {e}")
            return []

    @staticmethod
    def update_sport_status(date_str: str, sport: str, status: str) -> bool:
        """
        Updates the status of a specific sport for a date, increments attempts.
        """
        try:
            with db_manager.get_session() as session:
                log = session.query(DailyDiscoveryLog).filter(
                    DailyDiscoveryLog.date == date_str,
                    DailyDiscoveryLog.sport == sport
                ).first()
                if log:
                    log.status = status
                    log.attempts += 1
                    log.last_attempt_at = get_local_now()
                    logger.info(f"Updated DailyDiscoveryLog {date_str} - {sport} to {status}")
                    return True
                return False
        except Exception as e:
            logger.error(f"Error updating DailyDiscoveryLog for {date_str} - {sport}: {e}")
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
                    logger.info(f"ðŸ§¹ Cleaned up {deleted} DailyDiscoveryLog entries older than {cutoff_date}")
                return deleted
        except Exception as e:
            logger.error(f"Error cleaning up DailyDiscoveryLogs: {e}")
            return 0
