"""Durable per-sport progress, with errors distinguished from empty work."""

from datetime import timedelta
from infrastructure.persistence.models import DailyDiscoveryLog
from infrastructure.persistence.database import db_manager
from shared.temporal import now_in_timezone, utc_now
from infrastructure.settings import Config


class DailyDiscoveryRepository:
    @staticmethod
    def initialize_sports_for_slot(date_str, run_slot, sports):
        if run_slot not in {"AM", "PM"}:
            raise ValueError("Invalid discovery slot")
        if not sports:
            return
        with db_manager.get_session() as session:
            if session.get_bind().dialect.name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert
            else:
                from sqlalchemy.dialects.sqlite import insert
            session.execute(
                insert(DailyDiscoveryLog)
                .values(
                    [
                        dict(
                            date=date_str,
                            run_slot=run_slot,
                            sport=sport,
                            status="pending",
                            attempts=0,
                        )
                        for sport in sports
                    ]
                )
                .on_conflict_do_nothing()
            )

    @staticmethod
    def get_pending_sports(date_str, run_slot):
        with db_manager.get_session() as session:
            return [
                row[0]
                for row in session.query(DailyDiscoveryLog.sport)
                .filter(
                    DailyDiscoveryLog.date == date_str,
                    DailyDiscoveryLog.run_slot == run_slot,
                    DailyDiscoveryLog.status != "completed",
                )
                .order_by(DailyDiscoveryLog.id)
                .all()
            ]

    @staticmethod
    def update_sport_status(date_str, run_slot, sport, status):
        with db_manager.get_session() as session:
            count = (
                session.query(DailyDiscoveryLog)
                .filter(
                    DailyDiscoveryLog.date == date_str,
                    DailyDiscoveryLog.run_slot == run_slot,
                    DailyDiscoveryLog.sport == sport,
                )
                .update(
                    dict(
                        status=status,
                        attempts=DailyDiscoveryLog.attempts + 1,
                        last_attempt_at=utc_now(),
                    )
                )
            )
            if count != 1:
                raise RuntimeError("Missing initialized discovery progress row")

    @staticmethod
    def cleanup_old_logs(days_to_keep):
        if days_to_keep < 0:
            return 0
        cutoff = (
            (now_in_timezone(Config.TIMEZONE) - timedelta(days=days_to_keep)).date().isoformat()
        )
        with db_manager.get_session() as session:
            return (
                session.query(DailyDiscoveryLog)
                .filter(DailyDiscoveryLog.date < cutoff)
                .delete(synchronize_session=False)
            )
