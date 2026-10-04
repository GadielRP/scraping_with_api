import logging

from sqlalchemy import text

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, Result


def show_status():
    """Show system status."""
    logger = logging.getLogger(__name__)

    try:
        db_status = "Connected" if db_manager.test_connection() else "Disconnected"

        with db_manager.get_session() as session:
            event_count = session.query(Event).count()
            odds_count = session.execute(
                text("SELECT COUNT(*) FROM v_dual_process_event_odds")
            ).scalar()
            result_count = session.query(Result).count()

        from schedule import Scheduler
        from infrastructure.scheduler.schedules import configure_calendar
        from infrastructure.settings.job_execution import JobExecutionSettings

        calendar = Scheduler()
        configure_calendar(calendar, JobExecutionSettings())

        print("\n=== SofaScore Odds System Status ===")
        print(f"Database: {db_status}")
        print(f"Events in database: {event_count}")
        print(f"Events with dual-process odds: {odds_count}")
        print(f"Events with results: {result_count}")
        print("Pre-start notifications: Active")
        print("\nConfigured schedules:")
        for job in calendar.jobs:
            print(f"  - {job}")

        print("\n" + "=" * 40)
    except Exception as exc:
        logger.error(f"Error showing status: {exc}")
