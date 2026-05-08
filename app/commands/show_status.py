import logging

from sqlalchemy import text

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, Result
from infrastructure.scheduler import job_scheduler


def show_status():
    """Show system status."""
    logger = logging.getLogger(__name__)

    try:
        db_status = "Connected" if db_manager.test_connection() else "Disconnected"

        with db_manager.get_session() as session:
            event_count = session.query(Event).count()
            odds_count = session.execute(text("SELECT COUNT(*) FROM v_dual_process_event_odds")).scalar()
            result_count = session.query(Result).count()

        jobs = job_scheduler.get_scheduled_jobs()

        print("\n=== SofaScore Odds System Status ===")
        print(f"Database: {db_status}")
        print(f"Events in database: {event_count}")
        print(f"Events with dual-process odds: {odds_count}")
        print(f"Events with results: {result_count}")
        print("Pre-start notifications: Active")
        print("\nScheduled Jobs:")
        for job in jobs:
            if "display" in job:
                print(f"  - {job['display']}")
            else:
                print(f"  - {job['function']}: {job['interval']} {job['unit']}")

            if job["next_run"]:
                print(f"    Next run: {job['next_run']}")

        print("\n" + "=" * 40)
    except Exception as exc:
        logger.error(f"Error showing status: {exc}")
