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

        width = 52
        print()
        print("=" * width)
        print("SOFASCORE ODDS SYSTEM STATUS".center(width))
        print("=" * width)

        print("\nSYSTEM")
        print(f"  {'Database':<34} {db_status}")
        print(f"  {'Pre-start notifications':<34} Active")

        print("\nDATA")
        print(f"  {'Events in database':<34} {event_count:>10,}")
        print(f"  {'Events with dual-process odds':<34} {odds_count:>10,}")
        print(f"  {'Events with results':<34} {result_count:>10,}")

        print(f"\nCONFIGURED SCHEDULES ({len(calendar.jobs)})")
        if calendar.jobs:
            for job in calendar.jobs:
                print(f"  - {job}")
        else:
            print("  No schedules configured.")

        print("\n" + "=" * width)
    except Exception as exc:
        logger.error(f"Error showing status: {exc}")
