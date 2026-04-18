import logging

from infrastructure.persistence.repositories import EventRepository
from modules.alerts.dual_process.process_1 import alert_engine


def run_alerts():
    """Run alert evaluation on upcoming events."""
    logger = logging.getLogger(__name__)
    logger.info("Running alert evaluation...")

    try:
        event_repo = EventRepository()
        upcoming_events = event_repo.get_events_starting_soon(30)

        if not upcoming_events:
            logger.info("No upcoming events found for alert evaluation")
            return

        logger.info(f"Evaluating {len(upcoming_events)} upcoming events for alerts")

        alerts = alert_engine.evaluate_upcoming_events(upcoming_events)
        if alerts:
            logger.info(f"Generated {len(alerts)} alerts")
            alert_engine.send_alerts(alerts)
        else:
            logger.info("No alerts generated")
    except Exception as exc:
        logger.error(f"Error running alerts: {exc}")

