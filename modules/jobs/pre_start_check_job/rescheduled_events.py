"""Rescheduled event helpers for the pre-start job."""

from __future__ import annotations

import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event
from infrastructure.persistence.repositories import OddsRepository
from modules.sofascore import api_client
from modules.jobs.pre_start_check_job.odds_extraction import extract_final_odds_from_response

logger = logging.getLogger(__name__)


def reset_event_alert_sent(event_id: int) -> bool:
    try:
        with db_manager.get_session() as session:
            event = session.query(Event).filter(Event.id == event_id).first()
            if event:
                event.alert_sent = False
                session.commit()
                logger.info(f"✅ Reset alert_sent=False for event {event_id} (resurrected)")
                return True
            logger.warning(f"Event {event_id} not found when resetting alert_sent")
            return False
    except Exception as exc:
        logger.error(f"Error resetting alert_sent for event {event_id}: {exc}")
        return False


def handle_rescheduled_event(event_id: int, event_repo, minutes_until_start: int, metadata_snapshot: dict = None):
    """Minimal rescheduled-event handler used by the refactored pre-start job."""
    try:
        event = event_repo.get_event_by_id(event_id)
        if not event:
            logger.warning(f"Could not find event {event_id} after time update")
            return

        if minutes_until_start not in [30, 0] and minutes_until_start >= 0:
            return

        final_odds_response = api_client.get_event_final_odds(event_id, event.slug)
        if not final_odds_response:
            logger.warning(f"Failed to fetch odds for rescheduled event {event_id}")
            return

        final_odds_data = extract_final_odds_from_response(final_odds_response)
        if not final_odds_data:
            logger.warning(f"No odds data extracted for rescheduled event {event_id}")
            return

        upserted_id = OddsRepository.upsert_event_odds(event_id, final_odds_data)
        if upserted_id:
            OddsRepository.create_odds_snapshot(event_id, final_odds_data)
            logger.info(f"✅ Odds extracted for rescheduled event {event_id}")
    except Exception as exc:
        logger.error(f"Error checking rescheduled event {event_id}: {exc}")
