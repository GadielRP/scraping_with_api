"""Rescheduled event helpers for the pre-start job."""

from __future__ import annotations

import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event
from modules.odds_ingestion import MarketOddsIngestionService
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_id

logger = logging.getLogger(__name__)


def reset_event_alert_sent(event_id: int) -> bool:
    try:
        with db_manager.get_session() as session:
            event = session.query(Event).filter(Event.id == event_id).first()
            if event:
                event.alert_sent = False
                session.commit()
                logger.info("Reset alert_sent=False for event %s (resurrected)", event_id)
                return True
            logger.warning("Event %s not found when resetting alert_sent", event_id)
            return False
    except Exception as exc:
        logger.error("Error resetting alert_sent for event %s: %s", event_id, exc)
        return False


def handle_rescheduled_event(event_id: int, event_repo, minutes_until_start: int, metadata_snapshot: dict = None):
    """Minimal rescheduled-event handler used by the refactored pre-start job."""
    try:
        event = event_repo.get_event_by_id(event_id)
        if not event:
            logger.warning("Could not find event %s after time update", event_id)
            return

        if minutes_until_start not in [30, 0] and minutes_until_start >= 0:
            return

        sofascore_event_id = resolve_sofascore_event_id(event_id)
        final_odds_response = api_client.get_event_final_odds(sofascore_event_id, event.slug)
        if not final_odds_response:
            logger.warning("Failed to fetch odds for rescheduled event %s", event_id)
            return

        ingestion_result = MarketOddsIngestionService.save_from_event_odds_response(
            event_id,
            final_odds_response,
            source="rescheduled_event",
        )
        if ingestion_result.markets_saved > 0 or ingestion_result.dual_process_market_available:
            logger.info("Market odds extracted for rescheduled event %s", event_id)
        else:
            logger.warning("No market odds saved for rescheduled event %s: %s", event_id, ingestion_result.reason)
    except Exception as exc:
        logger.error("Error checking rescheduled event %s: %s", event_id, exc)
