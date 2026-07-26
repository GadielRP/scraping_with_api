"""Rescheduled event helpers for the pre-start job."""

from __future__ import annotations

import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event
from infrastructure.persistence.repositories import EventSourceMappingRepository
from modules.odds_ingestion import MarketOddsIngestionService
from modules.sofascore import api_client
from modules.sofascore.event_identity import resolve_sofascore_event_id
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher

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


def handle_rescheduled_event(event_id: int, event_repo, minutes_until_start: int, metadata_snapshot: dict = None, sofascore_event_id: int | None = None):
    """Minimal rescheduled-event handler used by the refactored pre-start job."""
    try:
        event = event_repo.get_event_by_id(event_id)
        if not event:
            logger.warning("Could not find event %s after time update", event_id)
            return

        if minutes_until_start not in [30, 0] and minutes_until_start >= 0:
            return

        source_states = EventSourceMappingRepository.get_odds_source_states(
            [event_id],
            ["sofascore"],
        )
        source_state = source_states.get(event_id, {}).get("sofascore")
        if source_state is not None and not source_state.has_odds:
            logger.info(
                "Skipping rescheduled event %s odds: endpoint marked unavailable",
                event_id,
            )
            return

        if sofascore_event_id is None:
            if source_state is not None:
                sofascore_event_id = int(source_state.source_event_id)
            else:
                sofascore_event_id = resolve_sofascore_event_id(event_id)

        fetch_result = SofaScoreOddsFetcher(api_client).fetch_odds(
            sofascore_event_id,
            event.slug,
        )
        if fetch_result.endpoint_missing:
            EventSourceMappingRepository.mark_odds_unavailable(
                [event_id],
                "sofascore",
            )
            logger.info(
                "🚫 SofaScore odds endpoint missing for rescheduled event %s",
                event_id,
            )
            return

        final_odds_response = fetch_result.payload
        if not final_odds_response:
            logger.warning("Failed to fetch odds for rescheduled event %s", event_id)
            return

        ingestion_result = MarketOddsIngestionService.save_from_event_odds_response(
            event_id,
            final_odds_response,
            source="sofascore",
            home_team=event.home_team,
            away_team=event.away_team,
        )
        if ingestion_result.markets_saved > 0 or ingestion_result.dual_process_market_available:
            logger.info("Market odds extracted for rescheduled event %s", event_id)
        else:
            logger.warning("No market odds saved for rescheduled event %s: %s", event_id, ingestion_result.reason)
    except Exception as exc:
        logger.error("Error checking rescheduled event %s: %s", event_id, exc)
