"""Persistence helpers for daily discovery events."""

from __future__ import annotations

import logging
from typing import Dict

from infrastructure.persistence.repositories import EventRepository
from modules.odds_ingestion import MarketOddsIngestionService

logger = logging.getLogger(__name__)


def persist_event_and_optional_odds(api_client, event: Dict, odds_data: Dict | None = None) -> bool:
    try:
        source = "sofascore"
        source_event_id = event.get("id")
        if not source_event_id:
            logger.warning("Event has no ID, skipping")
            return False

        event_data = api_client.normalize_event_payload(event, discovery_source="daily_discovery")
        event_payload = event_data.get("event", event_data) if event_data else {}
        if not event_payload or not event_payload.get("id"):
            logger.warning("Could not extract event information for source=%s source_event_id=%s", source, source_event_id)
            return False

        db_event = EventRepository.upsert_event(event_data)
        if not db_event:
            logger.error("Failed to upsert event source=%s source_event_id=%s to database", source, source_event_id)
            return False

        if odds_data:
            ingestion_result = MarketOddsIngestionService.save_from_event_odds_response(
                db_event.id,
                odds_data,
                source="daily_discovery",
            )
            if ingestion_result.markets_saved <= 0 and not ingestion_result.dual_process_market_available:
                logger.warning("Failed to save market odds for event %s: %s", db_event.id, ingestion_result.reason)
                return False

        return True
    except Exception as exc:
        logger.error("Error processing event %s: %s", event.get("id", "unknown"), exc)
        return False
