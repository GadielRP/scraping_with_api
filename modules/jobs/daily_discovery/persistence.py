"""Persistence helpers for daily discovery events."""

from __future__ import annotations

import logging
from typing import Dict

from infrastructure.persistence.repositories import EventRepository, OddsRepository
from shared.odds_utils import validate_odds_data

logger = logging.getLogger(__name__)


def persist_event_with_odds(api_client, event: Dict, odds_data: Dict) -> bool:
    try:
        event_id = event.get("id")
        if not event_id:
            logger.warning("Event has no ID, skipping")
            return False

        event_data = api_client.get_event_information(event, discovery_source="daily_discovery")
        if not event_data or not event_data.get("id"):
            logger.warning("Could not extract event information for event %s", event_id)
            return False

        db_event = EventRepository.upsert_event(event_data)
        if not db_event:
            logger.error("Failed to upsert event %s to database", event_id)
            return False

        logger.info("Upserted event %s: %s vs %s", event_id, event_data["homeTeam"], event_data["awayTeam"])

        if not validate_odds_data(odds_data):
            logger.warning("Invalid odds data for event %s, skipping odds insertion", event_id)
            return True

        snapshot = OddsRepository.create_odds_snapshot(event_id, odds_data)
        if snapshot:
            logger.debug("Created odds snapshot for event %s", event_id)

        event_odds_id = OddsRepository.upsert_event_odds(event_id, odds_data)
        if event_odds_id:
            logger.info(
                "Upserted odds for event %s: 1:%s, X:%s, 2:%s",
                event_id,
                odds_data.get("one_final"),
                odds_data.get("x_final"),
                odds_data.get("two_final"),
            )
        else:
            logger.warning("Failed to upsert event odds for event %s", event_id)

        return True
    except Exception as exc:
        logger.error("Error processing event %s: %s", event.get("id", "unknown"), exc)
        return False
