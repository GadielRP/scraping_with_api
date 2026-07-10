"""Discovery and secondary SofaScore feed helpers."""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple

from .event_normalizer import normalize_event_payload

logger = logging.getLogger(__name__)


def get_dropping_odds_with_odds_and_events_response(client, sport: str = None):
    if sport:
        endpoint = f"/odds/1/dropping/{sport}"
        logger.info("✈️ Fetching dropping odds feed for sport %s - /odds/1/dropping/%s", sport, sport)
    else:
        endpoint = "/odds/1/dropping/all"
        logger.info("✈️ Fetching dropping odds feed for all sports")
    return client._request_json(endpoint)


def get_high_value_streaks_events(client):
    logger.info("✈️ Fetching high value streaks feed")
    return client._request_json("/odds/1/high-value-streaks")


def get_team_streaks_events(client):
    logger.info("✈️ Fetching team streaks feed")
    return client._request_json("/odds/top-team-streaks/wins/all")


def get_h2h_events(client):
    logger.info("✈️ Fetching top H2H feed")
    return client._request_json("/odds/1/top-h2h/all")


def get_winning_odds_events(client):
    logger.info("✈️ Fetching winning odds discovery feed")
    return client._request_json("/odds/1/winning/all")


def extract_events_from_high_value_streaks(response: Dict) -> Tuple[List[Dict], List[Dict]]:
    events = []
    events_h2h = []

    try:
        if "general" in response:
            for item in response["general"]:
                event = item.get("event")
                if event:
                    events.append(event)

        if "head2head" in response:
            for item in response["head2head"]:
                event = item.get("event")
                if event:
                    events_h2h.append(event)

        logger.info(
            "Extracted %s general and %s head-to-head events from high value streaks",
            len(events),
            len(events_h2h),
        )
        return events, events_h2h
    except Exception as exc:
        logger.error("Error extracting high value streak events: %s", exc)
        return [], []


def extract_events_and_odds_from_dropping_response(
    response: Dict,
    odds_extraction: bool = True,
    discovery_source: str = "dropping_odds",
) -> Tuple[List[Dict], Dict]:
    events: List[Dict] = []
    odds_map: Dict = {}

    try:
        if not response or "events" not in response:
            logger.warning("No events found in dropping odds response")
            return events, odds_map

        for event in response["events"]:
            try:
                event_data = normalize_event_payload(event, discovery_source)
                event_payload = event_data.get("event", event_data)
                required_fields = ["id", "slug", "startTimestamp", "sport", "competition", "homeTeam", "awayTeam"]
                if all(event_payload.get(field) for field in required_fields):
                    events.append(event_data)
                else:
                    logger.info("Event %s missing required fields", event.get("id"))
            except Exception as exc:
                logger.error("Error processing event: %s", exc)

        if odds_extraction and "oddsMap" in response:
            odds_map = response["oddsMap"]
            logger.info("Extracted %s odds entries from response", len(odds_map))

        return events, odds_map
    except Exception as exc:
        logger.error("Error extracting events and odds: %s", exc)
        return events, odds_map
