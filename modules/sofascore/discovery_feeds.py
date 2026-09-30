"""Discovery and secondary SofaScore feed helpers."""

from __future__ import annotations

import logging
from typing import Dict, List, Tuple

from modules.competition.discovery_scope import (
    is_tracked_source_event,
    load_tracked_source_competitions,
    source_competition_ids,
)
from modules.sports.catalog import (
    canonical_sport_id,
    is_supported_sofascore_event,
    sofascore_sport_slug,
    sofascore_sport_slugs,
)

from .event_normalizer import normalize_event_payload

logger = logging.getLogger(__name__)


def get_dropping_odds_with_odds_and_events_response(client, sport: str):
    slug = sofascore_sport_slug(sport)
    if slug is None or slug not in sofascore_sport_slugs([sport]):
        raise ValueError(f"unsupported or disabled SofaScore sport: {sport!r}")
    endpoint = f"/odds/1/dropping/{slug}"
    logger.info(
        "✈️ Fetching SofaScore dropping feed requested_sport=%s canonical_sport=%s endpoint=%s",
        sport,
        canonical_sport_id(sport),
        endpoint,
    )
    return client.request_json_or_none(endpoint)


def get_high_value_streaks_events(client):
    logger.info("✈️ Fetching high value streaks feed")
    return client.request_json_or_none("/odds/1/high-value-streaks")


def get_team_streaks_events(client):
    logger.info("✈️ Fetching team streaks feed")
    return client.request_json_or_none("/odds/top-team-streaks/wins/all")


def get_h2h_events(client):
    logger.info("✈️ Fetching top H2H feed")
    return client.request_json_or_none("/odds/1/top-h2h/all")


def get_winning_odds_events(client):
    logger.info("✈️ Fetching winning odds discovery feed")
    return client.request_json_or_none("/odds/1/winning/all")


def extract_events_from_high_value_streaks(
    response: Dict,
    *,
    tracked_competitions=None,
) -> Tuple[List[Dict], List[Dict]]:
    if not isinstance(response, dict) or not response:
        return [], []
    tracked_competitions = (
        tracked_competitions
        if tracked_competitions is not None
        else load_tracked_source_competitions("sofascore")
    )
    events = []
    events_h2h = []

    try:
        if "general" in response:
            for item in response["general"]:
                event = item.get("event")
                if (
                    event
                    and is_supported_sofascore_event(event)
                    and is_tracked_source_event(event, tracked_competitions)
                ):
                    events.append(event)

        if "head2head" in response:
            for item in response["head2head"]:
                event = item.get("event")
                if (
                    event
                    and is_supported_sofascore_event(event)
                    and is_tracked_source_event(event, tracked_competitions)
                ):
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
    *,
    tracked_competitions=None,
) -> Tuple[List[Dict], Dict]:
    events: List[Dict] = []
    odds_map: Dict = {}
    supported_event_ids: set[str] = set()
    if not isinstance(response, dict):
        logger.warning("Dropping feed rejected response reason=invalid_response_payload")
        return events, odds_map
    if "events" not in response:
        logger.warning("Dropping feed rejected response reason=missing_events_key")
        return events, odds_map
    response_events = response.get("events")
    if response_events is None:
        response_events = []
    response_odds_map = response.get("oddsMap") or {}
    if not isinstance(response_events, list):
        logger.warning("Dropping feed rejected events payload reason=invalid_events_payload")
        return events, odds_map
    tracked_competitions = (
        tracked_competitions
        if tracked_competitions is not None
        else load_tracked_source_competitions("sofascore")
    )
    rejected_unsupported_sport = 0
    rejected_untracked_competition = 0
    rejected_missing_competition_ids = 0
    rejected_invalid_event_payload = 0
    normalization_errors = 0
    missing_required_fields = 0

    try:
        for event in response_events:
            if not isinstance(event, dict):
                rejected_invalid_event_payload += 1
                logger.debug(
                    "Dropping feed rejected event source_event_id=unknown reason=invalid_event_payload"
                )
                continue
            try:
                if not is_supported_sofascore_event(event):
                    rejected_unsupported_sport += 1
                    payload = event.get("event", event)
                    logger.debug(
                        "Dropping feed rejected event source_event_id=%s sport=%s "
                        "reason=unsupported_sport",
                        payload.get("id") if isinstance(payload, dict) else None,
                        payload.get("sport") if isinstance(payload, dict) else None,
                    )
                    continue
                if not is_tracked_source_event(event, tracked_competitions):
                    ids = source_competition_ids(event)
                    missing_ids = (
                        ids.source_tournament_id is None
                        and ids.source_unique_tournament_id is None
                    )
                    reason = "missing_source_competition_ids" if missing_ids else "untracked_competition"
                    if missing_ids:
                        rejected_missing_competition_ids += 1
                    else:
                        rejected_untracked_competition += 1
                    logger.debug(
                        "Dropping feed rejected event "
                        "source_event_id=%s tournament_id=%s unique_tournament_id=%s "
                        "reason=%s",
                        event.get("id"),
                        ids.source_tournament_id,
                        ids.source_unique_tournament_id,
                        reason,
                    )
                    continue
                event_data = normalize_event_payload(event, discovery_source)
                event_payload = event_data.get("event", event_data)
                required_fields = ["id", "slug", "startTimestamp", "sport", "competition", "homeTeam", "awayTeam"]
                missing_fields = [
                    field
                    for field in required_fields
                    if not isinstance(event_payload, dict) or not event_payload.get(field)
                ]
                if not missing_fields:
                    events.append(event_data)
                    supported_event_ids.add(str(event_payload["id"]))
                else:
                    missing_required_fields += 1
                    logger.debug(
                        "Dropping feed rejected event source_event_id=%s missing_fields=%s "
                        "reason=missing_required_fields",
                        event.get("id"),
                        missing_fields,
                    )
            except Exception as exc:
                normalization_errors += 1
                logger.warning(
                    "Dropping feed rejected event source_event_id=%s "
                    "reason=normalization_error error=%s",
                    event.get("id"),
                    exc,
                )

        response_odds_count = len(response_odds_map) if isinstance(response_odds_map, dict) else 0
        if odds_extraction and isinstance(response_odds_map, dict):
            odds_map = {
                event_id: odds_data
                for event_id, odds_data in response_odds_map.items()
                if str(event_id) in supported_event_ids
            }
            odds_without_eligible_event = response_odds_count - len(odds_map)
        else:
            odds_without_eligible_event = 0
            if odds_extraction and not isinstance(response_odds_map, dict):
                logger.warning(
                    "Dropping feed rejected odds payload reason=invalid_odds_map_payload"
                )

        logger.info(
            "Evaluated dropping feed response "
            "events=%s rejected_unsupported_sport=%s rejected_untracked_competition=%s "
            "rejected_missing_competition_ids=%s rejected_invalid_event_payload=%s "
            "rejected_missing_required_fields=%s rejected_normalization_error=%s "
            "eligible_events=%s response_odds=%s odds_without_eligible_event=%s "
            "eligible_odds=%s odds_extraction_enabled=%s",
            len(response_events),
            rejected_unsupported_sport,
            rejected_untracked_competition,
            rejected_missing_competition_ids,
            rejected_invalid_event_payload,
            missing_required_fields,
            normalization_errors,
            len(events),
            response_odds_count,
            odds_without_eligible_event,
            len(odds_map),
            odds_extraction,
        )

        return events, odds_map
    except Exception as exc:
        logger.error("Error extracting events and odds: %s", exc)
        return events, odds_map
