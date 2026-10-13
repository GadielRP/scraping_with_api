# LEGACY(EXEC-005): migrate materialized dropping/secondary feeds; see docs/maintenance/execution-legacy-cleanup.md.
"""Discovery and secondary SofaScore feed helpers."""

from __future__ import annotations

import logging
from collections import Counter
from typing import Dict, List, Tuple

from modules.jobs.discovery.filters import (
    load_tracked_source_competitions,
    source_competition_ids,
    UNRESOLVED_SCOPE,
    admit_sofascore_event,
)
from modules.sports.catalog import (
    canonical_sport_id,
    sofascore_sport_slug,
    sofascore_sport_slugs,
)

from .event_normalizer import normalize_event_payload
from shared.execution_context import WorkDeferred, check_execution_budget

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
    tracked_competitions=UNRESOLVED_SCOPE,
) -> Tuple[List[Dict], List[Dict]]:
    """Admit and normalize both streak groups once through the shared feed path."""
    if not isinstance(response, dict) or not response:
        return [], []
    tracked_competitions = (
        tracked_competitions
        if tracked_competitions is not UNRESOLVED_SCOPE
        else load_tracked_source_competitions("sofascore")
    )
    try:
        groups = []
        for key, discovery_source in (
            ("general", "high_value_streaks"),
            ("head2head", "high_value_streaks_h2h"),
        ):
            raw_events = []
            for item in response.get(key, []):
                check_execution_budget()
                if item.get("event"):
                    raw_events.append(item["event"])
            events, _ = extract_events_and_odds_from_dropping_response(
                {"events": raw_events}, odds_extraction=False,
                discovery_source=discovery_source, tracked_competitions=tracked_competitions,
            )
            groups.append(events)
        logger.info(
            "Extracted %s general and %s head-to-head events from high value streaks",
            len(groups[0]), len(groups[1]),
        )
        return groups[0], groups[1]
    except WorkDeferred:
        raise
    except Exception as exc:
        logger.error("Error extracting high value streak events: %s", exc)
        return [], []


def extract_events_and_odds_from_dropping_response(
    response: Dict,
    odds_extraction: bool = True,
    discovery_source: str = "dropping_odds",
    *,
    tracked_competitions=UNRESOLVED_SCOPE,
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
        if tracked_competitions is not UNRESOLVED_SCOPE
        else load_tracked_source_competitions("sofascore")
    )
    rejected_unsupported_sport = 0
    rejected_untracked_competition = 0
    rejected_missing_competition_ids = 0
    rejected_invalid_event_payload = 0
    normalization_errors = 0
    missing_required_fields = 0
    rejected_policy = Counter()

    try:
        for event in response_events:
            payload = event.get("event", event) if isinstance(event, dict) else None
            source_event_id = payload.get("id") if isinstance(payload, dict) else None
            try:
                check_execution_budget()
                admission = admit_sofascore_event(event, tracked_competitions)
                reason = admission.rejection_reason
                if reason:
                    if reason == "unsupported_sport":
                        rejected_unsupported_sport += 1
                    elif reason == "missing_source_competition_ids":
                        rejected_missing_competition_ids += 1
                    elif reason == "untracked_competition":
                        rejected_untracked_competition += 1
                    elif reason == "invalid_event_payload":
                        rejected_invalid_event_payload += 1
                    else:
                        rejected_policy[reason] += 1
                    ids = source_competition_ids(event)
                    logger.debug(
                        "Dropping feed rejected event source_event_id=%s tournament_id=%s "
                        "unique_tournament_id=%s reason=%s",
                        source_event_id, ids.source_tournament_id,
                        ids.source_unique_tournament_id, reason,
                    )
                    continue
                event_data = normalize_event_payload(
                    admission.raw_event, discovery_source, resolved_round=admission.round_name,
                )
                event_payload = event_data.get("event", event_data)
                required_fields = (
                    "id",
                    "slug",
                    "startTimestamp",
                    "sport",
                    "competition",
                    "homeTeam",
                    "awayTeam",
                )
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
                        source_event_id,
                        missing_fields,
                    )
            except WorkDeferred:
                raise
            except Exception as exc:
                normalization_errors += 1
                logger.warning(
                    "Dropping feed rejected event source_event_id=%s "
                    "reason=normalization_error error=%s",
                    source_event_id,
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

        logger.info("Discovery feed admission source=%s rejected=%s", discovery_source, dict(rejected_policy))

        return events, odds_map
    except WorkDeferred:
        raise
    except Exception as exc:
        logger.error("Error extracting events and odds: %s", exc)
        return events, odds_map
