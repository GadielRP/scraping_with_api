"""Event detail and synchronization helpers for SofaScore."""

from __future__ import annotations

import json
import logging
from typing import Dict, List, Optional

from infrastructure.settings import Config
from infrastructure.persistence.repositories import EventRepository, SeasonRepository, EventSourceMappingRepository
from modules.observations import sport_observation_service
from modules.observations.sofascore_extractor import extract_observations_from_sofascore_response

from .event_normalizer import normalize_event_payload
from .exceptions import SofaScoreNotFoundException, SofaScoreRateLimitException
from .results_parser import ParsedEventResult, parse_event_result
from modules.events.discards.contracts import DeletionBatch

logger = logging.getLogger(__name__)

EventResultsResponse = (
    Dict
    | List[Dict]
    | tuple[Optional[bool], Dict | ParsedEventResult | None]
    | bool
    | None
)


def _queue_canonical_event_for_deletion(
    canonical_event_id: int | None,
    sofascore_event_id: int,
    reason: str,
    deferred_deletion_event_ids: set[int] | None,
    *,
    parsed=None,
) -> bool:
    """Queue a deletion for a caller-owned batch; never delete inline."""
    if deferred_deletion_event_ids is None:
        logger.warning(
            "SofaScore event %s marked for deletion (%s), but no batch deletion "
            "collector was provided; leaving canonical event in place",
            sofascore_event_id,
            reason,
        )
        return False

    if canonical_event_id is None:
        canonical_event_id = EventSourceMappingRepository.get_event_id_by_source(
            "sofascore",
            str(sofascore_event_id),
        )
    if canonical_event_id is None:
        logger.warning(
            "Could not resolve canonical event_id for SofaScore event %s (%s)",
            sofascore_event_id,
            reason,
        )
        return False

    if parsed is not None and isinstance(deferred_deletion_event_ids, DeletionBatch):
        deferred_deletion_event_ids.record(canonical_event_id, sofascore_event_id, parsed, reason)
    elif isinstance(deferred_deletion_event_ids, DeletionBatch):
        deferred_deletion_event_ids.record_missing(canonical_event_id, sofascore_event_id, reason)
    else:
        deferred_deletion_event_ids.add(canonical_event_id)
    logger.info(
        "🗑️ Queued canonical event %s for batch deletion "
        "(SofaScore event %s, reason=%s)",
        canonical_event_id,
        sofascore_event_id,
        reason,
    )
    return True


def fetch_authoritative_event_response(
    client,
    event_id: int,
    *,
    canonical_event_id: int | None = None,
    deferred_deletion_event_ids: set[int] | None = None,
) -> Optional[Dict]:
    """Fetch `/event/{id}` and optionally queue a missing event for batch deletion."""
    endpoint = f"/event/{event_id}"
    try:
        return client.request_json(endpoint)
    except SofaScoreNotFoundException:
        _queue_canonical_event_for_deletion(
            canonical_event_id,
            event_id,
            "event_endpoint_not_found",
            deferred_deletion_event_ids,
        )
        return None
    except SofaScoreRateLimitException:
        logger.warning("Rate limited while fetching event %s", event_id)
        return None


def get_event_details(client, event_id: int) -> Optional[Dict]:
    response = fetch_authoritative_event_response(client, event_id)
    if not response or "event" not in response:
        return None
    return response["event"]


def update_event_information_from_response(response: Dict) -> bool:
    try:
        if not response or "event" not in response:
            logger.warning("No event data in response for information update")
            return False

        event_response = response["event"]
        event_data = normalize_event_payload(event_response, discovery_source="results_sync")
        event_payload = event_data.get("event", event_data) if event_data else {}
        if not event_payload or not event_payload.get("id"):
            logger.warning("Could not extract event information from response")
            return False

        event_payload.pop("discovery_source", None)
        updated_event = EventRepository.upsert_event(event_data)
        if updated_event:
            logger.info(
                "Event information updated for event %s from results sync (season_id=%s, round=%s, season_year=%s)",
                event_payload["id"],
                event_payload.get("season_id"),
                event_payload.get("round"),
                event_payload.get("season_year"),
            )
            return True

        logger.warning("Failed to update event information for event %s", event_payload.get("id"))
        return False
    except Exception as exc:
        logger.error("Error updating event information from response: %s", exc)
        return False


def extract_observations_from_response(response: Dict) -> Optional[List[Dict]]:
    """Extract observations directly using sofascore_extractor."""
    return extract_observations_from_sofascore_response(response)


def _extract_metadata_snapshot(response: Dict) -> Optional[Dict]:
    try:
        if not response or "event" not in response:
            return None

        event_data = response["event"]
        home_team = event_data.get("homeTeam", {})
        away_team = event_data.get("awayTeam", {})
        tournament = event_data.get("tournament", {})
        unique_tournament = tournament.get("uniqueTournament", {})
        season_data = event_data.get("season", {})

        season_year_raw = season_data.get("year")
        season_year = SeasonRepository._parse_year(season_year_raw) if season_year_raw is not None else None

        observations = extract_observations_from_response(response) or []
        if not any(observation.get("type") == "rankings" for observation in observations):
            home_ranking = home_team.get("ranking")
            away_ranking = away_team.get("ranking")
            if home_ranking is not None or away_ranking is not None:
                observations.append(
                    {
                        "type": "rankings",
                        "home_ranking": home_ranking,
                        "away_ranking": away_ranking,
                    }
                )

        return {
            "home_team_id": home_team.get("id"),
            "away_team_id": away_team.get("id"),
            "home_team_ranking": home_team.get("ranking"),
            "away_team_ranking": away_team.get("ranking"),
            "tournament_id": tournament.get("id"),
            "tournament_name": tournament.get("name"),
            "unique_tournament_id": unique_tournament.get("id"),
            "unique_tournament_name": unique_tournament.get("name"),
            "competition_slug": unique_tournament.get("slug"),
            "season_id": str(season_data.get("id", "")) if season_data.get("id") else None,
            "season_name": season_data.get("name"),
            "season_year": season_year,
            "observations": observations,
        }
    except Exception as exc:
        logger.warning("Error extracting metadata snapshot: %s", exc)
        return None


def result_from_response(response, event_id, *, canonical_event_id=None,
                         deferred_deletion_event_ids=None, on_not_started="ignore",
                         log_result_diagnostics=False):
    """Interpret a fetched response and collect deletion evidence without writing."""
    if on_not_started not in {"ignore", "delete"}:
        raise ValueError(
            f"Unsupported on_not_started policy: {on_not_started!r}"
        )

    parsed = parse_event_result(response)
    if log_result_diagnostics and parsed.kind != "finished":
        _log_result_parse_diagnostics(event_id, response, parsed.kind)
    if parsed.kind == "canceled":
        deletion_reason = (
            "walkover"
            if (parsed.status_description or "") == "walkover"
            else "canceled_or_postponed"
        )
        _queue_canonical_event_for_deletion(
            canonical_event_id,
            event_id,
            deletion_reason,
            deferred_deletion_event_ids,
            parsed=parsed,
        )
        return None

    if parsed.kind == "not_started" and on_not_started == "delete":
        _queue_canonical_event_for_deletion(
            canonical_event_id,
            event_id,
            "stale_not_started",
            deferred_deletion_event_ids,
            parsed=parsed,
        )
        return None

    if parsed.kind == "finished_empty_score":
        _queue_canonical_event_for_deletion(
            canonical_event_id,
            event_id,
            "finished_empty_score",
            deferred_deletion_event_ids,
            parsed=parsed,
        )
        return None

    if parsed.kind == "finished":
        return parsed.result

    return None


def get_event_results(
    client,
    event_id: int,
    update_time: bool = False,
    update_court_type: bool = False,
    minutes_until_start: int = 0,
    update_event_info: bool = True,
    return_snapshot: bool = False,
    current_start_time=None,
    canonical_event_id: int | None = None,
    deferred_deletion_event_ids: set[int] | None = None,
    on_not_started: str = "ignore",
    also_parse_result: bool = False,
    log_result_diagnostics: bool = False,
) -> EventResultsResponse:
    log_result_diagnostics = bool(
        log_result_diagnostics
        or getattr(Config, "global_debug_mode", False)
        or getattr(client, "debug_mode", False)
        or getattr(client, "challenge_evidence_enabled", False)
    )

    def _empty_response() -> EventResultsResponse:
        return (None, None) if return_snapshot or (update_time and also_parse_result) else None

    try:
        if update_court_type:
            logger.info("✈️ Fetching /event/%s endpoint to update court type", event_id)
        elif update_time:
            logger.info("⏱️ Fetching /event/%s endpoint to update time", event_id)
        elif return_snapshot and update_time==False:
            logger.info("✈️ Fetching /event/%s endpoint to get metadata snapshot (timestamp correction bypassed)", event_id)
        else:
            logger.info("✈️ Fetching event results for event %s", event_id)

        response = fetch_authoritative_event_response(
            client,
            event_id,
            canonical_event_id=canonical_event_id,
            deferred_deletion_event_ids=deferred_deletion_event_ids,
        )
        if not response:
            logger.warning("No response received for event %s", event_id)
            return _empty_response()

        if update_event_info and not update_court_type:
            update_event_information_from_response(response)

        if update_court_type:
            event_data = response.get("event", {})
            home_team_ranking = event_data.get("homeTeam", {}).get("ranking")
            away_team_ranking = event_data.get("awayTeam", {}).get("ranking")
            observation_event_id = canonical_event_id
            if observation_event_id is None:
                observation_event_id = EventSourceMappingRepository.get_event_id_by_source(
                    "sofascore",
                    str(event_id),
                )
            ground_type = None
            if observation_event_id is None:
                logger.warning(
                    "Skipping tennis ground-type persistence: canonical event ID was not resolved for SofaScore event %s",
                    event_id,
                )
            else:
                ground_type = sport_observation_service.extract_and_save_tennis_ground_type(
                    observation_event_id,
                    response,
                )
            return [
                {
                    "type": "ground_type",
                    "value": ground_type,
                },
                {
                    "type": "rankings",
                    "home_ranking": home_team_ranking,
                    "away_ranking": away_team_ranking,
                },
            ]

        if update_time:
            logger.info("🔎 Checking and updating starting time for event %s", event_id)
            event_data = response.get("event", {})
            start_timestamp = event_data.get("startTimestamp")
            timing_event_id = canonical_event_id
            if start_timestamp is not None and timing_event_id is None:
                timing_event_id = EventSourceMappingRepository.get_event_id_by_source(
                    "sofascore",
                    str(event_id),
                )

            timing_result = None
            if start_timestamp is None:
                logger.warning("No startTimestamp found in API response for event %s", event_id)
            elif timing_event_id is None:
                logger.warning(
                    "Skipping time update: canonical event ID was not resolved for SofaScore event %s",
                    event_id,
                )
            else:
                timing_result = client.check_and_update_starting_time(
                    timing_event_id,
                    start_timestamp,
                    send_alert=True,
                    current_starting_time=current_start_time,
                )

            if also_parse_result:
                parsed = parse_event_result(response)
                if log_result_diagnostics and parsed.kind != "finished":
                    _log_result_parse_diagnostics(event_id, response, parsed.kind)
                return timing_result, parsed

            if return_snapshot:
                return timing_result, _extract_metadata_snapshot(response)
            return timing_result

        elif return_snapshot and update_time==False:
            logger.info("Parsing metadata snapshot for event %s (timestamp correction bypassed)", event_id)
            return True, _extract_metadata_snapshot(response)

        return result_from_response(
            response, event_id, canonical_event_id=canonical_event_id,
            deferred_deletion_event_ids=deferred_deletion_event_ids,
            on_not_started=on_not_started, log_result_diagnostics=log_result_diagnostics,
        )
    except Exception as exc:
        logger.error("Error fetching event results for %s: %s", event_id, exc)
        return _empty_response()


def _log_result_parse_diagnostics(event_id: int, response: Dict, parsed_kind: str) -> None:
    """Log raw SofaScore status and score fields when a result was not parsed."""
    event_data = response.get("event") or {}
    payload = {
        "status": event_data.get("status"),
        "homeScore": event_data.get("homeScore"),
        "awayScore": event_data.get("awayScore"),
        "winnerCode": event_data.get("winnerCode"),
    }
    logger.warning(
        "Result parse diagnostics: sofascore_event_id=%s classification=%s raw=%s",
        event_id,
        parsed_kind,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
    )
