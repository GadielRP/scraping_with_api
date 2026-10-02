"""Discover tracked SofaScore events with dropping odds."""

from __future__ import annotations

import logging

from infrastructure.settings import Config
from modules.competition.discovery_scope import load_tracked_source_competitions
from modules.jobs.discovery_filters import filter_upcoming_events
from modules.jobs.discovery_persistence_summary import DiscoveryPersistenceSummary
from modules.jobs.parallelism import process_with_parallel_db_ops
from modules.sofascore import api_client
from modules.sports.catalog import configured_sport_ids, sofascore_sport_routes

logger = logging.getLogger(__name__)


def _event_payload(event):
    return event.get("event", event)


def _event_id(event):
    return _event_payload(event).get("id")


def run_discover_dropping_odds() -> None:
    """Fetch each configured SofaScore sport feed once and persist tracked events."""
    routes = sofascore_sport_routes()
    sports = list(dict.fromkeys(slug for _, slug in routes))
    logger.info(
        "Resolved dropping odds sport scope "
        "configured=%s canonical=%s sofascore_routes=%s endpoints=%s",
        Config.SUPPORTED_SPORTS,
        sorted(configured_sport_ids()),
        routes,
        [f"/odds/1/dropping/{sport}" for sport in sports],
    )
    if not sports:
        logger.info(
            "Skipping dropping odds calls reason=no_supported_sports_configured"
        )
        return
    tracked_competitions = load_tracked_source_competitions("sofascore")
    if tracked_competitions is not None and not tracked_competitions:
        logger.warning(
            "Skipping dropping odds calls tracked_competitions=0 "
            "reason=tracked_competition_scope_empty"
        )
        return
    logger.info(
        "Starting dropping odds discovery sports=%s tracked_competitions=%s",
        sports,
        "disabled" if tracked_competitions is None else len(tracked_competitions),
    )
    totals = {"processed": 0, "skipped": 0}
    processed_event_ids = set()
    persistence_summary = DiscoveryPersistenceSummary()

    for sport in sports:
        try:
            response = api_client.get_dropping_odds_with_odds_and_events_response(sport=sport)
            if not response:
                logger.warning(
                    "No dropping odds response sport=%s endpoint=/odds/1/dropping/%s "
                    "reason=empty_response_or_http_error",
                    sport,
                    sport,
                )
                continue

            events, odds_map = api_client.extract_events_and_odds_from_dropping_response(
                response,
                odds_extraction=True,
                discovery_source="dropping_odds",
                tracked_competitions=tracked_competitions,
            )
            response_eligible_count = len(events)
            upcoming_events = filter_upcoming_events(events)
            seen_event_ids = set(processed_event_ids)
            events = []
            duplicate_event_count = 0
            for event in upcoming_events:
                source_event_id = _event_id(event)
                if source_event_id in seen_event_ids:
                    duplicate_event_count += 1
                    logger.debug(
                        "Dropping feed rejected event source_event_id=%s "
                        "reason=duplicate_event_in_run",
                        source_event_id,
                    )
                    continue
                seen_event_ids.add(source_event_id)
                events.append(event)
            upcoming_unique_count = len(events)
            event_id_keys = {str(_event_id(event)) for event in events}
            event_ids = {_event_id(event) for event in events}
            response_eligible_odds_count = len(odds_map)
            eligible_odds_map = {}
            for event_id, odds in odds_map.items():
                if str(event_id) in event_id_keys:
                    eligible_odds_map[str(event_id)] = odds
                else:
                    logger.debug(
                        "Dropping feed excluded odds source_event_id=%s "
                        "reason=no_upcoming_new_event",
                        event_id,
                    )
            odds_map = eligible_odds_map
            event_ids_with_odds = set(odds_map)
            events_without_odds = [
                _event_id(event)
                for event in events
                if str(_event_id(event)) not in event_ids_with_odds
            ]
            for source_event_id in events_without_odds:
                logger.debug(
                    "Dropping feed event has no odds entry source_event_id=%s "
                    "reason=missing_odds_map_entry",
                    source_event_id,
                )
            logger.info(
                "Dropping feed selection "
                "sport=%s eligible_events=%s rejected_by_time_filter=%s "
                "upcoming_candidates=%s rejected_duplicate_events=%s upcoming_new_events=%s "
                "eligible_odds=%s odds_without_upcoming_new_event=%s "
                "odds_for_upcoming=%s events_without_odds=%s",
                sport,
                response_eligible_count,
                response_eligible_count - len(upcoming_events),
                len(upcoming_events),
                duplicate_event_count,
                upcoming_unique_count,
                response_eligible_odds_count,
                response_eligible_odds_count - len(odds_map),
                len(odds_map),
                len(events_without_odds),
            )
            if not events:
                logger.info(
                    "No dropping odds events to persist sport=%s "
                    "reason=no_events_remained_after_eligibility_time_and_deduplication_filters",
                    sport,
                )
                continue

            processed, skipped = process_with_parallel_db_ops(
                events,
                odds_map,
                discovery_source="dropping_odds",
                max_workers=10,
                tracked_competitions=tracked_competitions,
                persistence_summary=persistence_summary,
            )
            totals["processed"] += processed
            totals["skipped"] += skipped
            processed_event_ids.update(event_ids)
            logger.info(
                "Dropping odds sport=%s processed=%s/%s skipped=%s",
                sport,
                processed,
                len(events),
                skipped,
            )
        except Exception:
            logger.exception("Error processing dropping odds sport=%s", sport)

    logger.info(
        "Dropping odds discovery complete processed=%s skipped=%s unique_events=%s",
        totals["processed"],
        totals["skipped"],
        len(processed_event_ids),
    )
    persistence_summary.log(logger, job="dropping_odds")
