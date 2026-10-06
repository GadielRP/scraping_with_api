"""Discover admitted SofaScore events with dropping odds."""

from __future__ import annotations

import logging

from infrastructure.persistence.transient.discovery_run_store import DiscoveryRunStore
from infrastructure.settings import Config
from modules.jobs.discovery.filters import (
    load_tracked_source_competitions,
    sofascore_discovery_sport_routes,
)
from modules.jobs.discovery.persistence import persist_events_with_odds
from modules.jobs.discovery.summary import log_discovery_summary
from modules.sofascore import api_client
from modules.sports.catalog import configured_sport_ids
from shared.execution_context import WorkDeferred, check_execution_budget

logger = logging.getLogger(__name__)


def run_discover_dropping_odds() -> None:
    """Fetch each admitted sport feed once and persist events under the configured policy."""
    routes = sofascore_discovery_sport_routes()
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
        logger.info("Skipping dropping odds calls reason=no_supported_sports_configured")
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
    with DiscoveryRunStore() as store:
        try:
            for sport in sports:
                check_execution_budget()
                try:
                    response = api_client.get_dropping_odds_with_odds_and_events_response(
                        sport=sport
                    )
                    if not response:
                        logger.warning(
                            "No dropping odds response sport=%s endpoint=/odds/1/dropping/%s "
                            "reason=empty_response_or_http_error",
                            sport,
                            sport,
                        )
                        continue

                    admitted_events, odds_map = api_client.extract_events_and_odds_from_dropping_response(
                        response,
                        odds_extraction=True,
                        discovery_source="dropping_odds",
                        tracked_competitions=tracked_competitions,
                    )
                    del response
                    admitted_event_count = len(admitted_events)
                    selected_ids = set()
                    events = []
                    duplicate_event_count = 0
                    for event in admitted_events:
                        source_event_id = str(event.get("event", event)["id"])
                        if (
                            source_event_id in processed_event_ids
                            or source_event_id in selected_ids
                        ):
                            duplicate_event_count += 1
                            logger.debug(
                                "Dropping feed rejected event source_event_id=%s "
                                "reason=duplicate_event_in_run",
                                source_event_id,
                            )
                            continue
                        selected_ids.add(source_event_id)
                        events.append(event)
                    del admitted_events
                    response_eligible_odds_count = len(odds_map)
                    odds_map = {
                        str(sid): odds for sid, odds in odds_map.items() if str(sid) in selected_ids
                    }
                    events_without_odds_count = len(selected_ids - odds_map.keys())
                    logger.info(
                        "Dropping feed selection "
                        "sport=%s admitted_events=%s rejected_duplicate_events=%s new_events=%s "
                        "eligible_odds=%s odds_without_new_event=%s "
                        "odds_for_new_events=%s events_without_odds=%s",
                        sport,
                        admitted_event_count,
                        duplicate_event_count,
                        len(events),
                        response_eligible_odds_count,
                        response_eligible_odds_count - len(odds_map),
                        len(odds_map),
                        events_without_odds_count,
                    )
                    if not events:
                        logger.info(
                            "No dropping odds events to persist sport=%s "
                            "reason=no_events_remained_after_admission_and_deduplication_filters",
                            sport,
                        )
                        continue

                    processed, skipped = persist_events_with_odds(
                        events,
                        odds_map,
                        discovery_source="dropping_odds",
                        tracked_competitions=tracked_competitions,
                        run_store=store,
                    )
                    totals["processed"] += processed
                    totals["skipped"] += skipped
                    processed_event_ids.update(selected_ids)
                    logger.info(
                        "Dropping odds sport=%s processed=%s/%s skipped=%s",
                        sport,
                        processed,
                        len(events),
                        skipped,
                    )
                except WorkDeferred:
                    raise
                except Exception:
                    logger.exception("Error processing dropping odds sport=%s", sport)

            logger.info(
                "Dropping odds discovery complete processed=%s skipped=%s unique_events=%s",
                totals["processed"],
                totals["skipped"],
                len(processed_event_ids),
            )

        finally:
            log_discovery_summary(store, logger, job="dropping_odds")
