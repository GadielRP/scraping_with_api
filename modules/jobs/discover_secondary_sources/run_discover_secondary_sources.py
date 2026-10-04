"""Fetch and persist secondary feeds sequentially without retaining every response."""

import logging

from infrastructure.persistence.transient.discovery_run_store import DiscoveryRunStore
from modules.competition.discovery_scope import load_tracked_source_competitions
from modules.jobs.discovery.persistence import (
    persist_events,
    fetch_and_persist_events_with_odds,
    persist_events_with_odds,
)
from modules.jobs.discovery.summary import log_discovery_summary
from modules.sports.catalog import sofascore_sport_slugs
from shared.execution_context import check_execution_budget
from .run_high_value_streaks import run_high_value_streaks
from .run_team_streaks import run_team_streaks
from .run_top_h2h import run_top_h2h
from .run_winning_odds import run_winning_odds

logger = logging.getLogger(__name__)


def run_discover_secondary_sources() -> None:
    if not sofascore_sport_slugs():
        logger.info(
            "No supported SofaScore sport routes configured; skipping secondary discoveries"
        )
        return
    scope = load_tracked_source_competitions("sofascore")
    if scope is not None and not scope:
        logger.warning(
            "No tracked SofaScore tournament IDs are mapped; skipping secondary discoveries"
        )
        return
    logger.info("Starting secondary discovery")
    with DiscoveryRunStore() as store:
        try:
            check_execution_budget()
            streaks, streaks_h2h = run_high_value_streaks(scope)
            for source, events in (
                ("high_value_streaks", streaks),
                ("high_value_streaks_h2h", streaks_h2h),
            ):
                persist_events(events, source, tracked_competitions=scope, run_store=store)
            del streaks, streaks_h2h, events

            check_execution_budget()
            events = run_team_streaks(scope)
            processed, skipped = fetch_and_persist_events_with_odds(
                events, "team_streaks", tracked_competitions=scope, run_store=store
            )
            logger.info("Team streaks discovery processed=%s skipped=%s", processed, skipped)
            del events

            check_execution_budget()
            events = run_top_h2h(scope)
            persist_events(events, "h2h", tracked_competitions=scope, run_store=store)
            del events

            check_execution_budget()
            events, odds = run_winning_odds(scope)
            persist_events_with_odds(
                events, odds, "winning_odds", tracked_competitions=scope, run_store=store
            )
        finally:
            log_discovery_summary(store, logger, job="secondary_sources")
