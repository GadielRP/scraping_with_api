"""Secondary discovery orchestrator."""

from __future__ import annotations

import logging

from modules.competition.discovery_scope import load_tracked_source_competitions
from modules.jobs.parallelism import (
    process_events_only,
    process_odds_first,
    process_with_parallel_db_ops,
)
from modules.sports.catalog import sofascore_sport_slugs
from modules.jobs.discover_secondary_sources.run_high_value_streaks import run_high_value_streaks
from modules.jobs.discover_secondary_sources.run_team_streaks import run_team_streaks
from modules.jobs.discover_secondary_sources.run_top_h2h import run_top_h2h
from modules.jobs.discover_secondary_sources.run_winning_odds import run_winning_odds

logger = logging.getLogger(__name__)


def run_discover_secondary_sources() -> None:
    """Discover events from streaks, H2H and winning odds sources."""
    logger.info("Starting Job B: Event Discovery from streaks, team streaks, h2h and winning odds events")

    try:
        if not sofascore_sport_slugs():
            logger.info("No supported SofaScore sport routes configured; skipping secondary discoveries")
            return
        tracked_competitions = load_tracked_source_competitions("sofascore")
        if tracked_competitions is not None and not tracked_competitions:
            logger.warning("No tracked SofaScore tournament IDs are mapped; skipping secondary discoveries")
            return
        # HIGH VALUE STREAKS
        high_value_streaks_events, high_value_streaks_events_h2h = run_high_value_streaks(tracked_competitions)

        # TEAM STREAKS
        team_streaks_events = run_team_streaks(tracked_competitions)
        if not team_streaks_events:
            logger.warning("No events found after processing team streaks")
        else:
            processed_count, skipped_count = process_odds_first(
                team_streaks_events,
                discovery_source="team_streaks",
                max_workers=10,
                tracked_competitions=tracked_competitions,
            )
            logger.info(
                "team streaks events completed: processed %s/%s events, skipped %s events",
                processed_count,
                len(team_streaks_events),
                skipped_count,
            )

        # TOP H2H
        matchup_events = run_top_h2h(tracked_competitions)

        # WINNING ODDS
        winning_odds_events, winning_odds_events_odds_map = run_winning_odds(tracked_competitions)

        for source, events in (
            ("high_value_streaks", high_value_streaks_events),
            ("high_value_streaks_h2h", high_value_streaks_events_h2h),
            ("h2h", matchup_events),
        ):
            processed_count, skipped_count = process_events_only(
                events,
                discovery_source=source,
                tracked_competitions=tracked_competitions,
            )
            logger.info(
                "%s events completed: processed %s/%s events, skipped %s events",
                source,
                processed_count,
                len(events),
                skipped_count,
            )

        processed_count, skipped_count = process_with_parallel_db_ops(
            winning_odds_events,
            winning_odds_events_odds_map,
            discovery_source="winning_odds",
            max_workers=10,
            tracked_competitions=tracked_competitions,
        )
        logger.info(
            "winning odds events completed: processed %s/%s events, skipped %s events",
            processed_count,
            len(winning_odds_events),
            skipped_count,
        )
    except Exception as exc:
        logger.error("Error in Job B: %s", exc)
