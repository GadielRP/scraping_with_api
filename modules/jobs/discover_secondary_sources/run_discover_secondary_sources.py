"""Secondary discovery orchestrator."""

from __future__ import annotations

import logging

from modules.jobs.discover_secondary_sources.run_high_value_streaks import run_high_value_streaks
from modules.jobs.discover_secondary_sources.run_optimization import (
    run_optimization,
    run_winning_odds_optimization,
)
from modules.jobs.discover_secondary_sources.run_team_streaks import run_team_streaks
from modules.jobs.discover_secondary_sources.run_top_h2h import run_top_h2h
from modules.jobs.discover_secondary_sources.run_winning_odds import run_winning_odds

logger = logging.getLogger(__name__)


def run_discover_secondary_sources() -> None:
    """Discover events from streaks, H2H and winning odds sources."""
    logger.info("Starting Job B: Event Discovery from streaks, team streaks, h2h and winning odds events")

    try:
        high_value_streaks_events, high_value_streaks_events_h2h = run_high_value_streaks()
        if not high_value_streaks_events:
            return
        if not high_value_streaks_events_h2h:
            return

        team_streaks_events = run_team_streaks()
        if not team_streaks_events:
            logger.warning("No events found after processing team streaks")
        else:
            processed_count, skipped_count = run_optimization(team_streaks_events, discovery_source="team_streaks")
            logger.info(
                f"team streaks events completed: processed {processed_count}/{len(team_streaks_events)} events, skipped {skipped_count} events"
            )

        matchup_events = run_top_h2h()
        if not matchup_events:
            return

        winning_odds_events, winning_odds_events_odds_map = run_winning_odds()
        if not winning_odds_events:
            return

        processed_count, skipped_count = run_optimization(
            high_value_streaks_events,
            discovery_source="high_value_streaks",
        )
        logger.info(
            f"high value streaks events completed: processed {processed_count}/{len(high_value_streaks_events)} events, skipped {skipped_count} events"
        )

        processed_count, skipped_count = run_optimization(
            high_value_streaks_events_h2h,
            discovery_source="high_value_streaks_h2h",
        )
        logger.info(
            f"high value streaks events h2h completed: processed {processed_count}/{len(high_value_streaks_events_h2h)} events, skipped {skipped_count} events"
        )

        processed_count, skipped_count = run_optimization(matchup_events, discovery_source="h2h")
        logger.info(f"h2h events completed: processed {processed_count}/{len(matchup_events)} events, skipped {skipped_count} events")

        processed_count, skipped_count = run_winning_odds_optimization(
            winning_odds_events,
            winning_odds_events_odds_map,
        )
        logger.info(
            f"winning odds events completed: processed {processed_count}/{len(winning_odds_events)} events, skipped {skipped_count} events"
        )
    except Exception as exc:
        logger.error(f"Error in Job B: {exc}")
