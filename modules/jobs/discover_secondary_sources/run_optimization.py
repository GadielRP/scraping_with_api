"""Discovery optimization helper job."""

from __future__ import annotations

import logging

from optimization import process_events_only, process_with_batch_cleanup, process_with_parallel_db_ops

logger = logging.getLogger(__name__)


def run_optimization(events, discovery_source: str):
    if discovery_source == "team_streaks":
        return process_with_batch_cleanup(events, discovery_source=discovery_source, max_workers=10)
    if discovery_source == "winning_odds":
        raise ValueError("run_optimization does not handle winning odds maps")
    return process_events_only(events, discovery_source=discovery_source, max_workers=10)


def run_winning_odds_optimization(events, odds_map):
    return process_with_parallel_db_ops(events, odds_map, discovery_source="winning_odds", max_workers=10)
