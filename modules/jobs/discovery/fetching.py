"""Bounded concurrent SofaScore discovery requests; no database writes."""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from infrastructure.settings import discovery as settings
from modules.jobs.discovery.filters import (
    sofascore_event_filter_reason,
    load_tracked_source_competitions,
    UNRESOLVED_SCOPE,
)
from modules.odds_ingestion.fetch_result import OddsFetchStatus
from modules.sofascore import api_client
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher
from shared.concurrency import bounded_results
from shared.execution_context import WorkDeferred

logger = logging.getLogger(__name__)


def fetch_nearest_team_events(
    team_ids: List[int],
    max_workers: int | None = None,
    *,
    tracked_competitions=UNRESOLVED_SCOPE,
) -> List[Dict]:
    """Fetch nearest events for multiple teams in parallel."""
    max_workers = settings.SOFASCORE.team_event_workers if max_workers is None else max_workers
    tracked_competitions = (
        tracked_competitions
        if tracked_competitions is not UNRESOLVED_SCOPE
        else load_tracked_source_competitions("sofascore")
    )

    def fetch_team_event(team_id: int) -> Optional[Dict]:
        try:
            event_response = api_client.get_nearest_event_for_team(team_id)
            if not event_response:
                logger.debug("No nearest event found for team %s", team_id)
                return None

            reason = sofascore_event_filter_reason(event_response, tracked_competitions)
            if reason:
                logger.debug("Skipping team event team=%s reason=%s", team_id, reason)
                return None

            event_data = api_client.normalize_event_payload(
                event_response, discovery_source="team_streaks"
            )
            if not event_data:
                logger.debug("Failed to structure event data for team %s", team_id)
                return None
            logger.debug(
                "Fetched event %s for team %s",
                event_data.get("event", event_data).get("id"),
                team_id,
            )
            return event_data
        except WorkDeferred:
            raise
        except Exception as exc:
            logger.debug("Error processing team %s: %s", team_id, exc)
            return None

    return [event for event in bounded_results(fetch_team_event, team_ids, max_workers) if event]


@dataclass
class OddsFetchSummary:
    """Typed outcomes from fetching SofaScore odds concurrently."""

    odds_by_source_event_id: Dict[str, Dict] = field(default_factory=dict)
    endpoint_missing_source_event_ids: set[int] = field(default_factory=set)
    empty_source_event_ids: set[int] = field(default_factory=set)
    failed_source_event_ids: set[int] = field(default_factory=set)


def fetch_event_odds(
    events: List[Dict],
    max_workers: int | None = None,
    *,
    odds_fetcher: SofaScoreOddsFetcher | None = None,
) -> OddsFetchSummary:
    """Fetch odds without treating temporary failures as missing endpoints."""
    max_workers = settings.SOFASCORE.odds_workers if max_workers is None else max_workers
    fetcher = odds_fetcher or SofaScoreOddsFetcher(api_client)

    def fetch_odds(event_data: Dict):
        sofascore_event_id = str(event_data.get("event", event_data)["id"])
        try:
            return sofascore_event_id, fetcher.fetch_odds(int(sofascore_event_id))
        except WorkDeferred:
            raise
        except Exception:
            logger.exception("Temporary odds fetch failure source_event_id=%s", sofascore_event_id)
            return sofascore_event_id, None

    summary = OddsFetchSummary()
    for source_event_id, fetch_result in bounded_results(fetch_odds, events, max_workers):
        numeric_id = int(source_event_id)
        if fetch_result is None:
            summary.failed_source_event_ids.add(numeric_id)
        elif fetch_result.status is OddsFetchStatus.SUCCESS:
            summary.odds_by_source_event_id[source_event_id] = fetch_result.payload
        elif fetch_result.status is OddsFetchStatus.ENDPOINT_NOT_FOUND:
            summary.endpoint_missing_source_event_ids.add(numeric_id)
        else:
            summary.empty_source_event_ids.add(numeric_id)

    return summary
