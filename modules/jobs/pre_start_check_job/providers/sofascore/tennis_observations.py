"""Tennis observation enrichment triggered after a SofaScore odds ingest.

This is a SofaScore-specific side effect (it needs a resolved
`sofascore_event_id` to call the results endpoint), so it is kept out of the
generic provider odds phase and wired in as an `on_ingested` hook instead.
"""

from __future__ import annotations

import logging

from modules.observations import sport_observation_service
from modules.sofascore import api_client

logger = logging.getLogger(__name__)

_TENNIS_SPORTS = {"Tennis", "Tennis Doubles"}


def enrich_tennis_observations(candidate: dict) -> None:
    """Attach court-type observations to a just-ingested tennis event, if missing."""
    event_data = candidate["event_data"]
    if event_data.get("sport") not in _TENNIS_SPORTS:
        return

    event_id = candidate["event_id"]
    if sport_observation_service.event_has_observations(event_id):
        return

    snapshot = candidate.get("metadata_snapshot")
    if snapshot and snapshot.get("observations"):
        candidate["observations"] = snapshot["observations"]
        return

    sofascore_event_id = candidate.get("sofascore_event_id")
    if sofascore_event_id is None:
        return

    observations = api_client.get_event_results(
        sofascore_event_id,
        canonical_event_id=event_id,
        update_court_type=True,
    )
    if observations:
        candidate["observations"] = observations
