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


def persist_snapshot_observations(candidates: list[dict]) -> int:
    """Persist snapshot observations for tennis events still missing DB rows."""
    events: list[tuple[int, list[dict], str | None]] = []
    for candidate in candidates or []:
        if candidate.get("observations"):
            continue
        event_data = candidate.get("event_data") or {}
        if event_data.get("sport") not in _TENNIS_SPORTS:
            continue
        snapshot = candidate.get("metadata_snapshot") or {}
        observations = snapshot.get("observations") or []
        if not observations:
            continue
        candidate["observations"] = observations
        events.append(
            (candidate["event_id"], observations, event_data.get("sport"))
        )

    if not events:
        return 0

    saved = sport_observation_service.save_observations_for_events(events)
    logger.info(
        "Persisted tennis snapshot observations events=%s rows=%s",
        len(events),
        saved,
    )
    return saved


def attach_stored_observations(candidates: list[dict]) -> int:
    """Attach DB observations to tennis candidates missing them, one session."""
    missing: list[dict] = []
    for candidate in candidates or []:
        event_data = candidate.get("event_data") or {}
        if event_data.get("sport") not in _TENNIS_SPORTS:
            continue
        if candidate.get("observations"):
            continue
        if candidate.get("event_id") is None:
            continue
        missing.append(candidate)

    if not missing:
        return 0

    by_event = sport_observation_service.observations_for_events(
        [candidate["event_id"] for candidate in missing]
    )
    attached = 0
    for candidate in missing:
        observations = by_event.get(candidate["event_id"])
        if not observations:
            continue
        candidate["observations"] = observations
        attached += 1

    logger.info(
        "Attached stored tennis observations missing=%s attached=%s",
        len(missing),
        attached,
    )
    return attached


def enrich_tennis_observations(candidate: dict) -> None:
    """Attach court-type observations to a just-ingested tennis event, if missing."""
    event_data = candidate["event_data"]
    if event_data.get("sport") not in _TENNIS_SPORTS:
        return

    if candidate.get("observations"):
        return

    snapshot = candidate.get("metadata_snapshot")
    if snapshot and snapshot.get("observations"):
        candidate["observations"] = snapshot["observations"]
        return

    sofascore_event_id = candidate.get("sofascore_event_id")
    if sofascore_event_id is None:
        return

    event_id = candidate["event_id"]
    observations = api_client.get_event_results(
        sofascore_event_id,
        canonical_event_id=event_id,
        update_court_type=True,
    )
    if observations:
        candidate["observations"] = observations
