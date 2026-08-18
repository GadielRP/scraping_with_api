"""Build pre-start provider candidates from upcoming canonical events."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Collection

from infrastructure.settings import Config

from .odds_source_state import (
    SOFASCORE_SOURCE,
    PreStartOddsSourceStates,
    get_numeric_source_event_id,
)
from .rescheduled_events import handle_rescheduled_event
from .timing import minutes_until_start, should_extract_odds_for_event

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PreStartEventPlan:
    """Candidates selected for provider ingestion, indexed for later phases."""

    candidates: list[dict]
    by_event_id: dict[int, dict]


def build_pre_start_event_candidates(
    scheduler,
    upcoming_events: list[dict],
    pre_calculated_timings: dict[int, int],
    source_states: PreStartOddsSourceStates,
    *,
    key_moments: Collection[int] | None = None,
    timestamp_correction_enabled: bool | None = None,
    odds_extraction_competition_ids: Collection[int] | None = None,
) -> PreStartEventPlan:
    """Apply timing decisions and return the shared provider work payloads."""
    key_moments = Config.PRE_START_ODDS_MOMENTS if key_moments is None else key_moments
    events_to_process: list[dict] = []
    event_meta_lookup: dict[int, dict] = {}
    skipped_untracked_odds = 0

    for event_data in upcoming_events:
        try:
            event_id = event_data["id"]
            minutes = pre_calculated_timings.get(event_id)
            if minutes is None:
                minutes = minutes_until_start(event_data["start_time_utc"])
            preloaded_sofascore_event_id = get_numeric_source_event_id(
                source_states,
                event_id,
                SOFASCORE_SOURCE,
            )
            (
                should_extract_odds,
                metadata_snapshot,
                timing_changed,
                sofascore_event_id,
            ) = should_extract_odds_for_event(
                event_id,
                minutes,
                event_data.get("start_time_utc"),
                sofascore_event_id=preloaded_sofascore_event_id,
                key_moments=key_moments,
                timestamp_correction_enabled=timestamp_correction_enabled,
            )

            if (
                Config.ENABLE_ODDS_EXTRACTION
                and minutes in key_moments
                and not should_extract_odds
                and not timing_changed
                and metadata_snapshot is None
            ):
                logger.warning(
                    "Skipping event %s after unsuccessful SofaScore timing/event "
                    "validation at key moment %s",
                    event_id,
                    minutes,
                )
                continue

            if timing_changed:
                scheduler.recently_rescheduled.add(event_id)
                handle_rescheduled_event(
                    event_id,
                    scheduler.event_repo,
                    minutes,
                    metadata_snapshot=metadata_snapshot,
                    sofascore_event_id=sofascore_event_id,
                )

                refreshed_event = scheduler.event_repo.get_event_by_id(event_id)
                if refreshed_event:
                    event_data["season_id"] = refreshed_event.season_id
                    event_data["start_time_utc"] = refreshed_event.start_time_utc

            if (
                should_extract_odds
                and odds_extraction_competition_ids is not None
                and event_data["competition_id"] not in odds_extraction_competition_ids
            ):
                should_extract_odds = False
                skipped_untracked_odds += 1

            candidate = {
                "event_id": event_id,
                "event_data": event_data,
                "minutes_until_start": minutes,
                "should_extract_odds": should_extract_odds,
                "original_start_time": event_data["start_time_utc"],
                "metadata_snapshot": metadata_snapshot,
                "sofascore_event_id": sofascore_event_id,
            }
            events_to_process.append(candidate)
            event_meta_lookup[event_id] = candidate
        except Exception as exc:
            logger.error(
                "Error processing upcoming event %s: %s",
                event_data.get("id", "unknown"),
                exc,
            )

    if skipped_untracked_odds:
        logger.info(
            "🚫 Skipping odds extraction for %s events from untracked competitions",
            skipped_untracked_odds,
        )

    return PreStartEventPlan(
        candidates=events_to_process,
        by_event_id=event_meta_lookup,
    )
