"""Pillar pipeline for the pre-start job.

Runs pillar/module calculations for events at key moments.
Parallel to — but independent of — the existing alert pipeline.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from modules.jobs.pre_start_check_job.streak_analysis_resolver import (
    resolve_matchup_streak_analysis,
)
from modules.pillars.pillar_1_team_structure.run_pillar_1_team_structure import (
    calculate_pillar_1_team_structure,
)

logger = logging.getLogger(__name__)


class EventPillarProcessor:
    """Processes a single event through the pillar/module architecture."""

    def __init__(
        self,
        event_repo,
        debug_mode: bool = False,
    ):
        self.event_repo = event_repo
        self.debug_mode = debug_mode

    def process_event(self, event_payload: dict) -> Optional[dict]:
        """Calculate pillar modules for a single event.

        Returns a dictionary with pillar results or ``None`` on failure.
        """
        if not event_payload.get("success"):
            return None

        event_obj = event_payload.get("event_obj")
        if event_obj is None:
            return None

        season_id = getattr(event_obj, "season_id", None)
        minutes_until_start = event_payload.get("minutes_until_start")
        participants = f"{event_obj.home_team} vs {event_obj.away_team}"

        # --- Resolve streak analysis (shared with alert pipeline) ---
        streak_analysis, _should_send = resolve_matchup_streak_analysis(
            event_payload=event_payload,
            event_obj=event_obj,
            season_id=season_id,
            minutes_until_start=minutes_until_start,
            debug_mode=self.debug_mode,
        )

        if streak_analysis is None:
            logger.debug(
                "🧱 Pillar pipeline: no streak_analysis for event %s (%s), skipping",
                event_obj.id,
                participants,
            )
            return None

        # --- Calculate Pillar 1 ---
        try:
            p1_result = calculate_pillar_1_team_structure(streak_analysis)
        except Exception as exc:
            logger.error(
                "🧱 Error calculating P1 for event %s (%s): %s",
                event_obj.id,
                participants,
                exc,
            )
            return None

        # Log the M1 result
        m1 = p1_result.get("modules", [{}])[0] if p1_result.get("modules") else {}
        logger.info(
            "🧱 P1/M1 Base Strength calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m1.get("value", 0),
            m1.get("bias", "N/A"),
            m1.get("strength", "N/A"),
        )

        # Log component details
        for comp in m1.get("components", []):
            logger.info(
                "   ├─ %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        return {
            "event_id": event_obj.id,
            "participants": participants,
            "pillar_1": p1_result,
        }


def evaluate_and_calculate_pillars_batch(
    events_for_pillars: list,
    key_moments: list,
    event_repo,
    op_event_states=None,
    op_event_ids=None,
    op_data_cache=None,
    debug_mode: bool = False,
) -> None:
    """Entry point to evaluate pillar modules for a batch of events."""
    if not events_for_pillars:
        return

    logger.info(
        "🧱 Evaluating pillar modules for %d events...",
        len(events_for_pillars),
    )

    processor = EventPillarProcessor(
        event_repo=event_repo,
        debug_mode=debug_mode,
    )

    with ThreadPoolExecutor(max_workers=min(4, len(events_for_pillars))) as executor:
        futures = [
            executor.submit(processor.process_event, payload)
            for payload in events_for_pillars
        ]
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                logger.error("Critical failure in pillar processing thread: %s", exc)
