"""Pillar pipeline for the pre-start job.

Runs pillar/module calculations for events at key moments.
Parallel to — but independent of — the existing alert pipeline.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories import CompetitionRepository
from modules.jobs.pre_start_check_job.pillar_event_context import (
    build_event_context,
    summarize_number_of_teams_from_streak_analysis,
)
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

        minutes_until_start = event_payload.get("minutes_until_start")
        metadata_snapshot = event_payload.get("metadata_snapshot")
        event_context = event_payload.get("event_context")
        if event_context is None:
            event_context = build_event_context(
                event_obj=event_obj,
                minutes_until_start=minutes_until_start,
                metadata_snapshot=metadata_snapshot,
            )
        if event_context is None:
            logger.warning(
                "🧱 Pillar pipeline: missing_normalized_context for event %s; skipping pillar calculation",
                event_obj.id,
            )
            return None
        season_id = event_context.season_id
        participants = event_context.participants_label

        # --- Resolve streak analysis (shared with alert pipeline) ---
        streak_analysis, _should_send = resolve_matchup_streak_analysis(
            event_payload=event_payload,
            event_obj=event_obj,
            season_id=season_id,
            minutes_until_start=minutes_until_start,
            event_context=event_context,
            debug_mode=self.debug_mode,
        )

        if streak_analysis is None:
            logger.debug(
                "🧱 Pillar pipeline: no streak_analysis for event %s (%s), skipping",
                event_obj.id,
                participants,
            )
            return None

        number_of_teams_summary = summarize_number_of_teams_from_streak_analysis(streak_analysis)
        inferred_number_of_teams = number_of_teams_summary.inferred_number_of_teams
        unique_team_count = number_of_teams_summary.unique_team_count
        persisted_number_of_teams = False
        competition_id = event_context.competition.competition_id

        if inferred_number_of_teams is not None and event_context.competition.number_of_teams is None:
            event_context.competition.number_of_teams = inferred_number_of_teams
            event_context.competition.number_of_teams_source = "inferred_from_streak_analysis"

            if competition_id is not None:
                try:
                    with db_manager.get_session() as session:
                        persisted_number_of_teams = CompetitionRepository.update_number_of_teams_if_missing(
                            session=session,
                            competition_id=competition_id,
                            number_of_teams=inferred_number_of_teams,
                        )
                except Exception as exc:
                    logger.warning(
                        "🧭 Pillar pipeline: failed to persist inferred number_of_teams for competition %s: %s",
                        competition_id,
                        exc,
                    )

        logger.info(
            "🧭 Pillar context for %s: context_status=%s, event_context_present=%s, competition_id=%s, competition_number_of_teams=%s, number_of_teams_source=%s, inferred_number_of_teams=%s, unique_team_count=%s, persisted=%s",
            participants,
            event_context.context_status,
            True,
            competition_id,
            event_context.competition.number_of_teams,
            event_context.competition.number_of_teams_source,
            inferred_number_of_teams,
            unique_team_count,
            persisted_number_of_teams,
        )

        # --- Calculate Pillar 1 ---
        try:
            p1_result = calculate_pillar_1_team_structure(
                streak_analysis,
                event_context=event_context,
            )
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

        p1_result.setdefault("raw", {}).update(
            {
                "event_context_present": True,
                "context_status": event_context.context_status,
                "competition_id": competition_id,
                "competition_display_name": event_context.competition.display_name,
                "competition_number_of_teams": event_context.competition.number_of_teams,
                "competition_number_of_teams_source": event_context.competition.number_of_teams_source,
                "inferred_number_of_teams": inferred_number_of_teams,
                "inferred_number_of_teams_source": "streak_analysis_team_results",
                "unique_team_count": unique_team_count,
                "persisted_number_of_teams": persisted_number_of_teams,
            }
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
