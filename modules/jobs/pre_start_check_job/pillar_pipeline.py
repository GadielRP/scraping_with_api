"""Pillar pipeline for the pre-start job.

Runs pillar/module calculations for events at key moments.
Parallel to, but independent of, the existing alert pipeline.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from modules.pillars.context import (
    build_event_context,
    summarize_number_of_teams_from_streak_analysis,
)
from modules.pillars.competition_metadata_resolver import (
    apply_competition_metadata_resolution,
    resolve_competition_metadata,
)
from modules.pillars.streak_analysis_resolver import (
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
        event_obj = event_payload.get("event_obj")
        event_id = getattr(event_obj, "id", event_payload.get("event_id", "?"))

        if not event_payload.get("success"):
            logger.warning(f"☢️ Pillar pipeline: success is false for event {event_id}, skipping pillar calculation")
            return None

        if event_obj is None:
            logger.warning(f"☢️ Pillar pipeline: event obj is empty for {event_id}, skipping pillar calculation")
            return None

        logger.info(f"🏛️ Started pillars processing for event {event_id}")
        round_value = event_obj.round
        if round_value != "regular_season":
            logger.info(
                "🚫 Pillar pipeline: round is %s for event_id %s, skipping pillar calculation",
                round_value,
                event_id,
            )
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
                "☢️ Pillar pipeline: missing_normalized_context for event %s; skipping pillar calculation",
                event_obj.id,
            )
            return None

        logger.info(
            "Pillar pipeline metadata check for event %s: competition_id=%s source_unique_tournament_id=%s season_id=%s number_of_teams=%s total_regular_season_games=%s standings_grouping=%s league_config_source=%s",
            event_id,
            getattr(event_context.competition, "competition_id", None),
            getattr(event_context.competition, "source_unique_tournament_id", None),
            getattr(event_context, "season_id", None),
            getattr(event_context.competition, "number_of_teams", None),
            getattr(event_context.competition, "total_regular_season_games", None),
            getattr(event_context.competition, "standings_grouping", None),
            getattr(event_context.competition, "league_config_source", None),
        )

        missing_fields = []
        if getattr(event_context.competition, "number_of_teams", None) is None:
            missing_fields.append("number_of_teams")
        if getattr(event_context.competition, "total_regular_season_games", None) is None:
            missing_fields.append("total_regular_season_games")
        if getattr(event_context.competition, "standings_grouping", None) is None:
            missing_fields.append("standings_grouping")
        
        if missing_fields:
            logger.info(
                "Pillar pipeline metadata enrichment needed for event %s; missing fields: %s; calling competition metadata resolver",
                event_id,
                ", ".join(missing_fields),
            )
            resolution = resolve_competition_metadata(event_context, event_obj=event_obj)
            apply_competition_metadata_resolution(event_context, resolution)
            logger.info(
                "Pillar pipeline metadata enrichment result for event %s: source=%s standings_called=%s should_persist=%s number_of_teams=%s total_regular_season_games=%s standings_grouping=%s",
                event_id,
                resolution.league_config_source,
                resolution.standings_called,
                resolution.should_persist,
                resolution.number_of_teams,
                resolution.total_regular_season_games,
                resolution.standings_grouping,
            )

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

        if streak_analysis and self.debug_mode == True:
            import os
            import pprint
            
            debug_dir = "debug/matchup_streak_analysis"
            os.makedirs(debug_dir, exist_ok=True)
            
            # Format participants for filename
            safe_participants = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in streak_analysis.participants).replace(' ', '_')
            filename = f"{streak_analysis.event_id}_{safe_participants}.txt"
            filepath = os.path.join(debug_dir, filename)
            
            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    for attr, value in streak_analysis.__dict__.items():
                        f.write(f"{attr}:\n{pprint.pformat(value, width=120)}\n\n")
            except Exception as e:
                logger.error(f"Failed to save streak_analysis debug file: {e}")

        if streak_analysis is None:
            logger.info(
                "🗑️ Pillar pipeline: no streak_analysis for event %s (%s), skipping",
                event_obj.id,
                participants,
            )
            return None

        number_of_teams_summary = summarize_number_of_teams_from_streak_analysis(streak_analysis)
        inferred_number_of_teams = number_of_teams_summary.inferred_number_of_teams
        unique_team_count = number_of_teams_summary.unique_team_count
        inferred_number_of_teams_used = False
        competition_id = event_context.competition.competition_id

        logger.info(
            "Pillar context for %s: context_status=%s, event_context_present=%s, competition_id=%s, competition_number_of_teams=%s, number_of_teams_source=%s, inferred_number_of_teams=%s, unique_team_count=%s, inferred_used=%s, total_regular_season_games=%s",
            participants,
            event_context.context_status,
            True,
            competition_id,
            event_context.competition.number_of_teams,
            event_context.competition.number_of_teams_source,
            inferred_number_of_teams,
            unique_team_count,
            inferred_number_of_teams_used,
            event_context.competition.total_regular_season_games,
        )

        # --- Calculate Pillar 1 ---
        try:
            p1_result = calculate_pillar_1_team_structure(
                streak_analysis,
                event_context=event_context,
                debug_mode=self.debug_mode,
            )
        except Exception as exc:
            logger.error(
                "Error calculating P1 for event %s (%s): %s",
                event_obj.id,
                participants,
                exc,
            )
            return None

        # Log the M1 result.
        m1 = p1_result.get("modules", [{}])[0] if p1_result.get("modules") else {}
        logger.info(
            "P1/M1 Base Strength calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m1.get("value", 0),
            m1.get("bias", "N/A"),
            m1.get("strength", "N/A"),
        )

        for comp in m1.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        # Log the M2 result.
        modules = p1_result.get("modules", [])
        m2 = modules[1] if len(modules) > 1 else {}
        logger.info(
            "P1/M2 Offensive Profile Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m2.get("value", 0),
            m2.get("bias", "N/A"),
            m2.get("strength", "N/A"),
        )

        for comp in m2.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        # Log the M3 result.
        m3 = modules[2] if len(modules) > 2 else {}
        logger.info(
            "P1/M3 Direct Matchup Profile calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m3.get("value", 0),
            m3.get("bias", "N/A"),
            m3.get("strength", "N/A"),
        )

        for comp in m3.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        m4 = modules[3] if len(modules) > 3 else {}
        logger.info(
            "P1/M4 Quality-Adjusted Immediate State Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m4.get("value", 0),
            m4.get("bias", "N/A"),
            m4.get("strength", "N/A"),
        )

        for comp in m4.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        m5 = modules[4] if len(modules) > 4 else {}
        logger.info(
            "P1/M5 Recent Inertia Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m5.get("value", 0),
            m5.get("bias", "N/A"),
            m5.get("strength", "N/A"),
        )

        for comp in m5.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        m6 = modules[5] if len(modules) > 5 else {}
        logger.info(
            "P1/M6 Structural Drift Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m6.get("value", 0),
            m6.get("bias", "N/A"),
            m6.get("strength", "N/A"),
        )

        for comp in m6.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
                comp.get("name", "?"),
                comp.get("edge", 0),
                comp.get("weight", 0),
                comp.get("weighted_edge", 0),
                comp.get("bias", "?"),
                comp.get("strength", "?"),
            )

        m7 = modules[6] if len(modules) > 6 else {}
        logger.info(
            "P1/M7 Structural Drift Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
            participants,
            m7.get("value", 0),
            m7.get("bias", "N/A"),
            m7.get("strength", "N/A"),
        )

        for comp in m7.get("components", []):
            logger.info(
                "   - %s: edge=%.4f (weight=%.2f, weighted=%.4f) | bias=%s, strength=%s",
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
                "total_regular_season_games": event_context.competition.total_regular_season_games,
                "standings_grouping": event_context.competition.standings_grouping,
                "league_config_source": event_context.competition.league_config_source,
                "inferred_number_of_teams": inferred_number_of_teams,
                "inferred_number_of_teams_from_streak_analysis": inferred_number_of_teams,
                "inferred_number_of_teams_source": "streak_analysis_team_results",
                "inferred_number_of_teams_used": inferred_number_of_teams_used,
                "unique_team_count": unique_team_count,
                "persisted_number_of_teams": False,
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
        "Evaluating pillar modules for %d events...",
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
