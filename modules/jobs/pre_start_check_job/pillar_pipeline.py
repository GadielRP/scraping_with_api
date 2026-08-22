"""Pillar pipeline for the pre-start job.

Runs pillar/module calculations for events at key moments.
Parallel to, but independent of, the existing alert pipeline.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, is_dataclass
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from infrastructure.settings import Config
from modules.competition.tracked_competitions import is_tracked_competition
from modules.pillars.context import (
    EventContext,
    build_event_context,
    summarize_number_of_teams_from_streak_analysis,
)
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
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
from modules.pillars.pillar_4.run_pillar_4 import calculate_pillar_4
from modules.pillars.pillar_5.run_pillar_5 import calculate_pillar_5
from modules.pillars.pillar_1_team_structure.totals import (
    P1TotalsOutput,
)

logger = logging.getLogger(__name__)


def _serialize_p1_totals_output(output: P1TotalsOutput) -> dict:
    return asdict(output)


def _resolve_pillar_competition_id(event_context: Any):
    """Resolve the canonical competition ID used by the pillar scope policy."""
    if hasattr(event_context, "competition"):
        competition = getattr(event_context, "competition", None)
        if competition is not None:
            return getattr(competition, "competition_id", None)

    if isinstance(event_context, dict):
        ctx = event_context.get("event_context")
        if ctx and hasattr(ctx, "competition"):
            return getattr(ctx.competition, "competition_id", None)
        competition_id = event_context.get("competition_id")
        if competition_id is not None:
            return competition_id
        event_data = event_context.get("event_data")
        if isinstance(event_data, dict):
            return event_data.get("competition_id")

    return None


def _is_pillar_competition_in_scope(competition_id) -> bool:
    """Return whether the competition is inside the configured pillar scope."""
    return (
        not Config.FILTER_PIPELINES_BY_TRACKED_COMPETITIONS
        or is_tracked_competition(competition_id)
    )


def _build_p4_error_result(event_context, odds_trajectory_context, exc: Exception) -> dict:
    return {
        "pillar_id": "pillar_4",
        "pillar_name": "Temporal Market Drift",
        "event_id": getattr(event_context, "event_id", None),
        "participants": getattr(event_context, "participants_label", None),
        "P4_STATUS": "ERROR",
        "status": "ERROR",
        "modules": [],
        "market_period_results": {},
        "market_period_count": 0,
        "active_market_period_count": 0,
        "insufficient_market_period_count": 0,
        "error": str(exc),
        "raw": {
            "reason": "pillar_4_exception",
            "odds_trajectory_available": getattr(odds_trajectory_context, "available", False),
            "target_minutes_expected": getattr(odds_trajectory_context, "target_minutes_expected", []),
            "target_minutes_present": getattr(odds_trajectory_context, "target_minutes_present", []),
            "missing_target_minutes": getattr(odds_trajectory_context, "missing_target_minutes", []),
        },
    }


def _build_p5_error_result(event_context, ft_1x2_odds_trajectory, exc: Exception) -> dict:
    return {
        "pillar_id": "pillar_5",
        "pillar_name": "Exact Price Memory",
        "event_id": getattr(event_context, "event_id", None),
        "participants": getattr(event_context, "participants_label", None),
        "P5_STATUS": "ERROR",
        "status": "ERROR",
        "modules": [],
        "P5_VALID": False,
        "P5_DIRECTION": "NONE",
        "P5": 0.0,
        "P5_STRENGTH": "NONE",
        "error": str(exc),
        "raw": {
            "reason": "pillar_5_exception",
            "odds_trajectory_available": getattr(ft_1x2_odds_trajectory, "available", False),
            "target_minutes_expected": getattr(ft_1x2_odds_trajectory, "target_minutes_expected", []),
            "target_minutes_present": getattr(ft_1x2_odds_trajectory, "target_minutes_present", []),
            "missing_target_minutes": getattr(ft_1x2_odds_trajectory, "missing_target_minutes", []),
        },
    }


def _to_json_safe(value: Any):
    try:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value

        if isinstance(value, Decimal):
            return str(value)

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        to_dict = getattr(value, "to_dict", None)
        if callable(to_dict):
            try:
                return _to_json_safe(to_dict())
            except Exception:
                pass

        if is_dataclass(value) and not isinstance(value, type):
            return _to_json_safe(asdict(value))

        if isinstance(value, dict):
            return {str(key): _to_json_safe(item) for key, item in value.items()}

        if isinstance(value, (list, tuple)):
            return [_to_json_safe(item) for item in value]

        if isinstance(value, set):
            try:
                return [_to_json_safe(item) for item in sorted(value, key=lambda item: str(item))]
            except Exception:
                return [_to_json_safe(item) for item in list(value)]

        if hasattr(value, "__dict__"):
            return {
                str(key): _to_json_safe(item)
                for key, item in vars(value).items()
                if not str(key).startswith("_")
            }

        return str(value)
    except Exception:
        return str(value)


def _safe_debug_name(value: Any) -> str:
    try:
        safe_value = str(value)
        safe_value = re.sub(r"[^A-Za-z0-9 _-]+", "_", safe_value)
        safe_value = safe_value.replace(" ", "_")
        safe_value = re.sub(r"_+", "_", safe_value)
        safe_value = safe_value.strip("_")
        return safe_value or "unknown"
    except Exception:
        return "unknown"


def _write_debug_json(filepath: Path, payload: Any) -> None:
    with filepath.open("w", encoding="utf-8") as handle:
        json.dump(_to_json_safe(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)


def _save_pillar_debug_snapshots(
    *,
    streak_analysis: Any,
    event_context: Any,
    odds_trajectory_context: Any,
) -> None:
    try:
        event_id = getattr(event_context, "event_id", "unknown_event")
        participants = getattr(event_context, "participants_label", "unknown_matchup")

        safe_participants = _safe_debug_name(participants)
        debug_dir = Path("debug") / "matchup_streak_analysis" / f"{event_id}_{safe_participants}"
        debug_dir.mkdir(parents=True, exist_ok=True)

        _write_debug_json(debug_dir / f"{event_id}_streak_analysis.json", streak_analysis)
        _write_debug_json(debug_dir / f"{event_id}_event_context.json", event_context)
        _write_debug_json(debug_dir / f"{event_id}_odds_trajectory_context.json", odds_trajectory_context)


        logger.info(
            "Pillar debug snapshots saved for event %s at %s",
            event_id,
            debug_dir,
        )
    except Exception:
        logger.exception(
            "Failed to save pillar debug snapshots for event %s",
            event_id if "event_id" in locals() else "unknown_event",
        )


class EventPillarProcessor:
    """Processes a single event through the pillar/module architecture."""

    def __init__(
        self,
        event_repo,
        debug_mode: bool = False,
    ):
        self.event_repo = event_repo
        self.debug_mode = debug_mode

    def process_event(self, event_context: EventContext) -> Optional[dict]:
        """Calculate pillar modules for a single event context.

        Returns a dictionary with pillar results or ``None`` on failure.
        """
        # The pillar pipeline has one canonical input contract.  Do not
        # silently reconstruct it from the legacy event payload here: doing
        # so would bring back the duplicate objects this context refactor is
        # intended to remove.
        if event_context is not None and not isinstance(event_context, EventContext):
            logger.warning(
                "Pillar pipeline received a non-canonical event context (%s); skipping event",
                type(event_context).__name__,
            )
            return None

        if event_context is None or not getattr(event_context, "success", True):
            event_id = getattr(event_context, "event_id", "?")
            logger.warning(f"☢️ Pillar pipeline: success is false for event {event_id}, skipping pillar calculation")
            return None

        event_id = getattr(event_context, "event_id", "?")

        competition_id = event_context.competition.competition_id
        if not _is_pillar_competition_in_scope(competition_id):
            logger.info(
                "🚫 Pillar pipeline: competition_id=%s is outside the configured scope for event %s; skipping pillar calculation",
                competition_id,
                event_id,
            )
            return None

        logger.info(f"🏛️ Started pillars processing for event {event_id}")
        round_value = getattr(event_context, "round", None)
        if round_value != "regular_season":
            logger.info(
                "🚫 Pillar pipeline: round is %s for event_id %s, skipping pillar calculation",
                round_value,
                event_id,
            )
            return None

        minutes_until_start = getattr(event_context, "minutes_until_start", None)
        odds_trajectory = getattr(event_context, "odds_trajectory", [])
        odds_trajectory_context = build_odds_trajectory_context(odds_trajectory)
        event_context.odds_trajectory_context = odds_trajectory_context

        logger.info(
            "Pillar odds trajectory context for event %s: available=%s market_groups=%s present_minutes=%s missing_minutes=%s",
            event_id,
            odds_trajectory_context.available,
            len(odds_trajectory_context.markets),
            odds_trajectory_context.target_minutes_present,
            odds_trajectory_context.missing_target_minutes,
        )
        if self.debug_mode and odds_trajectory_context.available:
            trajectory_keys = []
            for market_group, periods in odds_trajectory_context.markets.items():
                for market_period in periods:
                    trajectory_keys.append(f"{market_group}/{market_period}")
            logger.info(
                "P4 pre-check trajectory sample for event %s: %s",
                event_id,
                trajectory_keys[:10],
            )

        try:
            # calculate pillar 4 (p4)
            p4_result = calculate_pillar_4(
                event_context=event_context,
                debug_mode=self.debug_mode,
            )
        except Exception as exc:
            logger.exception(
                "Error calculating P4 for event %s (%s): %s",
                event_id,
                event_context.participants_label,
                exc,
            )
            p4_result = _build_p4_error_result(event_context, odds_trajectory_context, exc)

        logger.info(
            "P4 calculated for %s: status=%s market_periods=%s active=%s insufficient=%s",
            event_context.participants_label,
            p4_result.get("P4_STATUS"),
            p4_result.get("market_period_count"),
            p4_result.get("active_market_period_count"),
            p4_result.get("insufficient_market_period_count"),
        )
        if self.debug_mode:
            logger.info(
                "P4 debug summary for %s: trajectory_keys=%s",
                event_context.participants_label,
                list((p4_result.get("market_period_results") or {}).keys())[:10],
            )

        ft_1x2_odds_trajectory = odds_trajectory_context
        try:
            if self.debug_mode:
                logger.info(
                    "P5: Context before filtering for event %s (%s): available=%s, markets=%s",
                    event_id,
                    event_context.participants_label,
                    odds_trajectory_context.available,
                    {group: list(periods.keys()) for group, periods in odds_trajectory_context.markets.items()}
                    if odds_trajectory_context.markets else "None",
                )

            ft_1x2_odds_trajectory = odds_trajectory_context.filter_by_market_groups(
                allowed_groups={"1X2", "Home/Away"}
            )
            event_context.ft_1x2_odds_trajectory_context = ft_1x2_odds_trajectory
            
            if self.debug_mode:
                logger.info(
                    "P5: Context after market group filtering for event %s (%s): available=%s, markets=%s",
                    event_id,
                    event_context.participants_label,
                    ft_1x2_odds_trajectory.available,
                    {group: list(periods.keys()) for group, periods in ft_1x2_odds_trajectory.markets.items()}
                    if ft_1x2_odds_trajectory.markets else "None",
                )

            ft_1x2_odds_trajectory = ft_1x2_odds_trajectory.filter_by_market_period(
                allowed_periods={"Full Time"}
            )

            if self.debug_mode:
                logger.info(
                    "P5: Context after period filtering for event %s (%s): available=%s, markets=%s",
                    event_id,
                    event_context.participants_label,
                    ft_1x2_odds_trajectory.available,
                    {group: list(periods.keys()) for group, periods in ft_1x2_odds_trajectory.markets.items()}
                    if ft_1x2_odds_trajectory.markets else "None",
                )

            ft_1x2_odds_trajectory = ft_1x2_odds_trajectory.filter_by_bookie_ids(
                allowed_bookie_ids={1}
            )

            if self.debug_mode:
                remaining_bookie_ids = sorted({
                    bookie.bookie_id
                    for periods in ft_1x2_odds_trajectory.markets.values()
                    for market_period in periods.values()
                    for market_name in market_period.values()
                    for market_line in market_name.values()
                    for bookie in market_line.bookies.values()
                    if bookie.bookie_id is not None
                })
                logger.info(
                    "P5: Context after bookie filtering for event %s (%s): available=%s, markets=%s, bookie_ids=%s",
                    event_id,
                    event_context.participants_label,
                    ft_1x2_odds_trajectory.available,
                    {group: list(periods.keys()) for group, periods in ft_1x2_odds_trajectory.markets.items()}
                    if ft_1x2_odds_trajectory.markets else "None",
                    remaining_bookie_ids if remaining_bookie_ids else "None",
                )

            p5_result = calculate_pillar_5(
                event_context=event_context,
                debug_mode=self.debug_mode,
            )
        except Exception as exc:
            logger.exception(
                "Error calculating P5 for event %s (%s): %s",
                event_id,
                event_context.participants_label,
                exc,
            )
            p5_result = _build_p5_error_result(
                event_context,
                ft_1x2_odds_trajectory,
                exc,
            )

        logger.info(
            "P5 calculated for %s: status=%s valid=%s direction=%s score=%.3f strength=%s sample_size=%s",
            event_context.participants_label,
            p5_result.get("P5_STATUS"),
            p5_result.get("P5_VALID"),
            p5_result.get("P5_DIRECTION"),
            p5_result.get("P5", 0),
            p5_result.get("P5_STRENGTH"),
            p5_result.get("sample_size"),
        )

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

        if missing_fields and getattr(event_context, "competition_metadata_resolved", False):
            logger.info(
                "Pillar pipeline metadata enrichment skipped for event %s; resolver already ran during payload build (missing fields: %s)",
                event_id,
                ", ".join(missing_fields),
            )
        elif missing_fields:
            logger.info(
                "Pillar pipeline metadata enrichment needed for event %s; missing fields: %s; calling competition metadata resolver",
                event_id,
                ", ".join(missing_fields),
            )
            resolution = resolve_competition_metadata(event_context)
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
            event_context=event_context,
            debug_mode=self.debug_mode,
        )

        if streak_analysis and self.debug_mode:
            _save_pillar_debug_snapshots(
                streak_analysis=streak_analysis,
                event_context=event_context,
                odds_trajectory_context=odds_trajectory_context,
            )

        if streak_analysis is None:
            logger.info(
                "Pillar pipeline: no streak_analysis for event %s (%s), returning P4 and P5 only",
                event_id,
                participants,
            )
            return {
                "event_id": event_id,
                "participants": participants,
                "pillar_1": None,
                "pillar_1_totals": None,
                "pillar_4": p4_result,
                "pillar_5": p5_result,
            }

        number_of_teams_summary = summarize_number_of_teams_from_streak_analysis(
            streak_analysis,
            event_context,
        )
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

        # --- Calculate Pillar 1 (Orchestrated) ---
        try:
            p1_output = calculate_pillar_1_team_structure(
                event_context=event_context,
                debug_mode=self.debug_mode,
            )
            p1_result = p1_output["side"]
            p1_totals_result = p1_output["totals"]
        except Exception as exc:
            logger.error(
                "Error calculating P1 for event %s (%s): %s",
                event_id,
                participants,
                exc,
            )
            return {
                "event_id": event_id,
                "participants": participants,
                "pillar_1": None,
                "pillar_1_totals": None,
                "pillar_4": p4_result,
                "pillar_5": p5_result,
            }

        p1_result.setdefault("raw", {}).update({
            "odds_trajectory_available": odds_trajectory_context.available,
            "odds_trajectory_target_minutes_present": odds_trajectory_context.target_minutes_present,
        })

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
            "P1/M5 Contextual Competitive Cost Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
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
            "P1/M7 Opponent Expectation Engine calculated for %s: value=%.3f, bias=%s, strength=%s",
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

        p1_final_raw = p1_result.get("raw", {}).get("final", {})
        logger.info(
            "P1/SIDE calculated for %s: value=%.3f, bias=%s, strength=%s, context_state=%s",
            participants,
            p1_result.get("value", 0),
            p1_result.get("raw", {}).get("final", {}).get("p1_final_bias", p1_result.get("bias", "N/A")),
            p1_result.get("raw", {}).get("final", {}).get("p1_final_strength", p1_result.get("strength", "N/A")),
            p1_final_raw.get("p1_context_state", "N/A"),
        )

        if p1_totals_result is not None:
            logger.info(
                "P1/P1_TOTALS Totals calculated for %s: directional_score=%.3f, direction=%s, strength=%s, variance_state=%s, status=%s",
                participants,
                p1_totals_result.P1_TOTALS_DIRECTIONAL_SCORE,
                p1_totals_result.P1_TOTALS_DIRECTION,
                p1_totals_result.P1_TOTALS_STRENGTH,
                p1_totals_result.P1_TOTALS_VARIANCE_STATE,
                p1_totals_result.status,
            )
            for layer in p1_totals_result.active_layers:
                logger.info(
                    "   - active layer: %s raw_signal=%s final_signal=%s weighted=%s",
                    layer.layer,
                    layer.raw_signal,
                    layer.final_signal,
                    layer.weighted_signal,
                )
            for layer in p1_totals_result.ignored_layers:
                logger.info(
                    "   - ignored layer: %s raw_signal=%s final_signal=%s reason=%s",
                    layer.layer,
                    layer.raw_signal,
                    layer.final_signal,
                    layer.ignored_reason,
                )
        else:
            logger.info(
                "P1/P1_TOTALS Totals skipped for %s: unavailable",
                participants,
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
            "event_id": event_id,
            "participants": participants,
            "pillar_1": p1_result,
            "pillar_1_totals": (
                _serialize_p1_totals_output(p1_totals_result)
                if p1_totals_result is not None
                else None
            ),
            "pillar_4": p4_result,
            "pillar_5": p5_result,
        }


def evaluate_and_calculate_pillars_batch(
    events_for_pillars: list[EventContext],
    key_moments: list,
    event_repo,
    op_event_states=None,
    op_event_ids=None,
    op_data_cache=None,
    debug_mode: bool = False,
):
    """Entry point to evaluate and calculate pillar modules for a batch of events."""
    if not events_for_pillars:
        return

    allowed_events = [
        event_context
        for event_context in events_for_pillars
        if _is_pillar_competition_in_scope(
            _resolve_pillar_competition_id(event_context)
        )
    ]
    skipped_count = len(events_for_pillars) - len(allowed_events)
    if skipped_count:
        logger.info(
            "🚫 Pillar pipeline competition filter skipped %s/%s events",
            skipped_count,
            len(events_for_pillars),
        )
    if not allowed_events:
        return

    logger.info(
        "Evaluating pillar modules for %d events...",
        len(allowed_events),
    )

    processor = EventPillarProcessor(
        event_repo=event_repo,
        debug_mode=debug_mode,
    )

    max_workers = min(Config.PILLAR_PIPELINE_WORKERS, len(allowed_events))
    logger.info(
        "Pillar pipeline concurrency events=%s workers=%s",
        len(allowed_events),
        max_workers,
    )
    if max_workers == 1:
        for event_context in allowed_events:
            try:
                processor.process_event(event_context)
            except Exception as exc:
                logger.error("Critical failure in pillar processing: %s", exc)
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(processor.process_event, event_context)
            for event_context in allowed_events
        ]
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                logger.error("Critical failure in pillar processing thread: %s", exc)
