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

from modules.pillars.context import (
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


def _resolve_event_payload_value(event_payload: dict, key: str, default=None):
    if key in event_payload:
        return event_payload.get(key), "top_level"

    event_data = event_payload.get("event_data")
    if isinstance(event_data, dict) and key in event_data:
        return event_data.get(key), "event_data"

    return default, "missing"


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

        if is_dataclass(value) and not isinstance(value, type):
            return _to_json_safe(asdict(value))

        to_dict = getattr(value, "to_dict", None)
        if callable(to_dict):
            try:
                return _to_json_safe(to_dict())
            except Exception:
                pass

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
        event_id = getattr(streak_analysis, "event_id", None)
        if event_id is None and isinstance(streak_analysis, dict):
            event_id = streak_analysis.get("event_id")
        if event_id is None:
            event_id = getattr(event_context, "event_id", None)
        if event_id is None and isinstance(event_context, dict):
            event_id = event_context.get("event_id")
        if event_id is None:
            event_id = "unknown_event"

        participants = getattr(streak_analysis, "participants", None)
        if participants is None and isinstance(streak_analysis, dict):
            participants = streak_analysis.get("participants")
        if participants is None:
            participants = getattr(event_context, "participants_label", None)
        if participants is None and isinstance(event_context, dict):
            participants = event_context.get("participants_label")
        if participants is None:
            participants = "unknown_matchup"

        safe_participants = _safe_debug_name(participants)
        debug_dir = Path("debug") / "matchup_streak_analysis" / f"{event_id}_{safe_participants}"
        debug_dir.mkdir(parents=True, exist_ok=True)

        _write_debug_json(debug_dir / f"{event_id}_streak_analysis.json", streak_analysis)
        _write_debug_json(debug_dir / f"{event_id}_event_context.json", event_context)
        _write_debug_json(debug_dir / f"{event_id}_odds_trajectory.json", odds_trajectory_context)

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

    def process_event(self, event_payload: dict) -> Optional[dict]:
        """Calculate pillar modules for a single event.

        Returns a dictionary with pillar results or ``None`` on failure.
        """
        event_obj, event_obj_source = _resolve_event_payload_value(event_payload, "event_obj")
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

        minutes_until_start, minutes_source = _resolve_event_payload_value(event_payload, "minutes_until_start")
        metadata_snapshot, metadata_source = _resolve_event_payload_value(event_payload, "metadata_snapshot")
        event_context, event_context_source = _resolve_event_payload_value(event_payload, "event_context")
        odds_trajectory, odds_trajectory_source = _resolve_event_payload_value(event_payload, "odds_trajectory")
        odds_trajectory_context = build_odds_trajectory_context(odds_trajectory)
        logger.info(
            "Pillar payload resolution for event %s: event_obj=%s minutes_until_start=%s metadata_snapshot=%s event_context=%s odds_trajectory=%s",
            event_id,
            event_obj_source,
            minutes_source,
            metadata_source,
            event_context_source,
            odds_trajectory_source,
        )
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
                odds_trajectory_context=odds_trajectory_context,
                debug_mode=self.debug_mode,
            )
        except Exception as exc:
            logger.exception(
                "Error calculating P4 for event %s (%s): %s",
                event_obj.id,
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
            ft_1x2_odds_trajectory = odds_trajectory_context.filter_by_market_groups(
                allowed_groups={"1X2", "Home/Away", "ML"}
            )
            ft_1x2_odds_trajectory = ft_1x2_odds_trajectory.filter_by_market_period(
                allowed_periods={"Full-time"}
            )
            p5_result = calculate_pillar_5(
                event_context=event_context,
                ft_1x2_odds_trajectory=ft_1x2_odds_trajectory,
                debug_mode=self.debug_mode,
            )
        except Exception as exc:
            logger.exception(
                "Error calculating P5 for event %s (%s): %s",
                event_obj.id,
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

        if streak_analysis and self.debug_mode:
            _save_pillar_debug_snapshots(
                streak_analysis=streak_analysis,
                event_context=event_context,
                odds_trajectory_context=odds_trajectory_context,
            )

        if streak_analysis is None:
            logger.info(
                "Pillar pipeline: no streak_analysis for event %s (%s), returning P4 and P5 only",
                event_obj.id,
                participants,
            )
            return {
                "event_id": event_obj.id,
                "participants": participants,
                "pillar_1": None,
                "pillar_1_totals": None,
                "pillar_4": p4_result,
                "pillar_5": p5_result,
            }

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

        # --- Calculate Pillar 1 (Orchestrated) ---
        try:
            p1_output = calculate_pillar_1_team_structure(
                streak_analysis,
                event_context=event_context,
                debug_mode=self.debug_mode,
            )
            p1_result = p1_output["side"]
            p1_totals_result = p1_output["totals"]
        except Exception as exc:
            logger.error(
                "Error calculating P1 for event %s (%s): %s",
                event_obj.id,
                participants,
                exc,
            )
            return {
                "event_id": event_obj.id,
                "participants": participants,
                "pillar_1": None,
                "pillar_1_totals": None,
                "pillar_4": p4_result,
                "pillar_5": p5_result,
            }

        p1_result.setdefault("raw", {}).update({
            "odds_trajectory_available": odds_trajectory_context.available,
            "odds_trajectory_target_minutes_present": odds_trajectory_context.target_minutes_present,
            "odds_trajectory_missing_target_minutes": odds_trajectory_context.missing_target_minutes,
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
            "event_id": event_obj.id,
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
