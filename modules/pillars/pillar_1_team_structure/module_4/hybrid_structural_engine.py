"""M4 - Hybrid Structural Engine module.

Measures structural competitive advantage inside the league using current
simulated standings at kickoff.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)

_COMPONENT_WEIGHTS = {
    "PPG_EDGE": 0.50,
    "GD_EDGE": 0.40,
    "RANK_EDGE": 0.10,
}


def _coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _standing_value(standing: Any, *keys: str) -> Any:
    if not isinstance(standing, dict):
        return None
    for key in keys:
        value = standing.get(key)
        if value is not None:
            return value
    return None


def _component(name: str, edge: float, weight: float, raw: Dict[str, Any]) -> ModuleComponentResult:
    return ModuleComponentResult(
        name=name,
        edge=edge,
        bias=calculate_bias(edge),
        strength=classify_strength(edge),
        weight=weight,
        weighted_edge=edge * weight,
        raw=raw,
    )


def _current_status(
    home_rank: Optional[int],
    away_rank: Optional[int],
    home_standing: Any,
    away_standing: Any,
    n_teams: Optional[int],
) -> Tuple[Optional[str], Optional[str]]:
    if home_rank is None or away_rank is None:
        return "INSUFFICIENT_DATA", "missing_current_standings_rank"
    if home_standing is None or away_standing is None:
        return "INSUFFICIENT_DATA", "missing_current_standings_standing"

    home_points = _coerce_float(_standing_value(home_standing, "points"))
    away_points = _coerce_float(_standing_value(away_standing, "points"))
    home_gp = _coerce_int(_standing_value(home_standing, "gp", "games_played", "matches"))
    away_gp = _coerce_int(_standing_value(away_standing, "gp", "games_played", "matches"))
    home_gd = _coerce_float(_standing_value(home_standing, "goal_diff", "diff"))
    away_gd = _coerce_float(_standing_value(away_standing, "goal_diff", "diff"))

    if None in (home_points, away_points, home_gp, away_gp, home_gd, away_gd):
        return "INSUFFICIENT_DATA", "missing_current_standings_metrics"
    if home_rank <= 0 or away_rank <= 0:
        return "INVALID_INPUT", "invalid_current_standings_rank"
    if home_gp <= 0 or away_gp <= 0:
        return "INVALID_INPUT", "invalid_current_standings_gp"
    if n_teams is None:
        return "INSUFFICIENT_DATA", "missing_n_teams"
    if n_teams <= 1:
        return "INVALID_INPUT", "invalid_n_teams"
    if home_rank > n_teams or away_rank > n_teams:
        return "INVALID_INPUT", "rank_out_of_range"
    return None, None


def _ppg(points: float, gp: int) -> float:
    return points / float(gp)


def _rank_context(rank: int, n_teams: int) -> float:
    return (float(n_teams) - float(rank)) / float(n_teams - 1)


def calculate_hybrid_structural_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    """Calculate M4 - Hybrid Structural Engine for an event."""
    event_id = getattr(streak_analysis, "event_id", 0)
    participants = getattr(streak_analysis, "participants", "") or event_context.participants_label
    home_team = getattr(streak_analysis, "home_team_name", None) or event_context.home.name
    away_team = getattr(streak_analysis, "away_team_name", None) or event_context.away.name

    # Do not use home_team_final_real_ranking / away_team_final_real_ranking here.
    # Those are batch-derived historical rankings, not the current simulated standings.
    home_rank = _coerce_int(getattr(streak_analysis, "home_team_current_rank", None))
    away_rank = _coerce_int(getattr(streak_analysis, "away_team_current_rank", None))
    home_standing = getattr(streak_analysis, "home_team_current_standing", None)
    away_standing = getattr(streak_analysis, "away_team_current_standing", None)
    cutoff_timestamp = getattr(streak_analysis, "current_standings_cutoff_timestamp", None)
    current_standings_source = getattr(streak_analysis, "current_standings_source", None)
    n_teams = _coerce_int(getattr(event_context.competition, "number_of_teams", None))

    status, status_reason = _current_status(
        home_rank,
        away_rank,
        home_standing,
        away_standing,
        n_teams,
    )
    raw_base = {
        "home_team": home_team,
        "away_team": away_team,
        "event_context_present": True,
        "context_status": event_context.context_status,
        "ranking_source": "streak_analysis.current_standings",
        "home_team_current_rank": home_rank,
        "away_team_current_rank": away_rank,
        "home_team_current_standing": home_standing,
        "away_team_current_standing": away_standing,
        "current_standings_cutoff_timestamp": cutoff_timestamp,
        "current_standings_source": current_standings_source,
        "n_teams": n_teams,
        "m4_component_weights": _COMPONENT_WEIGHTS,
    }

    if status is not None:
        if debug_mode:
            logger.info(
                "  [M4] %s for event %s (%s): %s",
                status,
                event_id,
                participants,
                status_reason,
            )
        raw = {
            **raw_base,
            "m4_edge_raw": 0.0,
            "m4_edge": 0.0,
            "m4_abs_edge": 0.0,
            "m4_status": status,
            "m4_status_reason": status_reason,
            "m4_bias": calculate_bias(0.0),
            "m4_strength": classify_strength(0.0),
        }
        return ModuleResult(
            pillar_id="pillar_1_team_structure",
            module_id="M4",
            module_name="Hybrid Structural Engine",
            event_id=event_id,
            participants=participants,
            value=0.0,
            bias=calculate_bias(0.0),
            strength=classify_strength(0.0),
            components=[],
            raw=raw,
        )

    home_points = _coerce_float(_standing_value(home_standing, "points"))
    away_points = _coerce_float(_standing_value(away_standing, "points"))
    home_gp = _coerce_int(_standing_value(home_standing, "gp", "games_played", "matches"))
    away_gp = _coerce_int(_standing_value(away_standing, "gp", "games_played", "matches"))
    home_gd = _coerce_float(_standing_value(home_standing, "goal_diff", "diff"))
    away_gd = _coerce_float(_standing_value(away_standing, "goal_diff", "diff"))

    home_ppg = _ppg(home_points, home_gp)
    away_ppg = _ppg(away_points, away_gp)
    ppg_edge = home_ppg - away_ppg

    home_gd_per_game = home_gd / float(home_gp)
    away_gd_per_game = away_gd / float(away_gp)
    gd_edge = home_gd_per_game - away_gd_per_game

    home_rank_context = _rank_context(home_rank, n_teams)
    away_rank_context = _rank_context(away_rank, n_teams)
    rank_edge = home_rank_context - away_rank_context

    m4_edge_raw = (
        _COMPONENT_WEIGHTS["PPG_EDGE"] * ppg_edge
        + _COMPONENT_WEIGHTS["GD_EDGE"] * gd_edge
        + _COMPONENT_WEIGHTS["RANK_EDGE"] * rank_edge
    )
    m4_edge = clamp(m4_edge_raw)
    m4_status = "ACTIVE"
    m4_status_reason = "active"

    if debug_mode:
        logger.info(f"--- M4 Hybrid Structural Engine Debug: Event {event_id} ({participants}) ---")
        logger.info(
            f"  home={home_team} rank={home_rank} standing={home_standing} | "
            f"away={away_team} rank={away_rank} standing={away_standing} | n_teams={n_teams}"
        )
        logger.info(
            f"  [PPG_EDGE] home_ppg = points({home_points}) / gp({home_gp}) = {home_ppg:.12f}"
        )
        logger.info(
            f"  [PPG_EDGE] away_ppg = points({away_points}) / gp({away_gp}) = {away_ppg:.12f}"
        )
        logger.info(
            f"  [PPG_EDGE] edge = home_ppg({home_ppg:.12f}) - away_ppg({away_ppg:.12f}) = {ppg_edge:.12f}"
        )
        
        logger.info(
            f"  [GD_EDGE] home_gd_pg = gd({home_gd}) / gp({home_gp}) = {home_gd_per_game:.12f}"
        )
        logger.info(
            f"  [GD_EDGE] away_gd_pg = gd({away_gd}) / gp({away_gp}) = {away_gd_per_game:.12f}"
        )
        logger.info(
            f"  [GD_EDGE] edge = home_gd_pg({home_gd_per_game:.12f}) - away_gd_pg({away_gd_per_game:.12f}) = {gd_edge:.12f}"
        )

        logger.info(
            f"  [RANK_EDGE] home_rank_context = (n_teams({n_teams}) - home_rank({home_rank})) / (n_teams({n_teams}) - 1) = "
            f"({n_teams - home_rank}) / {n_teams - 1} = {home_rank_context:.12f}"
        )
        logger.info(
            f"  [RANK_EDGE] away_rank_context = (n_teams({n_teams}) - away_rank({away_rank})) / (n_teams({n_teams}) - 1) = "
            f"({n_teams - away_rank}) / {n_teams - 1} = {away_rank_context:.12f}"
        )
        logger.info(
            f"  [RANK_EDGE] edge = home_rank_context({home_rank_context:.12f}) - away_rank_context({away_rank_context:.12f}) = {rank_edge:.12f}"
        )

        logger.info(
            f"  [M4_EDGE_RAW] ({_COMPONENT_WEIGHTS['PPG_EDGE']:.2f} * ppg_edge({ppg_edge:.12f})) + "
            f"({_COMPONENT_WEIGHTS['GD_EDGE']:.2f} * gd_edge({gd_edge:.12f})) + "
            f"({_COMPONENT_WEIGHTS['RANK_EDGE']:.2f} * rank_edge({rank_edge:.12f})) = "
            f"({_COMPONENT_WEIGHTS['PPG_EDGE'] * ppg_edge:.12f}) + "
            f"({_COMPONENT_WEIGHTS['GD_EDGE'] * gd_edge:.12f}) + "
            f"({_COMPONENT_WEIGHTS['RANK_EDGE'] * rank_edge:.12f}) = {m4_edge_raw:.12f}"
        )
        logger.info(
            f"  [M4_EDGE] clamped = {m4_edge:.12f}"
        )
        logger.info("  --- Component Summary ---")
        logger.info(
            f"  PPG_EDGE: edge={ppg_edge:.12f}  weight={_COMPONENT_WEIGHTS['PPG_EDGE']:.2f}  "
            f"weighted={ppg_edge * _COMPONENT_WEIGHTS['PPG_EDGE']:.12f}  "
            f"bias={calculate_bias(ppg_edge)}  strength={classify_strength(ppg_edge)}"
        )
        logger.info(
            f"  GD_EDGE: edge={gd_edge:.12f}  weight={_COMPONENT_WEIGHTS['GD_EDGE']:.2f}  "
            f"weighted={gd_edge * _COMPONENT_WEIGHTS['GD_EDGE']:.12f}  "
            f"bias={calculate_bias(gd_edge)}  strength={classify_strength(gd_edge)}"
        )
        logger.info(
            f"  RANK_EDGE: edge={rank_edge:.12f}  weight={_COMPONENT_WEIGHTS['RANK_EDGE']:.2f}  "
            f"weighted={rank_edge * _COMPONENT_WEIGHTS['RANK_EDGE']:.12f}  "
            f"bias={calculate_bias(rank_edge)}  strength={classify_strength(rank_edge)}"
        )
        logger.info(
            f"  M4 Final: edge_raw={m4_edge_raw:.12f}  edge_clamped={m4_edge:.12f}  "
            f"bias={calculate_bias(m4_edge)}  strength={classify_strength(m4_edge)}  "
            f"status={m4_status} ({m4_status_reason})"
        )
        logger.info("-" * 60)

    components = [
        _component(
            "PPG_EDGE",
            ppg_edge,
            _COMPONENT_WEIGHTS["PPG_EDGE"],
            {
                "home_points": home_points,
                "home_gp": home_gp,
                "home_ppg": home_ppg,
                "away_points": away_points,
                "away_gp": away_gp,
                "away_ppg": away_ppg,
                "formula": "home_ppg - away_ppg",
            },
        ),
        _component(
            "GD_EDGE",
            gd_edge,
            _COMPONENT_WEIGHTS["GD_EDGE"],
            {
                "home_table_gd": home_gd,
                "home_gp": home_gp,
                "home_gd_per_game": home_gd_per_game,
                "away_table_gd": away_gd,
                "away_gp": away_gp,
                "away_gd_per_game": away_gd_per_game,
                "formula": "home_gd_per_game - away_gd_per_game",
            },
        ),
        _component(
            "RANK_EDGE",
            rank_edge,
            _COMPONENT_WEIGHTS["RANK_EDGE"],
            {
                "home_rank": home_rank,
                "away_rank": away_rank,
                "n_teams": n_teams,
                "home_rank_context": home_rank_context,
                "away_rank_context": away_rank_context,
                "formula": "(n_teams - rank) / (n_teams - 1)",
            },
        ),
    ]

    raw = {
        **raw_base,
        "m4_ppg_edge": ppg_edge,
        "m4_gd_edge": gd_edge,
        "m4_rank_edge": rank_edge,
        "m4_edge_raw": m4_edge_raw,
        "m4_edge": m4_edge,
        "m4_abs_edge": abs(m4_edge),
        "m4_status": m4_status,
        "m4_status_reason": m4_status_reason,
        "m4_bias": calculate_bias(m4_edge),
        "m4_strength": classify_strength(m4_edge),
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M4",
        module_name="Hybrid Structural Engine",
        event_id=event_id,
        participants=participants,
        value=m4_edge,
        bias=calculate_bias(m4_edge),
        strength=classify_strength(m4_edge),
        components=components,
        raw=raw,
    )
