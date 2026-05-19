"""M6 - Contextual Competitive Cost Engine.

Measures how costly it is competitively for a team to fail to take points in
the upcoming match, using only simulated standings immediately before kickoff.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
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

_POINTS_PER_WIN = 3
_BASE_SEVERITY = {
    "EUROPE": 0.70,
    "PLAYOFF": 0.85,
    "SURVIVAL": 1.00,
    "MIDTABLE": 0.30,
    "NONE": 0.00,
}
_REGIME_MULTIPLIER = {
    "ON_TARGET_ZONE": 1.00,
    "OUTSIDE_TARGET": 1.10,
    "INSIDE_DANGER": 1.25,
    "SAFE_ZONE": 0.80,
    "NONE": 0.00,
}


@dataclass(frozen=True)
class TeamCompetitiveContext:
    team_name: str
    rank: int
    points: float
    games_played: int
    objective_type: str
    regime_state: str
    points_cutoff: Optional[float]
    gap: Optional[float]
    urgency_factor: float
    base_severity: float
    regime_multiplier: float
    final_cost: float


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


def _standing_value(standing: Dict, *keys: str) -> Optional[float]:
    if not isinstance(standing, dict):
        return None
    for key in keys:
        value = standing.get(key)
        if value is not None:
            return value
    return None


def _standings_by_rank(current_standings: Dict[str, Dict]) -> Dict[int, Dict]:
    by_rank: Dict[int, Dict] = {}
    if not isinstance(current_standings, dict):
        return by_rank

    for standing in current_standings.values():
        rank = _coerce_int(_standing_value(standing, "rank", "position"))
        if rank is not None and rank not in by_rank:
            by_rank[rank] = standing
    return by_rank


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


def _relative_edge(home_cost: float, away_cost: float) -> float:
    denominator = abs(home_cost) + abs(away_cost)
    if denominator == 0:
        return 0.0
    return (home_cost - away_cost) / denominator


def _games_remaining(total_regular_season_games: int, games_played_home: int, games_played_away: int) -> int:
    remaining = min(
        total_regular_season_games - games_played_home,
        total_regular_season_games - games_played_away,
    )
    return max(0, remaining)


def _resolve_team_context(
    team_name: str,
    rank: int,
    points: float,
    games_played: int,
    n_liga: int,
    standings_by_rank: Dict[int, Dict],
    max_points_remaining: int,
    football_like: bool,
) -> Tuple[TeamCompetitiveContext, Optional[str]]:
    objective_type = "NONE"
    regime_state = "NONE"
    points_cutoff: Optional[float] = None
    gap: Optional[float] = None
    base_severity = _BASE_SEVERITY["NONE"]
    regime_multiplier = _REGIME_MULTIPLIER["NONE"]
    degraded_reason: Optional[str] = None

    if football_like and n_liga >= 7:
        if rank <= 6:
            objective_type = "EUROPE"
            regime_state = "ON_TARGET_ZONE"
            base_severity = _BASE_SEVERITY[objective_type]
            regime_multiplier = _REGIME_MULTIPLIER[regime_state]
            cutoff_standing = standings_by_rank.get(7)
            points_cutoff = _coerce_float(_standing_value(cutoff_standing, "points"))
            if points_cutoff is not None:
                gap = points - points_cutoff
            else:
                degraded_reason = "missing_points_cutoff"
        elif rank == 7:
            objective_type = "EUROPE"
            regime_state = "OUTSIDE_TARGET"
            base_severity = _BASE_SEVERITY[objective_type]
            regime_multiplier = _REGIME_MULTIPLIER[regime_state]
            cutoff_standing = standings_by_rank.get(6)
            points_cutoff = _coerce_float(_standing_value(cutoff_standing, "points"))
            if points_cutoff is not None:
                gap = points_cutoff - points
            else:
                degraded_reason = "missing_points_cutoff"

    if objective_type == "NONE" and football_like and n_liga >= 4:
        survival_cutoff_rank = n_liga - 3
        danger_rank = n_liga - 2
        if rank >= danger_rank:
            objective_type = "SURVIVAL"
            regime_state = "INSIDE_DANGER"
            base_severity = _BASE_SEVERITY[objective_type]
            regime_multiplier = _REGIME_MULTIPLIER[regime_state]
            cutoff_standing = standings_by_rank.get(survival_cutoff_rank)
            points_cutoff = _coerce_float(_standing_value(cutoff_standing, "points"))
            if points_cutoff is not None:
                gap = points_cutoff - points
            else:
                degraded_reason = "missing_points_cutoff"
        elif rank == survival_cutoff_rank:
            objective_type = "SURVIVAL"
            regime_state = "SAFE_ZONE"
            base_severity = _BASE_SEVERITY[objective_type]
            regime_multiplier = _REGIME_MULTIPLIER[regime_state]
            cutoff_standing = standings_by_rank.get(danger_rank)
            points_cutoff = _coerce_float(_standing_value(cutoff_standing, "points"))
            if points_cutoff is not None:
                gap = points - points_cutoff
            else:
                degraded_reason = "missing_points_cutoff"

    if objective_type == "NONE" and football_like and 4 <= n_liga < 7:
        objective_type = "MIDTABLE"
        regime_state = "NONE"
        base_severity = _BASE_SEVERITY[objective_type]
        regime_multiplier = _REGIME_MULTIPLIER[regime_state]

    if not football_like:
        degraded_reason = degraded_reason or "unsupported_non_football_structure"

    urgency_factor = 0.0
    if gap is not None and max_points_remaining > 0:
        urgency_factor = clamp(1.0 - (gap / float(max_points_remaining)), 0.0, 1.0)

    final_cost = base_severity * regime_multiplier * urgency_factor
    return (
        TeamCompetitiveContext(
            team_name=team_name,
            rank=rank,
            points=points,
            games_played=games_played,
            objective_type=objective_type,
            regime_state=regime_state,
            points_cutoff=points_cutoff,
            gap=gap,
            urgency_factor=urgency_factor,
            base_severity=base_severity,
            regime_multiplier=regime_multiplier,
            final_cost=final_cost,
        ),
        degraded_reason,
    )


def _status_from_contexts(
    *,
    current_standings_present: bool,
    total_regular_season_games_missing: bool,
    football_like: bool,
    max_points_remaining: int,
    home_context: TeamCompetitiveContext,
    away_context: TeamCompetitiveContext,
    degraded_reason: Optional[str],
) -> Tuple[str, str]:
    if not current_standings_present:
        return "INSUFFICIENT_DATA", "missing_current_standings"
    if total_regular_season_games_missing:
        return "DEGRADED", "missing_total_regular_season_games"
    if max_points_remaining <= 0:
        return "INACTIVE", "no_points_remaining"
    if degraded_reason is not None:
        return "DEGRADED", degraded_reason

    active_objectives = {"EUROPE", "SURVIVAL"}
    if (
        home_context.objective_type not in active_objectives
        and away_context.objective_type not in active_objectives
    ):
        return "INACTIVE", "no_competitive_cost_context"

    if not football_like:
        return "DEGRADED", "unsupported_non_football_structure"

    return "ACTIVE", "active"


def calculate_contextual_competitive_cost_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    event_id = getattr(streak_analysis, "event_id", 0)
    participants = getattr(streak_analysis, "participants", "") or event_context.participants_label
    home_team = getattr(streak_analysis, "home_team_name", None) or event_context.home.name
    away_team = getattr(streak_analysis, "away_team_name", None) or event_context.away.name

    current_standings = getattr(streak_analysis, "current_standings", None) or {}
    current_standings_source = getattr(streak_analysis, "current_standings_source", None)
    current_standings_cutoff_timestamp = getattr(streak_analysis, "current_standings_cutoff_timestamp", None)
    home_standing = getattr(streak_analysis, "home_team_current_standing", None)
    away_standing = getattr(streak_analysis, "away_team_current_standing", None)
    home_rank = _coerce_int(
        getattr(streak_analysis, "home_team_current_rank", None)
        or _standing_value(home_standing, "rank", "position")
    )
    away_rank = _coerce_int(
        getattr(streak_analysis, "away_team_current_rank", None)
        or _standing_value(away_standing, "rank", "position")
    )

    home_points = _coerce_float(_standing_value(home_standing, "points"))
    away_points = _coerce_float(_standing_value(away_standing, "points"))
    home_games_played = _coerce_int(_standing_value(home_standing, "gp", "games_played"))
    away_games_played = _coerce_int(_standing_value(away_standing, "gp", "games_played"))

    n_liga = _coerce_int(len(current_standings)) or _coerce_int(getattr(event_context.competition, "number_of_teams", None))
    total_regular_season_games = _coerce_int(getattr(event_context.competition, "total_regular_season_games", None))
    football_like = str(getattr(event_context, "sport", "") or "").strip().lower() in {
        "football",
        "soccer",
        "futbol",
    }

    if n_liga is None or n_liga <= 1:
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "ranking_source": "streak_analysis.current_standings",
            "current_standings_source": current_standings_source,
            "current_standings_cutoff_timestamp": current_standings_cutoff_timestamp,
            "n_liga": n_liga,
            "points_per_win": _POINTS_PER_WIN,
            "total_regular_season_games": total_regular_season_games,
            "games_remaining": 0,
            "max_points_remaining": 0,
            "home_context": {},
            "away_context": {},
            "m6_edge_raw": 0.0,
            "m6_edge": 0.0,
            "m6_abs_edge": 0.0,
            "m6_status": "INSUFFICIENT_DATA",
            "m6_status_reason": "missing_league_size",
            "objective_rule_profile": "default_football_v1",
        }
        return ModuleResult(
            pillar_id="pillar_1_team_structure",
            module_id="M6",
            module_name="Contextual Competitive Cost Engine",
            event_id=event_id,
            participants=participants,
            value=0.0,
            bias=calculate_bias(0.0),
            strength=classify_strength(0.0),
            components=[],
            raw=raw,
        )

    if home_rank is None or away_rank is None:
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "ranking_source": "streak_analysis.current_standings",
            "current_standings_source": current_standings_source,
            "current_standings_cutoff_timestamp": current_standings_cutoff_timestamp,
            "n_liga": n_liga,
            "points_per_win": _POINTS_PER_WIN,
            "total_regular_season_games": total_regular_season_games,
            "games_remaining": 0,
            "max_points_remaining": 0,
            "home_context": {},
            "away_context": {},
            "m6_edge_raw": 0.0,
            "m6_edge": 0.0,
            "m6_abs_edge": 0.0,
            "m6_status": "INSUFFICIENT_DATA",
            "m6_status_reason": "missing_current_rank",
            "objective_rule_profile": "default_football_v1",
        }
        return ModuleResult(
            pillar_id="pillar_1_team_structure",
            module_id="M6",
            module_name="Contextual Competitive Cost Engine",
            event_id=event_id,
            participants=participants,
            value=0.0,
            bias=calculate_bias(0.0),
            strength=classify_strength(0.0),
            components=[],
            raw=raw,
        )

    if home_points is None or away_points is None:
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "ranking_source": "streak_analysis.current_standings",
            "current_standings_source": current_standings_source,
            "current_standings_cutoff_timestamp": current_standings_cutoff_timestamp,
            "n_liga": n_liga,
            "points_per_win": _POINTS_PER_WIN,
            "total_regular_season_games": total_regular_season_games,
            "games_remaining": 0,
            "max_points_remaining": 0,
            "home_context": {},
            "away_context": {},
            "m6_edge_raw": 0.0,
            "m6_edge": 0.0,
            "m6_abs_edge": 0.0,
            "m6_status": "INSUFFICIENT_DATA",
            "m6_status_reason": "missing_points",
            "objective_rule_profile": "default_football_v1",
        }
        return ModuleResult(
            pillar_id="pillar_1_team_structure",
            module_id="M6",
            module_name="Contextual Competitive Cost Engine",
            event_id=event_id,
            participants=participants,
            value=0.0,
            bias=calculate_bias(0.0),
            strength=classify_strength(0.0),
            components=[],
            raw=raw,
        )

    if home_games_played is None or away_games_played is None:
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "ranking_source": "streak_analysis.current_standings",
            "current_standings_source": current_standings_source,
            "current_standings_cutoff_timestamp": current_standings_cutoff_timestamp,
            "n_liga": n_liga,
            "points_per_win": _POINTS_PER_WIN,
            "total_regular_season_games": total_regular_season_games,
            "games_remaining": 0,
            "max_points_remaining": 0,
            "home_context": {},
            "away_context": {},
            "m6_edge_raw": 0.0,
            "m6_edge": 0.0,
            "m6_abs_edge": 0.0,
            "m6_status": "INSUFFICIENT_DATA",
            "m6_status_reason": "missing_games_played",
            "objective_rule_profile": "default_football_v1",
        }
        return ModuleResult(
            pillar_id="pillar_1_team_structure",
            module_id="M6",
            module_name="Contextual Competitive Cost Engine",
            event_id=event_id,
            participants=participants,
            value=0.0,
            bias=calculate_bias(0.0),
            strength=classify_strength(0.0),
            components=[],
            raw=raw,
        )

    standings_by_rank = _standings_by_rank(current_standings)
    total_regular_season_games_missing = total_regular_season_games is None
    if total_regular_season_games_missing:
        games_remaining = 1
        max_points_remaining = _POINTS_PER_WIN
    else:
        games_remaining = _games_remaining(total_regular_season_games, home_games_played, away_games_played)
        max_points_remaining = games_remaining * _POINTS_PER_WIN
    if max_points_remaining <= 0:
        home_context, home_degraded_reason = _resolve_team_context(
            team_name=home_team,
            rank=home_rank,
            points=home_points,
            games_played=home_games_played,
            n_liga=n_liga,
            standings_by_rank=standings_by_rank,
            max_points_remaining=0,
            football_like=football_like,
        )
        away_context, away_degraded_reason = _resolve_team_context(
            team_name=away_team,
            rank=away_rank,
            points=away_points,
            games_played=away_games_played,
            n_liga=n_liga,
            standings_by_rank=standings_by_rank,
            max_points_remaining=0,
            football_like=football_like,
        )
        m6_edge_raw = 0.0
        m6_edge = 0.0
        m6_status = "INACTIVE"
        m6_status_reason = "no_points_remaining"
        degraded_reason = home_degraded_reason or away_degraded_reason
    else:
        home_context, home_degraded_reason = _resolve_team_context(
            team_name=home_team,
            rank=home_rank,
            points=home_points,
            games_played=home_games_played,
            n_liga=n_liga,
            standings_by_rank=standings_by_rank,
            max_points_remaining=max_points_remaining,
            football_like=football_like,
        )
        away_context, away_degraded_reason = _resolve_team_context(
            team_name=away_team,
            rank=away_rank,
            points=away_points,
            games_played=away_games_played,
            n_liga=n_liga,
            standings_by_rank=standings_by_rank,
            max_points_remaining=max_points_remaining,
            football_like=football_like,
        )
        degraded_reason = home_degraded_reason or away_degraded_reason
        m6_edge_raw = _relative_edge(home_context.final_cost, away_context.final_cost)
        m6_edge = clamp(m6_edge_raw)
        m6_status, m6_status_reason = _status_from_contexts(
            current_standings_present=bool(current_standings),
            total_regular_season_games_missing=total_regular_season_games_missing,
            football_like=football_like,
            max_points_remaining=max_points_remaining,
            home_context=home_context,
            away_context=away_context,
            degraded_reason=degraded_reason,
        )
        if m6_status == "INACTIVE":
            m6_edge = 0.0
            m6_edge_raw = 0.0

    if total_regular_season_games_missing and m6_status != "INSUFFICIENT_DATA":
        m6_status = "DEGRADED"
        m6_status_reason = "missing_total_regular_season_games"

    if debug_mode:
        logger.info(
            f"--- M6 Contextual Competitive Cost Engine Debug: Event {event_id} ({participants}) ---"
        )
        logger.info(
            f"  standings_source={current_standings_source} cutoff_ts={current_standings_cutoff_timestamp} "
            f"ranking_source=streak_analysis.current_standings"
        )
        logger.info(
            f"  n_liga={n_liga} total_regular_season_games={total_regular_season_games} "
            f"games_remaining={games_remaining} max_points_remaining={max_points_remaining}"
        )
        logger.info(
            f"  sport_football_like={football_like} activation_status={m6_status} ({m6_status_reason})"
        )

        for label, context in (("HOME", home_context), ("AWAY", away_context)):
            logger.info(f"  [{label}_CONTEXT] rank={context.rank} points={context.points:.12f} gp={context.games_played}")
            logger.info(
                f"  [{label}_CONTEXT] objective_type={context.objective_type} regime_state={context.regime_state} "
                f"points_cutoff={context.points_cutoff if context.points_cutoff is not None else 'None'} "
                f"gap={context.gap if context.gap is not None else 'None'}"
            )
            logger.info(
                f"  [{label}_CONTEXT] base_severity={context.base_severity:.12f} "
                f"regime_multiplier={context.regime_multiplier:.12f}"
            )
            if context.gap is not None:
                logger.info(
                    f"  [{label}_CONTEXT] urgency_factor = max(0.0, min(1.0, 1.0 - ({context.gap:.12f} / {max_points_remaining:.12f}))) = {context.urgency_factor:.12f}"
                )
            else:
                logger.info(
                    f"  [{label}_CONTEXT] urgency_factor = 0.000000000000 (no gap context)"
                )
            logger.info(
                f"  [{label}_CONTEXT] final_cost = base_severity({context.base_severity:.2f}) * "
                f"regime_multiplier({context.regime_multiplier:.2f}) * urgency_factor({context.urgency_factor:.12f}) = {context.final_cost:.12f}"
            )

        # M6 Relative Edge
        denom_cost = abs(home_context.final_cost) + abs(away_context.final_cost)
        if denom_cost == 0:
            logger.info(
                f"  [M6_EDGE_RAW] denominator = 0 -> edge = 0.000000000000"
            )
        else:
            logger.info(
                f"  [M6_EDGE_RAW] edge = (home_cost({home_context.final_cost:.12f}) - away_cost({away_context.final_cost:.12f})) / "
                f"denominator({denom_cost:.12f}) = {m6_edge_raw:.12f}"
            )
        logger.info(
            f"  [M6_EDGE] clamped = {m6_edge:.12f}"
        )

        logger.info("  --- Component Summary ---")
        if m6_status != "INSUFFICIENT_DATA":
            logger.info(
                f"  CONTEXTUAL_COST_EDGE: edge={m6_edge:.12f}  weight=1.00  weighted={m6_edge * 1.0:.12f}  "
                f"bias={calculate_bias(m6_edge)}  strength={classify_strength(m6_edge)}"
            )
        else:
            logger.info("  (No components calculated due to INSUFFICIENT_DATA)")
        
        logger.info(
            f"  M6 Final: edge_raw={m6_edge_raw:.12f}  edge_clamped={m6_edge:.12f}  "
            f"bias={calculate_bias(m6_edge)}  strength={classify_strength(m6_edge)}  "
            f"status={m6_status} ({m6_status_reason})"
        )
        logger.info("-" * 60)

    home_context_raw = asdict(home_context)
    away_context_raw = asdict(away_context)
    components = []
    if m6_status != "INSUFFICIENT_DATA":
        components = [
            _component(
                "CONTEXTUAL_COST_EDGE",
                m6_edge,
                1.0,
                {
                    "home_final_cost": home_context.final_cost,
                    "away_final_cost": away_context.final_cost,
                    "formula": "(FINAL_COST_HOME - FINAL_COST_AWAY) / (abs(FINAL_COST_HOME) + abs(FINAL_COST_AWAY))",
                },
            )
        ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "ranking_source": "streak_analysis.current_standings",
        "current_standings_source": current_standings_source,
        "current_standings_cutoff_timestamp": current_standings_cutoff_timestamp,
        "n_liga": n_liga,
        "points_per_win": _POINTS_PER_WIN,
        "total_regular_season_games": None if total_regular_season_games_missing else total_regular_season_games,
        "games_remaining": games_remaining,
        "max_points_remaining": max_points_remaining,
        "home_context": home_context_raw,
        "away_context": away_context_raw,
        "m6_edge_raw": m6_edge_raw,
        "m6_edge": m6_edge,
        "m6_abs_edge": abs(m6_edge),
        "m6_status": m6_status,
        "m6_status_reason": m6_status_reason,
        "objective_rule_profile": "default_football_v1",
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M6",
        module_name="Contextual Competitive Cost Engine",
        event_id=event_id,
        participants=participants,
        value=m6_edge,
        bias=calculate_bias(m6_edge),
        strength=classify_strength(m6_edge),
        components=components,
        raw=raw,
    )
