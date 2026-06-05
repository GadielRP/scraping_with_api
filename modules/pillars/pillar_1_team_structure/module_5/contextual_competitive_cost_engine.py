"""M5 - Contextual Competitive Cost Engine.

This module measures how expensive it would be competitively not to add points
in the current match. It does not use recent form, goal difference, odds, H2H,
PPG, streaks, or market signals.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)

POINTS_PER_WIN = 3
EUROPE_CUTOFF_RANK = 7
SURVIVAL_DANGER_PLACES = 3

BASE_SEVERITY = {
    "EUROPE": 0.70,
    "PLAYOFF": 0.85,
    "SURVIVAL": 1.00,
    "MIDTABLE": 0.30,
    "NONE": 0.00,
}

REGIME_MULTIPLIER = {
    "ON_TARGET_ZONE": 1.00,
    "OUTSIDE_TARGET": 1.10,
    "INSIDE_DANGER": 1.25,
    "SAFE_ZONE": 0.80,
    "NONE": 0.00,
}


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if coerced != coerced or coerced == float("inf") or coerced == float("-inf"):
        return None
    return coerced


def _coerce_int(value: Any) -> Optional[int]:
    float_value = _coerce_float(value)
    if float_value is None:
        return None
    if not float_value.is_integer():
        return None
    return int(float_value)


def _pressure_side_from_edge(m5_edge: float) -> str:
    if m5_edge > 0:
        return "HOME"
    if m5_edge < 0:
        return "AWAY"
    return "NONE"


def _normalize_name(value: Any) -> str:
    return str(value or "").strip().casefold()


def _standings_items(current_standings: Any) -> List[Tuple[Any, Any]]:
    if isinstance(current_standings, dict):
        return list(current_standings.items())
    if isinstance(current_standings, list):
        return list(enumerate(current_standings))
    return []


def _extract_record_name(record: Any) -> Optional[str]:
    if not isinstance(record, dict):
        return None
    for key in ("team_name", "teamName", "name", "display_name", "short_name", "team"):
        value = record.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _extract_points(record: Any) -> Optional[float]:
    if not isinstance(record, dict):
        return None
    for key in ("points", "pts"):
        value = _coerce_float(record.get(key))
        if value is not None:
            return value
    return None


def _extract_rank(record: Any) -> Optional[int]:
    if not isinstance(record, dict):
        return None
    for key in ("rank", "position", "current_rank", "standing_rank"):
        value = _coerce_int(record.get(key))
        if value is not None and value > 0:
            return value
    return None


def _extract_games_played(record: Any) -> Optional[int]:
    if not isinstance(record, dict):
        return None
    for key in ("games_played", "gp", "matches"):
        value = _coerce_int(record.get(key))
        if value is not None and value >= 0:
            return value
    return None


def _standing_is_valid(record: Any) -> bool:
    points = _extract_points(record)
    rank = _extract_rank(record)
    return points is not None and rank is not None and points >= 0 and rank > 0


def _count_valid_standings(current_standings: Any) -> Optional[int]:
    items = _standings_items(current_standings)
    if not items:
        return None
    valid_count = sum(1 for _, record in items if _standing_is_valid(record))
    return valid_count if valid_count > 0 else None


def _find_standing_by_rank(current_standings: Any, target_rank: int) -> Optional[Dict[str, Any]]:
    if target_rank <= 0:
        return None
    for _, record in _standings_items(current_standings):
        if not isinstance(record, dict):
            continue
        rank = _extract_rank(record)
        if rank == target_rank:
            return record
    return None


def _find_nearest_standing_with_points(
    current_standings: Any,
    target_rank: int,
) -> Optional[Dict[str, Any]]:
    exact = _find_standing_by_rank(current_standings, target_rank)
    if exact is not None and _extract_points(exact) is not None:
        return exact

    best_record: Optional[Dict[str, Any]] = None
    best_distance: Optional[int] = None
    best_rank: Optional[int] = None

    for _, record in _standings_items(current_standings):
        if not isinstance(record, dict):
            continue
        rank = _extract_rank(record)
        points = _extract_points(record)
        if rank is None or points is None:
            continue
        distance = abs(rank - target_rank)
        if best_distance is None or distance < best_distance or (distance == best_distance and (best_rank is None or rank < best_rank)):
            best_record = record
            best_distance = distance
            best_rank = rank

    return best_record


def _find_standing_by_team(current_standings: Any, team_name: Optional[str]) -> Optional[Dict[str, Any]]:
    normalized_team = _normalize_name(team_name)
    if not normalized_team:
        return None

    for key, record in _standings_items(current_standings):
        if _normalize_name(key) == normalized_team and isinstance(record, dict):
            return record
        if not isinstance(record, dict):
            continue
        if _normalize_name(_extract_record_name(record)) == normalized_team:
            return record
    return None


def _resolve_team_standing(
    current_standings: Any,
    team_name: Optional[str],
    direct_standing: Any,
) -> Optional[Dict[str, Any]]:
    if isinstance(direct_standing, dict) and _extract_points(direct_standing) is not None and _extract_rank(direct_standing) is not None:
        return direct_standing
    return _find_standing_by_team(current_standings, team_name)


def _determine_objective_type(
    rank: Optional[int],
    n_league: Optional[int],
    competition_slug: Optional[str] = None,
    competition_name: Optional[str] = None,
) -> str:
    del competition_slug, competition_name

    if rank is None or n_league is None:
        return "NONE"
    if rank <= 0 or n_league <= 1:
        return "NONE"

    if n_league >= 18:
        if rank <= EUROPE_CUTOFF_RANK:
            return "EUROPE"
        survival_cutoff_rank = max(1, n_league - SURVIVAL_DANGER_PLACES + 1)
        if rank >= survival_cutoff_rank:
            return "SURVIVAL"
        return "MIDTABLE"

    return "MIDTABLE"


def _determine_regime_state(objective_type: str, rank: Optional[int], n_league: Optional[int]) -> str:
    if rank is None or rank <= 0 or n_league is None or n_league <= 1:
        return "NONE"

    if objective_type == "EUROPE":
        return "ON_TARGET_ZONE" if rank <= EUROPE_CUTOFF_RANK else "OUTSIDE_TARGET"

    if objective_type == "SURVIVAL":
        danger_cutoff_rank = max(1, n_league - SURVIVAL_DANGER_PLACES + 1)
        return "INSIDE_DANGER" if rank >= danger_cutoff_rank else "SAFE_ZONE"

    if objective_type == "PLAYOFF":
        return "ON_TARGET_ZONE"

    if objective_type == "MIDTABLE":
        return "SAFE_ZONE"

    return "NONE"


def _determine_points_cutoff(
    objective_type: str,
    regime_state: str,
    team_rank: Optional[int],
    team_points: Optional[float],
    current_standings: Any,
    n_league: Optional[int],
) -> Optional[float]:
    del regime_state

    if objective_type == "EUROPE":
        target_rank = EUROPE_CUTOFF_RANK
        if team_rank == EUROPE_CUTOFF_RANK:
            target_rank = EUROPE_CUTOFF_RANK + 1
        standing = _find_standing_by_rank(current_standings, target_rank)
        if standing is None or _extract_points(standing) is None:
            standing = _find_nearest_standing_with_points(current_standings, target_rank)
        return _extract_points(standing)

    if objective_type == "SURVIVAL":
        if n_league is None:
            return None
        target_rank = max(1, n_league - SURVIVAL_DANGER_PLACES)
        standing = _find_standing_by_rank(current_standings, target_rank)
        if standing is None or _extract_points(standing) is None:
            standing = _find_nearest_standing_with_points(current_standings, target_rank)
        return _extract_points(standing)

    if objective_type == "MIDTABLE":
        return team_points

    return None


def _calculate_gap(
    objective_type: str,
    regime_state: str,
    team_points: Optional[float],
    points_cutoff: Optional[float],
    max_points_remaining: Optional[float],
) -> Optional[float]:
    if team_points is None or points_cutoff is None:
        if objective_type in {"MIDTABLE", "NONE"} and max_points_remaining is not None:
            return max_points_remaining
        return None

    if objective_type == "EUROPE":
        if regime_state == "ON_TARGET_ZONE":
            return team_points - points_cutoff
        return points_cutoff - team_points

    if objective_type == "SURVIVAL":
        if regime_state == "INSIDE_DANGER":
            return points_cutoff - team_points
        return team_points - points_cutoff

    if objective_type in {"MIDTABLE", "NONE"}:
        return max_points_remaining

    return None


def _calculate_urgency_factor(gap: Optional[float], max_points_remaining: Optional[float]) -> Tuple[float, float, float]:
    if max_points_remaining is None or max_points_remaining <= 0:
        return 0.0, 0.0, 0.0

    gap_for_urgency = max(gap or 0.0, 0.0)
    raw_value = 1.0 - (gap_for_urgency / float(max_points_remaining))
    urgency_factor = clamp(raw_value, 0.0, 1.0)
    return urgency_factor, gap_for_urgency, raw_value


def _calculate_final_cost(
    base_severity: float,
    regime_multiplier: float,
    urgency_factor: float,
) -> float:
    return base_severity * regime_multiplier * urgency_factor


def _build_team_metrics(
    *,
    team_label: str,
    team_name: Optional[str],
    standing: Optional[Dict[str, Any]],
    current_standings: Any,
    competition_slug: Optional[str],
    competition_name: Optional[str],
    n_league: Optional[int],
    max_points_remaining: Optional[float],
) -> Dict[str, Any]:
    points = _extract_points(standing)
    rank = _extract_rank(standing)
    games_played = _extract_games_played(standing)
    objective_type = _determine_objective_type(rank, n_league, competition_slug, competition_name)
    regime_state = _determine_regime_state(objective_type, rank, n_league)
    base_severity = BASE_SEVERITY.get(objective_type, 0.0)
    regime_multiplier = REGIME_MULTIPLIER.get(regime_state, 0.0)
    points_cutoff = _determine_points_cutoff(
        objective_type,
        regime_state,
        rank,
        points,
        current_standings,
        n_league,
    )
    gap = _calculate_gap(objective_type, regime_state, points, points_cutoff, max_points_remaining)
    urgency_factor, gap_for_urgency, urgency_raw = _calculate_urgency_factor(gap, max_points_remaining)
    final_cost = _calculate_final_cost(base_severity, regime_multiplier, urgency_factor)

    return {
        "team": team_name,
        "team_label": team_label,
        "points": points,
        "rank": rank,
        "games_played": games_played,
        "objective_type": objective_type,
        "regime_state": regime_state,
        "points_cutoff": points_cutoff,
        "gap": gap,
        "gap_for_urgency": gap_for_urgency,
        "urgency_factor": urgency_factor,
        "urgency_formula_raw": urgency_raw,
        "base_severity": base_severity,
        "regime_multiplier": regime_multiplier,
        "final_cost": final_cost,
    }


def _build_component(name: str, edge: float, weight: float, raw: Dict[str, Any]) -> ModuleComponentResult:
    pressure_side = _pressure_side_from_edge(edge)
    return ModuleComponentResult(
        name=name,
        edge=edge,
        bias=pressure_side,
        strength=classify_strength(edge),
        weight=weight,
        weighted_edge=edge * weight,
        raw=raw,
    )


def _determine_status(
    *,
    current_standings: Any,
    home_standing: Optional[Dict[str, Any]],
    away_standing: Optional[Dict[str, Any]],
    home_metrics: Dict[str, Any],
    away_metrics: Dict[str, Any],
    n_league: Optional[int],
    max_points_remaining: Optional[float],
) -> Tuple[str, str]:
    if current_standings is None:
        return "INSUFFICIENT_DATA", "missing_current_standings"
    if n_league is None:
        return "INSUFFICIENT_DATA", "missing_current_standings"
    if n_league <= 1:
        return "INVALID", "invalid_league_size"
    if home_standing is None or away_standing is None:
        return "INSUFFICIENT_DATA", "missing_team_standing"
    if home_metrics["points"] is None or away_metrics["points"] is None or home_metrics["rank"] is None or away_metrics["rank"] is None:
        return "INSUFFICIENT_DATA", "missing_points_or_rank"
    if home_metrics["rank"] <= 0 or away_metrics["rank"] <= 0:
        return "INVALID", "invalid_team_rank"
    if home_metrics["points"] < 0 or away_metrics["points"] < 0:
        return "INVALID", "invalid_team_rank"
    if (
        home_metrics["objective_type"] in {"EUROPE", "SURVIVAL"}
        and home_metrics["points_cutoff"] is None
    ) or (
        away_metrics["objective_type"] in {"EUROPE", "SURVIVAL"}
        and away_metrics["points_cutoff"] is None
    ):
        return "INSUFFICIENT_DATA", "missing_points_or_rank"
    if max_points_remaining is None:
        return "INSUFFICIENT_DATA", "no_remaining_points_window"
    if max_points_remaining <= 0:
        return "INSUFFICIENT_DATA", "no_remaining_points_window"
    return "ACTIVE", "active"


def _serialize_team_raw(metrics: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "team": metrics["team"],
        "points": metrics["points"],
        "rank": metrics["rank"],
        "games_played": metrics["games_played"],
        "objective_type": metrics["objective_type"],
        "regime_state": metrics["regime_state"],
        "points_cutoff": metrics["points_cutoff"],
        "gap": metrics["gap"],
        "urgency_factor": metrics["urgency_factor"],
        "base_severity": metrics["base_severity"],
        "regime_multiplier": metrics["regime_multiplier"],
        "final_cost": metrics["final_cost"],
    }


def _league_total_games(n_league: Optional[int]) -> Optional[int]:
    if n_league is None:
        return None
    if n_league >= 18:
        return 38
    return max((n_league - 1) * 2, 0)


def _games_played_reference(values: Iterable[Optional[int]]) -> int:
    numeric_values = [value for value in values if value is not None and value >= 0]
    if not numeric_values:
        return 0
    return max(numeric_values)


def calculate_contextual_competitive_cost_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    event_id = getattr(streak_analysis, "event_id", getattr(event_context, "event_id", 0))
    participants = getattr(streak_analysis, "participants", None) or getattr(event_context, "participants_label", "") or ""
    home_team = getattr(streak_analysis, "home_team_name", None) or getattr(event_context.home, "name", None)
    away_team = getattr(streak_analysis, "away_team_name", None) or getattr(event_context.away, "name", None)
    competition_name = getattr(streak_analysis, "competition_name", None) or getattr(event_context.competition, "display_name", None) or getattr(event_context.competition, "canonical_name", None)
    competition_slug = getattr(streak_analysis, "competition_slug", None) or getattr(event_context.competition, "slug", None) or getattr(event_context.competition, "unique_slug", None)
    season_id = getattr(streak_analysis, "season_id", None) or getattr(event_context, "season_id", None)
    season_name = getattr(streak_analysis, "season_name", None) or getattr(event_context, "season_name", None)
    current_standings = getattr(streak_analysis, "current_standings", None)

    home_direct_standing = getattr(streak_analysis, "home_team_current_standing", None)
    away_direct_standing = getattr(streak_analysis, "away_team_current_standing", None)
    home_rank_direct = getattr(streak_analysis, "home_team_current_rank", None)
    away_rank_direct = getattr(streak_analysis, "away_team_current_rank", None)

    home_standing = _resolve_team_standing(current_standings, home_team, home_direct_standing)
    away_standing = _resolve_team_standing(current_standings, away_team, away_direct_standing)

    n_league = _count_valid_standings(current_standings)
    total_season_games = _league_total_games(n_league)
    games_played_home = _extract_games_played(home_standing)
    games_played_away = _extract_games_played(away_standing)
    games_played_max = 0
    if isinstance(current_standings, (dict, list)):
        for _, standing in _standings_items(current_standings):
            standing_gp = _extract_games_played(standing)
            if standing_gp is not None and standing_gp > games_played_max:
                games_played_max = standing_gp
    games_played_max_value = games_played_max if games_played_max > 0 else None
    games_played_reference = _games_played_reference([games_played_home, games_played_away, games_played_max_value])

    games_remaining: Optional[int] = None
    max_points_remaining: Optional[float] = None
    if total_season_games is not None:
        games_remaining = max(total_season_games - games_played_reference, 0)
        max_points_remaining = float(games_remaining * POINTS_PER_WIN)

    home_metrics = _build_team_metrics(
        team_label="HOME",
        team_name=home_team,
        standing=home_standing,
        current_standings=current_standings,
        competition_slug=competition_slug,
        competition_name=competition_name,
        n_league=n_league,
        max_points_remaining=max_points_remaining,
    )
    away_metrics = _build_team_metrics(
        team_label="AWAY",
        team_name=away_team,
        standing=away_standing,
        current_standings=current_standings,
        competition_slug=competition_slug,
        competition_name=competition_name,
        n_league=n_league,
        max_points_remaining=max_points_remaining,
    )

    m5_status, m5_status_reason = _determine_status(
        current_standings=current_standings,
        home_standing=home_standing,
        away_standing=away_standing,
        home_metrics=home_metrics,
        away_metrics=away_metrics,
        n_league=n_league,
        max_points_remaining=max_points_remaining,
    )

    if m5_status != "ACTIVE":
        home_metrics["final_cost"] = 0.0
        away_metrics["final_cost"] = 0.0

    final_cost_home = float(home_metrics["final_cost"])
    final_cost_away = float(away_metrics["final_cost"])

    m5_numerator = final_cost_home - final_cost_away
    m5_denominator = abs(final_cost_home) + abs(final_cost_away)
    if m5_denominator == 0:
        m5_edge = 0.0
    else:
        m5_edge = clamp(m5_numerator / m5_denominator)
    m5_abs_edge = abs(m5_edge)
    m5_pressure_side = _pressure_side_from_edge(m5_edge)
    m5_strength = classify_strength(m5_edge)

    if debug_mode:
        logger.info(
            "--- M5 Contextual Competitive Cost Engine Debug: Event %s (%s) ---",
            event_id,
            participants,
        )
        logger.info(
            "  home_team=%s | away_team=%s | competition=%s | competition_slug=%s | season_id=%s | season_name=%s",
            home_team,
            away_team,
            competition_name,
            competition_slug,
            season_id,
            season_name,
        )
        logger.info(
            "  n_league=%s | total_season_games=%s | games_played_reference=%s | games_remaining=%s | max_points_remaining=%s",
            n_league,
            total_season_games,
            games_played_reference,
            games_remaining,
            max_points_remaining,
        )
        logger.info(
            "  [HOME] points=%s rank=%s objective_type=%s regime_state=%s points_cutoff=%s",
            home_metrics["points"],
            home_metrics["rank"],
            home_metrics["objective_type"],
            home_metrics["regime_state"],
            home_metrics["points_cutoff"],
        )
        logger.info(
            "  [HOME] base_severity=%s regime_multiplier=%s gap=%s urgency=1 - (%s / %s) = %s final_cost=%s * %s * %s = %s",
            home_metrics["base_severity"],
            home_metrics["regime_multiplier"],
            home_metrics["gap"],
            home_metrics["gap_for_urgency"],
            max_points_remaining,
            home_metrics["urgency_factor"],
            home_metrics["base_severity"],
            home_metrics["regime_multiplier"],
            home_metrics["urgency_factor"],
            home_metrics["final_cost"],
        )
        logger.info(
            "  [AWAY] points=%s rank=%s objective_type=%s regime_state=%s points_cutoff=%s",
            away_metrics["points"],
            away_metrics["rank"],
            away_metrics["objective_type"],
            away_metrics["regime_state"],
            away_metrics["points_cutoff"],
        )
        logger.info(
            "  [AWAY] base_severity=%s regime_multiplier=%s gap=%s urgency=1 - (%s / %s) = %s final_cost=%s * %s * %s = %s",
            away_metrics["base_severity"],
            away_metrics["regime_multiplier"],
            away_metrics["gap"],
            away_metrics["gap_for_urgency"],
            max_points_remaining,
            away_metrics["urgency_factor"],
            away_metrics["base_severity"],
            away_metrics["regime_multiplier"],
            away_metrics["urgency_factor"],
            away_metrics["final_cost"],
        )
        logger.info(
            "  [M5_EDGE] numerator=%s denominator=%s division=%s edge=%s pressure_side=%s strength=%s status=%s (%s)",
            m5_numerator,
            m5_denominator,
            0.0 if m5_denominator == 0 else m5_numerator / m5_denominator,
            m5_edge,
            m5_pressure_side,
            m5_strength,
            m5_status,
            m5_status_reason,
        )

    home_component_raw = {
        "objective_type": home_metrics["objective_type"],
        "regime_state": home_metrics["regime_state"],
        "points": home_metrics["points"],
        "rank": home_metrics["rank"],
        "points_cutoff": home_metrics["points_cutoff"],
        "gap": home_metrics["gap"],
        "urgency_factor": home_metrics["urgency_factor"],
        "base_severity": home_metrics["base_severity"],
        "regime_multiplier": home_metrics["regime_multiplier"],
        "final_cost": home_metrics["final_cost"],
    }
    away_component_raw = {
        "objective_type": away_metrics["objective_type"],
        "regime_state": away_metrics["regime_state"],
        "points": away_metrics["points"],
        "rank": away_metrics["rank"],
        "points_cutoff": away_metrics["points_cutoff"],
        "gap": away_metrics["gap"],
        "urgency_factor": away_metrics["urgency_factor"],
        "base_severity": away_metrics["base_severity"],
        "regime_multiplier": away_metrics["regime_multiplier"],
        "final_cost": away_metrics["final_cost"],
    }

    components = [
        _build_component(
            "HOME_COMPETITIVE_COST",
            float(home_metrics["final_cost"]),
            0.50,
            home_component_raw,
        ),
        _build_component(
            "AWAY_COMPETITIVE_COST",
            -float(away_metrics["final_cost"]),
            0.50,
            away_component_raw,
        ),
        _build_component(
            "M5_RELATIVE_EDGE",
            m5_edge,
            1.00,
            {
                "formula": "(FINAL_COST_HOME - FINAL_COST_AWAY) / (abs(FINAL_COST_HOME) + abs(FINAL_COST_AWAY))",
                "numerator": m5_numerator,
                "denominator": m5_denominator,
                "final_cost_home": final_cost_home,
                "final_cost_away": final_cost_away,
            },
        ),
    ]

    raw = {
        "event_id": event_id,
        "participants": participants,
        "home_team": home_team,
        "away_team": away_team,
        "competition_name": competition_name,
        "competition_slug": competition_slug,
        "season_id": season_id,
        "season_name": season_name,
        "n_league": n_league,
        "points_per_win": POINTS_PER_WIN,
        "total_season_games": total_season_games,
        "games_played_reference": games_played_reference,
        "games_remaining": games_remaining,
        "max_points_remaining": max_points_remaining,
        "current_standings_count": n_league,
        "home_team_current_standing": home_standing,
        "away_team_current_standing": away_standing,
        "home_team_current_rank": home_rank_direct,
        "away_team_current_rank": away_rank_direct,
        "current_standings": current_standings,
        "home": _serialize_team_raw(home_metrics),
        "away": _serialize_team_raw(away_metrics),
        "m5_numerator": m5_numerator,
        "m5_denominator": m5_denominator,
        "m5_edge": m5_edge,
        "m5_abs_edge": m5_abs_edge,
        "m5_pressure_side": m5_pressure_side,
        "m5_strength": m5_strength,
        "m5_status": m5_status,
        "m5_status_reason": m5_status_reason,
        "formula": {
            "urgency_factor": "1 - (GAP / MAX_POINTS_REMAINING)",
            "final_cost": "BASE_SEVERITY * REGIME_MULTIPLIER * URGENCY_FACTOR",
            "m5_edge": "(FINAL_COST_HOME - FINAL_COST_AWAY) / (abs(FINAL_COST_HOME) + abs(FINAL_COST_AWAY))",
        },
        "sealed_rules": [
            "M5 measures contextual competitive cost.",
            "M5 does not measure emotional motivation.",
            "M5 does not measure team strength.",
            "M5 does not use PPG.",
            "M5 does not use goal difference.",
            "M5 does not use recent form.",
            "The cutoff is defined by points, not only ranking.",
            "FINAL_COST = BASE_SEVERITY * REGIME_MULTIPLIER * URGENCY_FACTOR.",
            "M5_EDGE uses the relative formula.",
            "M5 emits M5_PRESSURE_SIDE, not M5_BIAS.",
            "The side with greater FINAL_COST has greater measurable competitive pressure.",
        ],
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M5",
        module_name="Contextual Competitive Cost Engine",
        event_id=event_id,
        participants=participants,
        value=m5_edge if m5_status == "ACTIVE" else 0.0,
        bias=m5_pressure_side,
        strength=m5_strength,
        components=components,
        raw=raw,
    )
