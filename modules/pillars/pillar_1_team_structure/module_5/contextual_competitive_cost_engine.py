"""M5 - Contextual Competitive Cost Engine.

This module measures how expensive it would be competitively not to add points
in the current match. It does not use recent form, goal difference, odds, H2H,
PPG, streaks, or market signals.
"""

from __future__ import annotations

import logging
import math
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


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== M5_COMPETITIVE_COST_ENGINE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("M5_COMPETITIVE_COST_ENGINE DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("M5_COMPETITIVE_COST_ENGINE DEBUG | %s", name)
    logger.info("M5_COMPETITIVE_COST_ENGINE DEBUG |   Formula: %s", formula)
    logger.info("M5_COMPETITIVE_COST_ENGINE DEBUG |   Sustitución: %s", substitution)
    logger.info("M5_COMPETITIVE_COST_ENGINE DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("M5_COMPETITIVE_COST_ENGINE DEBUG |   Lectura: %s", meaning)


def _fmt(value: Any, decimals: int = 6) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isfinite(value):
            return f"{value:.{decimals}f}"
        return str(value)
    if isinstance(value, dict):
        items = list(value.items())
        preview = ", ".join(f"{k}: {_fmt(v, decimals)}" for k, v in items[:4])
        if len(items) > 4:
            preview += ", ..."
        return f"{{{preview}}} (n={len(items)})"
    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        preview = ", ".join(_fmt(item, decimals) for item in sequence[:5])
        if len(sequence) > 5:
            preview += ", ..."
        return f"[{preview}] (n={len(sequence)})"
    return str(value)


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
    debug_mode: bool = False,
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

    if debug_mode:
        _debug_section(f"Métricas del equipo: {team_label} ({team_name})")
        _debug_line("Datos de la tabla: rank=%s, puntos=%s, partidos_jugados=%s", _fmt(rank), _fmt(points), _fmt(games_played))
        _debug_line("Objetivo y Régimen: objective_type=%s, regime_state=%s", _fmt(objective_type), _fmt(regime_state))
        _debug_line("Severidad Base (BASE_SEVERITY): %s", _fmt(base_severity))
        _debug_line("Multiplicador de Régimen (REGIME_MULTIPLIER): %s", _fmt(regime_multiplier))

        if objective_type == "EUROPE":
            target_rank = EUROPE_CUTOFF_RANK
            if rank == EUROPE_CUTOFF_RANK:
                target_rank = EUROPE_CUTOFF_RANK + 1
            _debug_line("Determinación del cutoff para EUROPE:")
            ref_standing = _find_standing_by_rank(current_standings, target_rank)
            if ref_standing is None or _extract_points(ref_standing) is None:
                ref_standing = _find_nearest_standing_with_points(current_standings, target_rank)
            ref_name = _extract_record_name(ref_standing)
            ref_rank_val = _extract_rank(ref_standing)
            ref_pts_val = _extract_points(ref_standing)
            ref_gp_val = _extract_games_played(ref_standing)
            _debug_line("  Rival de referencia (target_rank=%d): equipo=%s | rank=%s | puntos=%s | partidos_jugados=%s", target_rank, _fmt(ref_name), _fmt(ref_rank_val), _fmt(ref_pts_val), _fmt(ref_gp_val))
            _debug_line("  Puntos de cutoff obtenidos: %s", _fmt(points_cutoff))
        elif objective_type == "SURVIVAL":
            if n_league is not None:
                target_rank = max(1, n_league - SURVIVAL_DANGER_PLACES)
                _debug_line("Determinación del cutoff para SURVIVAL:")
                ref_standing = _find_standing_by_rank(current_standings, target_rank)
                if ref_standing is None or _extract_points(ref_standing) is None:
                    ref_standing = _find_nearest_standing_with_points(current_standings, target_rank)
                ref_name = _extract_record_name(ref_standing)
                ref_rank_val = _extract_rank(ref_standing)
                ref_pts_val = _extract_points(ref_standing)
                ref_gp_val = _extract_games_played(ref_standing)
                _debug_line("  Rival de referencia (target_rank=%d): equipo=%s | rank=%s | puntos=%s | partidos_jugados=%s", target_rank, _fmt(ref_name), _fmt(ref_rank_val), _fmt(ref_pts_val), _fmt(ref_gp_val))
                _debug_line("  Puntos de cutoff obtenidos: %s", _fmt(points_cutoff))
        elif objective_type == "MIDTABLE":
            _debug_line("Determinación del cutoff para MIDTABLE: cutoff = team_points = %s", _fmt(points_cutoff))
        else:
            _debug_line("Determinación del cutoff para NONE: cutoff = None")

        # Gap calculation formula and substitution
        if objective_type == "EUROPE":
            if regime_state == "ON_TARGET_ZONE":
                formula_str = "team_points - points_cutoff"
                sub_str = f"{_fmt(points)} - {_fmt(points_cutoff)}"
            else:
                formula_str = "points_cutoff - team_points"
                sub_str = f"{_fmt(points_cutoff)} - {_fmt(points)}"
            _debug_formula("GAP_EUROPE", formula_str, sub_str, _fmt(gap), "Diferencia de puntos respecto a la zona europea")
        elif objective_type == "SURVIVAL":
            if regime_state == "INSIDE_DANGER":
                formula_str = "points_cutoff - team_points"
                sub_str = f"{_fmt(points_cutoff)} - {_fmt(points)}"
            else:
                formula_str = "team_points - points_cutoff"
                sub_str = f"{_fmt(points)} - {_fmt(points_cutoff)}"
            _debug_formula("GAP_SURVIVAL", formula_str, sub_str, _fmt(gap), "Diferencia de puntos respecto a la zona de permanencia")
        elif objective_type in {"MIDTABLE", "NONE"}:
            formula_str = "max_points_remaining"
            sub_str = f"{_fmt(max_points_remaining)}"
            _debug_formula("GAP_MIDTABLE_OR_NONE", formula_str, sub_str, _fmt(gap), "Diferencia por defecto para media tabla o sin objetivo")
        else:
            _debug_line("GAP: No aplica para objective_type=%s", _fmt(objective_type))

        # Urgency Factor
        _debug_formula(
            "URGENCY_FACTOR",
            "clamp(1.0 - (max(gap, 0.0) / max_points_remaining), 0.0, 1.0)",
            f"clamp(1.0 - (max({_fmt(gap)}, 0.0) / {_fmt(max_points_remaining)}), 0.0, 1.0)",
            _fmt(urgency_factor),
            f"Factor de urgencia con gap corregido para urgencia {_fmt(gap_for_urgency)} y raw={_fmt(urgency_raw)}"
        )

        # Final Competitive Cost
        _debug_formula(
            "FINAL_COMPETITIVE_COST",
            "base_severity * regime_multiplier * urgency_factor",
            f"{_fmt(base_severity)} * {_fmt(regime_multiplier)} * {_fmt(urgency_factor)}",
            _fmt(final_cost),
            "Costo competitivo final para el equipo"
        )

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

    if debug_mode:
        _debug_section("Propósito del módulo")
        _debug_line("M5 mide qué tan costoso sería competitivamente no sumar puntos en el partido actual.")
        _debug_line("No utiliza forma reciente, diferencia de goles, cuotas, H2H, PPG o rachas.")

        _debug_section("Parámetros Globales y Constantes")
        _debug_line("POINTS_PER_WIN: %s", _fmt(POINTS_PER_WIN))
        _debug_line("EUROPE_CUTOFF_RANK: %s", _fmt(EUROPE_CUTOFF_RANK))
        _debug_line("SURVIVAL_DANGER_PLACES: %s", _fmt(SURVIVAL_DANGER_PLACES))
        _debug_line("BASE_SEVERITY: %s", _fmt(BASE_SEVERITY))
        _debug_line("REGIME_MULTIPLIER: %s", _fmt(REGIME_MULTIPLIER))

        _debug_section("Datos del Evento")
        _debug_line("Event ID: %s", _fmt(event_id))
        _debug_line("Participantes: %s", _fmt(participants))
        _debug_line("Equipo Local (Home): %s", _fmt(home_team))
        _debug_line("Equipo Visitante (Away): %s", _fmt(away_team))
        _debug_line("Competición: %s (slug: %s)", _fmt(competition_name), _fmt(competition_slug))
        _debug_line("Temporada: %s (ID: %s)", _fmt(season_name), _fmt(season_id))
        _debug_line("Tamaño de la liga (n_league): %s", _fmt(n_league))
        _debug_line("Partidos totales de temporada (total_season_games): %s", _fmt(total_season_games))
        _debug_line("Partidos jugados de referencia (games_played_reference): %s", _fmt(games_played_reference))
        _debug_line("Partidos restantes (games_remaining): %s", _fmt(games_remaining))
        _debug_line("Puntos máximos restantes (max_points_remaining): %s", _fmt(max_points_remaining))

        valid_records = []
        for _, record in _standings_items(current_standings):
            if not isinstance(record, dict):
                continue
            rank = _extract_rank(record)
            points = _extract_points(record)
            if rank is not None and points is not None:
                name = _extract_record_name(record) or "Unknown"
                gp = _extract_games_played(record)
                gp_str = str(gp) if gp is not None else "N/A"
                valid_records.append((rank, name, points, gp_str))

        if valid_records:
            valid_records.sort(key=lambda x: x[0])
            _debug_section("Tabla de Posiciones (Standings pre-partido)")
            _debug_line(f"{'Pos':<3} | {'Equipo':<25} | {'Pts':<5} | {'PJ':<3}")
            _debug_line("-" * 45)
            for rank, name, points, gp_str in valid_records:
                pts_str = f"{points:.1f}" if points % 1 != 0 else f"{int(points)}"
                disp_name = name[:25]
                _debug_line(f"{rank:<3} | {disp_name:<25} | {pts_str:<5} | {gp_str:<3}")

    home_metrics = _build_team_metrics(
        team_label="HOME",
        team_name=home_team,
        standing=home_standing,
        current_standings=current_standings,
        competition_slug=competition_slug,
        competition_name=competition_name,
        n_league=n_league,
        max_points_remaining=max_points_remaining,
        debug_mode=debug_mode,
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
        debug_mode=debug_mode,
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

    if debug_mode:
        _debug_section("Validación de Estado")
        _debug_line("Estado M5 obtenido: %s (razón: %s)", _fmt(m5_status), _fmt(m5_status_reason))

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
        _debug_section("Cálculo de Relación y Ventaja (M5_EDGE)")
        _debug_line("Costo Local (final_cost_home): %s", _fmt(final_cost_home))
        _debug_line("Costo Visitante (final_cost_away): %s", _fmt(final_cost_away))

        # formula for relative edge
        _debug_formula(
            "M5_RELATIVE_EDGE",
            "(final_cost_home - final_cost_away) / (abs(final_cost_home) + abs(final_cost_away))",
            f"({_fmt(final_cost_home)} - {_fmt(final_cost_away)}) / (abs({_fmt(final_cost_home)}) + abs({_fmt(final_cost_away)}))",
            _fmt(m5_edge),
            f"Diferencia de costo competitivo relativo, bias hacia {m5_pressure_side} con fuerza {m5_strength}"
        )

        _debug_section("Resumen del Output")
        _debug_line("M5_EDGE: %s", _fmt(m5_edge))
        _debug_line("M5_PRESSURE_SIDE: %s", _fmt(m5_pressure_side))
        _debug_line("M5_STRENGTH: %s", _fmt(m5_strength))
        _debug_line("M5_STATUS: %s (%s)", _fmt(m5_status), _fmt(m5_status_reason))
        _debug_line("Componentes:")
        _debug_line("  - HOME_COMPETITIVE_COST: edge=%s, weight=0.50", _fmt(final_cost_home))
        _debug_line("  - AWAY_COMPETITIVE_COST: edge=%s, weight=0.50", _fmt(-final_cost_away))
        _debug_line("  - M5_RELATIVE_EDGE: edge=%s, weight=1.00", _fmt(m5_edge))

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
