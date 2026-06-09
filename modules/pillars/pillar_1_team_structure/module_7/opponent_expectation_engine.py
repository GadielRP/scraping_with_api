"""M7 - Opponent Expectation Engine.

Measures team performance against opponent difficulty rather than the result
alone. Positive values favor HOME; negative values favor AWAY.
"""

from __future__ import annotations

import datetime
import logging
import math
from typing import Any, Dict, List, Optional, Tuple

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
)
from modules.pillars.context import EventContext
from modules.pillars.score_series import extract_score_for_against

logger = logging.getLogger(__name__)

_MAX_RANK_DEFAULT = 20
_INITIAL_SEASON_GAMES_TO_SKIP = 1

_ROE_WEIGHT = 0.60
_GDOE_WEIGHT = 0.40

_EXPECTED_GD_FACTOR = 1.5
_GDOE_DIVISOR = 5.0

_MIN_VALID_GAMES_ACTIVE = 10
_MIN_VALID_GAMES_DEGRADED = 5

_M7_STRENGTH_THRESHOLDS = [
    (0.05, "VERY_LOW"),
    (0.15, "LOW"),
    (0.30, "MEDIUM"),
    (0.50, "HIGH"),
]

_M7_STRENGTH_MAX_LABEL = "VERY_HIGH"

_OPPONENT_NAME_KEYS = (
    "opponent_name",
    "opponent_team_name",
    "rival_name",
    "rival_team_name",
    "opponent",
)


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== M7_OPPONENT_EXPECTATION_ENGINE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("M7_OPPONENT_EXPECTATION_ENGINE DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("M7_OPPONENT_EXPECTATION_ENGINE DEBUG | %s", name)
    logger.info("M7_OPPONENT_EXPECTATION_ENGINE DEBUG |   Formula: %s", formula)
    logger.info("M7_OPPONENT_EXPECTATION_ENGINE DEBUG |   Sustitución: %s", substitution)
    logger.info("M7_OPPONENT_EXPECTATION_ENGINE DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("M7_OPPONENT_EXPECTATION_ENGINE DEBUG |   Lectura: %s", meaning)


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
        preview = ", ".join(f"{k}: {_fmt(v, decimals)}" for k, v in items)
        return f"{{{preview}}} (n={len(items)})"
    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        preview = ", ".join(_fmt(item, decimals) for item in sequence)
        return f"[{preview}] (n={len(sequence)})"
    return str(value)


def _format_timestamp(ts: Any) -> str:
    if ts is None:
        return "N/A"
    try:
        val = float(ts)
        if val > 0:
            dt = datetime.datetime.fromtimestamp(val, tz=datetime.timezone.utc)
            return dt.strftime("%y-%m-%d")
    except Exception:
        pass
    return str(ts)


def _coerce_float(value: Any) -> Optional[float]:
    """Convert values to float while treating invalid values as missing."""
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(coerced) or math.isinf(coerced):
        return None
    return coerced


def _coerce_int(value: Any) -> Optional[int]:
    """Convert values to int, accepting numeric strings and floats like 16.0."""
    float_value = _coerce_float(value)
    if float_value is None:
        return None
    int_value = int(float_value)
    if not math.isclose(float_value, float(int_value), abs_tol=1e-9):
        return None
    return int_value


def _safe_mean(values: List[float]) -> float:
    """Return the arithmetic mean, or 0.0 when the input is empty."""
    if not values:
        return 0.0
    return sum(values) / float(len(values))


def _population_std(values: List[float]) -> float:
    """Return population standard deviation for a numeric series."""
    if len(values) <= 1:
        return 0.0
    mean_value = _safe_mean(values)
    variance = sum((value - mean_value) ** 2 for value in values) / float(len(values))
    return math.sqrt(variance)


def _clamp(value: float, min_value: float = -1.0, max_value: float = 1.0) -> float:
    """Module-local wrapper around the shared clamp helper."""
    return clamp(value, min_value, max_value)


def _standings_items(standings: Any) -> List[Any]:
    if isinstance(standings, dict):
        return list(standings.values())
    if isinstance(standings, list):
        return standings
    return []


def _extract_timestamp_value(game: Dict[str, Any]) -> Optional[float]:
    for key in ("startTimestamp", "start_timestamp", "timestamp", "date"):
        value = _coerce_float(game.get(key))
        if value is not None:
            return value
    return None


def _extract_rank_from_record(record: Any) -> Optional[int]:
    direct_rank = _coerce_int(record)
    if direct_rank is not None and direct_rank > 0:
        return direct_rank
    if not isinstance(record, dict):
        return None
    for key in (
        "rank",
        "position",
        "current_rank",
        "standing_rank",
        "ranking",
        "final_real_ranking",
        "final_real_rank",
        "real_ranking",
        "sofascore_rank",
    ):
        rank = _coerce_int(record.get(key))
        if rank is not None and rank > 0:
            return rank
    return None


def _max_rank_from_standings(standings: Any) -> Optional[int]:
    ranks = [
        rank
        for rank in (_extract_rank_from_record(item) for item in _standings_items(standings))
        if rank is not None and rank > 0
    ]
    if not ranks:
        return None
    return max(ranks)


def _resolve_max_rank(streak_analysis: Any, event_context: EventContext, debug_mode: bool = False) -> Tuple[int, str]:
    """Resolve competition size using the most specific available source."""
    competition = getattr(event_context, "competition", None)
    number_of_teams = _coerce_int(getattr(competition, "number_of_teams", None))
    if debug_mode:
        _debug_line("Intento 1: number_of_teams de event_context.competition = %s", _fmt(number_of_teams))
    if number_of_teams is not None and number_of_teams > 1:
        return number_of_teams, "event_context.competition.number_of_teams"

    current_standings = getattr(streak_analysis, "current_standings", None)
    if debug_mode:
        _debug_line("Intento 2: len de current_standings = %s", _fmt(len(current_standings) if isinstance(current_standings, dict) else None))
    if isinstance(current_standings, dict) and len(current_standings) > 1:
        return len(current_standings), "streak_analysis.current_standings.len"

    standings_response = getattr(streak_analysis, "standings_response", None)
    if debug_mode:
        _debug_line("Intento 3: len de standings_response = %s", _fmt(len(standings_response) if isinstance(standings_response, list) else None))
    if isinstance(standings_response, list) and len(standings_response) > 1:
        return len(standings_response), "streak_analysis.standings_response.len"

    max_rank = _max_rank_from_standings(current_standings)
    if debug_mode:
        _debug_line("Intento 4: max_rank de current_standings = %s", _fmt(max_rank))
    if max_rank is not None and max_rank > 1:
        return max_rank, "streak_analysis.current_standings.max_rank"

    max_rank = _max_rank_from_standings(standings_response)
    if debug_mode:
        _debug_line("Intento 5: max_rank de standings_response = %s", _fmt(max_rank))
    if max_rank is not None and max_rank > 1:
        return max_rank, "streak_analysis.standings_response.max_rank"

    if debug_mode:
        _debug_line("Intento 6: Usando fallback predeterminado = %d", _MAX_RANK_DEFAULT)
    return _MAX_RANK_DEFAULT, "max_rank_default"


def _extract_opponent_name(game: Dict[str, Any]) -> Optional[str]:
    for key in _OPPONENT_NAME_KEYS:
        value = game.get(key)
        if isinstance(value, dict):
            for nested_key in ("name", "teamName", "team_name", "short_name"):
                nested_value = value.get(nested_key)
                if nested_value is not None and str(nested_value).strip():
                    return str(nested_value).strip()
            continue
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _extract_own_rank(game: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    rank = _coerce_int(game.get("own_ranking"))
    if rank is not None and rank > 0:
        return rank, "game.own_ranking"

    team_standing = game.get("team_standing")
    if isinstance(team_standing, dict):
        rank = _coerce_int(team_standing.get("rank"))
        if rank is not None and rank > 0:
            return rank, "game.team_standing.rank"
        rank = _coerce_int(team_standing.get("position"))
        if rank is not None and rank > 0:
            return rank, "game.team_standing.position"

    return None, None


def _normalize_team_name(value: Any) -> str:
    return str(value).strip().casefold()


def _standing_name_matches(standing_key: Any, standing_value: Any, opponent_name: str) -> bool:
    normalized_opponent = _normalize_team_name(opponent_name)
    if normalized_opponent and _normalize_team_name(standing_key) == normalized_opponent:
        return True
    if not isinstance(standing_value, dict):
        return False
    for name_key in ("name", "teamName", "team_name", "short_name", "display_name"):
        name_value = standing_value.get(name_key)
        if name_value is not None and _normalize_team_name(name_value) == normalized_opponent:
            return True
    return False


def _filter_initial_season_games(
    results: List[Dict[str, Any]],
    initial_games_to_skip: int = _INITIAL_SEASON_GAMES_TO_SKIP,
    team_label: str = "",
    debug_mode: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if initial_games_to_skip <= 0:
        return results, []

    indexed_games: List[Tuple[int, Dict[str, Any], Optional[float]]] = [
        (index, game, _extract_timestamp_value(game) if isinstance(game, dict) else None)
        for index, game in enumerate(results)
    ]

    sortable_games = sorted(
        indexed_games,
        key=lambda item: (
            0 if item[2] is not None else 1,
            item[2] if item[2] is not None else 0.0,
            -item[0],
        ),
    )
    filtered_indices = {index for index, _, _ in sortable_games[:initial_games_to_skip]}

    filtered_results: List[Dict[str, Any]] = []
    filtered_out_games: List[Dict[str, Any]] = []

    for index, game in enumerate(results):
        if index in filtered_indices:
            opp_name = _extract_opponent_name(game) if isinstance(game, dict) else None
            ts = _extract_timestamp_value(game) if isinstance(game, dict) else None
            filtered_out_games.append(
                {
                    "original_index": index,
                    "event_id": game.get("event_id") if isinstance(game, dict) else None,
                    "opponent_name": opp_name,
                    "startTimestamp": ts,
                    "reason": "initial_season_games_filter",
                }
            )
            if debug_mode:
                _debug_line("  [%s] Encuentro inicial OMITIDO (filtro de inicio de temporada): index=%d, event_id=%s, rival=%s, timestamp=%s",
                            team_label, index, _fmt(game.get("event_id") if isinstance(game, dict) else None), _fmt(opp_name), _format_timestamp(ts))
        else:
            filtered_results.append(game)

    return filtered_results, filtered_out_games


def _extract_rank_from_current_standings(
    opponent_name: Optional[str],
    current_standings: Optional[Dict[str, Dict[str, Any]]],
) -> Optional[int]:
    if not opponent_name or not isinstance(current_standings, dict):
        return None

    direct_record = current_standings.get(opponent_name)
    direct_rank = _extract_rank_from_record(direct_record)
    if direct_rank is not None:
        return direct_rank

    for key, value in current_standings.items():
        if _standing_name_matches(key, value, opponent_name):
            rank = _extract_rank_from_record(value)
            if rank is not None:
                return rank
    return None


def _extract_opponent_rank(
    game: Dict[str, Any],
    current_standings: Optional[Dict[str, Dict[str, Any]]] = None,
    max_rank: int = _MAX_RANK_DEFAULT,
    team_label: str = "",
    game_index: int = 0,
    debug_mode: bool = False,
) -> Tuple[Optional[int], Optional[str]]:
    """Extract opponent ranking from game payload or standings by rival name."""
    rank = _coerce_int(game.get("opponent_ranking"))
    if rank is not None and 1 <= rank <= max_rank:
        if debug_mode:
            _debug_line("    [%s][%d] Rank del rival encontrado en: game.opponent_ranking = %d", team_label, game_index, rank)
        return rank, "game.opponent_ranking"

    opponent_standing = game.get("opponent_standing")
    if isinstance(opponent_standing, dict):
        rank = _coerce_int(opponent_standing.get("rank"))
        if rank is not None and 1 <= rank <= max_rank:
            if debug_mode:
                _debug_line("    [%s][%d] Rank del rival encontrado en: game.opponent_standing.rank = %d", team_label, game_index, rank)
            return rank, "game.opponent_standing.rank"
        rank = _coerce_int(opponent_standing.get("position"))
        if rank is not None and 1 <= rank <= max_rank:
            if debug_mode:
                _debug_line("    [%s][%d] Rank del rival encontrado en: game.opponent_standing.position = %d", team_label, game_index, rank)
            return rank, "game.opponent_standing.position"

    for key in (
        "opponent_rank",
        "opponent_current_rank",
        "opponent_standing_rank",
        "opponent_position",
        "opponent_final_real_ranking",
        "opponent_final_real_rank",
        "opponent_real_ranking",
        "opponent_sofascore_rank",
        "rival_rank",
        "rival_position",
    ):
        rank = _coerce_int(game.get(key))
        if rank is not None and 1 <= rank <= max_rank:
            if debug_mode:
                _debug_line("    [%s][%d] Rank del rival encontrado en: game.%s = %d", team_label, game_index, key, rank)
            return rank, f"game.{key}"

    opponent_name = _extract_opponent_name(game)
    standing_rank = _extract_rank_from_current_standings(opponent_name, current_standings)
    if standing_rank is not None and 1 <= standing_rank <= max_rank:
        if debug_mode:
            _debug_line("    [%s][%d] Rank del rival encontrado buscando por nombre '%s' en standings: %d", team_label, game_index, opponent_name, standing_rank)
        return standing_rank, "current_standings.by_name.rank"
    
    if debug_mode:
        _debug_line("    [%s][%d] Rank del rival NO pudo ser resuelto para '%s'", team_label, game_index, opponent_name)
    return None, None


def _calculate_opponent_strength(opponent_rank: int, max_rank: int) -> Optional[float]:
    if max_rank <= 1:
        max_rank = _MAX_RANK_DEFAULT
    if opponent_rank < 1 or opponent_rank > max_rank:
        return None
    strength = (max_rank - opponent_rank) / float(max_rank - 1)
    return _clamp(strength, 0.0, 1.0)


def _calculate_expected_result(opp_strength: float) -> float:
    return _clamp(1.0 - (2.0 * opp_strength))


def _calculate_real_result(game_gd: float) -> int:
    if game_gd > 0:
        return 1
    if game_gd < 0:
        return -1
    return 0


def _calculate_roe(real_result: float, expected_result: float) -> float:
    return _clamp((real_result - expected_result) / 2.0)


def _calculate_expected_gd(expected_result: float) -> float:
    return expected_result * _EXPECTED_GD_FACTOR


def _calculate_gdoe(game_gd: float, expected_gd: float) -> float:
    return _clamp((game_gd - expected_gd) / _GDOE_DIVISOR)


def _calculate_context_score(roe: float, gdoe: float) -> float:
    score = (_ROE_WEIGHT * roe) + (_GDOE_WEIGHT * gdoe)
    return _clamp(score)


def _classify_game_context_score(score: float) -> str:
    if score >= 0.50:
        return "ELITE_OVERPERFORMANCE"
    if score >= 0.30:
        return "STRONG_OVERPERFORMANCE"
    if score >= 0.15:
        return "MODERATE_OVERPERFORMANCE"
    if score >= 0.05:
        return "LIGHT_OVERPERFORMANCE"
    if score > -0.05:
        return "NEUTRAL_EXPECTATION"
    if score > -0.15:
        return "LIGHT_UNDERPERFORMANCE"
    if score > -0.30:
        return "MODERATE_UNDERPERFORMANCE"
    if score > -0.50:
        return "STRONG_UNDERPERFORMANCE"
    return "SEVERE_CONTEXTUAL_COLLAPSE"


def _classify_m7_strength(edge: float) -> str:
    abs_edge = abs(edge)
    for threshold, label in _M7_STRENGTH_THRESHOLDS:
        if abs_edge < threshold:
            return label
    return _M7_STRENGTH_MAX_LABEL


def _skipped_game(index: int, team_label: str, reason: str, game: Any) -> Dict[str, Any]:
    opponent_name = _extract_opponent_name(game) if isinstance(game, dict) else None
    return {
        "index": index,
        "team_label": team_label,
        "opponent_name": opponent_name,
        "reason": reason,
    }


def _build_team_context_series(
    results: List[Dict[str, Any]],
    current_standings: Optional[Dict[str, Dict[str, Any]]],
    max_rank: int,
    team_label: str,
    debug_mode: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    valid_games: List[Dict[str, Any]] = []
    skipped_games: List[Dict[str, Any]] = []

    if debug_mode:
        _debug_section(f"Procesamiento de la Serie Contextual ({team_label})")
        _debug_line("building context series: total_games=%s max_rank=%s", len(results), max_rank)

    for index, game in enumerate(results):
        opp_name = _extract_opponent_name(game) if isinstance(game, dict) else "Desconocido"
        ts = _extract_timestamp_value(game) if isinstance(game, dict) else None
        
        if debug_mode:
            _debug_line("  [%s] Analizando partido %d: rival=%s, timestamp=%s", team_label, index + 1, opp_name, _format_timestamp(ts))

        if not isinstance(game, dict):
            skipped_games.append(_skipped_game(index, team_label, "not_a_dict", game))
            if debug_mode:
                _debug_line("    -> Omitido: el partido no es un diccionario")
            continue

        score = extract_score_for_against(game)
        if score is None:
            skipped_games.append(_skipped_game(index, team_label, "missing_score", game))
            if debug_mode:
                _debug_line("    -> Omitido: marcador no interpretable (missing_score)")
            continue

        goals_for, goals_against = score
        game_gd = goals_for - goals_against
        
        opponent_rank, opponent_rank_source = _extract_opponent_rank(
            game, current_standings, max_rank, team_label, index + 1, debug_mode
        )
        if opponent_rank is None:
            skipped_games.append(_skipped_game(index, team_label, "missing_opponent_rank", game))
            if debug_mode:
                _debug_line("    -> Omitido: ranking del rival no encontrado (missing_opponent_rank)")
            continue

        opp_strength = _calculate_opponent_strength(opponent_rank, max_rank)
        if opp_strength is None:
            skipped_games.append(_skipped_game(index, team_label, "invalid_opponent_rank", game))
            if debug_mode:
                _debug_line("    -> Omitido: ranking del rival inválido o fuera de rango (invalid_opponent_rank, rank=%s, max_rank=%s)", _fmt(opponent_rank), _fmt(max_rank))
            continue

        own_rank, own_rank_source = _extract_own_rank(game)
        expected_result = _calculate_expected_result(opp_strength)
        real_result = _calculate_real_result(game_gd)
        roe = _calculate_roe(float(real_result), expected_result)
        expected_gd = _calculate_expected_gd(expected_result)
        gdoe = _calculate_gdoe(game_gd, expected_gd)
        game_context_score = _calculate_context_score(roe, gdoe)
        classification = _classify_game_context_score(game_context_score)

        if debug_mode:
            _debug_line("    Marcador: goles_favor=%s, goles_contra=%s -> game_gd=%s", _fmt(goals_for), _fmt(goals_against), _fmt(game_gd))
            _debug_formula(
                f"OPP_STRENGTH_{team_label}_{index+1}",
                "(max_rank - opponent_rank) / (max_rank - 1)",
                f"({max_rank} - {opponent_rank}) / ({max_rank} - 1)",
                _fmt(opp_strength),
                "Fuerza normalizada del oponente"
            )
            _debug_formula(
                f"EXPECTED_RESULT_{team_label}_{index+1}",
                "1.0 - (2.0 * opp_strength)",
                f"1.0 - (2.0 * {_fmt(opp_strength)})",
                _fmt(expected_result),
                "Resultado esperado"
            )
            _debug_formula(
                f"REAL_RESULT_{team_label}_{index+1}",
                "sign(game_gd)",
                f"sign({_fmt(game_gd)})",
                _fmt(real_result),
                "Resultado real obtenido (ganó=1, empató=0, perdió=-1)"
            )
            _debug_formula(
                f"ROE_{team_label}_{index+1}",
                "clamp((real_result - expected_result) / 2.0)",
                f"clamp(({_fmt(real_result)} - {_fmt(expected_result)}) / 2.0)",
                _fmt(roe),
                "Result Over Expectation (Rendimiento sobre expectativa de resultado)"
            )
            _debug_formula(
                f"EXPECTED_GD_{team_label}_{index+1}",
                "expected_result * 1.5",
                f"{_fmt(expected_result)} * 1.5",
                _fmt(expected_gd),
                "Diferencia de goles esperada"
            )
            _debug_formula(
                f"GDOE_{team_label}_{index+1}",
                "clamp((game_gd - expected_gd) / 5.0)",
                f"clamp(({_fmt(game_gd)} - {_fmt(expected_gd)}) / 5.0)",
                _fmt(gdoe),
                "Goal Difference Over Expectation (Diferencia de goles sobre expectativa)"
            )
            _debug_formula(
                f"GAME_CONTEXT_SCORE_{team_label}_{index+1}",
                "(0.60 * roe) + (0.40 * gdoe)",
                f"(0.60 * {_fmt(roe)}) + (0.40 * {_fmt(gdoe)})",
                _fmt(game_context_score),
                f"Puntuación de contexto del partido: {classification}"
            )

        valid_game = {
            "index": index,
            "team_label": team_label,
            "opponent_name": opp_name,
            "opponent_rank": opponent_rank,
            "opponent_rank_source": opponent_rank_source,
            "own_rank": own_rank,
            "own_rank_source": own_rank_source,
            "max_rank": max_rank,
            "goals_for": goals_for,
            "goals_against": goals_against,
            "game_gd": game_gd,
            "opp_strength": opp_strength,
            "expected_result": expected_result,
            "real_result": real_result,
            "roe": roe,
            "expected_gd": expected_gd,
            "gdoe": gdoe,
            "game_context_score": game_context_score,
            "context_classification": classification,
        }
        valid_games.append(valid_game)

    if debug_mode:
        _debug_line("Serie contextual completada para [%s]: valid_games=%d, skipped_games=%d",
                    team_label, len(valid_games), len(skipped_games))

    return valid_games, skipped_games


def _aggregate_team_context(series: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not series:
        return {
            "team_context_score": 0.0,
            "avg_roe": 0.0,
            "avg_gdoe": 0.0,
            "ceiling": 0.0,
            "floor": 0.0,
            "positives": 0,
            "negatives": 0,
            "neutrals": 0,
            "volatility": 0.0,
            "games_valid": 0,
        }

    scores = [float(game["game_context_score"]) for game in series]
    roes = [float(game["roe"]) for game in series]
    gdoes = [float(game["gdoe"]) for game in series]
    neutral_epsilon = 1e-9

    return {
        "team_context_score": _safe_mean(scores),
        "avg_roe": _safe_mean(roes),
        "avg_gdoe": _safe_mean(gdoes),
        "ceiling": max(scores),
        "floor": min(scores),
        "positives": sum(1 for score in scores if score > neutral_epsilon),
        "negatives": sum(1 for score in scores if score < -neutral_epsilon),
        "neutrals": sum(1 for score in scores if abs(score) < neutral_epsilon),
        "volatility": _population_std(scores),
        "games_valid": len(series),
    }


def _determine_status(home_valid_count: int, away_valid_count: int) -> Tuple[str, str]:
    if home_valid_count == 0 or away_valid_count == 0:
        return "INSUFFICIENT_DATA", "missing_valid_context_games_for_one_or_both_teams"
    if min(home_valid_count, away_valid_count) < _MIN_VALID_GAMES_DEGRADED:
        return "INSUFFICIENT_DATA", "valid_context_sample_below_minimum"
    if min(home_valid_count, away_valid_count) < _MIN_VALID_GAMES_ACTIVE:
        return "DEGRADED", "partial_context_sample"
    return "ACTIVE", "active"


def calculate_m7_opponent_expectation_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    event_id = getattr(streak_analysis, "event_id", 0)
    participants = getattr(streak_analysis, "participants", "") or ""
    home_team = getattr(streak_analysis, "home_team_name", None)
    away_team = getattr(streak_analysis, "away_team_name", None)
    home_results = getattr(streak_analysis, "home_team_results", None) or []
    away_results = getattr(streak_analysis, "away_team_results", None) or []
    current_standings = getattr(streak_analysis, "current_standings", None)
    initial_games_to_skip = _INITIAL_SEASON_GAMES_TO_SKIP

    if debug_mode:
        _debug_section("Propósito del módulo")
        _debug_line("M7 mide el rendimiento del equipo contra la dificultad de los oponentes.")
        _debug_line("Valores positivos favorecen a HOME; valores negativos favorecen a AWAY.")

        _debug_section("Parámetros Globales y Constantes")
        _debug_line("MAX_RANK_DEFAULT: %s", _fmt(_MAX_RANK_DEFAULT))
        _debug_line("INITIAL_SEASON_GAMES_TO_SKIP: %s", _fmt(_INITIAL_SEASON_GAMES_TO_SKIP))
        _debug_line("ROE_WEIGHT: %s", _fmt(_ROE_WEIGHT))
        _debug_line("GDOE_WEIGHT: %s", _fmt(_GDOE_WEIGHT))
        _debug_line("EXPECTED_GD_FACTOR: %s", _fmt(_EXPECTED_GD_FACTOR))
        _debug_line("GDOE_DIVISOR: %s", _fmt(_GDOE_DIVISOR))
        _debug_line("MIN_VALID_GAMES_ACTIVE: %s", _fmt(_MIN_VALID_GAMES_ACTIVE))
        _debug_line("MIN_VALID_GAMES_DEGRADED: %s", _fmt(_MIN_VALID_GAMES_DEGRADED))

        _debug_section("Datos del Evento")
        _debug_line("Event ID: %s", _fmt(event_id))
        _debug_line("Participantes: %s", _fmt(participants))
        _debug_line("Equipo Local (Home): %s", _fmt(home_team))
        _debug_line("Equipo Visitante (Away): %s", _fmt(away_team))

        _debug_section("Resolución de max_rank (Tamaño de la Liga)")

    max_rank, max_rank_source = _resolve_max_rank(streak_analysis, event_context, debug_mode)

    if debug_mode:
        _debug_line("max_rank final resuelto = %d (fuente: %s)", max_rank, max_rank_source)
        _debug_section("Filtro de Temporada Inicial (Skip de partidos iniciales)")

    home_results_filtered, home_initial_filtered = _filter_initial_season_games(
        home_results,
        initial_games_to_skip,
        str(home_team or "HOME"),
        debug_mode,
    )
    away_results_filtered, away_initial_filtered = _filter_initial_season_games(
        away_results,
        initial_games_to_skip,
        str(away_team or "AWAY"),
        debug_mode,
    )

    home_series, home_skipped = _build_team_context_series(
        home_results_filtered,
        current_standings,
        max_rank,
        str(home_team or "HOME"),
        debug_mode,
    )
    away_series, away_skipped = _build_team_context_series(
        away_results_filtered,
        current_standings,
        max_rank,
        str(away_team or "AWAY"),
        debug_mode,
    )

    home_agg = _aggregate_team_context(home_series)
    away_agg = _aggregate_team_context(away_series)

    if debug_mode:
        _debug_section("Agregación de la Serie Contextual (HOME)")
        _debug_line("Agregado para HOME (%s):", _fmt(home_team))
        _debug_line("  - team_context_score (promedio): %s", _fmt(home_agg["team_context_score"]))
        _debug_line("  - avg_roe: %s", _fmt(home_agg["avg_roe"]))
        _debug_line("  - avg_gdoe: %s", _fmt(home_agg["avg_gdoe"]))
        _debug_line("  - ceiling (mejor partido): %s", _fmt(home_agg["ceiling"]))
        _debug_line("  - floor (peor partido): %s", _fmt(home_agg["floor"]))
        _debug_line("  - positives: %s, negatives: %s, neutrals: %s", _fmt(home_agg["positives"]), _fmt(home_agg["negatives"]), _fmt(home_agg["neutrals"]))
        _debug_line("  - volatility (desviación std): %s", _fmt(home_agg["volatility"]))
        _debug_line("  - games_valid: %s", _fmt(home_agg["games_valid"]))

        _debug_section("Agregación de la Serie Contextual (AWAY)")
        _debug_line("Agregado para AWAY (%s):", _fmt(away_team))
        _debug_line("  - team_context_score (promedio): %s", _fmt(away_agg["team_context_score"]))
        _debug_line("  - avg_roe: %s", _fmt(away_agg["avg_roe"]))
        _debug_line("  - avg_gdoe: %s", _fmt(away_agg["avg_gdoe"]))
        _debug_line("  - ceiling (mejor partido): %s", _fmt(away_agg["ceiling"]))
        _debug_line("  - floor (peor partido): %s", _fmt(away_agg["floor"]))
        _debug_line("  - positives: %s, negatives: %s, neutrals: %s", _fmt(away_agg["positives"]), _fmt(away_agg["negatives"]), _fmt(away_agg["neutrals"]))
        _debug_line("  - volatility (desviación std): %s", _fmt(away_agg["volatility"]))
        _debug_line("  - games_valid: %s", _fmt(away_agg["games_valid"]))

    m7_status, m7_status_reason = _determine_status(
        int(home_agg["games_valid"]),
        int(away_agg["games_valid"]),
    )

    home_team_context_score = float(home_agg["team_context_score"])
    away_team_context_score = float(away_agg["team_context_score"])
    m7_edge_raw = home_team_context_score - away_team_context_score
    m7_edge = _clamp(m7_edge_raw)
    if m7_status == "INSUFFICIENT_DATA":
        m7_edge = 0.0

    m7_bias = calculate_bias(m7_edge)
    m7_strength = _classify_m7_strength(m7_edge)

    if debug_mode:
        _debug_section("Validación de Estado del Motor")
        _debug_line("Estado M7 resuelto = %s (razón: %s)", _fmt(m7_status), _fmt(m7_status_reason))

        _debug_section("Cálculo de la Ventaja (M7_EDGE)")
        _debug_formula(
            "M7_EDGE_RAW",
            "home_team_context_score - away_team_context_score",
            f"{_fmt(home_team_context_score)} - {_fmt(away_team_context_score)}",
            _fmt(m7_edge_raw),
            "Diferencia cruda de los scores contextuales"
        )
        _debug_line("M7_EDGE (Clamped): %s", _fmt(m7_edge))

        _debug_section("Resumen de Output")
        _debug_line("M7_EDGE: %s", _fmt(m7_edge))
        _debug_line("M7_BIAS: %s", _fmt(m7_bias))
        _debug_line("M7_STRENGTH: %s", _fmt(m7_strength))
        _debug_line("M7_STATUS: %s (%s)", _fmt(m7_status), _fmt(m7_status_reason))
        _debug_line("-" * 60)

    components: List[ModuleComponentResult] = []
    if m7_status != "INSUFFICIENT_DATA":
        components = [
            ModuleComponentResult(
                name="TEAM_CONTEXT_SCORE_EDGE",
                edge=m7_edge,
                bias=m7_bias,
                strength=m7_strength,
                weight=1.0,
                weighted_edge=m7_edge,
                raw={
                    "formula": "HOME_TEAM_CONTEXT_SCORE - AWAY_TEAM_CONTEXT_SCORE",
                    "home_team_context_score": home_team_context_score,
                    "away_team_context_score": away_team_context_score,
                    "home_avg_roe": home_agg["avg_roe"],
                    "away_avg_roe": away_agg["avg_roe"],
                    "home_avg_gdoe": home_agg["avg_gdoe"],
                    "away_avg_gdoe": away_agg["avg_gdoe"],
                },
            )
        ]

    raw = {
        "module_version": "m7_opponent_expectation_engine_v2_3",
        "formula_profile": "m7_opponent_expectation_v2_3",
        "home_team": home_team,
        "away_team": away_team,
        "max_rank": max_rank,
        "max_rank_source": max_rank_source,
        "weights": {
            "roe": _ROE_WEIGHT,
            "gdoe": _GDOE_WEIGHT,
        },
        "constants": {
            "expected_gd_factor": _EXPECTED_GD_FACTOR,
            "gdoe_divisor": _GDOE_DIVISOR,
        },
        "filters": {
            "initial_season_games_to_skip": initial_games_to_skip,
            "home_initial_filtered_games": home_initial_filtered,
            "away_initial_filtered_games": away_initial_filtered,
        },
        "home": {
            "team": home_team,
            "aggregate": home_agg,
            "series": home_series,
            "skipped_games": home_skipped,
        },
        "away": {
            "team": away_team,
            "aggregate": away_agg,
            "series": away_series,
            "skipped_games": away_skipped,
        },
        "m7_edge_formula": "HOME_TEAM_CONTEXT_SCORE - AWAY_TEAM_CONTEXT_SCORE",
        "m7_edge_raw": m7_edge_raw,
        "m7_edge": m7_edge,
        "m7_abs_edge": abs(m7_edge),
        "m7_bias": m7_bias,
        "m7_strength": m7_strength,
        "m7_status": m7_status,
        "m7_status_reason": m7_status_reason,
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M7",
        module_name="Opponent Expectation Engine",
        event_id=event_id,
        participants=participants,
        value=m7_edge,
        bias=m7_bias,
        strength=m7_strength,
        components=components,
        raw=raw,
    )
