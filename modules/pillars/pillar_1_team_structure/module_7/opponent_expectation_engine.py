"""M7 - Opponent Expectation Engine.

Measures team performance against opponent difficulty rather than the result
alone. Positive values favor HOME; negative values favor AWAY.
"""

from __future__ import annotations

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


def _resolve_max_rank(streak_analysis: Any, event_context: EventContext) -> Tuple[int, str]:
    """Resolve competition size using the most specific available source."""
    competition = getattr(event_context, "competition", None)
    number_of_teams = _coerce_int(getattr(competition, "number_of_teams", None))
    if number_of_teams is not None and number_of_teams > 1:
        return number_of_teams, "event_context.competition.number_of_teams"

    current_standings = getattr(streak_analysis, "current_standings", None)
    if isinstance(current_standings, dict) and len(current_standings) > 1:
        return len(current_standings), "streak_analysis.current_standings.len"

    standings_response = getattr(streak_analysis, "standings_response", None)
    if isinstance(standings_response, list) and len(standings_response) > 1:
        return len(standings_response), "streak_analysis.standings_response.len"

    max_rank = _max_rank_from_standings(current_standings)
    if max_rank is not None and max_rank > 1:
        return max_rank, "streak_analysis.current_standings.max_rank"

    max_rank = _max_rank_from_standings(standings_response)
    if max_rank is not None and max_rank > 1:
        return max_rank, "streak_analysis.standings_response.max_rank"

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
            filtered_out_games.append(
                {
                    "original_index": index,
                    "event_id": game.get("event_id") if isinstance(game, dict) else None,
                    "opponent_name": _extract_opponent_name(game) if isinstance(game, dict) else None,
                    "startTimestamp": _extract_timestamp_value(game) if isinstance(game, dict) else None,
                    "reason": "initial_season_games_filter",
                }
            )
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
) -> Tuple[Optional[int], Optional[str]]:
    """Extract opponent ranking from game payload or standings by rival name."""
    rank = _coerce_int(game.get("opponent_ranking"))
    if rank is not None and 1 <= rank <= max_rank:
        return rank, "game.opponent_ranking"

    opponent_standing = game.get("opponent_standing")
    if isinstance(opponent_standing, dict):
        rank = _coerce_int(opponent_standing.get("rank"))
        if rank is not None and 1 <= rank <= max_rank:
            return rank, "game.opponent_standing.rank"
        rank = _coerce_int(opponent_standing.get("position"))
        if rank is not None and 1 <= rank <= max_rank:
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
            return rank, f"game.{key}"

    opponent_name = _extract_opponent_name(game)
    standing_rank = _extract_rank_from_current_standings(opponent_name, current_standings)
    if standing_rank is not None and 1 <= standing_rank <= max_rank:
        return standing_rank, "current_standings.by_name.rank"
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
        logger.info(
            "M7 [%s] building context series: total_games=%s max_rank=%s",
            team_label,
            len(results),
            max_rank,
        )

    for index, game in enumerate(results):
        if not isinstance(game, dict):
            skipped_games.append(_skipped_game(index, team_label, "not_a_dict", game))
            if debug_mode:
                logger.info(
                    "M7 [%s][%s] skipped: reason=not_a_dict raw_type=%s",
                    team_label,
                    index,
                    type(game).__name__,
                )
            continue

        score = extract_score_for_against(game)
        if score is None:
            skipped_games.append(_skipped_game(index, team_label, "missing_score", game))
            if debug_mode:
                logger.info(
                    "M7 [%s][%s] skipped: reason=missing_score opponent_name=%s keys=%s",
                    team_label,
                    index,
                    _extract_opponent_name(game),
                    sorted(game.keys()),
                )
            continue

        goals_for, goals_against = score
        game_gd = goals_for - goals_against
        opponent_rank, opponent_rank_source = _extract_opponent_rank(game, current_standings, max_rank)
        if opponent_rank is None:
            skipped_games.append(_skipped_game(index, team_label, "missing_opponent_rank", game))
            if debug_mode:
                logger.info(
                    "M7 [%s][%s] skipped: reason=missing_opponent_rank opponent_name=%s gf=%.12f ga=%.12f gd=%.12f",
                    team_label,
                    index,
                    _extract_opponent_name(game),
                    goals_for,
                    goals_against,
                    game_gd,
            )
            continue

        opp_strength = _calculate_opponent_strength(opponent_rank, max_rank)
        if opp_strength is None:
            skipped_games.append(_skipped_game(index, team_label, "invalid_opponent_rank", game))
            if debug_mode:
                logger.info(
                    "M7 [%s][%s] skipped: reason=invalid_opponent_rank opponent_name=%s opponent_rank=%s max_rank=%s",
                    team_label,
                    index,
                    _extract_opponent_name(game),
                    opponent_rank,
                    max_rank,
            )
            continue

        own_rank, own_rank_source = _extract_own_rank(game)
        expected_result = _calculate_expected_result(opp_strength)
        real_result = _calculate_real_result(game_gd)
        roe = _calculate_roe(float(real_result), expected_result)
        expected_gd = _calculate_expected_gd(expected_result)
        gdoe = _calculate_gdoe(game_gd, expected_gd)
        game_context_score = _calculate_context_score(roe, gdoe)

        valid_game = {
            "index": index,
            "team_label": team_label,
            "opponent_name": _extract_opponent_name(game),
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
            "context_classification": _classify_game_context_score(game_context_score),
        }
        if debug_mode:
            valid_game["raw_game"] = game
            team_name = game.get("team_name") if isinstance(game, dict) else None
            logger.info(
                "[M7_GAME_INPUT] team_label=%s index=%s event_id=%s startTimestamp=%s team_name=%s opponent_name=%s team_role=%s opponent_role=%s team_score=%s opponent_score=%s game_gd=%s own_rank=%s own_rank_source=%s opponent_rank=%s opponent_rank_source=%s",
                team_label,
                index,
                game.get("event_id"),
                game.get("startTimestamp"),
                team_name,
                valid_game["opponent_name"],
                game.get("team_role"),
                game.get("opponent_role"),
                goals_for,
                goals_against,
                game_gd,
                own_rank,
                own_rank_source,
                opponent_rank,
                opponent_rank_source,
            )
            logger.info(
                (
                    "[M7_FORMULA_TRACE] OPP_STRENGTH = (max_rank - opponent_rank) / (max_rank - 1) = %.12f; "
                    "EXPECTED_RESULT = 1 - (2 * opp_strength) = %.12f; REAL_RESULT = sign(game_gd) = %s; "
                    "ROE = (real_result - expected_result) / 2 = %.12f; EXPECTED_GD = expected_result * 1.5 = %.12f; "
                    "GDOE = (game_gd - expected_gd) / 5 = %.12f; GAME_CONTEXT_SCORE = (0.60 * roe) + (0.40 * gdoe) = %.12f"
                ),
                opp_strength,
                expected_result,
                real_result,
                roe,
                expected_gd,
                gdoe,
                game_context_score,
            )
        valid_games.append(valid_game)

    if debug_mode:
        logger.info(
            "M7 [%s] context series completed: valid_games=%s skipped_games=%s",
            team_label,
            len(valid_games),
            len(skipped_games),
        )

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

    home_results_filtered, home_initial_filtered = _filter_initial_season_games(
        home_results,
        initial_games_to_skip,
    )
    away_results_filtered, away_initial_filtered = _filter_initial_season_games(
        away_results,
        initial_games_to_skip,
    )

    max_rank, max_rank_source = _resolve_max_rank(streak_analysis, event_context)

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
        logger.info(
            "[M7_INITIAL_FILTER] team_label=%s input_games=%s initial_season_games_to_skip=%s filtered_out_count=%s remaining_games=%s filtered_out_event_ids=%s filtered_out_opponents=%s",
            home_team or "HOME",
            len(home_results),
            initial_games_to_skip,
            len(home_initial_filtered),
            len(home_results_filtered),
            [game.get("event_id") for game in home_initial_filtered],
            [game.get("opponent_name") for game in home_initial_filtered],
        )
        logger.info(
            "[M7_INITIAL_FILTER] team_label=%s input_games=%s initial_season_games_to_skip=%s filtered_out_count=%s remaining_games=%s filtered_out_event_ids=%s filtered_out_opponents=%s",
            away_team or "AWAY",
            len(away_results),
            initial_games_to_skip,
            len(away_initial_filtered),
            len(away_results_filtered),
            [game.get("event_id") for game in away_initial_filtered],
            [game.get("opponent_name") for game in away_initial_filtered],
        )
        logger.info(
            "M7 resolved max_rank=%s source=%s",
            max_rank,
            max_rank_source,
        )
        logger.info(
            (
                "M7 [HOME] aggregate: team_context_score=%.12f avg_roe=%.12f avg_gdoe=%.12f "
                "ceiling=%.12f floor=%.12f positives=%s negatives=%s neutrals=%s volatility=%.12f games_valid=%s"
            ),
            home_agg["team_context_score"],
            home_agg["avg_roe"],
            home_agg["avg_gdoe"],
            home_agg["ceiling"],
            home_agg["floor"],
            home_agg["positives"],
            home_agg["negatives"],
            home_agg["neutrals"],
            home_agg["volatility"],
            home_agg["games_valid"],
        )
        logger.info(
            (
                "M7 [AWAY] aggregate: team_context_score=%.12f avg_roe=%.12f avg_gdoe=%.12f "
                "ceiling=%.12f floor=%.12f positives=%s negatives=%s neutrals=%s volatility=%.12f games_valid=%s"
            ),
            away_agg["team_context_score"],
            away_agg["avg_roe"],
            away_agg["avg_gdoe"],
            away_agg["ceiling"],
            away_agg["floor"],
            away_agg["positives"],
            away_agg["negatives"],
            away_agg["neutrals"],
            away_agg["volatility"],
            away_agg["games_valid"],
        )

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
        logger.info(
            (
                "M7 edge calculation: home_team_context_score=%.12f away_team_context_score=%.12f "
                "formula=home-away edge_raw=%.12f edge_clamped=%.12f"
            ),
            home_team_context_score,
            away_team_context_score,
            m7_edge_raw,
            m7_edge,
        )

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

    if debug_mode:
        logger.info(
            "M7 Opponent Expectation Engine event_id=%s participants=%s home_team=%s away_team=%s",
            event_id,
            participants,
            home_team,
            away_team,
        )
        logger.info(
            "M7 skipped_games home=%s away=%s",
            home_skipped,
            away_skipped,
        )
        logger.info("M7 component_count=%s", len(components))
        logger.info(
            "M7 final status=%s reason=%s bias=%s strength=%s edge=%.12f abs_edge=%.12f",
            m7_status,
            m7_status_reason,
            m7_bias,
            m7_strength,
            m7_edge,
            abs(m7_edge),
        )

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
