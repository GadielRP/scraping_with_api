"""M7 - Opponent Expectation Engine v2.1."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)


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


def _encode_result(code: Any) -> Optional[int]:
    if code in (1, "1", "+1", True):
        return 1
    if code in (0, "0", "X", "x", "D", "d"):
        return 0
    if code in (-1, "2", "-1", False):
        return -1
    return None


def _extract_opponent_rank(result: Dict[str, Any]) -> Optional[int]:
    rank = _coerce_int(result.get("opponent_ranking"))
    if rank is not None and rank > 0:
        return rank

    opponent_standing = result.get("opponent_standing")
    if isinstance(opponent_standing, dict):
        for key in ("rank", "position"):
            rank = _coerce_int(opponent_standing.get(key))
            if rank is not None and rank > 0:
                return rank
    return None


def _extract_goal_diff(result: Dict[str, Any]) -> Optional[float]:
    net_score = _coerce_float(result.get("net_score"))
    if net_score is not None:
        return net_score

    team_score = _coerce_float(result.get("team_score"))
    opponent_score = _coerce_float(result.get("opponent_score"))
    if team_score is not None and opponent_score is not None:
        return team_score - opponent_score

    score_for = _coerce_float(result.get("score_for"))
    score_against = _coerce_float(result.get("score_against"))
    if score_for is not None and score_against is not None:
        return score_for - score_against
    return None


def _league_size(streak_analysis: Any, event_context: EventContext) -> Tuple[int, str]:
    competition = getattr(event_context, "competition", None)
    league_size = _coerce_int(getattr(competition, "number_of_teams", None))
    if league_size is not None and league_size > 1:
        return league_size, "event_context.competition.number_of_teams"

    current_standings = getattr(streak_analysis, "current_standings", None) or {}
    league_size = _coerce_int(len(current_standings))
    if league_size is not None and league_size > 1:
        return league_size, "streak_analysis.current_standings"

    return 20, "default_20_fallback"


def _game_context_score(result: Dict[str, Any], league_size: int) -> Optional[Dict[str, Any]]:
    if league_size <= 1:
        return None

    opponent_rank = _extract_opponent_rank(result)
    real_result = _encode_result(result.get("team_result_code"))
    game_gd = _extract_goal_diff(result)
    if opponent_rank is None or real_result is None or game_gd is None:
        return None

    opponent_strength = clamp((league_size - opponent_rank) / float(league_size - 1), 0.0, 1.0)
    expected_result = 1.0 - (2.0 * opponent_strength)
    result_over_expectation = clamp((float(real_result) - expected_result) / 2.0)
    expected_gd = expected_result * 2.0
    gd_over_expectation = clamp((game_gd - expected_gd) / 5.0)
    game_context_score = clamp((0.60 * result_over_expectation) + (0.40 * gd_over_expectation))

    opponent = result.get("opponent_name")
    if opponent is None:
        opponent = result.get("opponent")

    return {
        "opponent": opponent,
        "opponent_rank": opponent_rank,
        "real_result": real_result,
        "game_gd": game_gd,
        "opponent_strength": opponent_strength,
        "expected_result": expected_result,
        "result_over_expectation": result_over_expectation,
        "expected_gd": expected_gd,
        "gd_over_expectation": gd_over_expectation,
        "game_context_score": game_context_score,
        "startTimestamp": result.get("startTimestamp"),
    }


def _team_context(results: List[Dict[str, Any]], league_size: int) -> Dict[str, Any]:
    games: List[Dict[str, Any]] = []
    invalid_games: List[Dict[str, Any]] = []
    invalid_match_count = 0

    for idx, result in enumerate(results):
        if not isinstance(result, dict):
            invalid_match_count += 1
            invalid_games.append({
                "index": idx,
                "opponent": None,
                "startTimestamp": None,
                "missing": ["not_a_dict"],
            })
            continue
        game_context = _game_context_score(result, league_size)
        if game_context is None:
            invalid_match_count += 1
            opponent = result.get("opponent_name")
            if opponent is None:
                opponent = result.get("opponent")
            missing = []
            if league_size <= 1:
                missing.append("league_size")
            if _extract_opponent_rank(result) is None:
                missing.append("opponent_rank")
            if _encode_result(result.get("team_result_code")) is None:
                missing.append("team_result_code")
            if _extract_goal_diff(result) is None:
                missing.append("game_gd")
            invalid_games.append({
                "index": idx,
                "opponent": opponent,
                "startTimestamp": result.get("startTimestamp"),
                "missing": missing,
            })
            continue
        games.append(game_context)

    valid_match_count = len(games)
    team_context_score = (
        sum(game["game_context_score"] for game in games) / float(valid_match_count)
        if valid_match_count > 0
        else 0.0
    )
    top_positive_games = [
        {
            "opponent": game.get("opponent"),
            "opponent_rank": game.get("opponent_rank"),
            "real_result": game.get("real_result"),
            "game_gd": game.get("game_gd"),
            "game_context_score": game.get("game_context_score"),
            "startTimestamp": game.get("startTimestamp"),
        }
        for game in sorted(games, key=lambda item: float(item["game_context_score"]), reverse=True)[:5]
    ]
    top_negative_games = [
        {
            "opponent": game.get("opponent"),
            "opponent_rank": game.get("opponent_rank"),
            "real_result": game.get("real_result"),
            "game_gd": game.get("game_gd"),
            "game_context_score": game.get("game_context_score"),
            "startTimestamp": game.get("startTimestamp"),
        }
        for game in sorted(games, key=lambda item: float(item["game_context_score"]))[:5]
    ]

    return {
        "valid_match_count": valid_match_count,
        "invalid_match_count": invalid_match_count,
        "invalid_games": invalid_games,
        "team_context_score": team_context_score,
        "games": games,
        "top_positive_games": top_positive_games,
        "top_negative_games": top_negative_games,
    }


def _relative_m7_edge(home_score: float, away_score: float) -> float:
    return (home_score - away_score) / 2.0


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


def _determine_status(
    home_context: Dict[str, Any],
    away_context: Dict[str, Any],
    league_size_source: str,
) -> Tuple[str, str]:
    home_count = int(home_context.get("valid_match_count", 0) or 0)
    away_count = int(away_context.get("valid_match_count", 0) or 0)
    if home_count < 10 or away_count < 10:
        return "INSUFFICIENT_DATA", "insufficient_opponent_expectation_sample"
    if home_count < 20 or away_count < 20:
        return "DEGRADED", "partial_opponent_expectation_sample"
    if league_size_source == "default_20_fallback":
        return "DEGRADED", "default_league_size_fallback"
    return "ACTIVE", "active"


def calculate_opponent_expectation_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    home_results: List[Dict[str, Any]] = getattr(streak_analysis, "home_team_results", None) or []
    away_results: List[Dict[str, Any]] = getattr(streak_analysis, "away_team_results", None) or []
    home_team = getattr(streak_analysis, "home_team_name", None)
    away_team = getattr(streak_analysis, "away_team_name", None)
    event_id = getattr(streak_analysis, "event_id", 0)
    participants = getattr(streak_analysis, "participants", "") or ""

    league_size, league_size_source = _league_size(streak_analysis, event_context)
    home_context = _team_context(home_results, league_size)
    away_context = _team_context(away_results, league_size)
    m7_status, m7_status_reason = _determine_status(home_context, away_context, league_size_source)

    home_team_context_score = float(home_context["team_context_score"])
    away_team_context_score = float(away_context["team_context_score"])
    m7_edge_raw = _relative_m7_edge(home_team_context_score, away_team_context_score)
    m7_edge = clamp(m7_edge_raw)
    if m7_status == "INSUFFICIENT_DATA":
        m7_edge_raw = 0.0
        m7_edge = 0.0

    if debug_mode:
        logger.info(f"--- M7 Opponent Expectation Engine Debug: Event {event_id} ({participants}) ---")
        logger.info(f"  home_team={home_team} | away_team={away_team}")
        logger.info(f"  league_size={league_size} source={league_size_source}")
        logger.info(
            f"  home_valid_matches={home_context['valid_match_count']} "
            f"home_invalid_matches={home_context['invalid_match_count']}"
        )
        if home_context.get("invalid_games"):
            logger.info(f"  [HOME_INVALID_GAMES] {home_context['invalid_games']}")
        logger.info(
            f"  away_valid_matches={away_context['valid_match_count']} "
            f"away_invalid_matches={away_context['invalid_match_count']}"
        )
        if away_context.get("invalid_games"):
            logger.info(f"  [AWAY_INVALID_GAMES] {away_context['invalid_games']}")
        logger.info(f"  activation_status={m7_status} ({m7_status_reason})")
        logger.info(f"  [HOME_CONTEXT_SCORE] team_context_score={home_team_context_score:.12f}")
        logger.info(f"  [AWAY_CONTEXT_SCORE] team_context_score={away_team_context_score:.12f}")
        logger.info(f"  [HOME_TOP_POSITIVE] {home_context['top_positive_games']}")
        logger.info(f"  [HOME_TOP_NEGATIVE] {home_context['top_negative_games']}")
        logger.info(f"  [AWAY_TOP_POSITIVE] {away_context['top_positive_games']}")
        logger.info(f"  [AWAY_TOP_NEGATIVE] {away_context['top_negative_games']}")
        logger.info(
            f"  [M7_EDGE_RAW] edge = (home_context_score({home_team_context_score:.12f}) - "
            f"away_context_score({away_team_context_score:.12f})) / 2 = {m7_edge_raw:.12f}"
        )
        logger.info(f"  [M7_EDGE] clamped = {m7_edge:.12f}")
        logger.info("  --- Component Summary ---")
        logger.info(
            f"  OPPONENT_EXPECTATION_EDGE: edge={m7_edge:.12f}  weight=1.00  "
            f"weighted={m7_edge:.12f}  bias={calculate_bias(m7_edge)}  strength={classify_strength(m7_edge)}"
        )
        logger.info(
            f"  M7 Final: edge_raw={m7_edge_raw:.12f}  edge_clamped={m7_edge:.12f} "
            f"bias={calculate_bias(m7_edge)}  strength={classify_strength(m7_edge)} "
            f"status={m7_status} ({m7_status_reason})"
        )
        logger.info("-" * 60)

    components = []
    if m7_status != "INSUFFICIENT_DATA":
        components = [
            _component(
                "OPPONENT_EXPECTATION_EDGE",
                m7_edge,
                1.0,
                {
                    "home_team_context_score": home_team_context_score,
                    "away_team_context_score": away_team_context_score,
                    "formula": "(HOME_TEAM_CONTEXT_SCORE - AWAY_TEAM_CONTEXT_SCORE) / 2",
                },
            )
        ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "league_size": league_size,
        "league_size_source": league_size_source,
        "home_context": home_context,
        "away_context": away_context,
        "m7_edge_raw": m7_edge_raw,
        "m7_edge": m7_edge,
        "m7_abs_edge": abs(m7_edge),
        "m7_status": m7_status,
        "m7_status_reason": m7_status_reason,
        "engine_version": "opponent_expectation_v2_1",
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M7",
        module_name="Opponent Expectation Engine",
        event_id=event_id,
        participants=participants,
        value=m7_edge,
        bias=calculate_bias(m7_edge),
        strength=classify_strength(m7_edge),
        components=components,
        raw=raw,
    )
