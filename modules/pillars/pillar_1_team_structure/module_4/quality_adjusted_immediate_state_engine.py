"""M4 - Quality-Adjusted Immediate State Engine.

This module measures how teams are arriving right now using only the most
recent valid matches and adjusting each result by the quality of the rival
faced in that match.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
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

WINDOW_SIZE = 5
RESULT_SCORE_WEIGHT = 0.60
GD_SCORE_WEIGHT = 0.40
ENGINE_PROFILE = "quality_adjusted_immediate_state_v1"


@dataclass(frozen=True)
class ParsedImmediateMatch:
    event_id: Optional[int]
    start_timestamp: Optional[int]
    team_name: str
    opponent_name: str
    opponent_rank: int
    team_result: str
    result_value: float
    team_score: float
    opponent_score: float
    goal_difference: float
    opponent_strength: float
    adjusted_result: float
    adjusted_gd: float


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coerced):
        return None
    return coerced


def _coerce_int(value: Any) -> Optional[int]:
    coerced_float = _coerce_float(value)
    if coerced_float is None:
        return None
    if not coerced_float.is_integer():
        return None
    return int(coerced_float)


def _normalize_result(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    if not normalized:
        return None
    if normalized in {"W", "WIN", "1", "+1"}:
        return "WIN"
    if normalized in {"D", "DRAW", "X", "0"}:
        return "DRAW"
    if normalized in {"L", "LOSS", "2", "-1"}:
        return "LOSS"
    return None


def _result_value(normalized_result: str) -> float:
    if normalized_result == "WIN":
        return 1.0
    if normalized_result == "DRAW":
        return 0.0
    if normalized_result == "LOSS":
        return -1.0
    raise ValueError(f"unsupported_normalized_result={normalized_result!r}")


def _opponent_strength(rank: int, n_liga: int) -> float:
    if n_liga <= 1:
        return 0.0
    raw_strength = 1.0 - ((float(rank) - 1.0) / float(n_liga - 1))
    return clamp(raw_strength, 0.0, 1.0)


def _sort_matches_recent_first(matches: List[dict]) -> List[dict]:
    indexed_matches: List[Tuple[int, dict, Optional[int]]] = []
    for index, match in enumerate(matches):
        start_timestamp = _coerce_int(match.get("startTimestamp")) if isinstance(match, dict) else None
        indexed_matches.append((index, match, start_timestamp))

    if not any(timestamp is not None for _, _, timestamp in indexed_matches):
        return list(matches)

    ordered_matches = sorted(
        indexed_matches,
        key=lambda item: (
            0 if item[2] is not None else 1,
            -(item[2] or 0),
            item[0],
        ),
    )
    return [match for _, match, _ in ordered_matches]


def _extract_opponent_name(match: Dict[str, Any]) -> str:
    for key in ("opponent_name", "opponent", "opponent_team_name", "rival_name", "rival_team_name"):
        value = match.get(key)
        if isinstance(value, dict):
            for nested_key in ("name", "teamName", "team_name", "short_name"):
                nested_value = value.get(nested_key)
                if nested_value is not None and str(nested_value).strip():
                    return str(nested_value).strip()
            continue
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _extract_match_timestamp(match: Dict[str, Any]) -> Optional[int]:
    for key in ("startTimestamp", "start_timestamp"):
        timestamp = _coerce_int(match.get(key))
        if timestamp is not None:
            return timestamp
    return None


def _extract_event_id(match: Dict[str, Any]) -> Optional[int]:
    for key in ("event_id", "eventId", "id"):
        event_id = _coerce_int(match.get(key))
        if event_id is not None:
            return event_id
    return None


def _extract_candidate_ranks(match: Dict[str, Any]) -> List[int]:
    candidate_ranks: List[int] = []
    for key in (
        "opponent_ranking",
        "opponent_rank",
        "opponent_current_rank",
        "opponent_standing_rank",
        "opponent_position",
        "own_ranking",
        "own_rank",
        "team_ranking",
        "team_current_rank",
        "rank",
        "position",
        "standing_rank",
    ):
        rank = _coerce_int(match.get(key))
        if rank is not None and rank > 0:
            candidate_ranks.append(rank)

    for nested_key in ("team_standing", "opponent_standing"):
        nested = match.get(nested_key)
        if not isinstance(nested, dict):
            continue
        for key in ("rank", "position", "current_rank", "standing_rank"):
            rank = _coerce_int(nested.get(key))
            if rank is not None and rank > 0:
                candidate_ranks.append(rank)
    return candidate_ranks


def _resolve_n_liga(
    streak_analysis: Any,
    event_context: EventContext,
    all_matches: List[dict],
) -> Tuple[Optional[int], str]:
    current_standings = getattr(streak_analysis, "current_standings", None)
    if isinstance(current_standings, dict) and len(current_standings) > 0:
        return len(current_standings), "streak_analysis.current_standings.len"

    competition = getattr(event_context, "competition", None)
    number_of_teams = _coerce_int(getattr(competition, "number_of_teams", None))
    if number_of_teams is not None and number_of_teams > 1:
        return number_of_teams, "event_context.competition.number_of_teams"

    inferred_ranks: List[int] = []
    for match in all_matches:
        if isinstance(match, dict):
            inferred_ranks.extend(_extract_candidate_ranks(match))

    if inferred_ranks:
        inferred_n_liga = max(inferred_ranks)
        if inferred_n_liga > 1:
            return inferred_n_liga, "matches.inferred_ranking"

    return None, "unresolved"


def _parse_match(
    match: dict,
    team_name: str,
    n_liga: int,
) -> Tuple[Optional[ParsedImmediateMatch], Optional[dict]]:
    if not isinstance(match, dict):
        return None, {
            "event_id": None,
            "opponent_name": None,
            "reason": "invalid_match_type",
            "raw_fields": {
                "opponent_ranking": None,
                "team_result": None,
                "team_score": None,
                "opponent_score": None,
            },
        }

    opponent_name = _extract_opponent_name(match)
    event_id = _extract_event_id(match)
    raw_opponent_ranking = match.get("opponent_ranking")
    raw_team_result = match.get("team_result", match.get("team_result_code"))
    raw_team_score = match.get("team_score")
    raw_opponent_score = match.get("opponent_score")

    if n_liga is None or n_liga <= 1:
        return None, {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "invalid_n_liga",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }

    opponent_rank = _coerce_int(raw_opponent_ranking)
    if opponent_rank is None:
        return None, {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "missing_opponent_ranking",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }
    if opponent_rank < 1 or opponent_rank > n_liga:
        return None, {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "invalid_opponent_rank",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }

    normalized_result = _normalize_result(raw_team_result)
    if normalized_result is None:
        return None, {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "missing_or_uninterpretable_team_result",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }

    team_score = _coerce_float(raw_team_score)
    if team_score is None:
        return None, {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "missing_team_score",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }

    opponent_score = _coerce_float(raw_opponent_score)
    if opponent_score is None:
        return None, {
            "event_id": event_id,
            "opponent_name": opponent_name,
            "reason": "missing_opponent_score",
            "raw_fields": {
                "opponent_ranking": raw_opponent_ranking,
                "team_result": raw_team_result,
                "team_score": raw_team_score,
                "opponent_score": raw_opponent_score,
            },
        }

    result_value = _result_value(normalized_result)
    goal_difference = team_score - opponent_score
    opponent_strength = _opponent_strength(opponent_rank, n_liga)
    adjusted_result = result_value * opponent_strength
    adjusted_gd = goal_difference * opponent_strength

    return ParsedImmediateMatch(
        event_id=event_id,
        start_timestamp=_extract_match_timestamp(match),
        team_name=str(team_name or ""),
        opponent_name=opponent_name,
        opponent_rank=opponent_rank,
        team_result=normalized_result,
        result_value=result_value,
        team_score=team_score,
        opponent_score=opponent_score,
        goal_difference=goal_difference,
        opponent_strength=opponent_strength,
        adjusted_result=adjusted_result,
        adjusted_gd=adjusted_gd,
    ), None


def _serialize_parsed_match(match: ParsedImmediateMatch) -> Dict[str, Any]:
    return {
        "event_id": match.event_id,
        "start_timestamp": match.start_timestamp,
        "team_name": match.team_name,
        "opponent_name": match.opponent_name,
        "opponent_rank": match.opponent_rank,
        "team_result": match.team_result,
        "result_value": match.result_value,
        "team_score": match.team_score,
        "opponent_score": match.opponent_score,
        "goal_difference": match.goal_difference,
        "opponent_strength": match.opponent_strength,
        "adjusted_result": match.adjusted_result,
        "adjusted_gd": match.adjusted_gd,
    }


def _build_team_window(
    matches: List[dict],
    team_name: str,
    n_liga: int,
    window_size: int,
) -> Tuple[List[ParsedImmediateMatch], List[dict]]:
    parsed_matches: List[ParsedImmediateMatch] = []
    invalid_matches: List[dict] = []

    for match in _sort_matches_recent_first(matches):
        parsed_match, invalid_match = _parse_match(match, team_name, n_liga)
        if parsed_match is not None:
            parsed_matches.append(parsed_match)
            if len(parsed_matches) >= window_size:
                break
            continue
        if invalid_match is not None:
            invalid_matches.append(invalid_match)

    return parsed_matches, invalid_matches


def _relative_score(home_value: float, away_value: float) -> Tuple[float, float]:
    denominator = abs(home_value) + abs(away_value)
    if denominator == 0:
        return 0.0, 0.0
    return (home_value - away_value) / denominator, denominator


def _component(name: str, edge: float, weight: float, raw: dict) -> ModuleComponentResult:
    return ModuleComponentResult(
        name=name,
        edge=edge,
        bias=calculate_bias(edge),
        strength=classify_strength(edge),
        weight=weight,
        weighted_edge=edge * weight,
        raw=raw,
    )


def _build_insufficient_result(
    *,
    pillar_id: str,
    module_id: str,
    module_name: str,
    event_id: int,
    participants: str,
    raw: Dict[str, Any],
) -> ModuleResult:
    raw = dict(raw)
    raw.setdefault("m4_edge_raw", 0.0)
    raw.setdefault("m4_edge", 0.0)
    raw.setdefault("m4_abs_edge", 0.0)
    raw.setdefault("m4_bias", calculate_bias(0.0))
    raw.setdefault("m4_strength", classify_strength(0.0))
    raw.setdefault("m4_status", "INSUFFICIENT_DATA")
    raw.setdefault("m4_status_reason", "missing_team_results")
    raw.setdefault("engine_profile", ENGINE_PROFILE)

    return ModuleResult(
        pillar_id=pillar_id,
        module_id=module_id,
        module_name=module_name,
        event_id=event_id,
        participants=participants,
        value=0.0,
        bias=calculate_bias(0.0),
        strength=classify_strength(0.0),
        components=[],
        raw=raw,
    )


def calculate_quality_adjusted_immediate_state_engine(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    home_results: List[Dict[str, Any]] = list(getattr(streak_analysis, "home_team_results", None) or [])
    away_results: List[Dict[str, Any]] = list(getattr(streak_analysis, "away_team_results", None) or [])
    home_team = getattr(streak_analysis, "home_team_name", None) or getattr(event_context.home, "name", None) or ""
    away_team = getattr(streak_analysis, "away_team_name", None) or getattr(event_context.away, "name", None) or ""
    event_id = getattr(streak_analysis, "event_id", getattr(event_context, "event_id", 0)) or 0
    participants = getattr(streak_analysis, "participants", None) or getattr(event_context, "participants_label", None) or f"{home_team} vs {away_team}"

    if not home_results or not away_results:
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "window_size": WINDOW_SIZE,
            "home_valid_matches": 0,
            "away_valid_matches": 0,
            "home_window": [],
            "away_window": [],
            "n_liga": None,
            "n_liga_source": "missing_team_results",
            "home_adjusted_result": 0.0,
            "away_adjusted_result": 0.0,
            "home_adjusted_gd": 0.0,
            "away_adjusted_gd": 0.0,
            "result_score": 0.0,
            "gd_score": 0.0,
            "result_score_weight": RESULT_SCORE_WEIGHT,
            "gd_score_weight": GD_SCORE_WEIGHT,
            "m4_edge_raw": 0.0,
            "m4_edge": 0.0,
            "m4_abs_edge": 0.0,
            "m4_bias": calculate_bias(0.0),
            "m4_strength": classify_strength(0.0),
            "m4_status": "INSUFFICIENT_DATA",
            "m4_status_reason": "missing_team_results",
            "invalid_home_matches": [],
            "invalid_away_matches": [],
            "engine_profile": ENGINE_PROFILE,
        }
        if debug_mode:
            logger.info(
                "M4 [%s] insufficient: missing_team_results home_results=%s away_results=%s",
                event_id,
                len(home_results),
                len(away_results),
            )
        return _build_insufficient_result(
            pillar_id="pillar_1_team_structure",
            module_id="M4",
            module_name="Quality-Adjusted Immediate State Engine",
            event_id=event_id,
            participants=participants,
            raw=raw,
        )

    all_matches: List[dict] = [
        match for match in home_results + away_results if isinstance(match, dict)
    ]
    n_liga, n_liga_source = _resolve_n_liga(streak_analysis, event_context, all_matches)
    if n_liga is None or n_liga <= 1:
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "window_size": WINDOW_SIZE,
            "home_valid_matches": 0,
            "away_valid_matches": 0,
            "home_window": [],
            "away_window": [],
            "n_liga": n_liga,
            "n_liga_source": n_liga_source if n_liga is not None else "missing_league_size",
            "home_adjusted_result": 0.0,
            "away_adjusted_result": 0.0,
            "home_adjusted_gd": 0.0,
            "away_adjusted_gd": 0.0,
            "result_score": 0.0,
            "gd_score": 0.0,
            "result_score_weight": RESULT_SCORE_WEIGHT,
            "gd_score_weight": GD_SCORE_WEIGHT,
            "m4_edge_raw": 0.0,
            "m4_edge": 0.0,
            "m4_abs_edge": 0.0,
            "m4_bias": calculate_bias(0.0),
            "m4_strength": classify_strength(0.0),
            "m4_status": "INSUFFICIENT_DATA",
            "m4_status_reason": "missing_league_size",
            "invalid_home_matches": [],
            "invalid_away_matches": [],
            "engine_profile": ENGINE_PROFILE,
        }
        if debug_mode:
            logger.info(
                "M4 [%s] insufficient: missing_league_size source=%s resolved=%s",
                event_id,
                n_liga_source,
                n_liga,
            )
        return _build_insufficient_result(
            pillar_id="pillar_1_team_structure",
            module_id="M4",
            module_name="Quality-Adjusted Immediate State Engine",
            event_id=event_id,
            participants=participants,
            raw=raw,
        )

    home_window, invalid_home_matches = _build_team_window(home_results, str(home_team), n_liga, WINDOW_SIZE)
    away_window, invalid_away_matches = _build_team_window(away_results, str(away_team), n_liga, WINDOW_SIZE)

    home_valid_matches = len(home_window)
    away_valid_matches = len(away_window)

    if home_valid_matches == 0 or away_valid_matches == 0:
        status_reason = "invalid_match_data" if (invalid_home_matches or invalid_away_matches) else "not_enough_valid_matches"
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "window_size": WINDOW_SIZE,
            "home_valid_matches": home_valid_matches,
            "away_valid_matches": away_valid_matches,
            "home_window": [_serialize_parsed_match(match) for match in home_window],
            "away_window": [_serialize_parsed_match(match) for match in away_window],
            "n_liga": n_liga,
            "n_liga_source": n_liga_source,
            "home_adjusted_result": 0.0,
            "away_adjusted_result": 0.0,
            "home_adjusted_gd": 0.0,
            "away_adjusted_gd": 0.0,
            "result_score": 0.0,
            "gd_score": 0.0,
            "result_score_weight": RESULT_SCORE_WEIGHT,
            "gd_score_weight": GD_SCORE_WEIGHT,
            "m4_edge_raw": 0.0,
            "m4_edge": 0.0,
            "m4_abs_edge": 0.0,
            "m4_bias": calculate_bias(0.0),
            "m4_strength": classify_strength(0.0),
            "m4_status": "INSUFFICIENT_DATA",
            "m4_status_reason": status_reason,
            "invalid_home_matches": invalid_home_matches,
            "invalid_away_matches": invalid_away_matches,
            "engine_profile": ENGINE_PROFILE,
        }
        if debug_mode:
            logger.info(
                "M4 [%s] insufficient: status_reason=%s home_valid=%s away_valid=%s n_liga=%s source=%s",
                event_id,
                status_reason,
                home_valid_matches,
                away_valid_matches,
                n_liga,
                n_liga_source,
            )
        return _build_insufficient_result(
            pillar_id="pillar_1_team_structure",
            module_id="M4",
            module_name="Quality-Adjusted Immediate State Engine",
            event_id=event_id,
            participants=participants,
            raw=raw,
        )

    if home_valid_matches < 3 or away_valid_matches < 3:
        status_reason = "not_enough_valid_matches"
        raw = {
            "home_team": home_team,
            "away_team": away_team,
            "window_size": WINDOW_SIZE,
            "home_valid_matches": home_valid_matches,
            "away_valid_matches": away_valid_matches,
            "home_window": [_serialize_parsed_match(match) for match in home_window],
            "away_window": [_serialize_parsed_match(match) for match in away_window],
            "n_liga": n_liga,
            "n_liga_source": n_liga_source,
            "home_adjusted_result": 0.0,
            "away_adjusted_result": 0.0,
            "home_adjusted_gd": 0.0,
            "away_adjusted_gd": 0.0,
            "result_score": 0.0,
            "gd_score": 0.0,
            "result_score_weight": RESULT_SCORE_WEIGHT,
            "gd_score_weight": GD_SCORE_WEIGHT,
            "m4_edge_raw": 0.0,
            "m4_edge": 0.0,
            "m4_abs_edge": 0.0,
            "m4_bias": calculate_bias(0.0),
            "m4_strength": classify_strength(0.0),
            "m4_status": "INSUFFICIENT_DATA",
            "m4_status_reason": status_reason,
            "invalid_home_matches": invalid_home_matches,
            "invalid_away_matches": invalid_away_matches,
            "engine_profile": ENGINE_PROFILE,
        }
        if debug_mode:
            logger.info(
                "M4 [%s] insufficient: status_reason=%s home_valid=%s away_valid=%s n_liga=%s source=%s",
                event_id,
                status_reason,
                home_valid_matches,
                away_valid_matches,
                n_liga,
                n_liga_source,
            )
        return _build_insufficient_result(
            pillar_id="pillar_1_team_structure",
            module_id="M4",
            module_name="Quality-Adjusted Immediate State Engine",
            event_id=event_id,
            participants=participants,
            raw=raw,
        )

    home_adjusted_result = sum(match.adjusted_result for match in home_window)
    away_adjusted_result = sum(match.adjusted_result for match in away_window)
    home_adjusted_gd = sum(match.adjusted_gd for match in home_window)
    away_adjusted_gd = sum(match.adjusted_gd for match in away_window)

    result_score, result_denominator = _relative_score(home_adjusted_result, away_adjusted_result)
    gd_score, gd_denominator = _relative_score(home_adjusted_gd, away_adjusted_gd)

    m4_edge_raw = (RESULT_SCORE_WEIGHT * result_score) + (GD_SCORE_WEIGHT * gd_score)
    m4_edge = clamp(m4_edge_raw, -1.0, 1.0)
    m4_bias = calculate_bias(m4_edge)
    m4_strength = classify_strength(m4_edge)

    m4_status = "ACTIVE" if home_valid_matches >= WINDOW_SIZE and away_valid_matches >= WINDOW_SIZE else "DEGRADED"
    m4_status_reason = "active" if m4_status == "ACTIVE" else "partial_window"

    if debug_mode:
        logger.info("--- M4 Quality-Adjusted Immediate State Engine Debug: Event %s (%s) ---", event_id, participants)
        logger.info("  home_team=%s | away_team=%s", home_team, away_team)
        logger.info("  window_size=%s", WINDOW_SIZE)
        logger.info("  n_liga=%s | n_liga_source=%s", n_liga, n_liga_source)
        logger.info("  home_valid_matches=%s | away_valid_matches=%s", home_valid_matches, away_valid_matches)
        logger.info("  home_window=%s", [_serialize_parsed_match(match) for match in home_window])
        logger.info("  away_window=%s", [_serialize_parsed_match(match) for match in away_window])
        logger.info("  invalid_home_matches=%s", invalid_home_matches)
        logger.info("  invalid_away_matches=%s", invalid_away_matches)

        for label, window in (("HOME", home_window), ("AWAY", away_window)):
            for index, match in enumerate(window):
                logger.info(
                    (
                        "  [%s][%s] opponent=%s rank=%s strength=%.12f result=%s "
                        "result_value=%.12f gd=%.12f adjusted_result=%.12f adjusted_gd=%.12f"
                    ),
                    label,
                    index,
                    match.opponent_name,
                    match.opponent_rank,
                    match.opponent_strength,
                    match.team_result,
                    match.result_value,
                    match.goal_difference,
                    match.adjusted_result,
                    match.adjusted_gd,
                )

        logger.info(
            "  [HOME_SUMS] adjusted_result=%.12f adjusted_gd=%.12f",
            home_adjusted_result,
            home_adjusted_gd,
        )
        logger.info(
            "  [AWAY_SUMS] adjusted_result=%.12f adjusted_gd=%.12f",
            away_adjusted_result,
            away_adjusted_gd,
        )
        if result_denominator == 0:
            logger.info(
                "  [RESULT_SCORE] denominator=0 -> score=0.000000000000 reason=ZERO_DENOMINATOR"
            )
        else:
            logger.info(
                "  [RESULT_SCORE] (home_adjusted_result=%.12f - away_adjusted_result=%.12f) / denominator=%.12f = %.12f",
                home_adjusted_result,
                away_adjusted_result,
                result_denominator,
                result_score,
            )
        if gd_denominator == 0:
            logger.info(
                "  [GD_SCORE] denominator=0 -> score=0.000000000000 reason=ZERO_DENOMINATOR"
            )
        else:
            logger.info(
                "  [GD_SCORE] (home_adjusted_gd=%.12f - away_adjusted_gd=%.12f) / denominator=%.12f = %.12f",
                home_adjusted_gd,
                away_adjusted_gd,
                gd_denominator,
                gd_score,
            )
        logger.info(
            "  [M4_EDGE_RAW] (%.2f * %.12f) + (%.2f * %.12f) = %.12f",
            RESULT_SCORE_WEIGHT,
            result_score,
            GD_SCORE_WEIGHT,
            gd_score,
            m4_edge_raw,
        )
        logger.info("  [M4_EDGE] clamped=%.12f bias=%s strength=%s status=%s (%s)", m4_edge, m4_bias, m4_strength, m4_status, m4_status_reason)
        logger.info("  --- Component Summary ---")
        logger.info(
            "  RESULT_SCORE: edge=%.12f weight=%.2f weighted=%.12f bias=%s strength=%s",
            result_score,
            RESULT_SCORE_WEIGHT,
            result_score * RESULT_SCORE_WEIGHT,
            calculate_bias(result_score),
            classify_strength(result_score),
        )
        logger.info(
            "  GD_SCORE: edge=%.12f weight=%.2f weighted=%.12f bias=%s strength=%s",
            gd_score,
            GD_SCORE_WEIGHT,
            gd_score * GD_SCORE_WEIGHT,
            calculate_bias(gd_score),
            classify_strength(gd_score),
        )
        logger.info("------------------------------------------------------------")

    components = [
        _component(
            "RESULT_SCORE",
            result_score,
            RESULT_SCORE_WEIGHT,
            {
                "home_adjusted_result": home_adjusted_result,
                "away_adjusted_result": away_adjusted_result,
                "denominator": result_denominator,
                "formula": "(home_adjusted_result - away_adjusted_result) / (abs(home_adjusted_result) + abs(away_adjusted_result))",
                "reason": "ZERO_DENOMINATOR" if result_denominator == 0 else "active",
            },
        ),
        _component(
            "GD_SCORE",
            gd_score,
            GD_SCORE_WEIGHT,
            {
                "home_adjusted_gd": home_adjusted_gd,
                "away_adjusted_gd": away_adjusted_gd,
                "denominator": gd_denominator,
                "formula": "(home_adjusted_gd - away_adjusted_gd) / (abs(home_adjusted_gd) + abs(away_adjusted_gd))",
                "reason": "ZERO_DENOMINATOR" if gd_denominator == 0 else "active",
            },
        ),
    ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "window_size": WINDOW_SIZE,
        "home_valid_matches": home_valid_matches,
        "away_valid_matches": away_valid_matches,
        "home_window": [_serialize_parsed_match(match) for match in home_window],
        "away_window": [_serialize_parsed_match(match) for match in away_window],
        "n_liga": n_liga,
        "n_liga_source": n_liga_source,
        "home_adjusted_result": home_adjusted_result,
        "away_adjusted_result": away_adjusted_result,
        "home_adjusted_gd": home_adjusted_gd,
        "away_adjusted_gd": away_adjusted_gd,
        "result_score": result_score,
        "gd_score": gd_score,
        "result_score_weight": RESULT_SCORE_WEIGHT,
        "gd_score_weight": GD_SCORE_WEIGHT,
        "m4_edge_raw": m4_edge_raw,
        "m4_edge": m4_edge,
        "m4_abs_edge": abs(m4_edge),
        "m4_bias": m4_bias,
        "m4_strength": m4_strength,
        "m4_status": m4_status,
        "m4_status_reason": m4_status_reason,
        "invalid_home_matches": invalid_home_matches,
        "invalid_away_matches": invalid_away_matches,
        "engine_profile": ENGINE_PROFILE,
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M4",
        module_name="Quality-Adjusted Immediate State Engine",
        event_id=event_id,
        participants=participants,
        value=m4_edge,
        bias=m4_bias,
        strength=m4_strength,
        components=components,
        raw=raw,
    )
