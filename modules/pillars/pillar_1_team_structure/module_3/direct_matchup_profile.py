"""M3 - Matchup Engine / sample-aware direct matchup profile."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from modules.pillars.common import (
    DEFAULT_STRENGTH_MAX_LABEL,
    DEFAULT_STRENGTH_THRESHOLDS,
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)

M3_STRENGTH_THRESHOLDS = DEFAULT_STRENGTH_THRESHOLDS
M3_STRENGTH_MAX_LABEL = DEFAULT_STRENGTH_MAX_LABEL
M3_STRENGTH_PROFILE = "M3_matchup_engine_sample_aware_v1"

WIN_MATCHUP_WEIGHT = 0.70
GOAL_MATCHUP_WEIGHT = 0.30


@dataclass(frozen=True)
class ParsedH2HMatch:
    home_goals: float
    away_goals: float
    diff: float
    result_value_home: float
    current_home_was_home: bool
    start_timestamp: Optional[int]
    raw: Dict[str, Any]


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


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


def _win_from_diff(diff: float) -> float:
    if diff > 0:
        return 1.0
    if diff < 0:
        return 0.0
    return 0.5


def _infer_current_home_was_home(match: Dict[str, Any], home_name: str, away_name: str) -> Optional[bool]:
    role = _norm(match.get("upcoming_home_role"))
    if role == "home":
        return True
    if role == "away":
        return False

    hist_home = _norm(match.get("hist_home"))
    hist_away = _norm(match.get("hist_away"))
    if hist_home == home_name and hist_away == away_name:
        return True
    if hist_home == away_name and hist_away == home_name:
        return False
    return None


def _classify_sample_confidence(sample_factor: float) -> str:
    if sample_factor <= 0:
        return "NONE"
    if sample_factor < 0.25:
        return "VERY LOW"
    if sample_factor < 0.50:
        return "LOW"
    if sample_factor < 0.75:
        return "MEDIUM"
    if sample_factor <= 0.90:
        return "HIGH"
    return "VERY HIGH"


def _parse_h2h_match(match: Dict[str, Any], home_name: str, away_name: str) -> Optional[ParsedH2HMatch]:
    current_home_was_home = _infer_current_home_was_home(match, home_name, away_name)

    home_score = _coerce_float(match.get("home_score"))
    away_score = _coerce_float(match.get("away_score"))
    if home_score is not None and away_score is not None:
        diff = home_score - away_score
        return ParsedH2HMatch(
            home_goals=home_score,
            away_goals=away_score,
            diff=diff,
            result_value_home=_win_from_diff(diff),
            current_home_was_home=bool(current_home_was_home) if current_home_was_home is not None else False,
            start_timestamp=_coerce_int(match.get("startTimestamp")),
            raw=match,
        )

    hist_home_score = _coerce_float(match.get("hist_home_score"))
    hist_away_score = _coerce_float(match.get("hist_away_score"))
    hist_home = _norm(match.get("hist_home"))
    hist_away = _norm(match.get("hist_away"))
    if hist_home_score is None or hist_away_score is None:
        return None
    if hist_home == home_name and hist_away == away_name:
        home_goals = hist_home_score
        away_goals = hist_away_score
        current_home_was_home = True
    elif hist_home == away_name and hist_away == home_name:
        home_goals = hist_away_score
        away_goals = hist_home_score
        current_home_was_home = False
    else:
        return None

    diff = home_goals - away_goals
    return ParsedH2HMatch(
        home_goals=home_goals,
        away_goals=away_goals,
        diff=diff,
        result_value_home=_win_from_diff(diff),
        current_home_was_home=current_home_was_home,
        start_timestamp=_coerce_int(match.get("startTimestamp")),
        raw=match,
    )


def _component(name: str, edge: float, weight: float, raw: Dict[str, Any]) -> ModuleComponentResult:
    return ModuleComponentResult(
        name=name,
        edge=edge,
        bias=calculate_bias(edge),
        strength=classify_strength(
            edge,
            thresholds=DEFAULT_STRENGTH_THRESHOLDS,
            max_label=DEFAULT_STRENGTH_MAX_LABEL,
        ),
        weight=weight,
        weighted_edge=edge * weight,
        raw=raw,
    )


def _serialize_parsed_match(match: ParsedH2HMatch) -> Dict[str, Any]:
    return {
        "home_goals": match.home_goals,
        "away_goals": match.away_goals,
        "diff": match.diff,
        "result_value_home": match.result_value_home,
        "current_home_was_home": match.current_home_was_home,
        "start_timestamp": match.start_timestamp,
    }


def _build_inactive_result(
    *,
    event_id: int,
    participants: str,
    home_team: str,
    away_team: str,
    total_h2h: int,
    analyzed_total_h2h: Optional[int],
    parsed_h2h: List[Dict[str, Any]],
) -> ModuleResult:
    raw_edge = 0.0
    sample_factor = 0.0
    sample_confidence = _classify_sample_confidence(sample_factor)
    strength = classify_strength(
        0.0,
        thresholds=DEFAULT_STRENGTH_THRESHOLDS,
        max_label=DEFAULT_STRENGTH_MAX_LABEL,
    )
    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M3",
        module_name="Matchup Engine",
        event_id=event_id,
        participants=participants,
        value=0.0,
        bias=calculate_bias(0.0),
        strength=strength,
        components=[],
        raw={
            "home_team": home_team,
            "away_team": away_team,
            "total_h2h": total_h2h,
            "h2h_matchup_matches_analyzed": analyzed_total_h2h,
            "parsed_h2h_count": 0,
            "h2h_home_wins": 0,
            "h2h_away_wins": 0,
            "h2h_draws": 0,
            "h2h_gf_home": 0.0,
            "h2h_gf_away": 0.0,
            "win_rate_home": 0.0,
            "win_rate_away": 0.0,
            "win_matchup_edge_raw": 0.0,
            "goal_matchup_edge_raw": 0.0,
            "win_matchup_weight": WIN_MATCHUP_WEIGHT,
            "goal_matchup_weight": GOAL_MATCHUP_WEIGHT,
            "m3_raw_edge": raw_edge,
            "m3_edge_raw": raw_edge,
            "m3_raw_strength": strength,
            "m3_sample_factor": sample_factor,
            "m3_sample_confidence": sample_confidence,
            "m3_edge": 0.0,
            "m3_abs_edge": 0.0,
            "m3_strength": strength,
            "m3_bias": calculate_bias(0.0),
            "m3_status": "INACTIVE",
            "m3_status_reason": "NO_VALID_H2H_SAMPLE",
            "parsed_h2h": parsed_h2h,
            "strength_threshold_profile": M3_STRENGTH_PROFILE,
            "strength_thresholds": DEFAULT_STRENGTH_THRESHOLDS,
            "strength_max_label": DEFAULT_STRENGTH_MAX_LABEL,
        },
    )


def calculate_direct_matchup_profile(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    """Calculate M3 - Matchup Engine for an event."""
    home_team = getattr(streak_analysis, "home_team_name", None) or event_context.home.name
    away_team = getattr(streak_analysis, "away_team_name", None) or event_context.away.name
    participants = getattr(streak_analysis, "participants", None) or event_context.participants_label
    event_id = getattr(streak_analysis, "event_id", 0)
    h2h_matchup_matches = getattr(streak_analysis, "h2h_matchup_matches", []) or []
    analyzed_total_h2h = getattr(streak_analysis, "h2h_matchup_matches_analyzed", None)

    parsed_matches = [
        parsed
        for parsed in (
            _parse_h2h_match(match, _norm(home_team), _norm(away_team))
            for match in h2h_matchup_matches
        )
        if parsed is not None
    ]
    if any(match.start_timestamp is not None for match in parsed_matches):
        parsed_matches.sort(key=lambda match: match.start_timestamp or 0, reverse=True)

    parsed_h2h = [_serialize_parsed_match(match) for match in parsed_matches]
    total_h2h = len(h2h_matchup_matches)
    h2h_matches = len(parsed_matches)

    if debug_mode:
        logger.info(f"--- M3 Matchup Engine Debug: Event {event_id} ({participants}) ---")
        logger.info(
            f"  home_team={home_team}  away_team={away_team}  "
            f"total_h2h_raw={total_h2h}  parsed_valid={h2h_matches}  "
            f"activation={'ACTIVE' if h2h_matches >= 1 else 'INACTIVE'}"
        )

    if h2h_matches == 0:
        if debug_mode:
            logger.info("  => M3 INACTIVE: NO_VALID_H2H_SAMPLE")
            logger.info("  [M3_RAW_EDGE] 0.000000000000  strength=IGNORE")
            logger.info("  [SAMPLE_FACTOR] 0.000000000000")
            logger.info("  [SAMPLE_CONFIDENCE] NONE")
            logger.info(
                "  [M3_EDGE] clamped=0.000000000000  bias=NEUTRAL  strength=IGNORE  status=INACTIVE"
            )
            logger.info("-" * 60)
        return _build_inactive_result(
            event_id=event_id,
            participants=participants,
            home_team=home_team,
            away_team=away_team,
            total_h2h=total_h2h,
            analyzed_total_h2h=analyzed_total_h2h,
            parsed_h2h=parsed_h2h,
        )

    home_wins = sum(1 for match in parsed_matches if match.diff > 0)
    away_wins = sum(1 for match in parsed_matches if match.diff < 0)
    draws = sum(1 for match in parsed_matches if match.diff == 0)

    win_rate_home = (home_wins + 0.5 * draws) / float(h2h_matches)
    win_rate_away = (away_wins + 0.5 * draws) / float(h2h_matches)
    win_matchup_edge_raw = clamp(win_rate_home - win_rate_away)

    h2h_gf_home = sum(match.home_goals for match in parsed_matches)
    h2h_gf_away = sum(match.away_goals for match in parsed_matches)
    goal_total = h2h_gf_home + h2h_gf_away
    goal_matchup_reason = "active"
    if goal_total > 0:
        goal_matchup_edge_raw = clamp((h2h_gf_home - h2h_gf_away) / goal_total)
    else:
        goal_matchup_edge_raw = 0.0
        goal_matchup_reason = "NO_GOALS_TOTAL"

    m3_raw_edge = clamp(
        WIN_MATCHUP_WEIGHT * win_matchup_edge_raw
        + GOAL_MATCHUP_WEIGHT * goal_matchup_edge_raw
    )
    m3_sample_factor = min(h2h_matches / 5.0, 1.0)
    m3_sample_confidence = _classify_sample_confidence(m3_sample_factor)
    m3_edge = clamp(m3_raw_edge * m3_sample_factor)

    m3_raw_strength = classify_strength(
        m3_raw_edge,
        thresholds=DEFAULT_STRENGTH_THRESHOLDS,
        max_label=DEFAULT_STRENGTH_MAX_LABEL,
    )
    m3_strength = classify_strength(
        m3_edge,
        thresholds=DEFAULT_STRENGTH_THRESHOLDS,
        max_label=DEFAULT_STRENGTH_MAX_LABEL,
    )
    m3_bias = calculate_bias(m3_edge)

    if debug_mode:
        logger.info("  [WIN_MATCHUP_LAYER] substitution:")
        for i, match in enumerate(parsed_matches):
            logger.info(
                f"    i={i}: home_goals={match.home_goals:.12f} "
                f"away_goals={match.away_goals:.12f} diff={match.diff:+.12f} "
                f"result_value_home={match.result_value_home:.12f}"
            )
        logger.info(
            f"  [WIN_MATCHUP_LAYER] home_wins={home_wins}  away_wins={away_wins}  draws={draws}"
        )
        logger.info(
            f"  [WIN_MATCHUP_LAYER] win_rate_home={win_rate_home:.12f}  "
            f"win_rate_away={win_rate_away:.12f}  edge_raw={win_matchup_edge_raw:.12f}  "
            f"bias={calculate_bias(win_matchup_edge_raw)}  "
            f"strength={classify_strength(win_matchup_edge_raw, thresholds=DEFAULT_STRENGTH_THRESHOLDS, max_label=DEFAULT_STRENGTH_MAX_LABEL)}"
        )
        logger.info("  [GOAL_MATCHUP_LAYER] substitution:")
        for i, match in enumerate(parsed_matches):
            logger.info(
                f"    i={i}: home_goals={match.home_goals:.12f} "
                f"away_goals={match.away_goals:.12f} diff={match.diff:+.12f}"
            )
        logger.info(
            f"  [GOAL_MATCHUP_LAYER] h2h_gf_home={h2h_gf_home:.12f}  "
            f"h2h_gf_away={h2h_gf_away:.12f}  goal_total={goal_total:.12f}  "
            f"edge_raw={goal_matchup_edge_raw:.12f}  reason={goal_matchup_reason}  "
            f"bias={calculate_bias(goal_matchup_edge_raw)}  "
            f"strength={classify_strength(goal_matchup_edge_raw, thresholds=DEFAULT_STRENGTH_THRESHOLDS, max_label=DEFAULT_STRENGTH_MAX_LABEL)}"
        )
        logger.info(
            f"  [M3_RAW_EDGE] ({WIN_MATCHUP_WEIGHT:.2f} * {win_matchup_edge_raw:.12f}) + "
            f"({GOAL_MATCHUP_WEIGHT:.2f} * {goal_matchup_edge_raw:.12f}) = {m3_raw_edge:.12f}  "
            f"strength={m3_raw_strength}"
        )
        logger.info(
            f"  [SAMPLE_FACTOR] min({h2h_matches}/5, 1) = {m3_sample_factor:.12f}"
        )
        logger.info(f"  [SAMPLE_CONFIDENCE] {m3_sample_confidence}")
        logger.info(
            f"  [M3_EDGE] clamped={m3_edge:.12f}  bias={m3_bias}  "
            f"strength={m3_strength}  status=ACTIVE"
        )
        logger.info("-" * 60)

    components = [
        _component(
            "WIN_MATCHUP_EDGE_RAW",
            win_matchup_edge_raw,
            WIN_MATCHUP_WEIGHT,
            {
                "home_wins": home_wins,
                "away_wins": away_wins,
                "draws": draws,
                "win_rate_home": win_rate_home,
                "win_rate_away": win_rate_away,
            },
        ),
        _component(
            "GOAL_MATCHUP_EDGE_RAW",
            goal_matchup_edge_raw,
            GOAL_MATCHUP_WEIGHT,
            {
                "h2h_gf_home": h2h_gf_home,
                "h2h_gf_away": h2h_gf_away,
                "goal_total": goal_total,
                "reason": goal_matchup_reason,
            },
        ),
    ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "total_h2h": total_h2h,
        "h2h_matchup_matches_analyzed": analyzed_total_h2h,
        "parsed_h2h_count": h2h_matches,
        "h2h_home_wins": home_wins,
        "h2h_away_wins": away_wins,
        "h2h_draws": draws,
        "h2h_gf_home": h2h_gf_home,
        "h2h_gf_away": h2h_gf_away,
        "win_rate_home": win_rate_home,
        "win_rate_away": win_rate_away,
        "win_matchup_edge_raw": win_matchup_edge_raw,
        "goal_matchup_edge_raw": goal_matchup_edge_raw,
        "win_matchup_weight": WIN_MATCHUP_WEIGHT,
        "goal_matchup_weight": GOAL_MATCHUP_WEIGHT,
        "m3_raw_edge": m3_raw_edge,
        "m3_edge_raw": m3_raw_edge,
        "m3_raw_strength": m3_raw_strength,
        "m3_sample_factor": m3_sample_factor,
        "m3_sample_confidence": m3_sample_confidence,
        "m3_edge": m3_edge,
        "m3_abs_edge": abs(m3_edge),
        "m3_strength": m3_strength,
        "m3_bias": m3_bias,
        "m3_status": "ACTIVE",
        "m3_status_reason": "active",
        "parsed_h2h": parsed_h2h,
        "strength_threshold_profile": M3_STRENGTH_PROFILE,
        "strength_thresholds": DEFAULT_STRENGTH_THRESHOLDS,
        "strength_max_label": DEFAULT_STRENGTH_MAX_LABEL,
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M3",
        module_name="Matchup Engine",
        event_id=event_id,
        participants=participants,
        value=m3_edge,
        bias=m3_bias,
        strength=m3_strength,
        components=components,
        raw=raw,
    )
