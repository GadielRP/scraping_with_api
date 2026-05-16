"""M3 - Direct Matchup Profile / H2H module."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext


@dataclass(frozen=True)
class ParsedH2HMatch:
    diff: float
    win: float
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


def _parse_h2h_match(match: Dict[str, Any], home_name: str, away_name: str) -> Optional[ParsedH2HMatch]:
    current_home_was_home = _infer_current_home_was_home(match, home_name, away_name)
    if current_home_was_home is None:
        return None

    home_score = _coerce_float(match.get("home_score"))
    away_score = _coerce_float(match.get("away_score"))
    if home_score is not None and away_score is not None:
        diff = home_score - away_score
        return ParsedH2HMatch(
            diff=diff,
            win=_win_from_diff(diff),
            current_home_was_home=current_home_was_home,
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
        diff = hist_home_score - hist_away_score
    elif hist_home == away_name and hist_away == home_name:
        diff = hist_away_score - hist_home_score
    else:
        return None

    return ParsedH2HMatch(
        diff=diff,
        win=_win_from_diff(diff),
        current_home_was_home=current_home_was_home,
        start_timestamp=_coerce_int(match.get("startTimestamp")),
        raw=match,
    )


def _harmonic_weights(n: int) -> List[float]:
    if n <= 0:
        return []
    raw_weights = [1.0 / float(i + 1) for i in range(n)]
    total = sum(raw_weights)
    return [weight / total for weight in raw_weights] if total else []


def _sport_factor(sport: str) -> float:
    normalized = str(sport or "").lower()
    if "football" in normalized or "soccer" in normalized:
        return 1.25
    if "hockey" in normalized:
        return 2.0
    if "basketball" in normalized:
        return 10.0
    if "baseball" in normalized:
        return 3.0
    return 1.0


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


def _manual_strength_hint(edge: float) -> Optional[str]:
    abs_edge = abs(edge)
    if 0.60 <= abs_edge < 0.85:
        return "VERY_HIGH"
    return None


def _serialize_parsed_match(match: ParsedH2HMatch) -> Dict[str, Any]:
    return {
        "diff": match.diff,
        "win": match.win,
        "current_home_was_home": match.current_home_was_home,
        "start_timestamp": match.start_timestamp,
    }


def _inactive_result(
    *,
    event_id: int,
    participants: str,
    home_team: str,
    away_team: str,
    total_h2h: int,
    analyzed_total_h2h: Optional[int],
    h2h_note: str,
    parsed_h2h: List[Dict[str, Any]],
) -> ModuleResult:
    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M3",
        module_name="Direct Matchup Profile",
        event_id=event_id,
        participants=participants,
        value=0.0,
        bias=calculate_bias(0.0),
        strength=classify_strength(0.0),
        components=[],
        raw={
            "home_team": home_team,
            "away_team": away_team,
            "total_h2h": total_h2h,
            "h2h_matchup_matches_analyzed": analyzed_total_h2h,
            "parsed_h2h_count": len(parsed_h2h),
            "m3_edge_raw": 0.0,
            "m3_edge": 0.0,
            "m3_abs_edge": 0.0,
            "m3_status": "INACTIVE",
            "m3_status_reason": "INSUFFICIENT_H2H_SAMPLE",
            "h2h_note": h2h_note,
            "parsed_h2h": parsed_h2h,
        },
    )


def calculate_direct_matchup_profile(
    streak_analysis: Any,
    event_context: EventContext,
) -> ModuleResult:
    """Calculate M3 - Direct Matchup Profile for an event."""
    home_team = getattr(streak_analysis, "home_team_name", None) or event_context.home.name
    away_team = getattr(streak_analysis, "away_team_name", None) or event_context.away.name
    participants = getattr(streak_analysis, "participants", None) or event_context.participants_label
    event_id = getattr(streak_analysis, "event_id", 0)
    h2h_matches = getattr(streak_analysis, "h2h_matchup_matches", []) or []
    analyzed_total_h2h = getattr(streak_analysis, "h2h_matchup_matches_analyzed", None)
    sport = getattr(streak_analysis, "sport", None) or event_context.sport

    parsed_matches = [
        parsed
        for parsed in (
            _parse_h2h_match(match, _norm(home_team), _norm(away_team))
            for match in h2h_matches
        )
        if parsed is not None
    ]
    if any(match.start_timestamp is not None for match in parsed_matches):
        parsed_matches.sort(key=lambda match: match.start_timestamp or 0, reverse=True)

    parsed_h2h = [_serialize_parsed_match(match) for match in parsed_matches]
    total_h2h = len(h2h_matches)
    sample_h2h = len(parsed_matches)

    if sample_h2h < 2:
        h2h_note = (
            "Single recent H2H exists but sample is insufficient for operational edge."
            if total_h2h == 1
            else "No valid recent H2H sample."
        )
        return _inactive_result(
            event_id=event_id,
            participants=participants,
            home_team=home_team,
            away_team=away_team,
            total_h2h=total_h2h,
            analyzed_total_h2h=analyzed_total_h2h,
            h2h_note=h2h_note,
            parsed_h2h=parsed_h2h,
        )

    weights = _harmonic_weights(sample_h2h)
    weighted_winrate_home = sum(match.win * weight for match, weight in zip(parsed_matches, weights))
    weighted_winrate_away = 1.0 - weighted_winrate_home
    h2h_win_edge = (2.0 * weighted_winrate_home) - 1.0

    weighted_diff = sum(match.diff * weight for match, weight in zip(parsed_matches, weights))
    if sample_h2h >= 5:
        diff_scale = sum(abs(match.diff) for match in parsed_matches) / float(sample_h2h)
        diff_scale_source = "dynamic"
    else:
        diff_scale = _sport_factor(sport)
        diff_scale_source = "sport_factor"

    h2h_diff_reason = "active"
    if diff_scale <= 0:
        h2h_diff_edge = 0.0
        h2h_diff_reason = "missing_diff_scale"
    else:
        h2h_diff_edge = clamp(weighted_diff / diff_scale)

    venue_matches = [match for match in parsed_matches if match.current_home_was_home]
    if venue_matches:
        venue_winrate_home = sum(match.win for match in venue_matches) / float(len(venue_matches))
        h2h_venue_edge = (2.0 * venue_winrate_home) - 1.0
        venue_reason = "active"
    else:
        venue_winrate_home = 0.0
        h2h_venue_edge = 0.0
        venue_reason = "no_current_venue_h2h"

    m3_edge_raw = (
        0.50 * h2h_win_edge
        + 0.30 * h2h_diff_edge
        + 0.20 * h2h_venue_edge
    )
    m3_edge = clamp(m3_edge_raw)

    components = [
        _component(
            "H2H_WIN_EDGE",
            h2h_win_edge,
            0.50,
            {
                "weighted_winrate_home": weighted_winrate_home,
                "weighted_winrate_away": weighted_winrate_away,
            },
        ),
        _component(
            "H2H_DIFF_EDGE",
            h2h_diff_edge,
            0.30,
            {
                "weighted_diff": weighted_diff,
                "diff_scale": diff_scale,
                "diff_scale_source": diff_scale_source,
                "reason": h2h_diff_reason,
            },
        ),
        _component(
            "H2H_VENUE_EDGE",
            h2h_venue_edge,
            0.20,
            {
                "venue_match_count": len(venue_matches),
                "venue_winrate_home": venue_winrate_home,
                "reason": venue_reason,
            },
        ),
    ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "total_h2h": total_h2h,
        "h2h_matchup_matches_analyzed": analyzed_total_h2h,
        "parsed_h2h_count": sample_h2h,
        "m3_edge_raw": m3_edge_raw,
        "m3_edge": m3_edge,
        "m3_abs_edge": abs(m3_edge),
        "m3_status": "ACTIVE",
        "m3_status_reason": "active",
        "h2h_weighted_winrate_home": weighted_winrate_home,
        "h2h_weighted_winrate_away": weighted_winrate_away,
        "h2h_weighted_diff": weighted_diff,
        "h2h_diff_scale": diff_scale,
        "h2h_diff_scale_source": diff_scale_source,
        "sport_factor": _sport_factor(sport),
        "venue_match_count": len(venue_matches),
        "venue_winrate_home": venue_winrate_home,
        "weights": weights,
        "parsed_h2h": parsed_h2h,
    }

    manual_hint = _manual_strength_hint(m3_edge)
    if manual_hint is not None:
        raw["manual_strength_hint"] = manual_hint

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M3",
        module_name="Direct Matchup Profile",
        event_id=event_id,
        participants=participants,
        value=m3_edge,
        bias=calculate_bias(m3_edge),
        strength=classify_strength(m3_edge),
        components=components,
        raw=raw,
    )
