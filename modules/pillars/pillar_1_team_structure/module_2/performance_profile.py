"""M2 - Performance Profile module.

Measures how each team produces per match by comparing goals scored,
goals conceded, and net balance across the shared sample of results.

This module is pure: it receives a pre-built ``MatchupStreakContext`` and
returns a structured ``ModuleResult``. It never calls external APIs, sends
messages, or writes to the database.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext
from modules.pillars.score_series import extract_gf_ga_series


@dataclass(frozen=True)
class TeamPerformanceProfile:
    n: int
    total_gf: float
    total_ga: float
    gf_mean: float
    ga_mean: float
    net_mean: float


def _build_team_performance_profile(results: List[Dict]) -> TeamPerformanceProfile:
    series = extract_gf_ga_series(results)
    n = len(series)
    total_gf = sum(gf for gf, _ in series)
    total_ga = sum(ga for _, ga in series)
    if n == 0:
        return TeamPerformanceProfile(
            n=0,
            total_gf=0.0,
            total_ga=0.0,
            gf_mean=0.0,
            ga_mean=0.0,
            net_mean=0.0,
        )

    gf_mean = total_gf / float(n)
    ga_mean = total_ga / float(n)
    net_mean = gf_mean - ga_mean
    return TeamPerformanceProfile(
        n=n,
        total_gf=total_gf,
        total_ga=total_ga,
        gf_mean=gf_mean,
        ga_mean=ga_mean,
        net_mean=net_mean,
    )


def _as_profile_raw(profile: TeamPerformanceProfile) -> Dict[str, Any]:
    return asdict(profile)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _calculate_offense_edge(home_profile: TeamPerformanceProfile, away_profile: TeamPerformanceProfile) -> float:
    denominator = home_profile.gf_mean + away_profile.gf_mean
    return _safe_ratio(home_profile.gf_mean - away_profile.gf_mean, denominator)


def _calculate_defense_edge(home_profile: TeamPerformanceProfile, away_profile: TeamPerformanceProfile) -> float:
    denominator = home_profile.ga_mean + away_profile.ga_mean
    return _safe_ratio(away_profile.ga_mean - home_profile.ga_mean, denominator)


def _calculate_net_edge(home_profile: TeamPerformanceProfile, away_profile: TeamPerformanceProfile) -> float:
    denominator = abs(home_profile.net_mean) + abs(away_profile.net_mean)
    if denominator < 0.10:
        return 0.0
    return _safe_ratio(home_profile.net_mean - away_profile.net_mean, denominator)


def _coerce_positive_int(value: Any) -> Optional[int]:
    try:
        coerced = int(value)
    except (TypeError, ValueError):
        return None
    return coerced if coerced > 0 else None


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
    home_profile: TeamPerformanceProfile,
    away_profile: TeamPerformanceProfile,
    total_regular_season_games: Optional[int],
    maturity: float,
) -> Tuple[str, str]:
    if home_profile.n == 0 or away_profile.n == 0:
        return "INSUFFICIENT_DATA", "insufficient_results"
    if total_regular_season_games is None:
        return "DEGRADED", "missing_total_regular_season_games"
    if maturity < 0.25:
        return "DEGRADED", "low_maturity"
    return "ACTIVE", "active"


def calculate_performance_profile(
    streak_analysis: Any,
    event_context: EventContext,
) -> ModuleResult:
    """Calculate M2 - Performance Profile for an event."""
    home_results: List[Dict] = getattr(streak_analysis, "home_team_results", None) or []
    away_results: List[Dict] = getattr(streak_analysis, "away_team_results", None) or []
    event_id: int = getattr(streak_analysis, "event_id", 0)
    participants: str = getattr(streak_analysis, "participants", "")

    home_profile = _build_team_performance_profile(home_results)
    away_profile = _build_team_performance_profile(away_results)

    total_regular_season_games = _coerce_positive_int(
        getattr(event_context.competition, "total_regular_season_games", None)
    )

    common_n = min(home_profile.n, away_profile.n)
    if home_profile.n == 0 or away_profile.n == 0:
        maturity = 0.0
        maturity_source = "insufficient_data"
    elif total_regular_season_games is None:
        maturity = 1.0
        maturity_source = "missing_total_regular_season_games"
    else:
        maturity = min(1.0, max(0.0, common_n / float(total_regular_season_games)))
        maturity_source = "competition.total_regular_season_games"

    offense_edge = _calculate_offense_edge(home_profile, away_profile)
    defense_edge = _calculate_defense_edge(home_profile, away_profile)
    net_edge = _calculate_net_edge(home_profile, away_profile)

    m2_edge_raw = 0.40 * offense_edge + 0.30 * defense_edge + 0.30 * net_edge
    m2_edge_effective = clamp(m2_edge_raw * maturity)

    status, status_reason = _determine_status(
        home_profile=home_profile,
        away_profile=away_profile,
        total_regular_season_games=total_regular_season_games,
        maturity=maturity,
    )

    if status == "INSUFFICIENT_DATA":
        m2_edge_effective = 0.0
        m2_edge_raw = 0.0

    components = [
        _component(
            "OFFENSE_EDGE",
            offense_edge,
            0.40,
            {"home_gf_mean": home_profile.gf_mean, "away_gf_mean": away_profile.gf_mean},
        ),
        _component(
            "DEFENSE_EDGE",
            defense_edge,
            0.30,
            {"home_ga_mean": home_profile.ga_mean, "away_ga_mean": away_profile.ga_mean},
        ),
        _component(
            "NET_EDGE",
            net_edge,
            0.30,
            {"home_net_mean": home_profile.net_mean, "away_net_mean": away_profile.net_mean},
        ),
    ]

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M2",
        module_name="Performance Profile",
        event_id=event_id,
        participants=participants,
        value=m2_edge_effective,
        bias=calculate_bias(m2_edge_effective),
        strength=classify_strength(m2_edge_effective),
        components=components,
        raw={
            "home_team": getattr(streak_analysis, "home_team_name", None),
            "away_team": getattr(streak_analysis, "away_team_name", None),
            "home_profile": _as_profile_raw(home_profile),
            "away_profile": _as_profile_raw(away_profile),
            "m2_maturity": maturity,
            "m2_maturity_source": maturity_source,
            "total_regular_season_games": total_regular_season_games,
            "m2_edge_raw": m2_edge_raw,
            "m2_edge_effective": m2_edge_effective,
            "m2_status": status,
            "m2_status_reason": status_reason,
        },
    )
