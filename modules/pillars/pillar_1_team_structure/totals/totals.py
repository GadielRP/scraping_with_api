"""P1 Totals - structural + temporal totals engine.

This module estimates whether a matchup lives in an over, under, or neutral
goal environment using structural season totals, volatility, temporal form,
and trend pressure. The implementation is pure and defensive: it reads only
the provided ``streak_analysis`` and ``event_context`` inputs and returns a
``P1TotalsOutput``.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from modules.pillars.context import EventContext
from .totals_debug import _log_p1_totals_debug

logger = logging.getLogger(__name__)

TOTALS_TEMPORAL_WINDOW_CONFIG: Tuple[Tuple[str, float, float], ...] = (
    ("TOTALS_SHORT", 0.15, 0.30),
    ("TOTALS_RECENT", 0.35, 0.35),
    ("TOTALS_MID", 0.60, 0.20),
    ("TOTALS_FULL", 1.00, 0.15),
)

TOTALS_TEMPORAL_WINDOW_NAMES: Tuple[str, ...] = tuple(name for name, _, _ in TOTALS_TEMPORAL_WINDOW_CONFIG)

LAYER_BASE_WEIGHTS: Dict[str, float] = {
    "STRUCTURAL": 0.40,
    "VOL": 0.20,
    "TEMPORAL": 0.25,
    "TREND": 0.15,
}

IGNORE_THRESHOLD = 0.05
EPSILON = 1e-12


@dataclass(frozen=True)
class P1TotalsLayerOutput:
    layer: str
    raw_signal: Optional[float]
    final_signal: Optional[float]
    base_weight: float
    effective_weight: float
    weighted_signal: float
    ignored: bool
    ignored_reason: Optional[str]


@dataclass(frozen=True)
class P1TotalsOutput:
    P1_TOTALS_SCORE: float
    P1_TOTALS_DIRECTION: str
    P1_TOTALS_STRENGTH: str
    P1_TOTALS_INTERNAL_STATE: List[str]
    STRUCTURAL_ANCHOR: Optional[float]
    STRUCTURAL_FINAL: Optional[float]
    VOL_FINAL: Optional[float]
    TEMPORAL_FINAL: Optional[float]
    TREND_FINAL: Optional[float]
    STRUCTURAL_PROFILE_SCORE: Optional[float]
    VOL_EDGE: Optional[float]
    TEMPORAL_PROFILE_SCORE: Optional[float]
    TREND_SIGNAL: Optional[float]
    TREND_BASELINE: Optional[float]
    TREND_DYNAMIC_SCALE: Optional[float]
    MATCHUP_TREND_DELTA: Optional[float]
    LEAGUE_TREND_SAMPLE_COUNT: Optional[int]
    EXPECTED_TOTAL_STRUCTURAL: Optional[float]
    MATCHUP_VOLATILITY: Optional[float]
    MATCHUP_TEMPORAL_TOTAL: Optional[float]
    LEAGUE_TOTAL_BASELINE: Optional[float]
    TOTAL_DYNAMIC_SCALE: Optional[float]
    VOL_BASELINE: Optional[float]
    VOL_DYNAMIC_SCALE: Optional[float]
    active_layers: List[P1TotalsLayerOutput]
    ignored_layers: List[P1TotalsLayerOutput]
    confidence_total: float
    status: str
    status_reason: str
    TEMPORAL_CONFIG: Optional[Dict[str, Any]] = None


_TEAM_TOTAL_G_KEYS = (
    "goals_for",
    "goalsFor",
    "for",
    "gf",
    "scored",
    "scores_for",
    "points_for",
)
_TEAM_TOTAL_A_KEYS = (
    "goals_against",
    "goalsAgainst",
    "against",
    "ga",
    "conceded",
    "scores_against",
    "points_against",
)
_TEAM_TOTAL_GP_KEYS = ("gp", "games_played", "matches", "played")
_TEAM_NAME_KEYS = ("team_name", "teamName", "name", "display_name", "short_name", "team")

_GAME_TOTAL_PAIRS = (
    ("team_score", "opponent_score"),
    ("score_for", "score_against"),
    ("goals_for", "goals_against"),
    ("gf", "ga"),
)

_VOLATILITY_SAMPLE_KEYS = (
    "std_dev_totals",
    "totals_std_dev",
    "std_total",
    "volatility",
    "totals_volatility",
)


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _append_missing(missing: List[str], value: str) -> None:
    if value not in missing:
        missing.append(value)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        coerced = float(value)
        if math.isnan(coerced) or math.isinf(coerced):
            return None
        return coerced
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            coerced = float(text)
        except ValueError:
            return None
        if math.isnan(coerced) or math.isinf(coerced):
            return None
        return coerced
    return None


def _coerce_int(value: Any) -> Optional[int]:
    float_value = _coerce_float(value)
    if float_value is None:
        return None
    int_value = int(float_value)
    if not math.isclose(float_value, float(int_value), abs_tol=1e-9):
        return None
    return int_value


def _extract_float_field(data: Optional[Dict], keys: Tuple[str, ...]) -> Optional[float]:
    if not isinstance(data, dict):
        return None
    for key in keys:
        if key not in data:
            continue
        value = _coerce_float(data.get(key))
        if value is not None:
            return value
    return None


def _extract_int_field(data: Optional[Dict], keys: Tuple[str, ...]) -> Optional[int]:
    if not isinstance(data, dict):
        return None
    for key in keys:
        if key not in data:
            continue
        value = _coerce_int(data.get(key))
        if value is not None:
            return value
    return None


def _percentile_nearest_rank(values: List[float], percentile: float) -> Optional[float]:
    if not values:
        return None
    if percentile <= 0:
        return min(values)
    if percentile >= 1:
        return max(values)
    ordered = sorted(values)
    rank = math.ceil(percentile * len(ordered))
    index = max(0, min(rank - 1, len(ordered) - 1))
    return ordered[index]


def _pstdev(values: List[float]) -> float:
    if not values:
        return 0.0
    mean_value = sum(values) / float(len(values))
    variance = sum((value - mean_value) ** 2 for value in values) / float(len(values))
    return math.sqrt(variance)


def _clamp_range(value: Optional[float], min_value: float, max_value: float) -> Optional[float]:
    if value is None:
        return None
    return max(min_value, min(max_value, float(value)))


def _calculate_structural_anchor(structural_profile_score: Optional[float]) -> float:
    if structural_profile_score is None:
        return 0.50
    return float(_clamp_range(0.50 + abs(structural_profile_score) * 0.50, 0.50, 1.00))


def _resolve_window_size_from_ratio(n_season: int, ratio: float) -> int:
    """Resolve a normalized window size using half-up rounding."""

    if n_season <= 0 or ratio <= 0:
        return 1
    resolved_size = int(math.floor((float(n_season) * float(ratio)) + 0.5))
    return max(1, min(int(n_season), resolved_size))


def _extract_positive_season_length_from_payload(payload: Any) -> Optional[int]:
    if payload is None:
        return None
    payload_dict = payload if isinstance(payload, dict) else getattr(payload, "__dict__", None)
    if not isinstance(payload_dict, dict):
        return None
    value = _extract_int_field(payload_dict, ("total_regular_season_games",))
    if value is not None and value > 0:
        return value
    return None


def _is_active_signal(signal: Optional[float]) -> bool:
    return signal is not None and abs(signal) + EPSILON >= IGNORE_THRESHOLD


def _weighted_average(values_and_weights: List[Tuple[Optional[float], float]]) -> Tuple[float, float, Dict[str, float]]:
    active = [(index, value, weight) for index, (value, weight) in enumerate(values_and_weights) if value is not None and weight > 0]
    if not active:
        return 0.0, 0.0, {}

    total_weight = sum(weight for _, _, weight in active)
    if total_weight <= 0:
        return 0.0, 0.0, {}

    weighted_sum = sum(value * weight for _, value, weight in active if value is not None)
    effective_weights = {str(index): weight / total_weight for index, _, weight in active}
    return weighted_sum / total_weight, total_weight, effective_weights


def _calculate_profile_from_temporal_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    def _profile(window_a: str, window_b: str) -> Tuple[float, Dict[str, Any]]:
        values_and_weights: List[Tuple[Optional[float], float]] = [
            (payload.get(window_a, {}).get("total"), 0.60),
            (payload.get(window_b, {}).get("total"), 0.40),
        ]
        profile, total_weight, effective_weights = _weighted_average(values_and_weights)
        raw = {
            "windows": [window_a, window_b],
            "values": {
                window_a: payload.get(window_a, {}).get("total"),
                window_b: payload.get(window_b, {}).get("total"),
            },
            "base_weights": {
                window_a: 0.60,
                window_b: 0.40,
            },
            "effective_weight_sum": total_weight,
            "effective_weights": effective_weights,
            "profile": profile,
        }
        return profile, raw

    short_term_profile, short_raw = _profile("TOTALS_SHORT", "TOTALS_RECENT")
    long_term_profile, long_raw = _profile("TOTALS_MID", "TOTALS_FULL")

    return {
        "short_term_profile": short_term_profile,
        "long_term_profile": long_term_profile,
        "short_raw": short_raw,
        "long_raw": long_raw,
    }


def _calculate_profile_from_total_series(
    total_series: List[float],
    temporal_window_config: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    ordered_totals: List[float] = []
    for value in total_series:
        coerced = _coerce_float(value)
        if coerced is not None:
            ordered_totals.append(float(coerced))
    available_games = len(ordered_totals)

    windows: Dict[str, Dict[str, Any]] = {}
    weighted_pairs: List[Tuple[Optional[float], float]] = []
    active_window_weights: Dict[str, float] = {}

    for window_name in TOTALS_TEMPORAL_WINDOW_NAMES:
        window_metadata = temporal_window_config.get(window_name, {})
        window_size = int(window_metadata.get("target_window") or 0)
        base_weight = float(window_metadata.get("weight") or 0.0)
        ratio = float(window_metadata.get("ratio") or 0.0)
        subset = ordered_totals[: min(window_size, available_games)]
        if not subset:
            windows[window_name] = {
                "ratio": ratio,
                "total": None,
                "games_used": 0,
                "available_games": available_games,
                "target_window": window_size,
                "is_partial": False,
                "base_weight": base_weight,
                "effective_weight": 0.0,
                "missing_reason": "no_games",
            }
            weighted_pairs.append((None, base_weight))
            continue

        games_used = len(subset)
        total = sum(subset) / float(games_used)
        is_partial = games_used < window_size
        windows[window_name] = {
            "ratio": ratio,
            "total": total,
            "games_used": games_used,
            "available_games": available_games,
            "target_window": window_size,
            "is_partial": is_partial,
            "base_weight": base_weight,
            "effective_weight": 0.0,
            "missing_reason": None,
        }
        weighted_pairs.append((total, base_weight))

    weighted_total, total_weight, effective_weights = _weighted_average(weighted_pairs)
    if total_weight > 0:
        for index, window_name in enumerate(TOTALS_TEMPORAL_WINDOW_NAMES):
            if str(index) in effective_weights:
                windows[window_name]["effective_weight"] = effective_weights[str(index)]
                active_window_weights[window_name] = effective_weights[str(index)]

    return {
        **windows,
        "weighted_total": weighted_total,
        "available_games": available_games,
        "active_window_weights": active_window_weights,
        "base_window_weights": {
            name: float(temporal_window_config.get(name, {}).get("weight", weight))
            for name, _, weight in TOTALS_TEMPORAL_WINDOW_CONFIG
        },
        "ordered_results_count": available_games,
    }


def _calculate_trend_delta_from_score_series(
    score_series: Optional[List[Tuple[float, float]]],
    game_totals_series: Optional[List[float]],
    temporal_window_config: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    total_series: List[float] = []
    if score_series:
        total_series = [float(gf) + float(ga) for gf, ga in score_series if gf is not None and ga is not None]
    elif game_totals_series:
        for value in game_totals_series:
            coerced = _coerce_float(value)
            if coerced is not None:
                total_series.append(float(coerced))

    if not total_series:
        return {
            "short_term_profile": None,
            "long_term_profile": None,
            "trend_delta": None,
            "profile": None,
            "score_series_used": bool(score_series),
            "game_totals_series_used": not bool(score_series) and bool(game_totals_series),
        }

    profile = _calculate_profile_from_total_series(total_series, temporal_window_config)
    short_term_profile = _weighted_average(
        [
            (profile.get("TOTALS_SHORT", {}).get("total"), 0.60),
            (profile.get("TOTALS_RECENT", {}).get("total"), 0.40),
        ]
    )[0]
    long_term_profile = _weighted_average(
        [
            (profile.get("TOTALS_MID", {}).get("total"), 0.60),
            (profile.get("TOTALS_FULL", {}).get("total"), 0.40),
        ]
    )[0]

    return {
        "short_term_profile": short_term_profile,
        "long_term_profile": long_term_profile,
        "trend_delta": short_term_profile - long_term_profile,
        "profile": profile,
        "score_series_used": bool(score_series),
        "game_totals_series_used": not bool(score_series) and bool(game_totals_series),
    }


def _resolve_totals_temporal_window_config(
    event_context: EventContext,
) -> Dict[str, Any]:
    ratios = {name: ratio for name, ratio, _ in TOTALS_TEMPORAL_WINDOW_CONFIG}
    weights = {name: weight for name, _, weight in TOTALS_TEMPORAL_WINDOW_CONFIG}

    n_season = _extract_positive_season_length_from_payload(getattr(event_context, "competition", None))
    n_season_source = "event_context.competition.total_regular_season_games" if n_season is not None else None

    if n_season is None or n_season <= 0:
        return {
            "resolution_status": "INSUFFICIENT_DATA",
            "abort_reason": "missing_total_regular_season_games",
            "n_season": None,
            "n_season_source": None,
            "ratios": ratios,
            "weights": weights,
            "resolved_window_sizes": {},
            "window_config": {},
        }

    resolved_window_sizes = {
        name: _resolve_window_size_from_ratio(n_season, ratio)
        for name, ratio, _ in TOTALS_TEMPORAL_WINDOW_CONFIG
    }
    resolved_window_sizes["TOTALS_FULL"] = int(n_season)

    window_config = {
        name: {
            "ratio": ratio,
            "weight": weight,
            "target_window": resolved_window_sizes[name],
            "resolved_size": resolved_window_sizes[name],
        }
        for name, ratio, weight in TOTALS_TEMPORAL_WINDOW_CONFIG
    }

    return {
        "resolution_status": "RESOLVED",
        "abort_reason": None,
        "n_season": int(n_season),
        "n_season_source": n_season_source,
        "ratios": ratios,
        "weights": weights,
        "resolved_window_sizes": resolved_window_sizes,
        "window_config": window_config,
    }


# ---------------------------------------------------------------------------
# Standings helpers
# ---------------------------------------------------------------------------

def _normalize_team_name(value: Any) -> str:
    return " ".join(str(value or "").strip().split()).casefold()


def _extract_record_name(record: Any) -> Optional[str]:
    if not isinstance(record, dict):
        return None
    for key in _TEAM_NAME_KEYS:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            nested_name = _first_text(
                value.get("name"),
                value.get("teamName"),
                value.get("team_name"),
                value.get("display_name"),
                value.get("short_name"),
            )
            if nested_name:
                return nested_name
    return None


def _standings_items(payload: Any) -> List[Tuple[Any, Any]]:
    if isinstance(payload, dict):
        if "standings" in payload:
            return _standings_items(payload.get("standings"))
        if "rows" in payload:
            return _standings_items(payload.get("rows"))

        items: List[Tuple[Any, Any]] = []
        for key, value in payload.items():
            if isinstance(value, dict):
                items.append((key, value))
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    if isinstance(item, dict):
                        items.append((f"{key}[{index}]", item))
        return items

    if isinstance(payload, list):
        items: List[Tuple[Any, Any]] = []
        for index, item in enumerate(payload):
            if isinstance(item, dict):
                if "rows" in item and isinstance(item.get("rows"), list):
                    for row_index, row in enumerate(item.get("rows") or []):
                        if isinstance(row, dict):
                            items.append((f"{index}:{row_index}", row))
                    continue
                items.append((index, item))
        return items

    return []


def _find_team_standing(payload: Any, team_name: Optional[str]) -> Optional[Dict[str, Any]]:
    normalized_team_name = _normalize_team_name(team_name)
    if not normalized_team_name:
        return None

    if isinstance(payload, dict):
        direct = payload.get(team_name)
        if isinstance(direct, dict):
            return direct

    for key, record in _standings_items(payload):
        if not isinstance(record, dict):
            continue
        if _normalize_team_name(key) == normalized_team_name:
            return record
        record_name = _extract_record_name(record)
        if _normalize_team_name(record_name) == normalized_team_name:
            return record
    return None


def _get_league_totals_context(streak_analysis: Any) -> Dict[str, Any]:
    context = getattr(streak_analysis, "league_totals_context", None)
    return context if isinstance(context, dict) else {}


def _get_league_totals_teams(streak_analysis: Any) -> Dict[str, Dict[str, Any]]:
    context = _get_league_totals_context(streak_analysis)
    teams = context.get("teams")
    return teams if isinstance(teams, dict) else {}


def _find_league_totals_team_payload(
    teams: Dict[str, Dict[str, Any]],
    team_name: Optional[str],
) -> Optional[Dict[str, Any]]:
    normalized_target = _normalize_team_name(team_name)
    if not normalized_target:
        return None

    direct = teams.get(team_name)
    if isinstance(direct, dict):
        return direct

    for candidate_name, payload in teams.items():
        if _normalize_team_name(candidate_name) == normalized_target:
            return payload if isinstance(payload, dict) else None

        if isinstance(payload, dict):
            payload_name = _first_text(
                payload.get("team_name"),
                payload.get("name"),
                payload.get("display_name"),
            )
            if _normalize_team_name(payload_name) == normalized_target:
                return payload

    return None


def _extract_team_results_from_league_totals_context(
    streak_analysis: Any,
    team_name: Optional[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    teams = _get_league_totals_teams(streak_analysis)
    team_payload = _find_league_totals_team_payload(teams, team_name)

    if not isinstance(team_payload, dict):
        return [], {
            "source": "missing",
            "team_name": team_name,
            "reason": "team_not_found_in_league_totals_context",
        }

    results = team_payload.get("results")
    if not isinstance(results, list) or not results:
        return [], {
            "source": "streak_analysis.league_totals_context.teams",
            "team_name": team_name,
            "reason": "results_missing_or_empty",
        }

    return [
        result for result in results if isinstance(result, dict)
    ], {
        "source": "streak_analysis.league_totals_context.teams",
        "team_name": team_name,
        "result_count": len(results),
    }


def _extract_team_totals_record_from_payload(
    payload: Any,
    team_name: Optional[str],
    source: str,
) -> Optional[Dict[str, Any]]:
    record = _find_team_standing(payload, team_name)
    if not isinstance(record, dict):
        return None
    return _normalize_team_totals_record(record, source=source, team_name_hint=team_name)


def _normalize_team_totals_record(
    record: Dict[str, Any],
    *,
    source: str,
    team_name_hint: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(record, dict):
        return None

    team_name = _extract_record_name(record) or _first_text(team_name_hint)
    gf = _extract_float_field(record, _TEAM_TOTAL_G_KEYS)
    ga = _extract_float_field(record, _TEAM_TOTAL_A_KEYS)
    gp = _extract_int_field(record, _TEAM_TOTAL_GP_KEYS)

    nested_results: Any = None
    for key in ("results", "matches", "games", "fixtures", "recent_results", "recentMatches"):
        value = record.get(key)
        if isinstance(value, list):
            nested_results = value
            break

    explicit_game_totals = None
    for key in ("game_totals", "game_totals_series"):
        value = record.get(key)
        if isinstance(value, list):
            explicit_game_totals = value
            break

    explicit_game_totals_series = []
    if explicit_game_totals:
        for value in explicit_game_totals:
            coerced = _coerce_float(value)
            if coerced is not None:
                explicit_game_totals_series.append(coerced)

    game_totals_series = explicit_game_totals_series or _extract_game_totals_series(nested_results or [])
    score_series = _extract_team_score_series(nested_results or [])

    if (gf is None or ga is None or gp is None) and score_series:
        if gf is None:
            gf = sum(item[0] for item in score_series)
        if ga is None:
            ga = sum(item[1] for item in score_series)
        if gp is None:
            gp = len(score_series)

    if gp is None and game_totals_series:
        gp = len(game_totals_series)

    if gp is not None and gp <= 0:
        gp = None

    std_dev_totals = _extract_float_field(record, _VOLATILITY_SAMPLE_KEYS)
    if std_dev_totals is None and game_totals_series:
        std_dev_totals = _pstdev(game_totals_series)
    if std_dev_totals is None and score_series:
        std_dev_totals = _pstdev([gf_value + ga_value for gf_value, ga_value in score_series])

    team_total_per_game = None
    gfpg = None
    gapg = None
    if gf is not None and ga is not None and gp is not None and gp > 0:
        gfpg = gf / float(gp)
        gapg = ga / float(gp)
        team_total_per_game = gfpg + gapg

    completeness = sum(
        1
        for value in (
            team_name or None,
            gf,
            ga,
            gp,
            team_total_per_game,
            std_dev_totals,
        )
        if value is not None
    )

    if team_name is None and gf is None and ga is None and gp is None and std_dev_totals is None:
        return None

    return {
        "team_name": team_name,
        "source": source,
        "raw": record,
        "gf": gf,
        "ga": ga,
        "gp": gp,
        "gfpg": gfpg,
        "gapg": gapg,
        "team_total_per_game": team_total_per_game,
        "game_totals_series": game_totals_series,
        "score_series": score_series,
        "std_dev_totals": std_dev_totals,
        "completeness": completeness,
    }


def _build_record_from_results(
    results: List[Dict[str, Any]],
    *,
    team_name: Optional[str],
    source: str,
) -> Optional[Dict[str, Any]]:
    score_series = _extract_team_score_series(results)
    if not score_series:
        return None

    gp = len(score_series)
    gf = sum(gf_value for gf_value, _ in score_series)
    ga = sum(ga_value for _, ga_value in score_series)
    gfpg = gf / float(gp)
    gapg = ga / float(gp)
    game_totals_series = [gf_value + ga_value for gf_value, ga_value in score_series]

    return {
        "team_name": team_name,
        "source": source,
        "raw": None,
        "gf": gf,
        "ga": ga,
        "gp": gp,
        "gfpg": gfpg,
        "gapg": gapg,
        "team_total_per_game": gfpg + gapg,
        "game_totals_series": game_totals_series,
        "score_series": score_series,
        "std_dev_totals": _pstdev(game_totals_series),
        "completeness": 5,
    }


def _collect_league_records(
    streak_analysis: Any,
    home_results: List[Dict[str, Any]],
    away_results: List[Dict[str, Any]],
    event_context: EventContext,
) -> List[Dict[str, Any]]:
    """Collect best-effort league records from league context, standings and result snapshots."""
    payloads: List[Tuple[str, Any]] = [
        ("streak_analysis.current_standings", getattr(streak_analysis, "current_standings", None)),
        ("streak_analysis.standings_response", getattr(streak_analysis, "standings_response", None)),
        ("event_context.competition.standings_response", getattr(getattr(event_context, "competition", None), "standings_response", None)),
    ]

    source_metadata = getattr(streak_analysis, "standings_source", None) or getattr(streak_analysis, "standings_source_metadata", None)
    if source_metadata is not None:
        payloads.append(("streak_analysis.standings_source_metadata", source_metadata))

    candidates: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    def _consider(record: Optional[Dict[str, Any]]) -> None:
        if not record:
            return
        team_name = _normalize_team_name(record.get("team_name"))
        if not team_name:
            return
        completeness = int(record.get("completeness") or 0)
        existing = candidates.get(team_name)
        if existing is None or completeness > existing[0]:
            candidates[team_name] = (completeness, record)
    league_totals_teams = _get_league_totals_teams(streak_analysis)

    if league_totals_teams:
        for team_name, team_payload in league_totals_teams.items():
            if not isinstance(team_payload, dict):
                continue

            record = _normalize_team_totals_record(
                team_payload,
                source="streak_analysis.league_totals_context.teams",
                team_name_hint=_first_text(team_name, team_payload.get("team_name")),
            )
            _consider(record)

    for payload_source, payload in payloads:
        for key, standing in _standings_items(payload):
            if not isinstance(standing, dict):
                continue
            record = _normalize_team_totals_record(standing, source=payload_source, team_name_hint=_first_text(key))
            _consider(record)

    for index, result in enumerate(home_results):
        if not isinstance(result, dict):
            continue
        for key in ("team_standing", "standing", "current_standing", "home_team_standing", "away_team_standing", "opponent_standing"):
            standing = result.get(key)
            if isinstance(standing, dict):
                record = _normalize_team_totals_record(
                    standing,
                    source=f"home_results[{index}].{key}",
                    team_name_hint=_first_text(result.get("team_name"), result.get("opponent_name")),
                )
                _consider(record)
        nested_team_name = _first_text(result.get("team_name"), result.get("name"), result.get("display_name"))
        if nested_team_name:
            record = _normalize_team_totals_record(
                result,
                source=f"home_results[{index}]",
                team_name_hint=nested_team_name,
            )
            _consider(record)

    for index, result in enumerate(away_results):
        if not isinstance(result, dict):
            continue
        for key in ("team_standing", "standing", "current_standing", "home_team_standing", "away_team_standing", "opponent_standing"):
            standing = result.get(key)
            if isinstance(standing, dict):
                record = _normalize_team_totals_record(
                    standing,
                    source=f"away_results[{index}].{key}",
                    team_name_hint=_first_text(result.get("team_name"), result.get("opponent_name")),
                )
                _consider(record)
        nested_team_name = _first_text(result.get("team_name"), result.get("name"), result.get("display_name"))
        if nested_team_name:
            record = _normalize_team_totals_record(
                result,
                source=f"away_results[{index}]",
                team_name_hint=nested_team_name,
            )
            _consider(record)

    league_records = [record for _, record in sorted(candidates.values(), key=lambda item: (item[0], _normalize_team_name(item[1].get("team_name"))), reverse=True)]
    return league_records


def _extract_team_totals_record(
    streak_analysis: Any,
    side: str,
    results: List[Dict[str, Any]],
    event_context: EventContext,
    league_payloads: List[Tuple[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    missing: List[str] = []
    team_name = _first_text(
        getattr(streak_analysis, f"{side}_team_name", None),
        getattr(getattr(event_context, side, None), "name", None),
    )

    direct_sources = [
        (f"streak_analysis.{side}_team_current_standing", getattr(streak_analysis, f"{side}_team_current_standing", None)),
        (f"streak_analysis.{side}_team_standing", getattr(streak_analysis, f"{side}_team_standing", None)),
    ]

    for source_name, payload in league_payloads:
        if not payload:
            continue
        record = _extract_team_totals_record_from_payload(payload, team_name, source_name)
        if record is not None:
            return record, {"source": source_name, "mode": "league_lookup"}, missing

    for source_name, payload in direct_sources:
        if isinstance(payload, dict):
            record = _normalize_team_totals_record(payload, source=source_name, team_name_hint=team_name)
            if record is not None:
                return record, {"source": source_name, "mode": "direct"}, missing

    fallback_record = _build_record_from_results(results, team_name=team_name, source=f"{side}_results_fallback")
    if fallback_record is not None:
        _append_missing(missing, f"{side}_fallback_from_results")
        return fallback_record, {"source": f"{side}_results_fallback", "mode": "results_fallback"}, missing

    _append_missing(missing, f"{side}_team_totals_missing")
    return (
        {
            "team_name": team_name,
            "source": "missing",
            "raw": None,
            "gf": None,
            "ga": None,
            "gp": None,
            "gfpg": None,
            "gapg": None,
            "team_total_per_game": None,
            "game_totals_series": [],
            "score_series": [],
            "std_dev_totals": None,
            "completeness": 0,
        },
        {"source": "missing", "mode": "missing"},
        missing,
    )


# ---------------------------------------------------------------------------
# Result helpers
# ---------------------------------------------------------------------------

def _extract_timestamp_value(result: Dict[str, Any]) -> Optional[float]:
    for key in ("startTimestamp", "start_timestamp", "timestamp"):
        value = _coerce_float(result.get(key))
        if value is not None:
            return value
    return None


def _ordered_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not results:
        return []

    timestamps = [_extract_timestamp_value(result) if isinstance(result, dict) else None for result in results]
    if all(timestamp is not None for timestamp in timestamps):
        ordered = [result for _, result in sorted(zip(timestamps, results), key=lambda item: item[0], reverse=True)]
        return ordered
    return list(reversed(results))


def _infer_result_role(result: Dict[str, Any]) -> Optional[str]:
    for key in ("is_home", "team_is_home", "home_side"):
        value = result.get(key)
        if isinstance(value, bool):
            return "home" if value else "away"
    for key in ("team_role", "role", "side", "match_role"):
        value = result.get(key)
        if isinstance(value, str):
            text = value.strip().casefold()
            if text in {"home", "away"}:
                return text
    return None


def _extract_game_goals_for_against(result: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    if not isinstance(result, dict):
        return None

    for gf_key, ga_key in _GAME_TOTAL_PAIRS:
        gf = _coerce_float(result.get(gf_key))
        ga = _coerce_float(result.get(ga_key))
        if gf is not None and ga is not None:
            return gf, ga

    home_score = _coerce_float(result.get("home_score"))
    away_score = _coerce_float(result.get("away_score"))
    if home_score is not None and away_score is not None:
        role = _infer_result_role(result)
        if role == "home":
            return home_score, away_score
        if role == "away":
            return away_score, home_score

    return None


def _extract_game_total(result: Dict[str, Any]) -> Optional[float]:
    if not isinstance(result, dict):
        return None

    for gf_key, ga_key in _GAME_TOTAL_PAIRS:
        gf = _coerce_float(result.get(gf_key))
        ga = _coerce_float(result.get(ga_key))
        if gf is not None and ga is not None:
            return gf + ga

    home_score = _coerce_float(result.get("home_score"))
    away_score = _coerce_float(result.get("away_score"))
    if home_score is not None and away_score is not None:
        return home_score + away_score

    return None


def _extract_team_score_series(results: List[Dict[str, Any]]) -> List[Tuple[float, float]]:
    ordered_results = _ordered_results(results)
    series: List[Tuple[float, float]] = []
    for result in ordered_results:
        if not isinstance(result, dict):
            continue
        score = _extract_game_goals_for_against(result)
        if score is not None:
            series.append(score)
    return series


def _extract_game_totals_series(results: List[Dict[str, Any]]) -> List[float]:
    ordered_results = _ordered_results(results)
    series: List[float] = []
    for result in ordered_results:
        if not isinstance(result, dict):
            continue
        total = _extract_game_total(result)
        if total is not None:
            series.append(total)
    return series


# ---------------------------------------------------------------------------
# Layer calculators
# ---------------------------------------------------------------------------

def _calculate_structural_layer(
    *,
    home_record: Dict[str, Any],
    away_record: Dict[str, Any],
    league_records: List[Dict[str, Any]],
    event_context: EventContext,
    missing: List[str],
) -> Dict[str, Any]:
    home_gp = home_record.get("gp")
    away_gp = away_record.get("gp")
    home_gf = home_record.get("gf")
    away_gf = away_record.get("gf")
    home_ga = home_record.get("ga")
    away_ga = away_record.get("ga")

    home_gfpg = home_record.get("gfpg")
    home_gapg = home_record.get("gapg")
    away_gfpg = away_record.get("gfpg")
    away_gapg = away_record.get("gapg")

    league_samples = [
        record.get("team_total_per_game")
        for record in league_records
        if record.get("team_total_per_game") is not None
    ]
    league_samples = [float(value) for value in league_samples if value is not None]
    league_team_names = {
        _normalize_team_name(record.get("team_name"))
        for record in league_records
        if _normalize_team_name(record.get("team_name"))
    }
    home_team_name = _normalize_team_name(getattr(getattr(event_context, "home", None), "name", None))
    away_team_name = _normalize_team_name(getattr(getattr(event_context, "away", None), "name", None))
    only_home_away_league = bool(league_team_names) and league_team_names.issubset({home_team_name, away_team_name})

    league_total_baseline = _percentile_nearest_rank(league_samples, 0.50)
    league_sample_count = len(league_samples)
    expected_league_size = _coerce_int(getattr(event_context.competition, "number_of_teams", None))

    home_attack_environment = None
    away_attack_environment = None
    structural_components: List[Tuple[Optional[float], str]] = []

    if home_gfpg is not None:
        structural_components.append((home_gfpg, "home_gfpg"))
    else:
        _append_missing(missing, "home_gfpg_missing")
    if away_gapg is not None:
        structural_components.append((away_gapg, "away_gapg"))
    else:
        _append_missing(missing, "away_gapg_missing")
    if structural_components:
        home_attack_environment = sum(value for value, _ in structural_components if value is not None) / float(
            len([1 for value, _ in structural_components if value is not None])
        )

    structural_components = []
    if away_gfpg is not None:
        structural_components.append((away_gfpg, "away_gfpg"))
    else:
        _append_missing(missing, "away_gfpg_missing")
    if home_gapg is not None:
        structural_components.append((home_gapg, "home_gapg"))
    else:
        _append_missing(missing, "home_gapg_missing")
    if structural_components:
        away_attack_environment = sum(value for value, _ in structural_components if value is not None) / float(
            len([1 for value, _ in structural_components if value is not None])
        )

    expected_total_structural = None
    if home_attack_environment is not None or away_attack_environment is not None:
        active_components = [value for value in (home_attack_environment, away_attack_environment) if value is not None]
        expected_total_structural = sum(active_components) if active_components else None

    total_dynamic_scale_source = "league_total_distance_p75"
    total_dynamic_scale = None
    total_distances: List[float] = []
    if league_total_baseline is not None:
        for sample in league_samples:
            total_distances.append(abs(sample - league_total_baseline))
        total_dynamic_scale = _percentile_nearest_rank(total_distances, 0.75)

    structural_source = {
        "league_baseline_source": "fallback_home_away_only" if only_home_away_league else ("league_records" if league_sample_count > 0 else "fallback_home_away_only"),
        "total_dynamic_scale_source": total_dynamic_scale_source,
        "expected_league_size": expected_league_size,
        "league_sample_count": league_sample_count,
    }

    if league_total_baseline is None and league_sample_count == 0:
        home_total = home_record.get("team_total_per_game")
        away_total = away_record.get("team_total_per_game")
        fallback_samples = [value for value in (home_total, away_total) if value is not None]
        if fallback_samples:
            league_total_baseline = _percentile_nearest_rank([float(value) for value in fallback_samples], 0.50)
            structural_source["league_baseline_source"] = "fallback_home_away_only"
            _append_missing(missing, "league_total_baseline_fallback_home_away_only")

    if league_total_baseline is None:
        _append_missing(missing, "league_total_baseline_missing")

    if total_dynamic_scale is None or total_dynamic_scale <= 0:
        if league_total_baseline is not None and expected_total_structural is not None:
            total_dynamic_scale = 1.0
            structural_source["total_dynamic_scale_source"] = "fallback_total_dynamic_scale"
            _append_missing(missing, "fallback_total_dynamic_scale")
        else:
            _append_missing(missing, "total_dynamic_scale_missing")

    structural_profile_score = 0.0
    if (
        league_total_baseline is not None
        and total_dynamic_scale is not None
        and total_dynamic_scale > 0
        and expected_total_structural is not None
    ):
        structural_profile_score = _clamp_range(
            (expected_total_structural - league_total_baseline) / float(total_dynamic_scale),
            -1.0,
            1.0,
        )

    league_profile = {
        "league_total_baseline": league_total_baseline,
        "total_dynamic_scale": total_dynamic_scale,
        "league_samples": [
            {
                "team_name": record.get("team_name"),
                "team_total_per_game": record.get("team_total_per_game"),
                "gf": record.get("gf"),
                "ga": record.get("ga"),
                "gp": record.get("gp"),
                "source": record.get("source"),
            }
            for record in league_records
        ],
        "source": structural_source,
        "total_distance_samples": total_distances,
    }

    return {
        "signal": structural_profile_score,
        "expected_total_structural": expected_total_structural,
        "league_total_baseline": league_total_baseline,
        "total_dynamic_scale": total_dynamic_scale,
        "home_attack_environment": home_attack_environment,
        "away_attack_environment": away_attack_environment,
        "home_record": home_record,
        "away_record": away_record,
        "league_samples": league_profile["league_samples"],
        "source": structural_source,
        "missing": missing,
        "league_profile": league_profile,
        "home_gp": home_gp,
        "away_gp": away_gp,
        "home_gf": home_gf,
        "away_gf": away_gf,
        "home_ga": home_ga,
        "away_ga": away_ga,
    }


def _calculate_volatility_layer(
    *,
    home_results: List[Dict[str, Any]],
    away_results: List[Dict[str, Any]],
    league_records: List[Dict[str, Any]],
    home_team_name: Optional[str],
    away_team_name: Optional[str],
    missing: List[str],
) -> Dict[str, Any]:
    home_game_totals = _extract_game_totals_series(home_results)
    away_game_totals = _extract_game_totals_series(away_results)
    home_std = _pstdev(home_game_totals) if home_game_totals else None
    away_std = _pstdev(away_game_totals) if away_game_totals else None

    league_std_samples: List[float] = []
    league_sample_details: List[Dict[str, Any]] = []
    for record in league_records:
        std_value = record.get("std_dev_totals")
        if std_value is None:
            continue
        league_std_samples.append(float(std_value))
        league_sample_details.append(
            {
                "team_name": record.get("team_name"),
                "std_dev_totals": float(std_value),
                "source": record.get("source"),
            }
        )

    league_team_names = {
        _normalize_team_name(record.get("team_name"))
        for record in league_records
        if _normalize_team_name(record.get("team_name"))
    }

    vol_baseline = _percentile_nearest_rank(league_std_samples, 0.50)
    vol_baseline_source = "league_records" if len(league_std_samples) > 0 else None
    if league_team_names and len(league_std_samples) <= 2:
        normalized_home = _normalize_team_name(home_team_name)
        normalized_away = _normalize_team_name(away_team_name)
        if league_team_names.issubset({normalized_home, normalized_away}):
            vol_baseline_source = "fallback_home_away_only"

    if vol_baseline is None:
        available_team_vols = [value for value in (home_std, away_std) if value is not None]
        if available_team_vols:
            vol_baseline = _percentile_nearest_rank([float(value) for value in available_team_vols], 0.50)
            vol_baseline_source = "fallback_home_away_only"
            _append_missing(missing, "vol_baseline_fallback_home_away_only")

    if vol_baseline is None:
        _append_missing(missing, "vol_baseline_missing")

    matchup_volatility = None
    active_std_values = [value for value in (home_std, away_std) if value is not None]
    if active_std_values:
        matchup_volatility = sum(active_std_values) / float(len(active_std_values))
    else:
        _append_missing(missing, "matchup_volatility_missing")

    vol_dynamic_scale = None
    vol_dynamic_scale_source = "league_records"
    if vol_baseline is not None and league_std_samples:
        deviation_samples = [abs(sample - vol_baseline) for sample in league_std_samples]
        vol_dynamic_scale = _percentile_nearest_rank(deviation_samples, 0.75)

    if vol_dynamic_scale is None or vol_dynamic_scale <= 0:
        fallback_candidates = [abs(value - vol_baseline) for value in (home_std, away_std) if value is not None and vol_baseline is not None]
        fallback_candidates.append(1.0)
        vol_dynamic_scale = max(fallback_candidates) if fallback_candidates else 1.0
        vol_dynamic_scale_source = "fallback_available_teams"
        _append_missing(missing, "vol_dynamic_scale_fallback_available_teams")

    vol_edge = 0.0
    if matchup_volatility is not None and vol_baseline is not None and vol_dynamic_scale is not None and vol_dynamic_scale > 0:
        vol_edge = _clamp_range((matchup_volatility - vol_baseline) / float(vol_dynamic_scale), -1.0, 1.0)

    if home_std is None:
        _append_missing(missing, "home_std_missing")
    if away_std is None:
        _append_missing(missing, "away_std_missing")

    source = {
        "vol_baseline_source": vol_baseline_source or "missing",
        "vol_dynamic_scale_source": vol_dynamic_scale_source,
        "league_sample_count": len(league_std_samples),
    }

    return {
        "signal": vol_edge,
        "matchup_volatility": matchup_volatility,
        "vol_baseline": vol_baseline,
        "vol_dynamic_scale": vol_dynamic_scale,
        "home_std": home_std,
        "away_std": away_std,
        "home_game_totals": home_game_totals,
        "away_game_totals": away_game_totals,
        "source": source,
        "missing": missing,
        "league_samples": league_sample_details,
    }


def _calculate_team_temporal_windows(
    results: List[Dict[str, Any]],
    window_config: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    ordered_results = _ordered_results(results)
    score_series = _extract_team_score_series(ordered_results)
    available_games = len(score_series)

    windows: Dict[str, Dict[str, Any]] = {}
    weighted_pairs: List[Tuple[Optional[float], float]] = []
    active_window_weights: Dict[str, float] = {}

    for window_name in TOTALS_TEMPORAL_WINDOW_NAMES:
        window_metadata = window_config.get(window_name, {})
        window_size = int(window_metadata.get("target_window") or 0)
        base_weight = float(window_metadata.get("weight") or 0.0)
        ratio = float(window_metadata.get("ratio") or 0.0)
        subset = score_series[: min(window_size, available_games)]
        if not subset:
            windows[window_name] = {
                "ratio": ratio,
                "total": None,
                "gfpg": None,
                "gapg": None,
                "games_used": 0,
                "available_games": available_games,
                "target_window": window_size,
                "is_partial": False,
                "base_weight": base_weight,
                "effective_weight": 0.0,
                "missing_reason": "no_games",
            }
            weighted_pairs.append((None, base_weight))
            continue

        gf = sum(item[0] for item in subset)
        ga = sum(item[1] for item in subset)
        games_used = len(subset)
        gfpg = gf / float(games_used)
        gapg = ga / float(games_used)
        total = gfpg + gapg
        is_partial = games_used < window_size
        windows[window_name] = {
            "ratio": ratio,
            "total": total,
            "gfpg": gfpg,
            "gapg": gapg,
            "games_used": games_used,
            "available_games": available_games,
            "target_window": window_size,
            "is_partial": is_partial,
            "base_weight": base_weight,
            "effective_weight": 0.0,
            "missing_reason": None,
        }
        weighted_pairs.append((total, base_weight))

    weighted_total, total_weight, effective_weights = _weighted_average(weighted_pairs)
    if total_weight > 0:
        for index, window_name in enumerate(TOTALS_TEMPORAL_WINDOW_NAMES):
            if str(index) in effective_weights:
                windows[window_name]["effective_weight"] = effective_weights[str(index)]
                active_window_weights[window_name] = effective_weights[str(index)]

    return {
        **windows,
        "weighted_total": weighted_total,
        "available_games": available_games,
        "active_window_weights": active_window_weights,
        "base_window_weights": {name: weight for name, _, weight in TOTALS_TEMPORAL_WINDOW_CONFIG},
        "ordered_results_count": len(ordered_results),
        "game_scores": [
            {"gf": gf, "ga": ga, "total": gf + ga}
            for gf, ga in score_series
        ],
    }


def _calculate_temporal_layer(
    *,
    home_results: List[Dict[str, Any]],
    away_results: List[Dict[str, Any]],
    league_total_baseline: Optional[float],
    total_dynamic_scale: Optional[float],
    temporal_window_config: Dict[str, Dict[str, Any]],
    missing: List[str],
) -> Dict[str, Any]:
    home_temporal = _calculate_team_temporal_windows(home_results, temporal_window_config)
    away_temporal = _calculate_team_temporal_windows(away_results, temporal_window_config)

    if home_temporal["available_games"] == 0:
        _append_missing(missing, "home_temporal_missing")
    if away_temporal["available_games"] == 0:
        _append_missing(missing, "away_temporal_missing")

    matchup_temporal_total = None
    home_temporal_value = home_temporal["weighted_total"] if home_temporal["available_games"] > 0 else None
    away_temporal_value = away_temporal["weighted_total"] if away_temporal["available_games"] > 0 else None
    active_values = [value for value in (home_temporal_value, away_temporal_value) if value is not None]
    if active_values:
        matchup_temporal_total = sum(active_values) / float(len(active_values))

    temporal_profile_score = 0.0
    if (
        matchup_temporal_total is not None
        and league_total_baseline is not None
        and total_dynamic_scale is not None
        and total_dynamic_scale > 0
    ):
        temporal_profile_score = _clamp_range(
            (matchup_temporal_total - league_total_baseline) / float(total_dynamic_scale),
            -1.0,
            1.0,
        )
    else:
        _append_missing(missing, "temporal_score_missing_inputs")

    return {
        "signal": temporal_profile_score,
        "matchup_temporal_total": matchup_temporal_total,
        "home_temporal": home_temporal,
        "away_temporal": away_temporal,
        "league_total_baseline": league_total_baseline,
        "total_dynamic_scale": total_dynamic_scale,
        "missing": missing,
    }


def _calculate_trend_layer(
    *,
    home_temporal: Dict[str, Any],
    away_temporal: Dict[str, Any],
    league_records: List[Dict[str, Any]],
    temporal_window_config: Dict[str, Dict[str, Any]],
    missing: List[str],
) -> Dict[str, Any]:
    home_profile = _calculate_profile_from_temporal_payload(home_temporal)
    away_profile = _calculate_profile_from_temporal_payload(away_temporal)

    short_term_profile_matchup = (
        home_profile["short_term_profile"] + away_profile["short_term_profile"]
    ) / 2.0
    long_term_profile_matchup = (
        home_profile["long_term_profile"] + away_profile["long_term_profile"]
    ) / 2.0
    matchup_trend_delta = short_term_profile_matchup - long_term_profile_matchup

    league_trend_deltas: List[float] = []
    league_trend_samples: List[Dict[str, Any]] = []
    for record in league_records:
        score_series = record.get("score_series") if isinstance(record, dict) else None
        game_totals_series = record.get("game_totals_series") if isinstance(record, dict) else None
        trend_delta_payload = _calculate_trend_delta_from_score_series(score_series, game_totals_series, temporal_window_config)
        trend_delta = trend_delta_payload.get("trend_delta")
        if trend_delta is None:
            continue
        league_trend_deltas.append(float(trend_delta))
        league_trend_samples.append(
            {
                "team_name": record.get("team_name"),
                "source": record.get("source"),
                "score_series_used": trend_delta_payload.get("score_series_used"),
                "game_totals_series_used": trend_delta_payload.get("game_totals_series_used"),
                "trend_delta": trend_delta,
            }
        )

    league_trend_sample_count = len(league_trend_deltas)
    trend_baseline = _percentile_nearest_rank(league_trend_deltas, 0.50)
    trend_deviation_samples: List[float] = []
    trend_dynamic_scale = None
    if trend_baseline is not None:
        trend_deviation_samples = [abs(delta - trend_baseline) for delta in league_trend_deltas]
        trend_dynamic_scale = _percentile_nearest_rank(trend_deviation_samples, 0.75)

    trend_signal = 0.0
    if trend_baseline is not None and trend_dynamic_scale is not None and trend_dynamic_scale > 0:
        trend_signal = _clamp_range(
            (matchup_trend_delta - trend_baseline) / float(trend_dynamic_scale),
            -1.0,
            1.0,
        )
    else:
        _append_missing(missing, "trend_dynamic_scale_missing_or_zero")

    return {
        "signal": trend_signal,
        "matchup_trend_delta": matchup_trend_delta,
        "trend_baseline": trend_baseline,
        "trend_deviation_samples": trend_deviation_samples,
        "trend_dynamic_scale": trend_dynamic_scale,
        "league_trend_deltas": league_trend_deltas,
        "league_trend_samples": league_trend_samples,
        "league_trend_sample_count": league_trend_sample_count,
        "short_term_profile_matchup": short_term_profile_matchup,
        "long_term_profile_matchup": long_term_profile_matchup,
        "home_trend": {
            **home_profile,
        },
        "away_trend": {
            **away_profile,
        },
        "missing": missing,
    }


# ---------------------------------------------------------------------------
# Final helpers
# ---------------------------------------------------------------------------

def _classify_totals_strength(score: float) -> str:
    abs_score = abs(score)
    if abs_score < 0.05:
        return "NONE"
    if abs_score < 0.15:
        return "WEAK"
    if abs_score < 0.30:
        return "MODERATE"
    if abs_score < 0.60:
        return "STRONG"
    return "VERY_STRONG"


def _totals_direction(score: float) -> str:
    if score > 0:
        return "OVER_PROFILE"
    if score < 0:
        return "UNDER_PROFILE"
    return "NEUTRAL_PROFILE"


def _determine_internal_state(layer_signals: Dict[str, Optional[float]]) -> List[str]:
    states: List[str] = []
    structural = layer_signals.get("STRUCTURAL")
    vol = layer_signals.get("VOL")
    temporal = layer_signals.get("TEMPORAL")
    trend = layer_signals.get("TREND")

    active_signals = {
        layer_name: signal
        for layer_name, signal in (
            ("STRUCTURAL", structural),
            ("VOL", vol),
            ("TEMPORAL", temporal),
        ("TREND", trend),
        )
        if _is_active_signal(signal)
    }

    all_layers_active = len(active_signals) == 4

    if all_layers_active and all(signal is not None and signal > 0 for signal in active_signals.values()):
        states.append("CONSENSUS_OVER")
    if all_layers_active and all(signal is not None and signal < 0 for signal in active_signals.values()):
        states.append("CONSENSUS_UNDER")
    if _is_active_signal(active_signals.get("STRUCTURAL")) and _is_active_signal(active_signals.get("TREND")) and active_signals.get("STRUCTURAL") < 0 and active_signals.get("TREND") > 0:
        states.append("HEATING_CONFLICT")
    if _is_active_signal(active_signals.get("STRUCTURAL")) and _is_active_signal(active_signals.get("TREND")) and active_signals.get("STRUCTURAL") > 0 and active_signals.get("TREND") < 0:
        states.append("COOLING_CONFLICT")

    active_profile_signs = [
        math.copysign(1.0, signal)
        for signal in (active_signals.get("STRUCTURAL"), active_signals.get("TEMPORAL"), active_signals.get("TREND"))
        if _is_active_signal(signal)
    ]
    if _is_active_signal(vol) and vol >= 0.50 and len(set(active_profile_signs)) > 1:
        states.append("CHAOTIC_CONFLICT")

    if not states:
        states.append("NORMAL")
    return states


def _determine_status(
    *,
    confidence_total: float,
    league_total_baseline: Optional[float],
    active_layers: List[P1TotalsLayerOutput],
    structural_source: Dict[str, Any],
    vol_source: Dict[str, Any],
    n_avail: int,
) -> Tuple[str, str]:
    if confidence_total <= 0:
        return "INSUFFICIENT_DATA", "no_valid_game_totals"
    if league_total_baseline is None:
        return "INSUFFICIENT_DATA", "missing_league_total_baseline"
    if not active_layers:
        return "INACTIVE", "all_layers_ignored_or_missing"
    if n_avail < 5:
        return "DEGRADED", "low_sample_size"
    if (
        structural_source.get("league_baseline_source") == "fallback_home_away_only"
        or structural_source.get("total_dynamic_scale_source") == "fallback_total_dynamic_scale"
    ):
        if structural_source.get("league_baseline_source") == "fallback_home_away_only":
            return "DEGRADED", "fallback_home_away_only"
        if structural_source.get("total_dynamic_scale_source") == "fallback_total_dynamic_scale":
            return "DEGRADED", "fallback_total_dynamic_scale"
    if vol_source.get("vol_baseline_source") == "fallback_home_away_only":
        return "DEGRADED", "fallback_home_away_only"
    if vol_source.get("vol_dynamic_scale_source") == "fallback_available_teams":
        return "DEGRADED", "fallback_or_low_sample"
    return "ACTIVE", "active"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calculate_p1_totals(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> P1TotalsOutput:
    event_id = getattr(streak_analysis, "event_id", 0)
    home_team = _first_text(
        getattr(streak_analysis, "home_team_name", None),
        getattr(getattr(event_context, "home", None), "name", None),
    )
    away_team = _first_text(
        getattr(streak_analysis, "away_team_name", None),
        getattr(getattr(event_context, "away", None), "name", None),
    )
    participants = _first_text(
        getattr(streak_analysis, "participants", None),
        getattr(event_context, "participants_label", None),
    )
    if not participants:
        if home_team and away_team:
            participants = f"{home_team} vs {away_team}"
        else:
            participants = home_team or away_team

    raw_home_results = getattr(streak_analysis, "home_team_results", None) or []
    raw_away_results = getattr(streak_analysis, "away_team_results", None) or []

    league_totals_context = _get_league_totals_context(streak_analysis)
    league_totals_teams = _get_league_totals_teams(streak_analysis)

    league_home_results, league_home_results_source = _extract_team_results_from_league_totals_context(
        streak_analysis,
        home_team,
    )
    league_away_results, league_away_results_source = _extract_team_results_from_league_totals_context(
        streak_analysis,
        away_team,
    )

    home_results = league_home_results or raw_home_results
    away_results = league_away_results or raw_away_results

    home_results_source = (
        league_home_results_source
        if league_home_results
        else {
            "source": "streak_analysis.home_team_results",
            "result_count": len(raw_home_results),
            "fallback_reason": league_home_results_source.get("reason"),
        }
    )

    away_results_source = (
        league_away_results_source
        if league_away_results
        else {
            "source": "streak_analysis.away_team_results",
            "result_count": len(raw_away_results),
            "fallback_reason": league_away_results_source.get("reason"),
        }
    )

    league_payloads: List[Tuple[str, Any]] = [
        ("streak_analysis.league_totals_context.teams", league_totals_teams),
        ("streak_analysis.current_standings", getattr(streak_analysis, "current_standings", None)),
        ("streak_analysis.standings_response", getattr(streak_analysis, "standings_response", None)),
        ("event_context.competition.standings_response", getattr(getattr(event_context, "competition", None), "standings_response", None)),
    ]

    home_record, _, home_missing = _extract_team_totals_record(
        streak_analysis,
        "home",
        home_results,
        event_context,
        league_payloads,
    )
    away_record, _, away_missing = _extract_team_totals_record(
        streak_analysis,
        "away",
        away_results,
        event_context,
        league_payloads,
    )

    league_records = _collect_league_records(
        streak_analysis,
        home_results,
        away_results,
        event_context,
    )

    temporal_window_config = _resolve_totals_temporal_window_config(event_context=event_context)

    if debug_mode:
        logger.info(
            "[P1_TOTALS][TEMPORAL_CONFIG] n_season=%s n_season_source=%s resolution_status=%s ratios=%s resolved_window_sizes=%s weights=%s",
            temporal_window_config.get("n_season"),
            temporal_window_config.get("n_season_source"),
            temporal_window_config.get("resolution_status"),
            temporal_window_config.get("ratios"),
            temporal_window_config.get("resolved_window_sizes"),
            temporal_window_config.get("weights"),
        )

    if temporal_window_config.get("resolution_status") != "RESOLVED":
        if debug_mode:
            logger.info(
                "[P1_TOTALS][TEMPORAL_CONFIG][ABORT] reason=%s n_season_source=%s",
                temporal_window_config.get("abort_reason"),
                temporal_window_config.get("n_season_source"),
            )
        return P1TotalsOutput(
            P1_TOTALS_SCORE=0.0,
            P1_TOTALS_DIRECTION="NEUTRAL_PROFILE",
            P1_TOTALS_STRENGTH="NONE",
            P1_TOTALS_INTERNAL_STATE=[],
            STRUCTURAL_ANCHOR=None,
            STRUCTURAL_FINAL=None,
            VOL_FINAL=None,
            TEMPORAL_FINAL=None,
            TREND_FINAL=None,
            STRUCTURAL_PROFILE_SCORE=None,
            VOL_EDGE=None,
            TEMPORAL_PROFILE_SCORE=None,
            TREND_SIGNAL=None,
            TREND_BASELINE=None,
            TREND_DYNAMIC_SCALE=None,
            MATCHUP_TREND_DELTA=None,
            LEAGUE_TREND_SAMPLE_COUNT=None,
            EXPECTED_TOTAL_STRUCTURAL=None,
            MATCHUP_VOLATILITY=None,
            MATCHUP_TEMPORAL_TOTAL=None,
            LEAGUE_TOTAL_BASELINE=None,
            TOTAL_DYNAMIC_SCALE=None,
            VOL_BASELINE=None,
            VOL_DYNAMIC_SCALE=None,
            active_layers=[],
            ignored_layers=[],
            confidence_total=0.0,
            status="INSUFFICIENT_DATA",
            status_reason="missing_total_regular_season_games",
            TEMPORAL_CONFIG=temporal_window_config,
        )

    # If league records are empty, keep the home/away fallback visible for audit.
    if not league_records:
        league_records = [
            record
            for record in (home_record, away_record)
            if record.get("team_total_per_game") is not None
        ]

    missing_data: List[str] = []
    for item in home_missing + away_missing:
        _append_missing(missing_data, item)
    if not league_totals_context:
        _append_missing(missing_data, "league_totals_context_missing")
    elif not league_totals_teams:
        _append_missing(missing_data, "league_totals_context_teams_missing")
    else:
        if not league_home_results:
            _append_missing(missing_data, "home_league_totals_results_missing")
        if not league_away_results:
            _append_missing(missing_data, "away_league_totals_results_missing")

    structural_layer = _calculate_structural_layer(
        home_record=home_record,
        away_record=away_record,
        league_records=league_records,
        event_context=event_context,
        missing=missing_data,
    )

    vol_missing: List[str] = []
    volatility_layer = _calculate_volatility_layer(
        home_results=home_results,
        away_results=away_results,
        league_records=league_records,
        home_team_name=home_team,
        away_team_name=away_team,
        missing=vol_missing,
    )
    for item in vol_missing:
        _append_missing(missing_data, item)

    temporal_layer = _calculate_temporal_layer(
        home_results=home_results,
        away_results=away_results,
        league_total_baseline=structural_layer.get("league_total_baseline"),
        total_dynamic_scale=structural_layer.get("total_dynamic_scale"),
        temporal_window_config=temporal_window_config.get("window_config", {}),
        missing=missing_data,
    )

    trend_layer = _calculate_trend_layer(
        home_temporal=temporal_layer["home_temporal"],
        away_temporal=temporal_layer["away_temporal"],
        league_records=league_records,
        temporal_window_config=temporal_window_config.get("window_config", {}),
        missing=missing_data,
    )

    home_game_totals = volatility_layer["home_game_totals"]
    away_game_totals = volatility_layer["away_game_totals"]
    n_avail = min(len(home_game_totals), len(away_game_totals))
    n_season = temporal_window_config.get("n_season") or 0
    confidence_total = min(n_avail / float(n_season), 1.0) if n_season > 0 else 0.0

    if n_avail == 0:
        _append_missing(missing_data, "n_avail_zero")

    raw_layer_signals: Dict[str, Optional[float]] = {
        "STRUCTURAL": structural_layer.get("signal"),
        "VOL": volatility_layer.get("signal"),
        "TEMPORAL": temporal_layer.get("signal"),
        "TREND": trend_layer.get("signal"),
    }

    structural_anchor = _calculate_structural_anchor(raw_layer_signals.get("STRUCTURAL"))
    if raw_layer_signals.get("STRUCTURAL") is None:
        _append_missing(missing_data, "structural_anchor_missing")

    final_layer_signals: Dict[str, Optional[float]] = {
        "STRUCTURAL": raw_layer_signals.get("STRUCTURAL"),
        "VOL": raw_layer_signals.get("VOL") * structural_anchor if raw_layer_signals.get("VOL") is not None else None,
        "TEMPORAL": raw_layer_signals.get("TEMPORAL") * structural_anchor if raw_layer_signals.get("TEMPORAL") is not None else None,
        "TREND": raw_layer_signals.get("TREND") * structural_anchor if raw_layer_signals.get("TREND") is not None else None,
    }

    active_layers: List[P1TotalsLayerOutput] = []
    ignored_layers: List[P1TotalsLayerOutput] = []
    weighted_sum = 0.0
    active_weight_sum = 0.0

    for layer_name in ("STRUCTURAL", "VOL", "TEMPORAL", "TREND"):
        raw_signal = raw_layer_signals[layer_name]
        final_signal = final_layer_signals[layer_name]
        base_weight = LAYER_BASE_WEIGHTS[layer_name]
        effective_weight = base_weight * confidence_total
        ignored_reason = None
        ignored = False
        active_by_threshold = _is_active_signal(raw_signal)
        if raw_signal is None:
            ignored = True
            ignored_reason = "missing_signal"
        elif not active_by_threshold:
            ignored = True
            ignored_reason = "below_ignore_threshold"
        elif final_signal is None:
            ignored = True
            ignored_reason = "missing_final_signal"
        elif effective_weight <= 0:
            ignored = True
            ignored_reason = "zero_confidence"

        if ignored:
            ignored_layers.append(
                P1TotalsLayerOutput(
                    layer=layer_name,
                    raw_signal=raw_signal,
                    final_signal=final_signal,
                    base_weight=base_weight,
                    effective_weight=0.0,
                    weighted_signal=0.0,
                    ignored=True,
                    ignored_reason=ignored_reason,
                )
            )
        else:
            weighted_signal = float(final_signal or 0.0) * effective_weight
            active_layers.append(
                P1TotalsLayerOutput(
                    layer=layer_name,
                    raw_signal=raw_signal,
                    final_signal=final_signal,
                    base_weight=base_weight,
                    effective_weight=effective_weight,
                    weighted_signal=weighted_signal,
                    ignored=False,
                    ignored_reason=None,
                )
            )
            weighted_sum += weighted_signal
            active_weight_sum += effective_weight

    if active_weight_sum > 0:
        p1_totals_score = _clamp_range(weighted_sum / float(active_weight_sum), -1.0, 1.0) or 0.0
    else:
        p1_totals_score = 0.0

    p1_totals_direction = _totals_direction(p1_totals_score)
    p1_totals_strength = _classify_totals_strength(p1_totals_score)

    internal_state = _determine_internal_state(raw_layer_signals)

    p1_totals_status, p1_totals_status_reason = _determine_status(
        confidence_total=confidence_total,
        league_total_baseline=structural_layer.get("league_total_baseline"),
        active_layers=active_layers,
        structural_source=structural_layer.get("source", {}),
        vol_source=volatility_layer.get("source", {}),
        n_avail=n_avail,
    )

    if confidence_total <= 0:
        p1_totals_score = 0.0
        p1_totals_direction = "NEUTRAL_PROFILE"
        p1_totals_strength = "NONE"
        p1_totals_status = "INSUFFICIENT_DATA"
        p1_totals_status_reason = "no_valid_game_totals"
    elif not active_layers and p1_totals_status != "INSUFFICIENT_DATA":
        p1_totals_score = 0.0
        p1_totals_direction = "NEUTRAL_PROFILE"
        p1_totals_strength = "NONE"
        p1_totals_status = "INACTIVE"
        p1_totals_status_reason = "all_layers_ignored_or_missing"

    p1_totals_score = _clamp_range(p1_totals_score, -1.0, 1.0) or 0.0
    p1_totals_direction = _totals_direction(p1_totals_score) if p1_totals_status != "INSUFFICIENT_DATA" else "NEUTRAL_PROFILE"
    if p1_totals_status == "INSUFFICIENT_DATA":
        p1_totals_strength = "NONE"
        p1_totals_score = 0.0

    if debug_mode:
        _log_p1_totals_debug(
            logger=logger,
            event_id=event_id,
            participants=participants,
            home_team=home_team,
            away_team=away_team,
            home_results_source=home_results_source,
            away_results_source=away_results_source,
            league_totals_context=league_totals_context,
            n_avail=n_avail,
            n_season=n_season,
            confidence_total=confidence_total,
            home_record=home_record,
            away_record=away_record,
            league_records=league_records,
            structural_layer=structural_layer,
            volatility_layer=volatility_layer,
            temporal_layer=temporal_layer,
            trend_layer=trend_layer,
            raw_layer_signals=raw_layer_signals,
            final_layer_signals=final_layer_signals,
            structural_anchor=structural_anchor,
            active_layers=active_layers,
            ignored_layers=ignored_layers,
            active_weight_sum=active_weight_sum,
            weighted_sum=weighted_sum,
            p1_totals_score=p1_totals_score,
            p1_totals_direction=p1_totals_direction,
            p1_totals_strength=p1_totals_strength,
            internal_state=internal_state,
            p1_totals_status=p1_totals_status,
            p1_totals_status_reason=p1_totals_status_reason,
            ignore_threshold=IGNORE_THRESHOLD,
            epsilon=EPSILON,
            layer_base_weights=LAYER_BASE_WEIGHTS,
            block_names=list(TOTALS_TEMPORAL_WINDOW_NAMES),
        )

    return P1TotalsOutput(
        P1_TOTALS_SCORE=p1_totals_score,
        P1_TOTALS_DIRECTION=p1_totals_direction,
        P1_TOTALS_STRENGTH=p1_totals_strength,
        P1_TOTALS_INTERNAL_STATE=internal_state,
        STRUCTURAL_ANCHOR=structural_anchor,
        STRUCTURAL_FINAL=final_layer_signals.get("STRUCTURAL"),
        VOL_FINAL=final_layer_signals.get("VOL"),
        TEMPORAL_FINAL=final_layer_signals.get("TEMPORAL"),
        TREND_FINAL=final_layer_signals.get("TREND"),
        STRUCTURAL_PROFILE_SCORE=raw_layer_signals.get("STRUCTURAL"),
        VOL_EDGE=raw_layer_signals.get("VOL"),
        TEMPORAL_PROFILE_SCORE=raw_layer_signals.get("TEMPORAL"),
        TREND_SIGNAL=raw_layer_signals.get("TREND"),
        TREND_BASELINE=trend_layer.get("trend_baseline"),
        TREND_DYNAMIC_SCALE=trend_layer.get("trend_dynamic_scale"),
        MATCHUP_TREND_DELTA=trend_layer.get("matchup_trend_delta"),
        LEAGUE_TREND_SAMPLE_COUNT=trend_layer.get("league_trend_sample_count"),
        EXPECTED_TOTAL_STRUCTURAL=structural_layer.get("expected_total_structural"),
        MATCHUP_VOLATILITY=volatility_layer.get("matchup_volatility"),
        MATCHUP_TEMPORAL_TOTAL=temporal_layer.get("matchup_temporal_total"),
        LEAGUE_TOTAL_BASELINE=structural_layer.get("league_total_baseline"),
        TOTAL_DYNAMIC_SCALE=structural_layer.get("total_dynamic_scale"),
        VOL_BASELINE=volatility_layer.get("vol_baseline"),
        VOL_DYNAMIC_SCALE=volatility_layer.get("vol_dynamic_scale"),
        active_layers=active_layers,
        ignored_layers=ignored_layers,
        confidence_total=confidence_total,
        status=p1_totals_status,
        status_reason=p1_totals_status_reason,
        TEMPORAL_CONFIG=temporal_window_config,
    )
