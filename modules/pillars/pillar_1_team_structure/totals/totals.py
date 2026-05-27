"""P1 Totals - structural + temporal totals engine.

This module estimates whether a matchup lives in an over, under, or neutral
goal environment using structural season totals, volatility, temporal form,
and trend pressure. The implementation is pure and defensive: it reads only
the provided ``streak_analysis`` and ``event_context`` inputs and returns a
``ModuleResult``.
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
    classify_strength,
)
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)

MODULE_ID = "P1_TOTALS"
MODULE_NAME = "P1 Totals"
ENGINE_VERSION = "p1_totals_structural_temporal_engine_v1.0"

TEMPORAL_WINDOW_SIZES: Dict[str, int] = {
    "L5": 5,
    "L20": 20,
    "L40": 40,
    "L60": 60,
}

TEMPORAL_WINDOW_WEIGHTS: Dict[str, float] = {
    "L5": 0.30,
    "L20": 0.35,
    "L40": 0.20,
    "L60": 0.15,
}

LAYER_BASE_WEIGHTS: Dict[str, float] = {
    "STRUCTURAL": 0.40,
    "VOL": 0.20,
    "TEMPORAL": 0.25,
    "TREND": 0.15,
}

IGNORE_THRESHOLD = 0.05

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


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator == 0:
        return default
    return numerator / denominator


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

    game_totals_series = _extract_game_totals_series(nested_results or [])
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
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Collect best-effort league records from standings and result snapshots."""
    payloads: List[Tuple[str, Any]] = [
        ("streak_analysis.current_standings", getattr(streak_analysis, "current_standings", None)),
        ("streak_analysis.standings_response", getattr(streak_analysis, "standings_response", None)),
        ("event_context.competition.standings_response", getattr(getattr(event_context, "competition", None), "standings_response", None)),
    ]

    source_metadata = getattr(streak_analysis, "standings_source", None) or getattr(streak_analysis, "standings_source_metadata", None)
    if source_metadata is not None:
        payloads.append(("streak_analysis.standings_source_metadata", source_metadata))

    candidates: Dict[str, Tuple[int, Dict[str, Any]]] = {}
    source_trace: Dict[str, str] = {}

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
            source_trace[team_name] = str(record.get("source") or "")

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
    league_samples = [
        {
            "team_name": record.get("team_name"),
            "team_total_per_game": record.get("team_total_per_game"),
            "gf": record.get("gf"),
            "ga": record.get("ga"),
            "gp": record.get("gp"),
            "std_dev_totals": record.get("std_dev_totals"),
            "source": record.get("source"),
        }
        for record in league_records
    ]

    league_source = {
        "payload_sources": [source for source, payload in payloads if payload is not None],
        "source_trace": source_trace,
        "sample_count": len(league_samples),
    }
    return league_records, league_source


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

    for source_name, payload in direct_sources:
        if isinstance(payload, dict):
            record = _normalize_team_totals_record(payload, source=source_name, team_name_hint=team_name)
            if record is not None:
                return record, {"source": source_name, "mode": "direct"}, missing

    for source_name, payload in league_payloads:
        if not payload:
            continue
        record = _extract_team_totals_record_from_payload(payload, team_name, source_name)
        if record is not None:
            return record, {"source": source_name, "mode": "league_lookup"}, missing

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
        structural_profile_score = clamp(
            (expected_total_structural - league_total_baseline) / float(total_dynamic_scale)
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
        vol_edge = clamp((matchup_volatility - vol_baseline) / float(vol_dynamic_scale))

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


def _calculate_team_temporal_windows(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    ordered_results = _ordered_results(results)
    score_series = _extract_team_score_series(ordered_results)
    available_games = len(score_series)

    windows: Dict[str, Dict[str, Any]] = {}
    weighted_pairs: List[Tuple[Optional[float], float]] = []
    active_window_weights: Dict[str, float] = {}

    for window_name, window_size in TEMPORAL_WINDOW_SIZES.items():
        base_weight = TEMPORAL_WINDOW_WEIGHTS[window_name]
        subset = score_series[: min(window_size, available_games)]
        if not subset:
            windows[window_name] = {
                "total": None,
                "gfpg": None,
                "gapg": None,
                "games_used": 0,
                "available_games": 0,
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
            "total": total,
            "gfpg": gfpg,
            "gapg": gapg,
            "games_used": games_used,
            "available_games": games_used,
            "target_window": window_size,
            "is_partial": is_partial,
            "base_weight": base_weight,
            "effective_weight": 0.0,
            "missing_reason": None,
        }
        weighted_pairs.append((total, base_weight))

    weighted_total, total_weight, effective_weights = _weighted_average(weighted_pairs)
    if total_weight > 0:
        for index, window_name in enumerate(TEMPORAL_WINDOW_SIZES.keys()):
            if str(index) in effective_weights:
                windows[window_name]["effective_weight"] = effective_weights[str(index)]
                active_window_weights[window_name] = effective_weights[str(index)]

    return {
        "L5": windows["L5"],
        "L20": windows["L20"],
        "L40": windows["L40"],
        "L60": windows["L60"],
        "weighted_total": weighted_total,
        "available_games": available_games,
        "active_window_weights": active_window_weights,
        "base_window_weights": dict(TEMPORAL_WINDOW_WEIGHTS),
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
    missing: List[str],
) -> Dict[str, Any]:
    home_temporal = _calculate_team_temporal_windows(home_results)
    away_temporal = _calculate_team_temporal_windows(away_results)

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
        temporal_profile_score = clamp(
            (matchup_temporal_total - league_total_baseline) / float(total_dynamic_scale)
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
    total_dynamic_scale: Optional[float],
    missing: List[str],
) -> Dict[str, Any]:
    def _profile(window_a: str, window_b: str) -> Tuple[float, Dict[str, Any]]:
        values_and_weights: List[Tuple[Optional[float], float]] = [
            (home_temporal.get(window_a, {}).get("total"), 0.60),
            (home_temporal.get(window_b, {}).get("total"), 0.40),
        ]
        profile, total_weight, effective_weights = _weighted_average(values_and_weights)
        raw = {
            "windows": [window_a, window_b],
            "values": {
                window_a: home_temporal.get(window_a, {}).get("total"),
                window_b: home_temporal.get(window_b, {}).get("total"),
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

    home_short_term_profile, home_short_raw = _profile("L5", "L20")
    home_long_term_profile, home_long_raw = _profile("L40", "L60")

    def _away_profile(window_a: str, window_b: str) -> Tuple[float, Dict[str, Any]]:
        values_and_weights: List[Tuple[Optional[float], float]] = [
            (away_temporal.get(window_a, {}).get("total"), 0.60),
            (away_temporal.get(window_b, {}).get("total"), 0.40),
        ]
        profile, total_weight, effective_weights = _weighted_average(values_and_weights)
        raw = {
            "windows": [window_a, window_b],
            "values": {
                window_a: away_temporal.get(window_a, {}).get("total"),
                window_b: away_temporal.get(window_b, {}).get("total"),
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

    away_short_term_profile, away_short_raw = _away_profile("L5", "L20")
    away_long_term_profile, away_long_raw = _away_profile("L40", "L60")

    short_term_profile_matchup = (home_short_term_profile + away_short_term_profile) / 2.0
    long_term_profile_matchup = (home_long_term_profile + away_long_term_profile) / 2.0
    trend_delta = short_term_profile_matchup - long_term_profile_matchup

    trend_signal = 0.0
    if total_dynamic_scale is not None and total_dynamic_scale > 0:
        trend_signal = clamp(trend_delta / float(total_dynamic_scale))
    else:
        _append_missing(missing, "trend_scale_missing")

    return {
        "signal": trend_signal,
        "trend_delta": trend_delta,
        "short_term_profile_matchup": short_term_profile_matchup,
        "long_term_profile_matchup": long_term_profile_matchup,
        "home_trend": {
            "short_term_profile": home_short_term_profile,
            "long_term_profile": home_long_term_profile,
            "short_raw": home_short_raw,
            "long_raw": home_long_raw,
        },
        "away_trend": {
            "short_term_profile": away_short_term_profile,
            "long_term_profile": away_long_term_profile,
            "short_raw": away_short_raw,
            "long_raw": away_long_raw,
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


def _determine_internal_state(layer_signals: Dict[str, Optional[float]], vol_edge: float) -> List[str]:
    states: List[str] = []
    structural = layer_signals.get("STRUCTURAL")
    vol = layer_signals.get("VOL")
    temporal = layer_signals.get("TEMPORAL")
    trend = layer_signals.get("TREND")

    if all(signal is not None and signal > 0 for signal in (structural, vol, temporal, trend)):
        states.append("CONSENSUS_OVER")
    if all(signal is not None and signal < 0 for signal in (structural, vol, temporal, trend)):
        states.append("CONSENSUS_UNDER")
    if structural is not None and structural < 0 and trend is not None and trend > 0:
        states.append("HEATING_CONFLICT")
    if structural is not None and structural > 0 and trend is not None and trend < 0:
        states.append("COOLING_CONFLICT")

    active_profile_signs = [
        math.copysign(1.0, signal)
        for signal in (structural, temporal, trend)
        if signal is not None and abs(signal) >= IGNORE_THRESHOLD
    ]
    if vol is not None and vol >= 0.50 and len(set(active_profile_signs)) > 1:
        states.append("CHAOTIC_CONFLICT")

    if not states:
        states.append("NORMAL")
    return states


def _component_raw(
    *,
    layer_name: str,
    signal: Optional[float],
    base_weight: float,
    effective_weight: float,
    ignored: bool,
    ignored_reason: Optional[str],
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "layer": layer_name,
        "signal": signal,
        "base_weight": base_weight,
        "effective_weight": effective_weight,
        "ignored": ignored,
        "ignored_reason": ignored_reason,
        "payload": payload,
    }


def _build_component(
    name: str,
    edge: float,
    weight: float,
    raw: Dict[str, Any],
) -> ModuleComponentResult:
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
    *,
    confidence_total: float,
    league_total_baseline: Optional[float],
    active_layers: List[str],
    ignored_layers: List[Dict[str, Any]],
    missing_data: List[str],
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


def _summarize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "team_name": record.get("team_name"),
        "source": record.get("source"),
        "gf": record.get("gf"),
        "ga": record.get("ga"),
        "gp": record.get("gp"),
        "gfpg": record.get("gfpg"),
        "gapg": record.get("gapg"),
        "team_total_per_game": record.get("team_total_per_game"),
        "std_dev_totals": record.get("std_dev_totals"),
    }


def _sum_components(components: List[ModuleComponentResult]) -> Tuple[float, float]:
    active_weight_sum = sum(component.weight for component in components if component.weight > 0)
    if active_weight_sum <= 0:
        return 0.0, 0.0
    weighted_sum = sum(component.weighted_edge for component in components)
    return weighted_sum, active_weight_sum


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calculate_p1_totals(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
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

    home_results = getattr(streak_analysis, "home_team_results", None) or []
    away_results = getattr(streak_analysis, "away_team_results", None) or []

    league_payloads: List[Tuple[str, Any]] = [
        ("streak_analysis.current_standings", getattr(streak_analysis, "current_standings", None)),
        ("streak_analysis.standings_response", getattr(streak_analysis, "standings_response", None)),
        ("event_context.competition.standings_response", getattr(getattr(event_context, "competition", None), "standings_response", None)),
    ]

    home_record, home_record_source, home_missing = _extract_team_totals_record(
        streak_analysis,
        "home",
        home_results,
        event_context,
        league_payloads,
    )
    away_record, away_record_source, away_missing = _extract_team_totals_record(
        streak_analysis,
        "away",
        away_results,
        event_context,
        league_payloads,
    )

    league_records, league_source = _collect_league_records(
        streak_analysis,
        home_results,
        away_results,
        event_context,
    )

    # If league records are empty, keep the home/away fallback visible for audit.
    if not league_records:
        league_records = [
            record
            for record in (home_record, away_record)
            if record.get("team_total_per_game") is not None
        ]
        if league_records:
            league_source["league_baseline_source"] = "fallback_home_away_only"
            league_source["sample_count"] = len(league_records)

    missing_data: List[str] = []
    for item in home_missing + away_missing:
        _append_missing(missing_data, item)

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
        missing=missing_data,
    )

    trend_layer = _calculate_trend_layer(
        home_temporal=temporal_layer["home_temporal"],
        away_temporal=temporal_layer["away_temporal"],
        total_dynamic_scale=structural_layer.get("total_dynamic_scale"),
        missing=missing_data,
    )

    home_game_totals = volatility_layer["home_game_totals"]
    away_game_totals = volatility_layer["away_game_totals"]
    valid_home_game_totals_count = len(home_game_totals)
    valid_away_game_totals_count = len(away_game_totals)
    n_avail = min(valid_home_game_totals_count, valid_away_game_totals_count)
    confidence_total = min(n_avail / 60.0, 1.0)

    if n_avail == 0:
        _append_missing(missing_data, "n_avail_zero")

    layer_signals: Dict[str, Optional[float]] = {
        "STRUCTURAL": structural_layer.get("signal"),
        "VOL": volatility_layer.get("signal"),
        "TEMPORAL": temporal_layer.get("signal"),
        "TREND": trend_layer.get("signal"),
    }

    layer_weights: Dict[str, Dict[str, float]] = {}
    active_layers: List[str] = []
    ignored_layers: List[Dict[str, Any]] = []
    components: List[ModuleComponentResult] = []

    layer_payloads = {
        "STRUCTURAL": structural_layer,
        "VOL": volatility_layer,
        "TEMPORAL": temporal_layer,
        "TREND": trend_layer,
    }

    for layer_name in ("STRUCTURAL", "VOL", "TEMPORAL", "TREND"):
        signal = layer_signals[layer_name]
        base_weight = LAYER_BASE_WEIGHTS[layer_name]
        effective_weight = base_weight * confidence_total
        ignored_reason = None
        ignored = False
        if signal is None:
            ignored = True
            ignored_reason = "missing_signal"
        elif abs(signal) < IGNORE_THRESHOLD:
            ignored = True
            ignored_reason = "below_ignore_threshold"
        elif effective_weight <= 0:
            ignored = True
            ignored_reason = "zero_confidence"

        if ignored:
            effective_weight = 0.0
            ignored_layers.append(
                {
                    "layer": layer_name,
                    "signal": signal,
                    "reason": ignored_reason,
                    "base_weight": base_weight,
                }
            )
        else:
            active_layers.append(layer_name)

        layer_weights[layer_name] = {
            "base_weight": base_weight,
            "confidence_total": confidence_total,
            "effective_weight": effective_weight,
        }

        component_raw = _component_raw(
            layer_name=layer_name,
            signal=signal,
            base_weight=base_weight,
            effective_weight=effective_weight,
            ignored=ignored,
            ignored_reason=ignored_reason,
            payload=layer_payloads[layer_name],
        )
        components.append(
            _build_component(
                layer_name + "_PROFILE_SCORE" if layer_name != "VOL" else "VOL_EDGE",
                signal or 0.0,
                effective_weight,
                component_raw,
            )
        )

    weighted_sum = sum(component.weighted_edge for component in components)
    active_weight_sum = sum(component.weight for component in components if component.weight > 0)

    if active_weight_sum > 0:
        p1_totals_score = clamp(weighted_sum / float(active_weight_sum))
    else:
        p1_totals_score = 0.0

    p1_totals_direction = _totals_direction(p1_totals_score)
    p1_totals_strength = _classify_totals_strength(p1_totals_score)

    internal_state = _determine_internal_state(layer_signals, float(volatility_layer.get("signal") or 0.0))

    p1_totals_status, p1_totals_status_reason = _determine_status(
        confidence_total=confidence_total,
        league_total_baseline=structural_layer.get("league_total_baseline"),
        active_layers=active_layers,
        ignored_layers=ignored_layers,
        missing_data=missing_data,
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

    p1_totals_score = clamp(p1_totals_score)
    p1_totals_direction = _totals_direction(p1_totals_score) if p1_totals_status != "INSUFFICIENT_DATA" else "NEUTRAL_PROFILE"
    if p1_totals_status == "INSUFFICIENT_DATA":
        p1_totals_strength = "NONE"
        p1_totals_score = 0.0

    if debug_mode:
        logger.info("--- P1_TOTALS Debug: Event %s (%s) ---", event_id, participants)
        logger.info("  home_team=%s away_team=%s", home_team, away_team)
        logger.info(
            "  league baseline=%.4f source=%s dynamic_scale=%.4f source=%s",
            float(structural_layer.get("league_total_baseline") or 0.0),
            structural_layer.get("source", {}).get("league_baseline_source"),
            float(structural_layer.get("total_dynamic_scale") or 0.0),
            structural_layer.get("source", {}).get("total_dynamic_scale_source"),
        )
        logger.info(
            "  home_record=%s away_record=%s",
            _summarize_record(home_record),
            _summarize_record(away_record),
        )
        logger.info(
            "  structural=%.4f vol=%.4f temporal=%.4f trend=%.4f",
            float(layer_signals["STRUCTURAL"] or 0.0),
            float(layer_signals["VOL"] or 0.0),
            float(layer_signals["TEMPORAL"] or 0.0),
            float(layer_signals["TREND"] or 0.0),
        )
        logger.info("  active_layers=%s ignored_layers=%s", active_layers, ignored_layers)
        logger.info(
            "  final score=%.4f direction=%s strength=%s status=%s reason=%s",
            p1_totals_score,
            p1_totals_direction,
            p1_totals_strength,
            p1_totals_status,
            p1_totals_status_reason,
        )

    raw = {
        "P1_TOTALS_SCORE": p1_totals_score,
        "P1_TOTALS_DIRECTION": p1_totals_direction,
        "P1_TOTALS_STRENGTH": p1_totals_strength,
        "P1_TOTALS_INTERNAL_STATE": internal_state,
        "STRUCTURAL_PROFILE_SCORE": structural_layer.get("signal"),
        "VOL_EDGE": volatility_layer.get("signal"),
        "TEMPORAL_PROFILE_SCORE": temporal_layer.get("signal"),
        "TREND_SIGNAL": trend_layer.get("signal"),
        "EXPECTED_TOTAL_STRUCTURAL": structural_layer.get("expected_total_structural"),
        "MATCHUP_VOLATILITY": volatility_layer.get("matchup_volatility"),
        "MATCHUP_TEMPORAL_TOTAL": temporal_layer.get("matchup_temporal_total"),
        "LEAGUE_TOTAL_BASELINE": structural_layer.get("league_total_baseline"),
        "TOTAL_DYNAMIC_SCALE": structural_layer.get("total_dynamic_scale"),
        "VOL_BASELINE": volatility_layer.get("vol_baseline"),
        "VOL_DYNAMIC_SCALE": volatility_layer.get("vol_dynamic_scale"),
        "home_team": home_team,
        "away_team": away_team,
        "event_id": event_id,
        "participants": participants,
        "engine_version": ENGINE_VERSION,
        "p1_totals_status": p1_totals_status,
        "p1_totals_status_reason": p1_totals_status_reason,
        "confidence_total": confidence_total,
        "n_avail": n_avail,
        "active_layers": active_layers,
        "ignored_layers": ignored_layers,
        "layer_weights": layer_weights,
        "layer_signals": layer_signals,
        "layer_contributions": [
            {
                "layer": component.name,
                "signal": component.edge,
                "weight": component.weight,
                "weighted_edge": component.weighted_edge,
                "bias": component.bias,
                "strength": component.strength,
                "raw": component.raw,
            }
            for component in components
        ],
        "data_sources": {
            "structural": structural_layer.get("source", {}),
            "volatility": volatility_layer.get("source", {}),
            "league_records_source": league_source,
            "home_record_source": home_record_source,
            "away_record_source": away_record_source,
            "league_baseline_source": structural_layer.get("source", {}).get("league_baseline_source"),
            "vol_baseline_source": volatility_layer.get("source", {}).get("vol_baseline_source"),
            "vol_dynamic_scale_source": volatility_layer.get("source", {}).get("vol_dynamic_scale_source"),
        },
        "missing_data": missing_data,
        "home_profile": {
            **_summarize_record(home_record),
            "temporal": temporal_layer["home_temporal"],
            "trend": trend_layer["home_trend"],
            "game_totals_series": home_game_totals,
            "valid_game_totals_count": valid_home_game_totals_count,
        },
        "away_profile": {
            **_summarize_record(away_record),
            "temporal": temporal_layer["away_temporal"],
            "trend": trend_layer["away_trend"],
            "game_totals_series": away_game_totals,
            "valid_game_totals_count": valid_away_game_totals_count,
        },
        "league_profile": {
            "structural": structural_layer.get("league_profile", {}),
            "volatility": {
                "vol_baseline": volatility_layer.get("vol_baseline"),
                "vol_dynamic_scale": volatility_layer.get("vol_dynamic_scale"),
                "league_samples": volatility_layer.get("league_samples", []),
            },
            "league_source": league_source,
        },
        "temporal_windows": {
            "home": temporal_layer["home_temporal"],
            "away": temporal_layer["away_temporal"],
            "weights": dict(TEMPORAL_WINDOW_WEIGHTS),
        },
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id=MODULE_ID,
        module_name=MODULE_NAME,
        event_id=event_id,
        participants=participants,
        value=p1_totals_score,
        bias=calculate_bias(p1_totals_score),
        strength=p1_totals_strength,
        components=components,
        raw=raw,
    )
