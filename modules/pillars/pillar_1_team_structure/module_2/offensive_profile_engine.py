"""M2 - Offensive Profile Engine v2.0."""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional

from modules.pillars.common import (
    ModuleComponentResult,
    ModuleResult,
    calculate_bias,
    clamp,
    classify_strength,
)
from modules.pillars.context import EventContext

logger = logging.getLogger(__name__)

_ENGINE_VERSION = "offensive_profile_engine_v2.0"
_STRENGTH_THRESHOLD_PROFILE = "common.DEFAULT_STRENGTH_THRESHOLDS"
_COMPONENT_WEIGHTS = {
    "SCORING_CONSISTENCY_EDGE": 0.35,
    "OFFENSIVE_CEILING_EDGE": 0.30,
    "BLANK_RATE_EDGE": 0.20,
    "EXPLOSION_FREQUENCY_EDGE": 0.15,
}
_EXPLOSION_THRESHOLD = 2.0


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_text(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if text is not None:
            return text
    return ""


def _coerce_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        coerced = float(value)
        return coerced if math.isfinite(coerced) else None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            coerced = float(stripped)
        except ValueError:
            return None
        return coerced if math.isfinite(coerced) else None
    return None


def _extract_game_gf_trace(game: Dict[str, Any]) -> tuple[Optional[float], Optional[str], Any, Optional[str]]:
    for key in ("team_score", "score_for", "goals_for", "gf"):
        raw_value = game.get(key)
        gf = _coerce_float(raw_value)
        if gf is not None:
            return gf, key, raw_value, None

    for key in ("team_score", "score_for", "goals_for", "gf"):
        if key in game:
            raw_value = game.get(key)
            if raw_value is None:
                return None, key, raw_value, "null_gf_value"
            return None, key, raw_value, "invalid_gf_value"

    return None, None, None, "missing_gf_keys"


def _extract_gf_series(results: Any, side_label: str, debug_mode: bool = False) -> List[float]:
    if not isinstance(results, (list, tuple)):
        return []

    series: List[float] = []
    for index, result in enumerate(results):
        if not isinstance(result, dict):
            if debug_mode:
                logger.info("[M2 EXTRACT_SKIP] %s idx=%s reason=non_dict raw=%r", side_label, index, result)
            continue
        gf, source_key, raw_value, reason = _extract_game_gf_trace(result)
        if gf is None:
            if debug_mode:
                logger.info(
                    "[M2 EXTRACT_SKIP] %s idx=%s reason=%s raw=%r",
                    side_label,
                    index,
                    reason,
                    result,
                )
            continue
        if debug_mode:
            logger.info(
                "[M2 EXTRACT] %s idx=%s source_key=%s raw_value=%r gf=%s",
                side_label,
                index,
                source_key,
                raw_value,
                gf,
            )
        series.append(gf)
    return series


def _population_std_trace(values: List[float]) -> tuple[float, float, List[float], float]:
    if not values:
        return 0.0, 0.0, [], 0.0
    mean = sum(values) / len(values)
    squared_diffs = [(value - mean) ** 2 for value in values]
    variance = sum(squared_diffs) / len(values)
    return math.sqrt(variance), mean, squared_diffs, variance


def _average(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _top_n_average(values: List[float], n: int = 5) -> tuple[List[float], float, int]:
    if not values:
        return [], 0.0, 0
    top_values = sorted(values, reverse=True)[: min(n, len(values))]
    return top_values, _average(top_values), len(top_values)


def _rate(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return count / float(total)


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


def _m2_bias_label(edge: float) -> str:
    if edge == 0:
        return "NEUTRAL"
    if edge > 0 and abs(edge) < 0.05:
        return "SLIGHT_HOME"
    if edge < 0 and abs(edge) < 0.05:
        return "SLIGHT_AWAY"
    if edge > 0:
        return "HOME"
    return "AWAY"


def _inactive_result(
    *,
    event_id: int,
    participants: str,
    home_team: str,
    away_team: str,
    home_gf_series: List[float],
    away_gf_series: List[float],
    m2_status_reason: str,
) -> ModuleResult:
    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "home_gp": len(home_gf_series),
        "away_gp": len(away_gf_series),
        "home_game_gf": home_gf_series,
        "away_game_gf": away_gf_series,
        "std_gf_home": 0.0,
        "std_gf_away": 0.0,
        "scoring_consistency_edge": 0.0,
        "top_n_home": 0,
        "top_n_away": 0,
        "top_gf_home": [],
        "top_gf_away": [],
        "offensive_ceiling_home": 0.0,
        "offensive_ceiling_away": 0.0,
        "offensive_ceiling_edge": 0.0,
        "blanks_home": 0,
        "blanks_away": 0,
        "blank_rate_home": 0.0,
        "blank_rate_away": 0.0,
        "blank_rate_edge": 0.0,
        "explosion_threshold": _EXPLOSION_THRESHOLD,
        "explosion_games_home": 0,
        "explosion_games_away": 0,
        "explosion_rate_home": 0.0,
        "explosion_rate_away": 0.0,
        "explosion_frequency_edge": 0.0,
        "component_weights": dict(_COMPONENT_WEIGHTS),
        "weighted_edges": {
            "SCORING_CONSISTENCY_EDGE": 0.0,
            "OFFENSIVE_CEILING_EDGE": 0.0,
            "BLANK_RATE_EDGE": 0.0,
            "EXPLOSION_FREQUENCY_EDGE": 0.0,
        },
        "m2_edge_raw": 0.0,
        "m2_edge": 0.0,
        "m2_abs_edge": 0.0,
        "m2_bias": calculate_bias(0.0),
        "m2_bias_label": _m2_bias_label(0.0),
        "m2_strength": classify_strength(0.0),
        "m2_status": "INSUFFICIENT_DATA",
        "m2_status_reason": m2_status_reason,
        "engine_version": _ENGINE_VERSION,
        "strength_threshold_profile": _STRENGTH_THRESHOLD_PROFILE,
    }
    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M2",
        module_name="Offensive Profile Engine",
        event_id=event_id,
        participants=participants,
        value=0.0,
        bias=calculate_bias(0.0),
        strength=classify_strength(0.0),
        components=[],
        raw=raw,
    )


def calculate_performance_profile(
    streak_analysis: Any,
    event_context: EventContext,
    debug_mode: bool = False,
) -> ModuleResult:
    event_id = getattr(streak_analysis, "event_id", 0)
    participants = _first_text(
        getattr(streak_analysis, "participants", None),
        getattr(event_context, "participants_label", None) if event_context is not None else None,
    )
    home_team = _first_text(
        getattr(streak_analysis, "home_team_name", None),
        getattr(getattr(event_context, "home", None), "name", None) if event_context is not None else None,
    )
    away_team = _first_text(
        getattr(streak_analysis, "away_team_name", None),
        getattr(getattr(event_context, "away", None), "name", None) if event_context is not None else None,
    )

    home_results = getattr(streak_analysis, "home_team_results", None) or []
    away_results = getattr(streak_analysis, "away_team_results", None) or []

    home_gf_series = _extract_gf_series(home_results, "HOME", debug_mode=debug_mode)
    away_gf_series = _extract_gf_series(away_results, "AWAY", debug_mode=debug_mode)

    home_gp = len(home_gf_series)
    away_gp = len(away_gf_series)
    home_sum = sum(home_gf_series)
    away_sum = sum(away_gf_series)
    home_zero_indexes = [index for index, gf in enumerate(home_gf_series) if gf == 0]
    away_zero_indexes = [index for index, gf in enumerate(away_gf_series) if gf == 0]
    home_explosion_indexes = [index for index, gf in enumerate(home_gf_series) if gf >= _EXPLOSION_THRESHOLD]
    away_explosion_indexes = [index for index, gf in enumerate(away_gf_series) if gf >= _EXPLOSION_THRESHOLD]

    if debug_mode:
        logger.info(
            "[M2 SERIES] HOME gp=%s sum=%.12f zeros=%s ge2=%s series=%s",
            home_gp,
            home_sum,
            len(home_zero_indexes),
            len(home_explosion_indexes),
            home_gf_series,
        )
        logger.info(
            "[M2 SERIES] AWAY gp=%s sum=%.12f zeros=%s ge2=%s series=%s",
            away_gp,
            away_sum,
            len(away_zero_indexes),
            len(away_explosion_indexes),
            away_gf_series,
        )
        logger.info("[M2 ZERO_INDEXES] HOME=%s AWAY=%s", home_zero_indexes, away_zero_indexes)
        logger.info(
            "[M2 EXPLOSION_INDEXES] HOME=%s AWAY=%s",
            home_explosion_indexes,
            away_explosion_indexes,
        )

    if not home_gf_series or not away_gf_series:
        if debug_mode:
            logger.info(
                "[M2 FINAL CHECK] edge_raw=%.12f edge_clamped=%.12f abs=%.12f bias=%s label=%s strength=%s status=%s reason=%s",
                0.0,
                0.0,
                0.0,
                calculate_bias(0.0),
                _m2_bias_label(0.0),
                classify_strength(0.0),
                "INSUFFICIENT_DATA",
                "missing_game_gf_series",
            )
        return _inactive_result(
            event_id=event_id,
            participants=participants,
            home_team=home_team,
            away_team=away_team,
            home_gf_series=home_gf_series,
            away_gf_series=away_gf_series,
            m2_status_reason="missing_game_gf_series",
        )

    std_gf_home, mean_home, home_squared_diffs, variance_home = _population_std_trace(home_gf_series)
    std_gf_away, mean_away, away_squared_diffs, variance_away = _population_std_trace(away_gf_series)
    scoring_consistency_edge = std_gf_away - std_gf_home

    top_gf_home, offensive_ceiling_home, top_n_home = _top_n_average(home_gf_series, 5)
    top_gf_away, offensive_ceiling_away, top_n_away = _top_n_average(away_gf_series, 5)
    offensive_ceiling_edge = offensive_ceiling_home - offensive_ceiling_away

    blanks_home = sum(1 for gf in home_gf_series if gf == 0)
    blanks_away = sum(1 for gf in away_gf_series if gf == 0)
    blank_rate_home = _rate(blanks_home, home_gp)
    blank_rate_away = _rate(blanks_away, away_gp)
    blank_rate_edge = blank_rate_away - blank_rate_home

    explosion_games_home = sum(1 for gf in home_gf_series if gf >= _EXPLOSION_THRESHOLD)
    explosion_games_away = sum(1 for gf in away_gf_series if gf >= _EXPLOSION_THRESHOLD)
    explosion_rate_home = _rate(explosion_games_home, home_gp)
    explosion_rate_away = _rate(explosion_games_away, away_gp)
    explosion_frequency_edge = explosion_rate_home - explosion_rate_away

    consistency_weighted = scoring_consistency_edge * _COMPONENT_WEIGHTS["SCORING_CONSISTENCY_EDGE"]
    ceiling_weighted = offensive_ceiling_edge * _COMPONENT_WEIGHTS["OFFENSIVE_CEILING_EDGE"]
    blank_weighted = blank_rate_edge * _COMPONENT_WEIGHTS["BLANK_RATE_EDGE"]
    explosion_weighted = explosion_frequency_edge * _COMPONENT_WEIGHTS["EXPLOSION_FREQUENCY_EDGE"]
    m2_edge_raw = consistency_weighted + ceiling_weighted + blank_weighted + explosion_weighted
    m2_edge = clamp(m2_edge_raw)

    m2_bias = calculate_bias(m2_edge)
    m2_bias_label = _m2_bias_label(m2_edge)
    m2_strength = classify_strength(m2_edge)

    m2_status = "ACTIVE"
    m2_status_reason = "active"
    if home_gp < 5 or away_gp < 5:
        m2_status = "DEGRADED"
        m2_status_reason = "low_sample_size"

    if debug_mode:
        logger.info(
            "--- M2 Offensive Profile Engine Debug: Event %s (%s) ---",
            event_id,
            participants,
        )
        logger.info(
            "[M2 STD TRACE] HOME mean=%.12f squared_diffs=%s variance=%.12f std=%.12f",
            mean_home,
            home_squared_diffs,
            variance_home,
            std_gf_home,
        )
        logger.info(
            "[M2 STD TRACE] AWAY mean=%.12f squared_diffs=%s variance=%.12f std=%.12f",
            mean_away,
            away_squared_diffs,
            variance_away,
            std_gf_away,
        )
        logger.info(
            "[M2 COMPONENT] SCORING_CONSISTENCY_EDGE = std_away(%.12f) - std_home(%.12f) = %.12f",
            std_gf_away,
            std_gf_home,
            scoring_consistency_edge,
        )
        logger.info(
            "[M2 TOP SORTED] HOME sorted_desc=%s top=%s top_n=%s",
            sorted(home_gf_series, reverse=True),
            top_gf_home,
            top_n_home,
        )
        logger.info(
            "[M2 TOP SORTED] AWAY sorted_desc=%s top=%s top_n=%s",
            sorted(away_gf_series, reverse=True),
            top_gf_away,
            top_n_away,
        )
        logger.info(
            "[M2 COMPONENT] OFFENSIVE_CEILING_EDGE = ceiling_home(%.12f) - ceiling_away(%.12f) = %.12f",
            offensive_ceiling_home,
            offensive_ceiling_away,
            offensive_ceiling_edge,
        )
        logger.info(
            "[M2 COMPONENT] BLANK_RATE_EDGE = blank_rate_away(%.12f) - blank_rate_home(%.12f) = %.12f",
            blank_rate_away,
            blank_rate_home,
            blank_rate_edge,
        )
        logger.info(
            "[M2 COMPONENT] EXPLOSION_FREQUENCY_EDGE = explosion_rate_home(%.12f) - explosion_rate_away(%.12f) = %.12f",
            explosion_rate_home,
            explosion_rate_away,
            explosion_frequency_edge,
        )
        logger.info(
            "[M2 FINAL FORMULA] edge_raw =\n0.35*%.12f\n+ 0.30*%.12f\n+ 0.20*%.12f\n+ 0.15*%.12f",
            scoring_consistency_edge,
            offensive_ceiling_edge,
            blank_rate_edge,
            explosion_frequency_edge,
        )
        logger.info(
            "[M2 FINAL WEIGHTED] consistency=%.12f ceiling=%.12f blank=%.12f explosion=%.12f sum=%.12f",
            consistency_weighted,
            ceiling_weighted,
            blank_weighted,
            explosion_weighted,
            m2_edge_raw,
        )
        logger.info(
            "[M2 FINAL CHECK] edge_raw=%.12f edge_clamped=%.12f abs=%.12f bias=%s label=%s strength=%s status=%s reason=%s",
            m2_edge_raw,
            m2_edge,
            abs(m2_edge),
            m2_bias,
            m2_bias_label,
            m2_strength,
            m2_status,
            m2_status_reason,
        )

    components = [
        _component(
            "SCORING_CONSISTENCY_EDGE",
            scoring_consistency_edge,
            _COMPONENT_WEIGHTS["SCORING_CONSISTENCY_EDGE"],
            {
                "std_gf_home": std_gf_home,
                "std_gf_away": std_gf_away,
                "scoring_consistency_edge": scoring_consistency_edge,
            },
        ),
        _component(
            "OFFENSIVE_CEILING_EDGE",
            offensive_ceiling_edge,
            _COMPONENT_WEIGHTS["OFFENSIVE_CEILING_EDGE"],
            {
                "top_n_home": top_n_home,
                "top_n_away": top_n_away,
                "top_gf_home": top_gf_home,
                "top_gf_away": top_gf_away,
                "offensive_ceiling_home": offensive_ceiling_home,
                "offensive_ceiling_away": offensive_ceiling_away,
                "offensive_ceiling_edge": offensive_ceiling_edge,
            },
        ),
        _component(
            "BLANK_RATE_EDGE",
            blank_rate_edge,
            _COMPONENT_WEIGHTS["BLANK_RATE_EDGE"],
            {
                "blanks_home": blanks_home,
                "blanks_away": blanks_away,
                "blank_rate_home": blank_rate_home,
                "blank_rate_away": blank_rate_away,
                "blank_rate_edge": blank_rate_edge,
            },
        ),
        _component(
            "EXPLOSION_FREQUENCY_EDGE",
            explosion_frequency_edge,
            _COMPONENT_WEIGHTS["EXPLOSION_FREQUENCY_EDGE"],
            {
                "explosion_threshold": _EXPLOSION_THRESHOLD,
                "explosion_games_home": explosion_games_home,
                "explosion_games_away": explosion_games_away,
                "explosion_rate_home": explosion_rate_home,
                "explosion_rate_away": explosion_rate_away,
                "explosion_frequency_edge": explosion_frequency_edge,
            },
        ),
    ]

    raw = {
        "home_team": home_team,
        "away_team": away_team,
        "home_gp": home_gp,
        "away_gp": away_gp,
        "home_game_gf": home_gf_series,
        "away_game_gf": away_gf_series,
        "home_game_gf_sum": home_sum,
        "away_game_gf_sum": away_sum,
        "std_gf_home": std_gf_home,
        "std_gf_away": std_gf_away,
        "scoring_consistency_edge": scoring_consistency_edge,
        "top_n_home": top_n_home,
        "top_n_away": top_n_away,
        "top_gf_home": top_gf_home,
        "top_gf_away": top_gf_away,
        "offensive_ceiling_home": offensive_ceiling_home,
        "offensive_ceiling_away": offensive_ceiling_away,
        "offensive_ceiling_edge": offensive_ceiling_edge,
        "blanks_home": blanks_home,
        "blanks_away": blanks_away,
        "blank_rate_home": blank_rate_home,
        "blank_rate_away": blank_rate_away,
        "blank_rate_edge": blank_rate_edge,
        "explosion_threshold": _EXPLOSION_THRESHOLD,
        "explosion_games_home": explosion_games_home,
        "explosion_games_away": explosion_games_away,
        "explosion_rate_home": explosion_rate_home,
        "explosion_rate_away": explosion_rate_away,
        "explosion_frequency_edge": explosion_frequency_edge,
        "component_weights": dict(_COMPONENT_WEIGHTS),
        "weighted_edges": {
            "SCORING_CONSISTENCY_EDGE": consistency_weighted,
            "OFFENSIVE_CEILING_EDGE": ceiling_weighted,
            "BLANK_RATE_EDGE": blank_weighted,
            "EXPLOSION_FREQUENCY_EDGE": explosion_weighted,
        },
        "m2_edge_raw": m2_edge_raw,
        "m2_edge": m2_edge,
        "m2_abs_edge": abs(m2_edge),
        "m2_bias": m2_bias,
        "m2_bias_label": m2_bias_label,
        "m2_strength": m2_strength,
        "m2_status": m2_status,
        "m2_status_reason": m2_status_reason,
        "engine_version": _ENGINE_VERSION,
        "strength_threshold_profile": _STRENGTH_THRESHOLD_PROFILE,
    }

    return ModuleResult(
        pillar_id="pillar_1_team_structure",
        module_id="M2",
        module_name="Offensive Profile Engine",
        event_id=event_id,
        participants=participants,
        value=m2_edge,
        bias=calculate_bias(m2_edge),
        strength=classify_strength(m2_edge),
        components=components,
        raw=raw,
    )
