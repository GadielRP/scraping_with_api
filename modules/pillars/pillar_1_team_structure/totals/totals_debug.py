from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional


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


def _debug_float(value: Any) -> str:
    coerced = _coerce_float(value)
    if coerced is None:
        return "None"
    return f"{coerced:.12g} (~{coerced:.4f})"


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


def _debug_sample_summary(values: List[float]) -> Dict[str, Any]:
    samples = [float(value) for value in values if _coerce_float(value) is not None]
    return {
        "count": len(samples),
        "min": min(samples) if samples else None,
        "p50": _percentile_nearest_rank(samples, 0.50) if samples else None,
        "p75": _percentile_nearest_rank(samples, 0.75) if samples else None,
        "max": max(samples) if samples else None,
    }


def _debug_temporal_block_payload(team_payload: Dict[str, Any], block_names: List[str]) -> Dict[str, Dict[str, Any]]:
    return {
        block_name: {
            "ratio": team_payload.get(block_name, {}).get("ratio"),
            "target_window": team_payload.get(block_name, {}).get("target_window"),
            "games_used": team_payload.get(block_name, {}).get("games_used"),
            "is_partial": team_payload.get(block_name, {}).get("is_partial"),
            "gfpg": team_payload.get(block_name, {}).get("gfpg"),
            "gapg": team_payload.get(block_name, {}).get("gapg"),
            "total": team_payload.get(block_name, {}).get("total"),
            "base_weight": team_payload.get(block_name, {}).get("base_weight"),
            "effective_weight": team_payload.get(block_name, {}).get("effective_weight"),
        }
        for block_name in block_names
    }


def _log_p1_totals_debug(
    *,
    logger: logging.Logger,
    event_id: Any,
    participants: str,
    home_team: str,
    away_team: str,
    home_results_source: Dict[str, Any],
    away_results_source: Dict[str, Any],
    league_totals_context: Dict[str, Any],
    n_avail: int,
    n_season: int,
    confidence_total: float,
    home_record: Dict[str, Any],
    away_record: Dict[str, Any],
    league_records: List[Dict[str, Any]],
    structural_layer: Dict[str, Any],
    volatility_layer: Dict[str, Any],
    temporal_layer: Dict[str, Any],
    trend_layer: Dict[str, Any],
    layer_signals: Dict[str, Optional[float]],
    active_layers: List[Any],
    ignored_layers: List[Any],
    active_weight_sum: float,
    weighted_sum: float,
    p1_totals_score: float,
    p1_totals_direction: str,
    p1_totals_strength: str,
    internal_state: List[str],
    p1_totals_status: str,
    p1_totals_status_reason: str,
    ignore_threshold: float,
    epsilon: float,
    layer_base_weights: Dict[str, float],
    block_names: List[str],
) -> None:
    league_samples = [
        float(record.get("team_total_per_game"))
        for record in league_records
        if record.get("team_total_per_game") is not None
    ]
    total_distance_samples = structural_layer.get("league_profile", {}).get("total_distance_samples", [])
    layer_audit = {layer.layer: layer for layer in active_layers + ignored_layers}

    logger.info("[P1_TOTALS][INPUTS] event_id=%s participants=%s", event_id, participants)
    logger.info("[P1_TOTALS][INPUTS] home_team=%s away_team=%s", home_team, away_team)
    logger.info(
        "[P1_TOTALS][INPUTS] home_results_source=%s result_count=%s fallback_reason=%s",
        home_results_source.get("source"),
        home_results_source.get("result_count"),
        home_results_source.get("fallback_reason"),
    )
    logger.info(
        "[P1_TOTALS][INPUTS] away_results_source=%s result_count=%s fallback_reason=%s",
        away_results_source.get("source"),
        away_results_source.get("result_count"),
        away_results_source.get("fallback_reason"),
    )
    logger.info(
        "[P1_TOTALS][INPUTS] league_totals_context present=%s team_count=%s match_count=%s",
        bool(league_totals_context),
        league_totals_context.get("team_count") if league_totals_context else None,
        league_totals_context.get("match_count") if league_totals_context else None,
    )
    logger.info(
        "[P1_TOTALS][INPUTS] n_avail=%s denominator=%s confidence_total=%s",
        n_avail,
        n_season,
        _debug_float(confidence_total),
    )

    logger.info(
        "[P1_TOTALS][STRUCTURAL] home GF=%s GA=%s GP=%s GFPG=%s GAPG=%s TEAM_TOTAL_PER_GAME=%s",
        _debug_float(home_record.get("gf")),
        _debug_float(home_record.get("ga")),
        home_record.get("gp"),
        _debug_float(home_record.get("gfpg")),
        _debug_float(home_record.get("gapg")),
        _debug_float(home_record.get("team_total_per_game")),
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL] away GF=%s GA=%s GP=%s GFPG=%s GAPG=%s TEAM_TOTAL_PER_GAME=%s",
        _debug_float(away_record.get("gf")),
        _debug_float(away_record.get("ga")),
        away_record.get("gp"),
        _debug_float(away_record.get("gfpg")),
        _debug_float(away_record.get("gapg")),
        _debug_float(away_record.get("team_total_per_game")),
    )
    logger.info("[P1_TOTALS][STRUCTURAL] league_samples summary=%s", _debug_sample_summary(league_samples))
    logger.info(
        "[P1_TOTALS][STRUCTURAL] league_samples by_team=%s",
        [
            {
                "team_name": record.get("team_name"),
                "team_total_per_game": record.get("team_total_per_game"),
                "source": record.get("source"),
            }
            for record in league_records
            if record.get("team_total_per_game") is not None
        ],
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL] LEAGUE_TOTAL_BASELINE=%s TOTAL_DISTANCE summary=%s TOTAL_DYNAMIC_SCALE=%s",
        _debug_float(structural_layer.get("league_total_baseline")),
        _debug_sample_summary(total_distance_samples),
        _debug_float(structural_layer.get("total_dynamic_scale")),
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL] HOME_ATTACK_ENVIRONMENT=%s AWAY_ATTACK_ENVIRONMENT=%s EXPECTED_TOTAL_STRUCTURAL=%s STRUCTURAL_PROFILE_SCORE=%s",
        _debug_float(structural_layer.get("home_attack_environment")),
        _debug_float(structural_layer.get("away_attack_environment")),
        _debug_float(structural_layer.get("expected_total_structural")),
        _debug_float(structural_layer.get("signal")),
    )

    logger.info("[P1_TOTALS][VOLATILITY] home_game_totals=%s", volatility_layer.get("home_game_totals"))
    logger.info("[P1_TOTALS][VOLATILITY] away_game_totals=%s", volatility_layer.get("away_game_totals"))
    logger.info(
        "[P1_TOTALS][VOLATILITY] STD_DEV_TOTALS_HOME=%s STD_DEV_TOTALS_AWAY=%s VOL_BASELINE=%s VOL_DYNAMIC_SCALE=%s MATCHUP_VOLATILITY=%s VOL_EDGE=%s",
        _debug_float(volatility_layer.get("home_std")),
        _debug_float(volatility_layer.get("away_std")),
        _debug_float(volatility_layer.get("vol_baseline")),
        _debug_float(volatility_layer.get("vol_dynamic_scale")),
        _debug_float(volatility_layer.get("matchup_volatility")),
        _debug_float(volatility_layer.get("signal")),
    )
    logger.info(
        "[P1_TOTALS][VOLATILITY] league_std_samples summary=%s by_team=%s",
        _debug_sample_summary([
            sample.get("std_dev_totals")
            for sample in volatility_layer.get("league_samples", [])
            if sample.get("std_dev_totals") is not None
        ]),
        volatility_layer.get("league_samples", []),
    )

    for side_name, temporal_payload in (("home", temporal_layer["home_temporal"]), ("away", temporal_layer["away_temporal"])):
        for block_name, block_payload in _debug_temporal_block_payload(temporal_payload, block_names).items():
            logger.info(
                "[P1_TOTALS][TEMPORAL] side=%s block_name=%s ratio=%s target_window=%s games_used=%s available_games=%s is_partial=%s gfpg=%s gapg=%s total=%s base_weight=%s effective_weight=%s",
                side_name,
                block_name,
                _debug_float(block_payload.get("ratio")),
                block_payload.get("target_window"),
                block_payload.get("games_used"),
                temporal_payload.get("available_games"),
                block_payload.get("is_partial"),
                _debug_float(block_payload.get("gfpg")),
                _debug_float(block_payload.get("gapg")),
                _debug_float(block_payload.get("total")),
                _debug_float(block_payload.get("base_weight")),
                _debug_float(block_payload.get("effective_weight")),
            )
    logger.info(
        "[P1_TOTALS][TEMPORAL] TEAM_TOTAL_WEIGHTED_HOME=%s TEAM_TOTAL_WEIGHTED_AWAY=%s MATCHUP_TEMPORAL_TOTAL=%s TEMPORAL_PROFILE_SCORE=%s",
        _debug_float(temporal_layer["home_temporal"].get("weighted_total")),
        _debug_float(temporal_layer["away_temporal"].get("weighted_total")),
        _debug_float(temporal_layer.get("matchup_temporal_total")),
        _debug_float(temporal_layer.get("signal")),
    )

    logger.info(
        "[P1_TOTALS][TREND] HOME_SHORT_TERM_PROFILE=%s from TOTALS_SHORT/TOTALS_RECENT HOME_LONG_TERM_PROFILE=%s from TOTALS_MID/TOTALS_FULL AWAY_SHORT_TERM_PROFILE=%s from TOTALS_SHORT/TOTALS_RECENT AWAY_LONG_TERM_PROFILE=%s from TOTALS_MID/TOTALS_FULL",
        _debug_float(trend_layer["home_trend"].get("short_term_profile")),
        _debug_float(trend_layer["home_trend"].get("long_term_profile")),
        _debug_float(trend_layer["away_trend"].get("short_term_profile")),
        _debug_float(trend_layer["away_trend"].get("long_term_profile")),
    )
    logger.info(
        "[P1_TOTALS][TREND] short_term_profile_matchup=%s long_term_profile_matchup=%s TREND_DELTA=%s TREND_SIGNAL=%s",
        _debug_float(trend_layer.get("short_term_profile_matchup")),
        _debug_float(trend_layer.get("long_term_profile_matchup")),
        _debug_float(trend_layer.get("trend_delta")),
        _debug_float(trend_layer.get("signal")),
    )

    for layer_name in ("STRUCTURAL", "VOL", "TEMPORAL", "TREND"):
        layer_output = layer_audit.get(layer_name)
        signal = layer_signals.get(layer_name)
        logger.info(
            "[P1_TOTALS][IGNORE_SCORE] layer=%s signal_raw=%s abs_signal=%s threshold=%s epsilon=%s active_by_threshold=%s base_weight=%s effective_weight=%s weighted_signal=%s ignored=%s reason=%s",
            layer_name,
            _debug_float(signal),
            _debug_float(abs(signal) if signal is not None else None),
            _debug_float(ignore_threshold),
            _debug_float(epsilon),
            signal_is_active(signal, ignore_threshold, epsilon),
            _debug_float(layer_base_weights.get(layer_name)),
            _debug_float(layer_output.effective_weight if layer_output else None),
            _debug_float(layer_output.weighted_signal if layer_output else None),
            layer_output.ignored if layer_output else None,
            layer_output.ignored_reason if layer_output else None,
        )
    logger.info(
        "[P1_TOTALS][IGNORE_SCORE] active_weight_sum=%s weighted_sum=%s",
        _debug_float(active_weight_sum),
        _debug_float(weighted_sum),
    )
    logger.info(
        "[P1_TOTALS][IGNORE_SCORE] P1_TOTALS_SCORE=%s P1_TOTALS_DIRECTION=%s P1_TOTALS_STRENGTH=%s INTERNAL_STATE=%s status=%s status_reason=%s",
        _debug_float(p1_totals_score),
        p1_totals_direction,
        p1_totals_strength,
        internal_state,
        p1_totals_status,
        p1_totals_status_reason,
    )
    internal_state_signals = {key: value for key, value in layer_signals.items() if signal_is_active(value, ignore_threshold, epsilon)}
    logger.info(
        "[P1_TOTALS][INTERNAL_STATE] signals_used=%s active_profile_signs=%s states_detected=%s",
        {key: _debug_float(value) for key, value in internal_state_signals.items()},
        [
            math.copysign(1.0, signal)
            for signal in (
                internal_state_signals.get("STRUCTURAL"),
                internal_state_signals.get("TEMPORAL"),
                internal_state_signals.get("TREND"),
            )
            if signal_is_active(signal, ignore_threshold, epsilon)
        ],
        internal_state,
    )


def signal_is_active(signal: Optional[float], threshold: float, epsilon: float) -> bool:
    return signal is not None and abs(signal) + epsilon >= threshold
