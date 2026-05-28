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
    raw_layer_signals: Dict[str, Optional[float]],
    final_layer_signals: Dict[str, Optional[float]],
    structural_anchor: float,
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
        "[P1_TOTALS][STRUCTURAL_CALC] home_gfpg = home_gf / home_gp = %s / %s = %s",
        _debug_float(home_record.get("gf")),
        home_record.get("gp"),
        _debug_float(home_record.get("gfpg")),
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] home_gapg = home_ga / home_gp = %s / %s = %s",
        _debug_float(home_record.get("ga")),
        home_record.get("gp"),
        _debug_float(home_record.get("gapg")),
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] home_team_total_per_game = home_gfpg + home_gapg = %s + %s = %s",
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
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] away_gfpg = away_gf / away_gp = %s / %s = %s",
        _debug_float(away_record.get("gf")),
        away_record.get("gp"),
        _debug_float(away_record.get("gfpg")),
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] away_gapg = away_ga / away_gp = %s / %s = %s",
        _debug_float(away_record.get("ga")),
        away_record.get("gp"),
        _debug_float(away_record.get("gapg")),
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] away_team_total_per_game = away_gfpg + away_gapg = %s + %s = %s",
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
        "[P1_TOTALS][STRUCTURAL_CALC] LEAGUE_TOTAL_BASELINE = median(league_samples) = %s",
        _debug_float(structural_layer.get("league_total_baseline")),
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] total_distances = [abs(sample - LEAGUE_TOTAL_BASELINE) for sample in league_samples]"
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] TOTAL_DYNAMIC_SCALE = percentile(total_distances, 75) = %s",
        _debug_float(structural_layer.get("total_dynamic_scale")),
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL] HOME_ATTACK_ENVIRONMENT=%s AWAY_ATTACK_ENVIRONMENT=%s EXPECTED_TOTAL_STRUCTURAL=%s STRUCTURAL_PROFILE_SCORE=%s",
        _debug_float(structural_layer.get("home_attack_environment")),
        _debug_float(structural_layer.get("away_attack_environment")),
        _debug_float(structural_layer.get("expected_total_structural")),
        _debug_float(structural_layer.get("signal")),
    )
    home_attack_components = []
    if home_record.get("gfpg") is not None:
        home_attack_components.append(f"home_gfpg={_debug_float(home_record.get('gfpg'))}")
    if away_record.get("gapg") is not None:
        home_attack_components.append(f"away_gapg={_debug_float(away_record.get('gapg'))}")
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] HOME_ATTACK_ENVIRONMENT = avg(%s) = %s",
        ", ".join(home_attack_components) if home_attack_components else "None",
        _debug_float(structural_layer.get("home_attack_environment")),
    )
    away_attack_components = []
    if away_record.get("gfpg") is not None:
        away_attack_components.append(f"away_gfpg={_debug_float(away_record.get('gfpg'))}")
    if home_record.get("gapg") is not None:
        away_attack_components.append(f"home_gapg={_debug_float(home_record.get('gapg'))}")
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] AWAY_ATTACK_ENVIRONMENT = avg(%s) = %s",
        ", ".join(away_attack_components) if away_attack_components else "None",
        _debug_float(structural_layer.get("away_attack_environment")),
    )
    expected_components = []
    if structural_layer.get("home_attack_environment") is not None:
        expected_components.append(_debug_float(structural_layer.get("home_attack_environment")))
    if structural_layer.get("away_attack_environment") is not None:
        expected_components.append(_debug_float(structural_layer.get("away_attack_environment")))
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] EXPECTED_TOTAL_STRUCTURAL = %s = %s",
        " + ".join(expected_components) if expected_components else "None",
        _debug_float(structural_layer.get("expected_total_structural")),
    )
    logger.info(
        "[P1_TOTALS][STRUCTURAL_CALC] STRUCTURAL_PROFILE_SCORE = clamp((EXPECTED_TOTAL_STRUCTURAL - LEAGUE_TOTAL_BASELINE) / TOTAL_DYNAMIC_SCALE) = clamp((%s - %s) / %s) = %s",
        _debug_float(structural_layer.get("expected_total_structural")),
        _debug_float(structural_layer.get("league_total_baseline")),
        _debug_float(structural_layer.get("total_dynamic_scale")),
        _debug_float(structural_layer.get("signal")),
    )
    logger.info(
        "[P1_TOTALS][ANCHOR] STRUCTURAL_PROFILE_SCORE=%s STRUCTURAL_ANCHOR = 0.50 + abs(STRUCTURAL_PROFILE_SCORE) * 0.50 = %s",
        _debug_float(raw_layer_signals.get("STRUCTURAL")),
        _debug_float(structural_anchor),
    )
    logger.info(
        "[P1_TOTALS][ANCHOR] VOL_FINAL = VOL_EDGE * STRUCTURAL_ANCHOR = %s",
        _debug_float(final_layer_signals.get("VOL")),
    )
    logger.info(
        "[P1_TOTALS][ANCHOR] TEMPORAL_FINAL = TEMPORAL_PROFILE_SCORE * STRUCTURAL_ANCHOR = %s",
        _debug_float(final_layer_signals.get("TEMPORAL")),
    )
    logger.info(
        "[P1_TOTALS][ANCHOR] TREND_FINAL = TREND_SIGNAL * STRUCTURAL_ANCHOR = %s",
        _debug_float(final_layer_signals.get("TREND")),
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
        "[P1_TOTALS][VOLATILITY_CALC] STD_DEV_TOTALS_HOME = pstdev(home_game_totals) = %s",
        _debug_float(volatility_layer.get("home_std")),
    )
    logger.info(
        "[P1_TOTALS][VOLATILITY_CALC] STD_DEV_TOTALS_AWAY = pstdev(away_game_totals) = %s",
        _debug_float(volatility_layer.get("away_std")),
    )
    logger.info(
        "[P1_TOTALS][VOLATILITY_CALC] VOL_BASELINE = median(league_std_samples) = %s",
        _debug_float(volatility_layer.get("vol_baseline")),
    )
    logger.info(
        "[P1_TOTALS][VOLATILITY_CALC] deviation_samples = [abs(sample - VOL_BASELINE) for sample in league_std_samples]"
    )
    logger.info(
        "[P1_TOTALS][VOLATILITY_CALC] VOL_DYNAMIC_SCALE = percentile(deviation_samples, 75) = %s",
        _debug_float(volatility_layer.get("vol_dynamic_scale")),
    )
    vol_components = []
    if volatility_layer.get("home_std") is not None:
        vol_components.append(f"home_std={_debug_float(volatility_layer.get('home_std'))}")
    if volatility_layer.get("away_std") is not None:
        vol_components.append(f"away_std={_debug_float(volatility_layer.get('away_std'))}")
    logger.info(
        "[P1_TOTALS][VOLATILITY_CALC] MATCHUP_VOLATILITY = avg(%s) = %s",
        ", ".join(vol_components) if vol_components else "None",
        _debug_float(volatility_layer.get("matchup_volatility")),
    )
    logger.info(
        "[P1_TOTALS][VOLATILITY_CALC] VOL_EDGE = clamp((MATCHUP_VOLATILITY - VOL_BASELINE) / VOL_DYNAMIC_SCALE) = clamp((%s - %s) / %s) = %s",
        _debug_float(volatility_layer.get("matchup_volatility")),
        _debug_float(volatility_layer.get("vol_baseline")),
        _debug_float(volatility_layer.get("vol_dynamic_scale")),
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
    for side_name, temporal_payload in (("home", temporal_layer["home_temporal"]), ("away", temporal_layer["away_temporal"])):
        logger.info("[P1_TOTALS][TEMPORAL_CALC] --- %s TEMPORAL BLOCKS ---", side_name.upper())
        for block_name, block_payload in _debug_temporal_block_payload(temporal_payload, block_names).items():
            games_used = block_payload.get("games_used") or 0
            if games_used > 0:
                gf_sum = sum(item["gf"] for item in (temporal_payload.get("game_scores") or [])[:games_used])
                ga_sum = sum(item["ga"] for item in (temporal_payload.get("game_scores") or [])[:games_used])
                logger.info(
                    "[P1_TOTALS][TEMPORAL_CALC] %s %s: gfpg = gf / games_used = %s / %s = %s",
                    side_name,
                    block_name,
                    _debug_float(gf_sum),
                    games_used,
                    _debug_float(block_payload.get("gfpg")),
                )
                logger.info(
                    "[P1_TOTALS][TEMPORAL_CALC] %s %s: gapg = ga / games_used = %s / %s = %s",
                    side_name,
                    block_name,
                    _debug_float(ga_sum),
                    games_used,
                    _debug_float(block_payload.get("gapg")),
                )
                logger.info(
                    "[P1_TOTALS][TEMPORAL_CALC] %s %s: total = gfpg + gapg = %s + %s = %s",
                    side_name,
                    block_name,
                    _debug_float(block_payload.get("gfpg")),
                    _debug_float(block_payload.get("gapg")),
                    _debug_float(block_payload.get("total")),
                )
            else:
                logger.info(
                    "[P1_TOTALS][TEMPORAL_CALC] %s %s: no games available",
                    side_name,
                    block_name,
                )
        weighted_terms = []
        for block_name, block_payload in _debug_temporal_block_payload(temporal_payload, block_names).items():
            total_val = block_payload.get("total")
            weight_val = block_payload.get("base_weight")
            if total_val is not None and weight_val is not None:
                weighted_terms.append(f"({_debug_float(total_val)} * {_debug_float(weight_val)})")
        active_weights = [
            block_payload.get("base_weight")
            for block_name, block_payload in _debug_temporal_block_payload(temporal_payload, block_names).items()
            if block_payload.get("total") is not None
        ]
        sum_weights = sum(active_weights) if active_weights else 0.0
        logger.info(
            "[P1_TOTALS][TEMPORAL_CALC] %s TEAM_TOTAL_WEIGHTED = (%s) / %s = %s",
            side_name.upper(),
            " + ".join(weighted_terms) if weighted_terms else "0",
            _debug_float(sum_weights),
            _debug_float(temporal_payload.get("weighted_total")),
        )
    temporal_components = []
    if temporal_layer["home_temporal"].get("weighted_total") is not None:
        temporal_components.append(f"home={_debug_float(temporal_layer['home_temporal'].get('weighted_total'))}")
    if temporal_layer["away_temporal"].get("weighted_total") is not None:
        temporal_components.append(f"away={_debug_float(temporal_layer['away_temporal'].get('weighted_total'))}")
    logger.info(
        "[P1_TOTALS][TEMPORAL_CALC] MATCHUP_TEMPORAL_TOTAL = avg(%s) = %s",
        ", ".join(temporal_components) if temporal_components else "None",
        _debug_float(temporal_layer.get("matchup_temporal_total")),
    )
    logger.info(
        "[P1_TOTALS][TEMPORAL_CALC] TEMPORAL_PROFILE_SCORE = clamp((MATCHUP_TEMPORAL_TOTAL - LEAGUE_TOTAL_BASELINE) / TOTAL_DYNAMIC_SCALE) = clamp((%s - %s) / %s) = %s",
        _debug_float(temporal_layer.get("matchup_temporal_total")),
        _debug_float(temporal_layer.get("league_total_baseline")),
        _debug_float(temporal_layer.get("total_dynamic_scale")),
        _debug_float(temporal_layer.get("signal")),
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
    for side in ("home", "away"):
        side_trend = trend_layer.get(f"{side}_trend", {})
        side_temporal = temporal_layer[f"{side}_temporal"]
        short_val_a = side_temporal.get("TOTALS_SHORT", {}).get("total")
        short_val_b = side_temporal.get("TOTALS_RECENT", {}).get("total")
        long_val_a = side_temporal.get("TOTALS_MID", {}).get("total")
        long_val_b = side_temporal.get("TOTALS_FULL", {}).get("total")
        logger.info(
            "[P1_TOTALS][TREND_CALC] %s_SHORT_TERM_PROFILE = (TOTALS_SHORT * 0.6 + TOTALS_RECENT * 0.4) = (%s * 0.6 + %s * 0.4) = %s",
            side.upper(),
            _debug_float(short_val_a),
            _debug_float(short_val_b),
            _debug_float(side_trend.get("short_term_profile")),
        )
        logger.info(
            "[P1_TOTALS][TREND_CALC] %s_LONG_TERM_PROFILE = (TOTALS_MID * 0.6 + TOTALS_FULL * 0.4) = (%s * 0.6 + %s * 0.4) = %s",
            side.upper(),
            _debug_float(long_val_a),
            _debug_float(long_val_b),
            _debug_float(side_trend.get("long_term_profile")),
        )
    logger.info(
        "[P1_TOTALS][TREND_CALC] short_term_profile_matchup = (HOME_SHORT_TERM_PROFILE + AWAY_SHORT_TERM_PROFILE) / 2 = (%s + %s) / 2 = %s",
        _debug_float(trend_layer.get("home_trend", {}).get("short_term_profile")),
        _debug_float(trend_layer.get("away_trend", {}).get("short_term_profile")),
        _debug_float(trend_layer.get("short_term_profile_matchup")),
    )
    logger.info(
        "[P1_TOTALS][TREND_CALC] long_term_profile_matchup = (HOME_LONG_TERM_PROFILE + AWAY_LONG_TERM_PROFILE) / 2 = (%s + %s) / 2 = %s",
        _debug_float(trend_layer.get("home_trend", {}).get("long_term_profile")),
        _debug_float(trend_layer.get("away_trend", {}).get("long_term_profile")),
        _debug_float(trend_layer.get("long_term_profile_matchup")),
    )
    logger.info(
        "[P1_TOTALS][TREND_SCALE] league_trend_deltas summary=%s",
        _debug_sample_summary(trend_layer.get("league_trend_deltas") or []),
    )
    logger.info(
        "[P1_TOTALS][TREND_SCALE] league_trend_sample_count=%s",
        trend_layer.get("league_trend_sample_count"),
    )
    logger.info(
        "[P1_TOTALS][TREND_SCALE] TREND_BASELINE=%s",
        _debug_float(trend_layer.get("trend_baseline")),
    )
    logger.info(
        "[P1_TOTALS][TREND_SCALE] trend_deviation_samples summary=%s",
        _debug_sample_summary(trend_layer.get("trend_deviation_samples") or []),
    )
    logger.info(
        "[P1_TOTALS][TREND_SCALE] TREND_DYNAMIC_SCALE=%s",
        _debug_float(trend_layer.get("trend_dynamic_scale")),
    )
    logger.info(
        "[P1_TOTALS][TREND_SCALE] MATCHUP_TREND_DELTA=%s",
        _debug_float(trend_layer.get("matchup_trend_delta")),
    )
    logger.info(
        "[P1_TOTALS][TREND_SCALE] TREND_SIGNAL = clamp((MATCHUP_TREND_DELTA - TREND_BASELINE) / TREND_DYNAMIC_SCALE) = clamp((%s - %s) / %s) = %s",
        _debug_float(trend_layer.get("matchup_trend_delta")),
        _debug_float(trend_layer.get("trend_baseline")),
        _debug_float(trend_layer.get("trend_dynamic_scale")),
        _debug_float(trend_layer.get("signal")),
    )
    logger.info(
        "[P1_TOTALS][TREND] short_term_profile_matchup=%s long_term_profile_matchup=%s MATCHUP_TREND_DELTA=%s TREND_SIGNAL=%s",
        _debug_float(trend_layer.get("short_term_profile_matchup")),
        _debug_float(trend_layer.get("long_term_profile_matchup")),
        _debug_float(trend_layer.get("matchup_trend_delta")),
        _debug_float(trend_layer.get("signal")),
    )

    for layer_name in ("STRUCTURAL", "VOL", "TEMPORAL", "TREND"):
        layer_output = layer_audit.get(layer_name)
        raw_signal = raw_layer_signals.get(layer_name)
        final_signal = final_layer_signals.get(layer_name)
        logger.info(
            "[P1_TOTALS][IGNORE_SCORE] layer=%s raw_signal=%s abs_raw_signal=%s threshold=%s epsilon=%s active_by_threshold=%s structural_anchor=%s final_signal=%s base_weight=%s effective_weight=%s weighted_signal=%s ignored=%s reason=%s",
            layer_name,
            _debug_float(raw_signal),
            _debug_float(abs(raw_signal) if raw_signal is not None else None),
            _debug_float(ignore_threshold),
            _debug_float(epsilon),
            signal_is_active(raw_signal, ignore_threshold, epsilon),
            _debug_float(structural_anchor),
            _debug_float(final_signal),
            _debug_float(layer_base_weights.get(layer_name)),
            _debug_float(layer_output.effective_weight if layer_output else None),
            _debug_float(layer_output.weighted_signal if layer_output else None),
            layer_output.ignored if layer_output else None,
            layer_output.ignored_reason if layer_output else None,
        )
    logger.info("[P1_TOTALS][IGNORE_SCORE_CALC] --- LAYER WEIGHTING AND SCORE CALCULATION ---")
    for layer_name in ("STRUCTURAL", "VOL", "TEMPORAL", "TREND"):
        layer_output = layer_audit.get(layer_name)
        raw_signal = raw_layer_signals.get(layer_name)
        final_signal = final_layer_signals.get(layer_name)
        base_weight = layer_base_weights.get(layer_name)
        if layer_output and not layer_output.ignored:
            logger.info(
                "[P1_TOTALS][IGNORE_SCORE_CALC] layer=%s: effective_weight = base_weight * confidence_total = %s * %s = %s",
                layer_name,
                _debug_float(base_weight),
                _debug_float(confidence_total),
                _debug_float(layer_output.effective_weight),
            )
            logger.info(
                "[P1_TOTALS][IGNORE_SCORE_CALC] layer=%s: weighted_signal = final_signal * effective_weight = %s * %s = %s",
                layer_name,
                _debug_float(final_signal),
                _debug_float(layer_output.effective_weight),
                _debug_float(layer_output.weighted_signal),
            )
        else:
            reason = layer_output.ignored_reason if layer_output else "missing"
            logger.info(
                "[P1_TOTALS][IGNORE_SCORE_CALC] layer=%s: IGNORED (reason: %s)",
                layer_name,
                reason,
            )
    weighted_terms = []
    weight_terms = []
    for layer_name in ("STRUCTURAL", "VOL", "TEMPORAL", "TREND"):
        layer_output = layer_audit.get(layer_name)
        if layer_output and not layer_output.ignored:
            weighted_terms.append(f"{_debug_float(layer_output.weighted_signal)} ({layer_name})")
            weight_terms.append(f"{_debug_float(layer_output.effective_weight)} ({layer_name})")
    logger.info(
        "[P1_TOTALS][IGNORE_SCORE_CALC] weighted_sum = %s = %s",
        " + ".join(weighted_terms) if weighted_terms else "0",
        _debug_float(weighted_sum),
    )
    logger.info(
        "[P1_TOTALS][IGNORE_SCORE_CALC] active_weight_sum = %s = %s",
        " + ".join(weight_terms) if weight_terms else "0",
        _debug_float(active_weight_sum),
    )
    logger.info(
        "[P1_TOTALS][IGNORE_SCORE_CALC] P1_TOTALS_SCORE = clamp(weighted_sum / active_weight_sum) = clamp(%s / %s) = %s",
        _debug_float(weighted_sum),
        _debug_float(active_weight_sum),
        _debug_float(p1_totals_score),
    )
    logger.info(
        "[P1_TOTALS][IGNORE_SCORE_CALC] raw_signal decides activity; final_signal contributes to score.",
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
    internal_state_signals = {
        key: value
        for key, value in raw_layer_signals.items()
        if signal_is_active(value, ignore_threshold, epsilon)
    }
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
