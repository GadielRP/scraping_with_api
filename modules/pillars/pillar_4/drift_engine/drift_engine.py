"""Temporal market drift engine for Pillar 4."""

from __future__ import annotations

import logging
import math
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, Optional

from modules.pillars.context import EventContext
from modules.pillars.odds_trajectory_context import (
    ChoiceOddsTrajectory,
    OddsTrajectoryContext,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== P4_DRIFT_ENGINE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("P4_DRIFT_ENGINE DEBUG | " + message, *args)


def _debug_formula(
    name: str,
    formula: str,
    substitution: str,
    result: Any,
    meaning: Optional[str] = None,
) -> None:
    logger.info("P4_DRIFT_ENGINE DEBUG | %s", name)
    logger.info("P4_DRIFT_ENGINE DEBUG |   Formula: %s", formula)
    logger.info("P4_DRIFT_ENGINE DEBUG |   Sustitución: %s", substitution)
    logger.info("P4_DRIFT_ENGINE DEBUG |   Resultado: %s", result)
    if meaning:
        logger.info("P4_DRIFT_ENGINE DEBUG |   Lectura: %s", meaning)


def _fmt(value: Any, decimals: int = 6) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        try:
            return f"{value:.{decimals}f}"
        except Exception:
            return str(value)
    if isinstance(value, float):
        if math.isfinite(value):
            return f"{value:.{decimals}f}"
        return str(value)
    if isinstance(value, dict):
        items = list(value.items())
        preview = ", ".join(f"{k}: {_fmt(v, decimals)}" for k, v in items[:4])
        if len(items) > 4:
            preview += ", ..."
        return f"{{{preview}}} (n={len(items)})"
    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        preview = ", ".join(_fmt(item, decimals) for item in sequence[:5])
        if len(sequence) > 5:
            preview += ", ..."
        return f"[{preview}] (n={len(sequence)})"
    return str(value)

MIN_MOVE_THRESHOLD = Decimal("0.02")
MIN_MOVE_THRESHOLD_STATUS = "EMPIRICAL_PENDING"
P4_EDGE_STATUS = "NOT_NORMALIZED"
ENGINE_VERSION = "p4_temporal_market_engine_v2_1"
MODULE_ID = "DRIFT_ENGINE"
MODULE_NAME = "Temporal Market Drift Engine"

_SEGMENTS = ("D1", "D2", "D3", "D4")
_REQUIRED_INPUT_LABELS = (
    "open_odds",
    "t120_odds",
    "t30_odds",
    "t5_odds",
    "kickoff_odds",
)
_META_MINUTES = (120, 30, 5, 0, -5)


def _format_decimal(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def _to_decimal_or_none(value: Any) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        if isinstance(value, float):
            return Decimal(str(value))
        text = str(value).strip()
        if not text:
            return None
        return Decimal(text)
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return None


def _calculate_implied_prob(odds: Optional[Decimal]) -> Optional[Decimal]:
    if odds is None:
        return None
    if odds <= 0:
        return None
    return Decimal("1") / odds


def _prob_direction(value: Optional[Decimal]) -> int:
    if value is None:
        return 0
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _decimal_to_serializable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _decimal_to_serializable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_decimal_to_serializable(item) for item in value]
    return value


def _sign_decimal(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _slugify_key_part(value: Any, lowercase: bool = False) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return "unknown"
    sanitized = re.sub(r"[\/\s\-.]+", "_", text)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    if not sanitized:
        return "unknown"
    return sanitized.lower() if lowercase else sanitized


def _build_trajectory_key(market_group: Any, market_period: Any) -> str:
    group_part = _slugify_key_part(market_group, lowercase=False)
    period_part = _slugify_key_part(market_period, lowercase=True)
    return f"{group_part}_{period_part}"


def _build_choice_result_key(
    market_name: Any,
    choice_group: Any,
    bookie_name: Any,
    choice_name: Any,
) -> str:
    resolved_choice_group = "__default__" if choice_group in (None, "") else str(choice_group)
    resolved_bookie_name = "__unknown__" if bookie_name in (None, "") else str(bookie_name)
    return f"{resolved_bookie_name}|{resolved_choice_group}|{choice_name}|{market_name}"


def _get_required_inputs(choice: ChoiceOddsTrajectory) -> Dict[str, Optional[Decimal]]:
    required_inputs = {
        "open_odds": _to_decimal_or_none(choice.initial_odds),
        "t120_odds": _to_decimal_or_none(choice.odds_values.get(120)),
        "t30_odds": _to_decimal_or_none(choice.odds_values.get(30)),
        "t5_odds": _to_decimal_or_none(choice.odds_values.get(5)),
        "kickoff_odds": _to_decimal_or_none(choice.odds_values.get(0)),
    }
    return required_inputs


def _build_missing_inputs(required_inputs: Dict[str, Optional[Decimal]]) -> list[str]:
    return [label for label in _REQUIRED_INPUT_LABELS if required_inputs.get(label) is None]


def _calculate_dirs(deltas: Dict[str, Decimal]) -> tuple[Dict[str, int], Dict[str, Decimal], Dict[str, Decimal]]:
    dirs: Dict[str, int] = {}
    abs_deltas: Dict[str, Decimal] = {}
    effective_abs_deltas: Dict[str, Decimal] = {}

    for segment_name, delta in deltas.items():
        abs_delta = abs(delta)
        abs_deltas[segment_name] = abs_delta
        if abs_delta < MIN_MOVE_THRESHOLD:
            dirs[segment_name] = 0
            effective_abs_deltas[segment_name] = Decimal("0")
        else:
            dirs[segment_name] = _sign_decimal(delta)
            effective_abs_deltas[segment_name] = abs_delta

    return dirs, abs_deltas, effective_abs_deltas


def _count_sign_changes(non_zero_dirs: Iterable[int]) -> int:
    sequence = list(non_zero_dirs)
    if len(sequence) < 2:
        return 0

    sign_changes = 0
    previous = sequence[0]
    for current in sequence[1:]:
        if current != previous:
            sign_changes += 1
        previous = current
    return sign_changes


def _calculate_correction_metrics(
    deltas: Dict[str, Decimal],
    dirs: Dict[str, int],
) -> Dict[str, Any]:
    initial_sign: Optional[int] = None
    initial_segments: list[str] = []
    correction_segments: list[str] = []
    mode = "NO_REAL_MOVE"
    correction_started = False

    for segment in _SEGMENTS:
        current_dir = dirs[segment]

        if current_dir == 0:
            continue

        if initial_sign is None:
            initial_sign = current_dir
            initial_segments.append(segment)
            mode = "FIRST_DIRECTIONAL_LEG"
            continue

        if current_dir == initial_sign and not correction_started:
            initial_segments.append(segment)
            continue

        if current_dir != initial_sign:
            correction_started = True
            correction_segments.append(segment)
            mode = "FIRST_CORRECTION_LEG"
            continue

        if correction_started and current_dir == initial_sign:
            break

    initial_move_abs = abs(sum((deltas[segment] for segment in initial_segments), Decimal("0")))
    correction_abs = abs(sum((deltas[segment] for segment in correction_segments), Decimal("0")))

    if initial_move_abs == 0:
        correction_ratio = None
        move_retention = Decimal("1.00")
        if not initial_segments:
            mode = "NO_REAL_MOVE"
    elif not correction_segments:
        correction_ratio = None
        move_retention = Decimal("1.00")
    else:
        correction_ratio = correction_abs / initial_move_abs
        move_retention = max(Decimal("0"), Decimal("1") - correction_ratio)

    return {
        "INITIAL_MOVE_ABS": initial_move_abs,
        "CORRECTION_ABS": correction_abs,
        "CORRECTION_RATIO": correction_ratio,
        "MOVE_RETENTION": move_retention,
        "CORRECTION_CALC_MODE": mode,
        "INITIAL_MOVE_SEGMENTS": initial_segments,
        "CORRECTION_SEGMENTS": correction_segments,
    }


def _classify_drift_pattern(direction_sequence: list[int]) -> tuple[str, int]:
    directions_for_sign_change = [
        direction for direction in direction_sequence if direction != 0
    ]

    if not directions_for_sign_change:
        return "NO_MOVE", 0

    sign_change_count = _count_sign_changes(directions_for_sign_change)

    if sign_change_count == 0:
        return "UNIDIRECTIONAL", sign_change_count
    if sign_change_count == 1:
        return "REVERSAL", sign_change_count
    return "CHOPPY", sign_change_count


def _classify_drift_confidence(pattern: str) -> str:
    return {
        "NO_MOVE": "NONE",
        "UNIDIRECTIONAL": "HIGH",
        "REVERSAL": "MEDIUM",
        "CHOPPY": "LOW",
    }[pattern]


def _calculate_move_timing(abs_deltas: Dict[str, Decimal]) -> tuple[str, Dict[str, Any]]:
    effective_abs = {
        segment: Decimal("0") if value < MIN_MOVE_THRESHOLD else value
        for segment, value in abs_deltas.items()
    }
    total_effective_segment_move = sum(effective_abs.values(), Decimal("0"))
    max_effective_segment_move = max(effective_abs.values(), default=Decimal("0"))

    if total_effective_segment_move == 0:
        return (
            "N/A",
            {
                "effective_abs": effective_abs,
                "total_effective_segment_move": Decimal("0"),
                "max_effective_segment_move": Decimal("0"),
                "max_share": Decimal("0"),
                "dominant_segments": [],
            },
        )

    dominant_segments = [
        segment
        for segment in _SEGMENTS
        if effective_abs.get(segment, Decimal("0")) == max_effective_segment_move
    ]
    max_share = max_effective_segment_move / total_effective_segment_move
    early_segments = {"D1", "D2"}
    late_segments = {"D3", "D4"}

    if max_share < Decimal("0.50"):
        move_timing = "DISTRIBUTED"
    elif all(segment in early_segments for segment in dominant_segments):
        move_timing = "EARLY"
    elif all(segment in late_segments for segment in dominant_segments):
        move_timing = "LATE"
    else:
        move_timing = "SPLIT"

    return (
        move_timing,
        {
            "effective_abs": effective_abs,
            "total_effective_segment_move": total_effective_segment_move,
            "max_effective_segment_move": max_effective_segment_move,
            "max_share": max_share,
            "dominant_segments": dominant_segments,
        },
    )


def _serialize_meta_by_minute(choice: ChoiceOddsTrajectory) -> Dict[int, Dict[str, Any]]:
    serialized: Dict[int, Dict[str, Any]] = {}
    for minute in _META_MINUTES:
        meta = choice.meta_by_minute.get(minute)
        if meta is None:
            continue
        serialized[minute] = {
            "snapshot_id": meta.snapshot_id,
            "collected_at": meta.collected_at.isoformat() if meta.collected_at else None,
            "minutes_before_start": meta.minutes_before_start,
            "target_minute": meta.target_minute,
            "distance_from_target": meta.distance_from_target,
        }
    return serialized


def _build_missing_result(
    market_group: str,
    market_period: str,
    market_name: str,
    choice_group: Optional[str],
    bookie_name: str,
    choice_name: str,
    required_inputs: Dict[str, Optional[Decimal]],
    choice: ChoiceOddsTrajectory,
    debug_mode: bool = False,
) -> Dict[str, Any]:
    missing_inputs = _build_missing_inputs(required_inputs)
    if debug_mode:
        _debug_section(f"CHOICE INSUFFICIENT: {market_group} | {market_period} | {bookie_name} | {choice_name}")
        _debug_line("Faltan los siguientes inputs requeridos para calcular P4:")
        for label, val in required_inputs.items():
            _debug_line("  %s: %s", label, _fmt(val) if val is not None else "FALTANTE")
        _debug_line("Detalle de inputs faltantes: %s", missing_inputs)
    return {
        "status": "INSUFFICIENT_DATA",
        "P4_STATUS": "INSUFFICIENT_DATA",
        "MISSING_INPUTS": missing_inputs,
        "market_group": market_group,
        "market_period": market_period,
        "market_name": market_name,
        "choice_group": choice_group,
        "bookie_name": bookie_name,
        "choice_name": choice_name,
        "post_kickoff_odds": _to_decimal_or_none(choice.odds_values.get(-5)),
        "effective_kickoff_odds": None,
        "POST_KICKOFF_AUDIT_STATUS": "NOT_AVAILABLE",
        "POST_KICKOFF_REPLACED_KICKOFF": False,
        "raw": {
            "required_inputs": {key: required_inputs.get(key) for key in _REQUIRED_INPUT_LABELS},
            "meta_by_minute": _serialize_meta_by_minute(choice),
        },
    }


def _build_active_result(
    market_group: str,
    market_period: str,
    market_name: str,
    choice_group: Optional[str],
    bookie_name: str,
    choice_name: str,
    required_inputs: Dict[str, Decimal],
    choice: ChoiceOddsTrajectory,
    event_id: Optional[int],
    trajectory_key: str,
    debug_mode: bool = False,
) -> Dict[str, Any]:
    open_odds = required_inputs["open_odds"]
    t120_odds = required_inputs["t120_odds"]
    t30_odds = required_inputs["t30_odds"]
    t5_odds = required_inputs["t5_odds"]
    kickoff_odds = required_inputs["kickoff_odds"]

    post_kickoff_odds = _to_decimal_or_none(choice.odds_values.get(-5))

    if post_kickoff_odds is None:
        post_kickoff_audit_status = "NOT_AVAILABLE"
        effective_kickoff_odds = kickoff_odds
        post_kickoff_replaced_kickoff = False
    elif post_kickoff_odds == kickoff_odds:
        post_kickoff_audit_status = "UNCHANGED"
        effective_kickoff_odds = kickoff_odds
        post_kickoff_replaced_kickoff = False
    else:
        post_kickoff_audit_status = "CHANGED_AFTER_KICKOFF"
        effective_kickoff_odds = post_kickoff_odds
        post_kickoff_replaced_kickoff = True

        msg = (
            f"WARNING:\n"
            f"P4_POST_KICKOFF_ODDS_CHANGED\n"
            f"event_id={event_id}\n"
            f"trajectory_key={trajectory_key}\n"
            f"market_group={market_group}\n"
            f"market_period={market_period}\n"
            f"bookie={bookie_name}\n"
            f"choice={choice_name}\n"
            f"kickoff_odds={kickoff_odds}\n"
            f"post_kickoff_odds={post_kickoff_odds}\n"
            f"target_minute=-5\n"
            f"reason=odds_changed_after_kickoff"
        )
        logger.warning(msg)

    deltas = {
        "D1": t120_odds - open_odds,
        "D2": t30_odds - t120_odds,
        "D3": t5_odds - t30_odds,
        "D4": effective_kickoff_odds - t5_odds,
    }
    dirs, abs_deltas, effective_abs_deltas = _calculate_dirs(deltas)
    direction_sequence = [dirs[segment] for segment in _SEGMENTS]
    drift_pattern, sign_change_count = _classify_drift_pattern(direction_sequence)
    drift_confidence = _classify_drift_confidence(drift_pattern)
    total_move = effective_kickoff_odds - open_odds
    total_move_abs = abs(total_move)
    drift_direction = 0 if drift_pattern == "NO_MOVE" else _sign_decimal(total_move)

    move_timing, move_timing_raw = _calculate_move_timing(abs_deltas)
    if drift_pattern == "NO_MOVE":
        move_timing = "N/A"

    correction_metrics = _calculate_correction_metrics(
        deltas=deltas,
        dirs=dirs,
    )
    initial_move_abs = correction_metrics["INITIAL_MOVE_ABS"]
    correction_abs = correction_metrics["CORRECTION_ABS"]
    correction_ratio = correction_metrics["CORRECTION_RATIO"]
    move_retention = correction_metrics["MOVE_RETENTION"]
    correction_calc_mode = correction_metrics["CORRECTION_CALC_MODE"]
    initial_move_segments = correction_metrics["INITIAL_MOVE_SEGMENTS"]
    correction_segments = correction_metrics["CORRECTION_SEGMENTS"]

    open_implied_prob = _calculate_implied_prob(open_odds)
    close_implied_prob = _calculate_implied_prob(effective_kickoff_odds)
    if open_implied_prob is not None and close_implied_prob is not None:
        implied_prob_move = close_implied_prob - open_implied_prob
    else:
        implied_prob_move = None
    p4_prob_direction = _prob_direction(implied_prob_move)
    close_implied_prob_odds_source = "effective_kickoff_odds"

    if implied_prob_move is None:
        raw_market_transfer_pp = None
    else:
        raw_market_transfer_pp = implied_prob_move * move_retention * Decimal("100")

    if debug_mode:
        _debug_section(f"CHOICE ACTIVE: {market_group} | {market_period} | {bookie_name} | {choice_name}")
        _debug_line("Inputs cuotas:")
        _debug_line("  open_odds (O)   = %s", _fmt(open_odds))
        _debug_line("  t120_odds (120) = %s", _fmt(t120_odds))
        _debug_line("  t30_odds (30)   = %s", _fmt(t30_odds))
        _debug_line("  t5_odds (5)     = %s", _fmt(t5_odds))
        _debug_line("  kickoff_odds (K)= %s", _fmt(kickoff_odds))
        _debug_line("  post_kickoff_odds (PK) = %s", _fmt(post_kickoff_odds))
        _debug_line("  effective_kickoff_odds = %s", _fmt(effective_kickoff_odds))
        _debug_line("  post_kickoff_audit_status = %s", post_kickoff_audit_status)
        _debug_line("  post_kickoff_replaced_kickoff = %s", post_kickoff_replaced_kickoff)

        _debug_section("CALCULO DE DELTAS")
        _debug_formula("Delta 1 (D1)", "t120_odds - open_odds", f"{_fmt(t120_odds)} - {_fmt(open_odds)}", deltas["D1"])
        _debug_formula("Delta 2 (D2)", "t30_odds - t120_odds", f"{_fmt(t30_odds)} - {_fmt(t120_odds)}", deltas["D2"])
        _debug_formula("Delta 3 (D3)", "t5_odds - t30_odds", f"{_fmt(t5_odds)} - {_fmt(t30_odds)}", deltas["D3"])
        _debug_formula("Delta 4 (D4)", "effective_kickoff_odds - t5_odds", f"{_fmt(effective_kickoff_odds)} - {_fmt(t5_odds)}", deltas["D4"])

        _debug_line("Umbral de movimiento minimo (MIN_MOVE_THRESHOLD): %s", _fmt(MIN_MOVE_THRESHOLD))
        for segment in _SEGMENTS:
            _debug_line("Segmento %s: delta=%s, abs_delta=%s, effective_abs=%s, dir=%s",
                        segment, _fmt(deltas[segment]), _fmt(abs_deltas[segment]), _fmt(effective_abs_deltas[segment]), dirs[segment])

        _debug_section("CLASIFICACION DE PATRON DE DRIFT")
        _debug_formula("Movimiento Total (total_move)", "effective_kickoff_odds - open_odds", f"{_fmt(effective_kickoff_odds)} - {_fmt(open_odds)}", total_move)
        _debug_line("DIRECTION_SEQUENCE: %s", direction_sequence)
        _debug_line("SIGN_CHANGE_COUNT ignorando ceros: %s", sign_change_count)
        _debug_line("Patron clasificado: %s", drift_pattern)
        _debug_line("Confianza clasificada: %s", drift_confidence)
        _debug_line("Direccion final del drift: %s", drift_direction)

        _debug_section("CALCULO DE TIMING")
        if drift_pattern == "NO_MOVE":
            _debug_line("Timing: N/A (No hubo movimiento suficiente)")
        else:
            _debug_formula("Suma de movimientos efectivos", "sum(effective_abs)", " + ".join(f"{_fmt(effective_abs_deltas[s])}" for s in _SEGMENTS), move_timing_raw["total_effective_segment_move"])
            _debug_formula("Proporcion del maximo segmento", "max_effective / total_effective", f"{_fmt(move_timing_raw['max_effective_segment_move'])} / {_fmt(move_timing_raw['total_effective_segment_move'])}", move_timing_raw["max_share"])
            _debug_line("Segmento(s) dominante(s): %s", move_timing_raw["dominant_segments"])
            _debug_line("Timing clasificado: %s", move_timing)

        _debug_section("IMPLIED PROBABILITY TRANSFER")
        _debug_line("open_odds = %s", _fmt(open_odds))
        _debug_line("effective_kickoff_odds = %s", _fmt(effective_kickoff_odds))
        _debug_line("OPEN_IMPLIED_PROB = %s", _fmt(open_implied_prob))
        _debug_line("CLOSE_IMPLIED_PROB = %s", _fmt(close_implied_prob))
        _debug_line("CLOSE_IMPLIED_PROB_ODDS_SOURCE = %s", close_implied_prob_odds_source)
        _debug_line("IMPLIED_PROB_MOVE = %s", _fmt(implied_prob_move))
        _debug_line("P4_PROB_DIRECTION = %s", p4_prob_direction)

        _debug_section("CORRECTION / RETENTION")
        _debug_line("DIRECTION_SEQUENCE = %s", direction_sequence)
        _debug_line("SIGN_CHANGE_COUNT = %s", sign_change_count)
        _debug_line("INITIAL_MOVE_SEGMENTS = %s", initial_move_segments)
        _debug_line("INITIAL_MOVE_ABS = %s", _fmt(initial_move_abs))
        _debug_line("CORRECTION_SEGMENTS = %s", correction_segments)
        _debug_line("CORRECTION_ABS = %s", _fmt(correction_abs))
        _debug_line("CORRECTION_RATIO = %s", _fmt(correction_ratio))
        _debug_line("MOVE_RETENTION = %s", _fmt(move_retention))
        _debug_line("CORRECTION_CALC_MODE = %s", correction_calc_mode)

        _debug_section("RAW MARKET TRANSFER")
        _debug_line("RAW_MARKET_TRANSFER_PP = %s", _fmt(raw_market_transfer_pp))
        _debug_line("P4_EDGE_STATUS = %s", P4_EDGE_STATUS)
        _debug_line("raw_transfer_is_normalized = %s", False)
        _debug_line("normalization_pending = %s", True)

    return {
        "status": "ACTIVE",
        "P4_STATUS": "ACTIVE",
        "P4_EDGE_STATUS": P4_EDGE_STATUS,
        "market_group": market_group,
        "market_period": market_period,
        "market_name": market_name,
        "choice_group": choice_group,
        "bookie_name": bookie_name,
        "choice_name": choice_name,
        "open_odds": open_odds,
        "t120_odds": t120_odds,
        "t30_odds": t30_odds,
        "t5_odds": t5_odds,
        "kickoff_odds": kickoff_odds,
        "post_kickoff_odds": post_kickoff_odds,
        "effective_kickoff_odds": effective_kickoff_odds,
        "POST_KICKOFF_AUDIT_STATUS": post_kickoff_audit_status,
        "POST_KICKOFF_REPLACED_KICKOFF": post_kickoff_replaced_kickoff,
        "DRIFT_PATTERN": drift_pattern,
        "DRIFT_DIRECTION": drift_direction,
        "DRIFT_CONFIDENCE": drift_confidence,
        "MOVE_TIMING": move_timing,
        "TOTAL_MOVE": total_move,
        "TOTAL_MOVE_ABS": total_move_abs,
        "OPEN_IMPLIED_PROB": open_implied_prob,
        "CLOSE_IMPLIED_PROB": close_implied_prob,
        "IMPLIED_PROB_MOVE": implied_prob_move,
        "P4_PROB_DIRECTION": p4_prob_direction,
        "CLOSE_IMPLIED_PROB_ODDS_SOURCE": close_implied_prob_odds_source,
        "INITIAL_MOVE_ABS": initial_move_abs,
        "CORRECTION_ABS": correction_abs,
        "CORRECTION_RATIO": correction_ratio,
        "MOVE_RETENTION": move_retention,
        "RAW_MARKET_TRANSFER_PP": raw_market_transfer_pp,
        "D1": deltas["D1"],
        "D2": deltas["D2"],
        "D3": deltas["D3"],
        "D4": deltas["D4"],
        "ABS_D1": abs_deltas["D1"],
        "ABS_D2": abs_deltas["D2"],
        "ABS_D3": abs_deltas["D3"],
        "ABS_D4": abs_deltas["D4"],
        "DIR1": dirs["D1"],
        "DIR2": dirs["D2"],
        "DIR3": dirs["D3"],
        "DIR4": dirs["D4"],
        "DIRECTION_SEQUENCE": direction_sequence,
        "MISSING_INPUTS": [],
        "SIGN_CHANGE_COUNT": sign_change_count,
        "raw": {
            "min_move_threshold": MIN_MOVE_THRESHOLD,
            "min_move_threshold_status": MIN_MOVE_THRESHOLD_STATUS,
            "effective_abs": effective_abs_deltas,
            "total_effective_segment_move": move_timing_raw["total_effective_segment_move"],
            "max_effective_segment_move": move_timing_raw["max_effective_segment_move"],
            "max_share": move_timing_raw["max_share"],
            "dominant_segments": move_timing_raw["dominant_segments"],
            "sign_change_count": sign_change_count,
            "meta_by_minute": _serialize_meta_by_minute(choice),
            "post_kickoff_odds": post_kickoff_odds,
            "effective_kickoff_odds": effective_kickoff_odds,
            "post_kickoff_audit_status": post_kickoff_audit_status,
            "post_kickoff_replaced_kickoff": post_kickoff_replaced_kickoff,
            "implied_probability": {
                "open_implied_prob": open_implied_prob,
                "close_implied_prob": close_implied_prob,
                "close_implied_prob_odds_source": close_implied_prob_odds_source,
                "implied_prob_move": implied_prob_move,
                "p4_prob_direction": p4_prob_direction,
            },
            "correction_metrics": {
                "initial_move_abs": initial_move_abs,
                "correction_abs": correction_abs,
                "correction_ratio": correction_ratio,
                "move_retention": move_retention,
                "correction_calc_mode": correction_calc_mode,
                "initial_move_segments": initial_move_segments,
                "correction_segments": correction_segments,
            },
            "raw_market_transfer": {
                "raw_market_transfer_pp": raw_market_transfer_pp,
                "p4_edge_status": P4_EDGE_STATUS,
                "raw_transfer_is_normalized": False,
                "raw_transfer_uses_edge_threshold": False,
                "normalization_pending": True,
            },
        },
    }


def _derive_global_status(market_period_results: Dict[str, Dict[str, Any]]) -> str:
    if not market_period_results:
        return "INSUFFICIENT_DATA"

    statuses = [result.get("status") for result in market_period_results.values()]
    active_count = sum(1 for status in statuses if status == "ACTIVE")
    insufficient_count = sum(1 for status in statuses if status == "INSUFFICIENT_DATA")

    if active_count == len(statuses):
        return "ACTIVE"
    if active_count > 0 and insufficient_count > 0:
        return "PARTIAL"
    return "INSUFFICIENT_DATA"


def calculate_p4_drift_engine(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext,
    debug_mode: bool = False,
) -> dict:
    """Calculate temporal market drift across every available market trajectory."""
    if debug_mode:
        _debug_section("INICIO P4 DRIFT ENGINE")
        _debug_line("Event ID: %s", event_context.event_id)
        _debug_line("Participantes: %s", event_context.participants_label)
        _debug_line("Odds Trajectory Available: %s", odds_trajectory_context.available)
        _debug_line("Market Groups Count: %s", len(odds_trajectory_context.markets))
        _debug_line("Expected Target Minutes: %s", odds_trajectory_context.target_minutes_expected)
        _debug_line("Present Target Minutes: %s", odds_trajectory_context.target_minutes_present)
        _debug_line("Missing Target Minutes: %s", odds_trajectory_context.missing_target_minutes)

    base_result = {
        "module_id": MODULE_ID,
        "module_name": MODULE_NAME,
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P4_STATUS": "INSUFFICIENT_DATA",
        "status": "INSUFFICIENT_DATA",
        "market_period_results": {},
        "market_period_count": 0,
        "active_market_period_count": 0,
        "insufficient_market_period_count": 0,
        "raw": {
            "odds_trajectory_available": odds_trajectory_context.available,
            "target_minutes_expected": odds_trajectory_context.target_minutes_expected,
            "target_minutes_present": odds_trajectory_context.target_minutes_present,
            "missing_target_minutes": odds_trajectory_context.missing_target_minutes,
            "min_move_threshold": MIN_MOVE_THRESHOLD,
            "min_move_threshold_status": MIN_MOVE_THRESHOLD_STATUS,
            "p4_edge_status": P4_EDGE_STATUS,
        },
    }

    if not odds_trajectory_context.available or not odds_trajectory_context.markets:
        if debug_mode:
            _debug_line("Short-circuit: Trayectoria de cuotas no disponible.")
        base_result["raw"]["reason"] = "odds_trajectory_unavailable"
        return _decimal_to_serializable(base_result)

    market_period_results: Dict[str, Dict[str, Any]] = {}

    for market_group in sorted(odds_trajectory_context.markets):
        periods = odds_trajectory_context.markets[market_group]
        if debug_mode:
            _debug_section(f"ESCANEANDO MARKET GROUP: {market_group}")
            _debug_line("Periodos que contiene: %s", list(sorted(periods.keys())))
            for p_name in sorted(periods.keys()):
                p_choices = []
                m_names = periods[p_name]
                for m_name in sorted(m_names.keys()):
                    c_groups = m_names[m_name]
                    for c_grp in sorted(c_groups.keys()):
                        m_line = c_groups[c_grp]
                        for b_name in sorted(m_line.bookies.keys()):
                            bk = m_line.bookies[b_name]
                            for c_name in sorted(bk.choices.keys()):
                                c_lbl = f"{c_name}"
                                if c_grp != "__default__":
                                    c_lbl += f" ({c_grp})"
                                if c_lbl not in p_choices:
                                    p_choices.append(c_lbl)
                _debug_line("  -> Periodo '%s' tiene opciones (choices): %s", p_name, p_choices)
        for market_period in sorted(periods):
            market_names = periods[market_period]
            trajectory_key = _build_trajectory_key(market_group, market_period)
            if debug_mode:
                _debug_section(f"TRAYECTORIA: {trajectory_key}")
            choice_results: Dict[str, Dict[str, Any]] = {}
            missing_inputs_summary: Dict[str, int] = {}
            choice_count = 0
            active_choice_count = 0
            insufficient_choice_count = 0

            for market_name in sorted(market_names):
                choice_groups = market_names[market_name]
                if debug_mode:
                    _debug_line("Market name: %s, choice group count: %s", market_name, len(choice_groups))
                for choice_group_key in sorted(choice_groups):
                    market_line = choice_groups[choice_group_key]
                    resolved_choice_group = market_line.choice_group
                    if debug_mode:
                        _debug_line("Línea de mercado resuelta: %s (choice_group=%s, bookies=%s)",
                                    market_line.market_name, resolved_choice_group or "__default__", list(market_line.bookies.keys()))

                    for bookie_name in sorted(market_line.bookies):
                        bookie = market_line.bookies[bookie_name]
                        resolved_bookie_name = bookie.bookie_name or "__unknown__"
                        if debug_mode:
                            _debug_line("Evaluando bookie: %s (choice_count=%s)", resolved_bookie_name, len(bookie.choices))
                        for choice_name in sorted(bookie.choices):
                            choice = bookie.choices[choice_name]
                            required_inputs = _get_required_inputs(choice)
                            child_key = _build_choice_result_key(
                                market_name=market_line.market_name,
                                choice_group=resolved_choice_group,
                                bookie_name=resolved_bookie_name,
                                choice_name=choice.choice_name,
                            )

                            missing_inputs = _build_missing_inputs(required_inputs)
                            if debug_mode:
                                _debug_line("Choice key: %s (presentes=%s, faltantes=%s)",
                                            child_key, [key for key, value in required_inputs.items() if value is not None], missing_inputs)
                            if missing_inputs:
                                choice_result = _build_missing_result(
                                    market_group=market_line.market_group,
                                    market_period=market_line.market_period,
                                    market_name=market_line.market_name,
                                    choice_group=resolved_choice_group,
                                    bookie_name=resolved_bookie_name,
                                    choice_name=choice.choice_name,
                                    required_inputs=required_inputs,
                                    choice=choice,
                                    debug_mode=debug_mode,
                                )
                                insufficient_choice_count += 1
                                for missing_input in missing_inputs:
                                    missing_inputs_summary[missing_input] = (
                                        missing_inputs_summary.get(missing_input, 0) + 1
                                    )
                            else:
                                choice_result = _build_active_result(
                                    market_group=market_line.market_group,
                                    market_period=market_line.market_period,
                                    market_name=market_line.market_name,
                                    choice_group=resolved_choice_group,
                                    bookie_name=resolved_bookie_name,
                                    choice_name=choice.choice_name,
                                    required_inputs=required_inputs,
                                    choice=choice,
                                    event_id=event_context.event_id,
                                    trajectory_key=trajectory_key,
                                    debug_mode=debug_mode,
                                )
                                active_choice_count += 1

                            choice_results[child_key] = choice_result
                            choice_count += 1

            group_status = (
                "ACTIVE" if choice_count > 0 and insufficient_choice_count == 0 else "INSUFFICIENT_DATA"
            )
            if debug_mode:
                _debug_section(f"RESUMEN TRAYECTORIA: {trajectory_key}")
                _debug_line("Total choices: %s", choice_count)
                _debug_line("Choices activas: %s", active_choice_count)
                _debug_line("Choices insuficientes: %s", insufficient_choice_count)
                _debug_line("Estado de trayectoria: %s", group_status)
                _debug_line("Resumen de inputs faltantes: %s", missing_inputs_summary)

            market_period_results[trajectory_key] = {
                "trajectory_key": trajectory_key,
                "market_group": market_group,
                "market_period": market_period,
                "status": group_status,
                "P4_STATUS": group_status,
                "market_group_period_status": group_status,
                "choice_count": choice_count,
                "active_choice_count": active_choice_count,
                "insufficient_choice_count": insufficient_choice_count,
                "choice_results": choice_results,
                "missing_inputs_summary": missing_inputs_summary,
                "raw": {
                    "market_name_count": len(market_names),
                    "has_missing_inputs": insufficient_choice_count > 0,
                    "debug_mode": debug_mode,
                },
            }

    global_status = _derive_global_status(market_period_results)
    if debug_mode:
        _debug_section("AGREGACION GLOBAL P4 DRIFT ENGINE")
        _debug_line("Participantes: %s", event_context.participants_label)
        _debug_line("Total de periodos de mercado: %s", len(market_period_results))
        _debug_line("Periodos activos: %s", sum(1 for result in market_period_results.values() if result["status"] == "ACTIVE"))
        _debug_line("Periodos insuficientes: %s", sum(1 for result in market_period_results.values() if result["status"] == "INSUFFICIENT_DATA"))
        _debug_line("Estado global: %s", global_status)

    active_market_period_count = sum(
        1 for result in market_period_results.values() if result["status"] == "ACTIVE"
    )
    insufficient_market_period_count = sum(
        1 for result in market_period_results.values() if result["status"] == "INSUFFICIENT_DATA"
    )

    result = {
        **base_result,
        "P4_STATUS": global_status,
        "status": global_status,
        "market_period_results": market_period_results,
        "market_period_count": len(market_period_results),
        "active_market_period_count": active_market_period_count,
        "insufficient_market_period_count": insufficient_market_period_count,
    }

    serialized_result = _decimal_to_serializable(result)
    if debug_mode:
        _debug_section("RETORNO FINAL P4 DRIFT ENGINE")
        import json
        try:
            formatted_json = json.dumps(serialized_result, indent=2, ensure_ascii=False)
            for line in formatted_json.splitlines():
                _debug_line(line)
        except Exception as e:
            _debug_line("Error al serializar el resultado a JSON: %s", str(e))
            _debug_line("Resultado crudo: %s", serialized_result)
    return serialized_result
