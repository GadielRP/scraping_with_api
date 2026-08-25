"""Translate the P2 RAW output into the common mining observation contract."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.pillar_2_side_market.raw_engine import ENGINE_VERSION

from .contracts import PillarMiningObservation
from .json_normalizer import to_json_value


P2_RAW_METRIC_NAMES = (
    "PIN_SIDE_EDGE",
    "B365_SIDE_EDGE",
    "BOOK_GAP",
    "BOOK_EDGE",
    "AH_LINE_GAP",
    "PIN_AH_EDGE",
    "B365_AH_EDGE",
    "AH_PRICE_GAP",
    "PIN_1H_SIDE_EDGE",
    "B365_1H_SIDE_EDGE",
    "BOOK_1H_GAP",
    "BOOK_1H_EDGE",
    "PIN_AH_1H_EDGE",
    "B365_AH_1H_EDGE",
    "AH_1H_LINE_GAP",
    "AH_1H_PRICE_GAP",
    "BOOK_DIRECTION_FT",
    "BOOK_DIRECTION_1H",
    "FT_1H_GAP",
    "FT_1H_SAME_DIRECTION",
    "BACK_EDGE",
    "LAY_EDGE",
    "EXCHANGE_INTERNAL_GAP",
    "EXCHANGE_EDGE",
    "HOME_SPREAD",
    "AWAY_SPREAD",
    "SIDE_SPREAD",
    "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE",
    "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE",
    "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE",
    "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE",
    "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE",
    "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE",
    "Q_AGREEMENT",
    "Q_COMPLETE",
    "EXCHANGE_QUALITY_BASE",
    "TENSION_RAW",
    "DISLOCATION",
    "DISLOCATION_STRENGTH",
    "SIDE_MARKET_EDGE",
    "P2_DIRECTION_RAW",
)


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _observation_slot(target_minute: int | None, evaluation_minute: int | None) -> str:
    if target_minute is not None:
        return f"target:{target_minute}"
    if evaluation_minute is not None:
        return f"evaluation:{evaluation_minute}"
    return "event"


def _dictionary(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def build_p2_mining_observation(
    event_context: EventContext,
    p2_result: dict[str, Any],
) -> PillarMiningObservation:
    """Build one lossless, organized P2 mining observation."""
    raw = _dictionary(p2_result.get("raw"))
    engine_raw = _dictionary(raw.get("p2_raw_engine"))
    mining_context = _dictionary(
        engine_raw.get("mining_context") or raw.get("mining_context")
    )

    target_minute = _optional_int(p2_result.get("P2_TARGET_MINUTE"))
    evaluation_minute = _optional_int(
        getattr(event_context, "minutes_until_start", None)
    )
    status = str(
        p2_result.get("P2_STATUS") or p2_result.get("status") or "ERROR"
    ).upper()

    metrics = {
        metric_name: to_json_value(p2_result[metric_name])
        for metric_name in P2_RAW_METRIC_NAMES
        if metric_name in p2_result
    }
    outer_raw_diagnostics = {
        key: value
        for key, value in raw.items()
        if key not in {"p2_raw_engine", "mining_context"}
    }
    engine_diagnostics = {
        key: value
        for key, value in engine_raw.items()
        if key not in {"mining_context", "inputs"}
    }
    diagnostics = {
        "reason": raw.get("reason"),
        "error": p2_result.get("error"),
        "missing_inputs": p2_result.get("MISSING_INPUTS", []),
        "invalid_inputs": p2_result.get("INVALID_INPUTS", []),
        "ambiguous_inputs": p2_result.get("AMBIGUOUS_INPUTS", []),
        "module_ids": raw.get("module_ids", []),
        "outer_raw": outer_raw_diagnostics,
        "engine": engine_diagnostics,
    }

    competition = getattr(event_context, "competition", None)
    competition_id = _optional_int(getattr(competition, "competition_id", None))
    context = {
        **mining_context,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "sport": event_context.sport,
        "competition_id": competition_id,
        "competition": getattr(competition, "display_name", None),
        "market_type": mining_context.get("market_type", "1X2"),
        "minutes_to_start": evaluation_minute,
        "P2_TARGET_MINUTE": target_minute,
        "event_start_time_utc": getattr(event_context, "start_time_utc", None),
        "context_status": getattr(event_context, "context_status", None),
    }

    modules = p2_result.get("modules") or []
    module = modules[0] if modules and isinstance(modules[0], dict) else {}

    return PillarMiningObservation(
        event_id=int(event_context.event_id),
        pillar_id="pillar_2_side_market",
        result_scope="side_market",
        module_id=str(module.get("module_id") or "p2_raw_engine"),
        engine_version=str(p2_result.get("engine_version") or ENGINE_VERSION),
        payload_schema_version=1,
        evaluation_minute=evaluation_minute,
        target_minute=target_minute,
        observation_slot=_observation_slot(target_minute, evaluation_minute),
        sport=str(event_context.sport),
        competition_id=competition_id,
        market_type=str(mining_context.get("market_type") or "1X2"),
        status=status,
        is_successful=status == "ACTIVE",
        is_valid=None,
        score_name="SIDE_MARKET_EDGE",
        score=_optional_decimal(p2_result.get("SIDE_MARKET_EDGE")),
        direction=(
            str(p2_result["P2_DIRECTION_RAW"])
            if p2_result.get("P2_DIRECTION_RAW") is not None
            else None
        ),
        strength=None,
        metrics=to_json_value(metrics),
        context=to_json_value(context),
        inputs=to_json_value(_dictionary(engine_raw.get("inputs"))),
        diagnostics=to_json_value(diagnostics),
    )
