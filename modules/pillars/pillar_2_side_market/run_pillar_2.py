"""Thin orchestrator for the Pillar 2 Side Market Signal Profile."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.market_snapshot_extractor import TargetMinuteSelection
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext

from .periods import (
    EXCHANGE_AH_1H_LINE_INPUT_NAME,
    EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
    EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
    P2_SIDE_PERIOD_SCOPES,
    resolve_pillar_status,
)
from .signal_engine import ENGINE_VERSION, build_p2_signal_profile
from .snapshot_policy import extract_p2_market_snapshot


logger = logging.getLogger(__name__)


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _json_inputs(values: dict[str, Decimal | None]) -> dict[str, float | None]:
    return {name: _number(value) for name, value in values.items()}


def _empty_inputs() -> dict[str, float | None]:
    values = {
        name: None
        for scope in P2_SIDE_PERIOD_SCOPES
        for name in scope.input_names()
    }
    values.update({
        name: None
        for name in (
            EXCHANGE_AH_1H_LINE_INPUT_NAME,
            *EXCHANGE_AH_1H_ODDS_INPUT_NAMES,
            *EXCHANGE_AH_1H_SIZE_TRACE_INPUT_NAMES,
        )
    })
    return values


def _raw_audit(
    *,
    odds_context: OddsTrajectoryContext | None,
    periods: dict[str, Any],
    inputs: dict[str, float | None],
    input_trace: dict[str, dict[str, Any]],
    reason: str | None = None,
) -> dict[str, Any]:
    raw = {
        "inputs": inputs,
        "input_trace": input_trace,
        "periods": periods,
        "target_minutes_expected": list(
            getattr(odds_context, "target_minutes_expected", [])
        ),
        "target_minutes_present": list(
            getattr(odds_context, "target_minutes_present", [])
        ),
    }
    if reason is not None:
        raw["reason"] = reason
    return raw


def _debug_snapshot_inputs(snapshot: Any) -> None:
    """Log every selected input assignment and its available source lineage."""
    values = snapshot.input_values()
    traces = snapshot.input_trace()
    logger.info("P2 DEBUG | snapshot | target_minute=%s", snapshot.target_minute)
    for name, value in values.items():
        trace = traces.get(name)
        logger.info(
            "P2 DEBUG | input assignment | name=%s | value=%s",
            name,
            value,
        )
        if not trace:
            logger.info(
                "P2 DEBUG | input lineage | name=%s | unavailable_optional_or_not_selected=true",
                name,
            )
            continue
        logger.info(
            "P2 DEBUG | input lineage | name=%s | target=%s | snapshot=%s | quote=%s",
            name,
            trace.get("target_minute"),
            trace.get("snapshot_id"),
            trace.get("quote_id"),
        )
        logger.info(
            "P2 DEBUG | input lineage | name=%s | bookie_id=%s | bookie=%s | source=%s",
            name,
            trace.get("bookie_id"),
            trace.get("bookie_name"),
            trace.get("source"),
        )
        logger.info(
            "P2 DEBUG | input lineage | name=%s | market_group=%s | period=%s | market_name=%s",
            name,
            trace.get("market_group"),
            trace.get("market_period"),
            trace.get("market_name"),
        )
        logger.info(
            "P2 DEBUG | input lineage | name=%s | choice=%s | choice_group=%s | exchange_side=%s | level=%s",
            name,
            trace.get("choice_name"),
            trace.get("choice_group"),
            trace.get("exchange_side"),
            trace.get("exchange_level"),
        )


def _log_signal_profile(profile: dict[str, Any]) -> None:
    def log_block(block_name: str, block: dict[str, Any] | None) -> None:
        logger.info("P2 SIGNAL | %s | begin", block_name)
        if block is None:
            logger.info(
                "P2 SIGNAL | %s | unavailable because required dependencies are incomplete",
                block_name,
            )
            return
        for field_name, value in block.items():
            if isinstance(value, dict):
                for nested_name, nested_value in value.items():
                    logger.info(
                        "P2 SIGNAL | %s | field=%s.%s | value=%s",
                        block_name,
                        field_name,
                        nested_name,
                        nested_value,
                    )
            else:
                logger.info(
                    "P2 SIGNAL | %s | field=%s | value=%s",
                    block_name,
                    field_name,
                    value,
                )

    log_block("FT 1X2", profile["FT"]["1X2"])
    log_block("FT AH", profile["FT"]["AH"])
    log_block("FT CROSS MARKET", profile["FT"]["CROSS_MARKET"])
    if profile["1H"] is not None:
        log_block("1H 1X2", profile["1H"]["1X2"])
        log_block("1H AH", profile["1H"]["AH"])
        log_block("1H CROSS MARKET", profile["1H"]["CROSS_MARKET"])
        log_block("FT_1H", profile["FT_1H"])
    log_block("EXCHANGE", profile["EXCHANGE"])
    log_block("BOOK_EXCHANGE", profile["BOOK_EXCHANGE"])
    log_block("BETFAIR_FT_AH", profile.get("BETFAIR_FT_AH"))
    log_block("BOOK_EXCHANGE_AH", profile.get("BOOK_EXCHANGE_AH"))
    log_block("BETFAIR_1H_AH", profile.get("BETFAIR_1H_AH"))
    log_block("BOOK_EXCHANGE_1H_AH", profile.get("BOOK_EXCHANGE_1H_AH"))


def calculate_pillar_2(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None = None,
    *,
    target_selection: TargetMinuteSelection,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Extract one canonical minute and return its structural signal profile.

    The returned dict is the mining producer output. The pipeline persists it
    through ``P2MiningAdapter`` immediately after this function returns.
    """
    odds_context = (
        odds_trajectory_context
        or getattr(event_context, "odds_trajectory_context", None)
    )
    extraction = extract_p2_market_snapshot(
        event_context.event_id,
        odds_context,
        target_selection,
    )
    periods = extraction.period_diagnostics()
    if debug_mode:
        logger.info(
            "P2 DEBUG | extraction | event_id=%s | target_minute=%s",
            event_context.event_id,
            extraction.target_minute,
        )
        logger.info(
            "P2 DEBUG | extraction | abort_reason=%s",
            extraction.abort_reason,
        )
        logger.info(
            "P2 DEBUG | period gates | full_time=%s | first_half=%s",
            extraction.full_time.status,
            extraction.first_half.status,
        )
        logger.info(
            "P2 DEBUG | period gates | betfair_ah_ft=%s | betfair_ah_1h=%s",
            extraction.exchange_ah.status,
            extraction.exchange_ah_1h.status,
        )
        logger.info(
            "P2 DEBUG | period gates | missing=%s | invalid=%s",
            extraction.missing_inputs,
            extraction.invalid_inputs,
        )
        logger.info(
            "P2 DEBUG | period gates | ambiguous=%s",
            extraction.ambiguous_inputs,
        )
    base = {
        "pillar_id": "pillar_2_side_market",
        "pillar_name": "Side Market Signal Profile",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P2_TARGET_MINUTE": extraction.target_minute,
        "PERIODS": periods,
        "MISSING_INPUTS": list(extraction.missing_inputs),
        "INVALID_INPUTS": list(extraction.invalid_inputs),
        "AMBIGUOUS_INPUTS": list(extraction.ambiguous_inputs),
    }

    snapshot = extraction.snapshot
    if snapshot is None:
        inputs = _empty_inputs()
        traces: dict[str, dict[str, Any]] = {}
        if extraction.first_half_snapshot is not None:
            inputs.update(_json_inputs(extraction.first_half_snapshot.input_values()))
            traces.update(extraction.first_half_snapshot.input_trace())
        if extraction.exchange_ah_snapshot is not None:
            optional = extraction.exchange_ah_snapshot
            inputs.update(_json_inputs(optional.input_values()))
            traces.update(optional.input_trace())
        raw = _raw_audit(
            odds_context=odds_context,
            periods=periods,
            inputs=inputs,
            input_trace=traces,
            reason=extraction.abort_reason or "full_time_completeness_gate_failed",
        )
        logger.info(
            "P2 signal profile unavailable for event_id=%s target_minute=%s missing=%s invalid=%s ambiguous=%s",
            event_context.event_id,
            extraction.target_minute,
            extraction.missing_inputs,
            extraction.invalid_inputs,
            extraction.ambiguous_inputs,
        )
        return {
            **base,
            "P2_STATUS": "INSUFFICIENT_DATA",
            "status": "INSUFFICIENT_DATA",
            "P2_SIGNAL_PROFILE": None,
            "modules": [],
            "raw": raw,
        }

    if debug_mode:
        _debug_snapshot_inputs(snapshot)
    profile_dto = build_p2_signal_profile(snapshot, debug_mode=debug_mode)
    profile = profile_dto.to_dict()
    optional_complete = extraction.first_half.status == "COMPLETE"
    status = resolve_pillar_status(
        required_complete=True,
        optional_complete=optional_complete,
    )
    raw = _raw_audit(
        odds_context=odds_context,
        periods=periods,
        inputs=_json_inputs(snapshot.input_values()),
        input_trace=snapshot.input_trace(),
        reason=None if optional_complete else "first_half_incomplete",
    )
    module = {
        "pillar_id": "pillar_2_side_market",
        "module_id": "p2_signal_engine",
        "module_name": "Side Market Signal Engine",
        "engine_version": ENGINE_VERSION,
        "P2_STATUS": status,
        "status": status,
        "P2_TARGET_MINUTE": extraction.target_minute,
        "PERIODS": periods,
        "P2_SIGNAL_PROFILE": profile,
        "raw": raw,
    }
    if debug_mode:
        _log_signal_profile(profile)
    logger.info(
        "P2 signal profile calculated for event_id=%s target_minute=%s status=%s first_half=%s betfair_ah_ft=%s betfair_ah_1h=%s",
        event_context.event_id,
        extraction.target_minute,
        status,
        extraction.first_half.status,
        extraction.exchange_ah.status,
        extraction.exchange_ah_1h.status,
    )
    return {
        **base,
        "P2_STATUS": status,
        "status": status,
        "P2_SIGNAL_PROFILE": profile,
        "modules": [module],
        "raw": raw,
    }


__all__ = ["ENGINE_VERSION", "calculate_pillar_2"]
