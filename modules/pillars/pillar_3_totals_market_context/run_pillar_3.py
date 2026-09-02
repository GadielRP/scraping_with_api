"""Thin orchestrator for the Pillar 3 Over/Under Signal Profile."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.market_snapshot_extractor import TargetMinuteSelection
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext

from .periods import (
    EXCHANGE_OU_1H_LINE_INPUT_NAME,
    EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
    EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES,
    EXCHANGE_OU_LINE_INPUT_NAME,
    EXCHANGE_OU_ODDS_INPUT_NAMES,
    EXCHANGE_OU_SIZE_TRACE_INPUT_NAMES,
    P3_TOTALS_PERIOD_SCOPES,
    resolve_pillar_status,
)
from .signal_engine import ENGINE_VERSION, build_p3_signal_profile
from .snapshot_policy import extract_p3_market_snapshot


logger = logging.getLogger(__name__)


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _json_inputs(values: dict[str, Decimal | None]) -> dict[str, float | None]:
    return {name: _number(value) for name, value in values.items()}


def _empty_inputs() -> dict[str, float | None]:
    values = {
        name: None
        for scope in P3_TOTALS_PERIOD_SCOPES
        for name in scope.input_names()
    }
    values.update(
        {
            name: None
            for name in (
                EXCHANGE_OU_LINE_INPUT_NAME,
                *EXCHANGE_OU_ODDS_INPUT_NAMES,
                *EXCHANGE_OU_SIZE_TRACE_INPUT_NAMES,
            )
        }
    )
    values.update({
        name: None
        for name in (
            EXCHANGE_OU_1H_LINE_INPUT_NAME,
            *EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
            *EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES,
        )
    })
    return values


def _raw_audit(
    *,
    odds_context: OddsTrajectoryContext | None,
    periods: dict[str, Any],
    inputs: dict[str, float | None],
    input_trace: dict[str, dict[str, Any]],
    extraction_diagnostics: dict[str, Any],
    reason: str | None,
) -> dict[str, Any]:
    raw = {
        "inputs": inputs,
        "input_trace": input_trace,
        "periods": periods,
        "extraction_diagnostics": extraction_diagnostics,
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
    values = snapshot.input_values()
    traces = snapshot.input_trace()
    logger.info("P3 DEBUG | snapshot | target_minute=%s", snapshot.target_minute)
    for name, value in values.items():
        trace = traces.get(name)
        logger.info("P3 DEBUG | input assignment | name=%s | value=%s", name, value)
        if trace is None:
            logger.info(
                "P3 DEBUG | input lineage | name=%s | unavailable=true",
                name,
            )
            continue
        logger.info(
            "P3 DEBUG | input lineage | name=%s | target=%s | snapshot=%s | quote=%s",
            name,
            trace.get("target_minute"),
            trace.get("snapshot_id"),
            trace.get("quote_id"),
        )
        logger.info(
            "P3 DEBUG | input lineage | name=%s | bookie_id=%s | bookie=%s | source=%s",
            name,
            trace.get("bookie_id"),
            trace.get("bookie_name"),
            trace.get("source"),
        )
        logger.info(
            "P3 DEBUG | input lineage | name=%s | market_group=%s | period=%s | market_name=%s",
            name,
            trace.get("market_group"),
            trace.get("market_period"),
            trace.get("market_name"),
        )
        logger.info(
            "P3 DEBUG | input lineage | name=%s | choice=%s | choice_group=%s",
            name,
            trace.get("choice_name"),
            trace.get("choice_group"),
        )


def _log_signal_profile(profile: dict[str, Any]) -> None:
    for period in ("FT", "1H"):
        block = profile.get(period)
        if not isinstance(block, dict):
            logger.info("P3 SIGNAL | %s | value=None", period)
            continue
        for section, values in block.items():
            if isinstance(values, dict):
                for field, value in values.items():
                    logger.info(
                        "P3 SIGNAL | %s.%s | field=%s | value=%s",
                        period,
                        section,
                        field,
                        value,
                    )
            else:
                logger.info(
                    "P3 SIGNAL | %s | field=%s | value=%s",
                    period,
                    section,
                    values,
                )
    ft_1h = profile.get("FT_1H")
    if not isinstance(ft_1h, dict):
        logger.info("P3 SIGNAL | FT_1H | value=None")
        return
    for field, value in ft_1h.items():
        if isinstance(value, dict):
            for nested_field, nested_value in value.items():
                logger.info(
                    "P3 SIGNAL | FT_1H | field=%s.%s | value=%s",
                    field,
                    nested_field,
                    nested_value,
                )
        else:
            logger.info(
                "P3 SIGNAL | FT_1H | field=%s | value=%s",
                field,
                value,
            )


def calculate_pillar_3(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None = None,
    *,
    target_selection: TargetMinuteSelection,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Return the structural P3 profile for one pipeline-selected minute.

    The returned dict is the mining producer output. The pipeline persists it
    through ``P3MiningAdapter`` immediately after this function returns.
    """
    odds_context = odds_trajectory_context or getattr(
        event_context,
        "odds_trajectory_context",
        None,
    )
    extraction = extract_p3_market_snapshot(
        event_context.event_id,
        odds_context,
        target_selection,
    )
    periods = extraction.period_diagnostics()
    if debug_mode:
        logger.info(
            "P3 DEBUG | extraction | event_id=%s | target_minute=%s | abort_reason=%s",
            event_context.event_id,
            extraction.target_minute,
            extraction.abort_reason,
        )
        logger.info(
            "P3 DEBUG | period gates | full_time=%s | first_half=%s | betfair_ou_ft=%s | betfair_ou_1h=%s",
            extraction.full_time.status,
            extraction.first_half.status,
            extraction.exchange_ou.status,
            extraction.exchange_ou_1h.status,
        )
        logger.info(
            "P3 DEBUG | period gates | missing=%s | invalid=%s | ambiguous=%s",
            extraction.missing_inputs,
            extraction.invalid_inputs,
            extraction.ambiguous_inputs,
        )

    base = {
        "pillar_id": "pillar_3_totals_market_context",
        "pillar_name": "Over/Under Market Signal Profile",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P3_TARGET_MINUTE": extraction.target_minute,
        "PERIODS": periods,
        "MISSING_INPUTS": list(extraction.missing_inputs),
        "INVALID_INPUTS": list(extraction.invalid_inputs),
        "AMBIGUOUS_INPUTS": list(extraction.ambiguous_inputs),
    }

    snapshot = extraction.snapshot
    if snapshot is None:
        inputs = _empty_inputs()
        traces: dict[str, dict[str, Any]] = {}
        for period_snapshot in (
            extraction.full_time_snapshot,
            extraction.first_half_snapshot,
        ):
            if period_snapshot is not None:
                inputs.update(_json_inputs(period_snapshot.input_values()))
                traces.update(period_snapshot.input_trace())
        if extraction.exchange_ou_snapshot is not None:
            inputs.update(_json_inputs(extraction.exchange_ou_snapshot.input_values()))
            traces.update(extraction.exchange_ou_snapshot.input_trace())
        if extraction.exchange_ou_1h_snapshot is not None:
            inputs.update(_json_inputs(extraction.exchange_ou_1h_snapshot.input_values(
                line_name=EXCHANGE_OU_1H_LINE_INPUT_NAME,
                odds_names=EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
                size_names=EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES,
            )))
            traces.update(extraction.exchange_ou_1h_snapshot.input_trace(
                line_name=EXCHANGE_OU_1H_LINE_INPUT_NAME,
                odds_names=EXCHANGE_OU_1H_ODDS_INPUT_NAMES,
                size_names=EXCHANGE_OU_1H_SIZE_TRACE_INPUT_NAMES,
            ))
        raw = _raw_audit(
            odds_context=odds_context,
            periods=periods,
            inputs=inputs,
            input_trace=traces,
            extraction_diagnostics=extraction.extraction_diagnostics,
            reason=extraction.abort_reason or "full_time_completeness_gate_failed",
        )
        logger.info(
            "P3 signal profile unavailable for event_id=%s target_minute=%s reason=%s",
            event_context.event_id,
            extraction.target_minute,
            raw.get("reason"),
        )
        return {
            **base,
            "P3_STATUS": "INSUFFICIENT_DATA",
            "status": "INSUFFICIENT_DATA",
            "P3_SIGNAL_PROFILE": None,
            "modules": [],
            "raw": raw,
        }

    if debug_mode:
        _debug_snapshot_inputs(snapshot)
    profile_dto = build_p3_signal_profile(snapshot, debug_mode=debug_mode)
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
        extraction_diagnostics=extraction.extraction_diagnostics,
        reason=None if optional_complete else "first_half_incomplete",
    )
    module = {
        "pillar_id": "pillar_3_totals_market_context",
        "module_id": "p3_signal_engine",
        "module_name": "Over/Under Market Signal Engine",
        "engine_version": ENGINE_VERSION,
        "P3_STATUS": status,
        "status": status,
        "P3_TARGET_MINUTE": extraction.target_minute,
        "PERIODS": periods,
        "P3_SIGNAL_PROFILE": profile,
        "raw": raw,
    }
    if debug_mode:
        _log_signal_profile(profile)
    logger.info(
        "P3 signal profile calculated for event_id=%s target_minute=%s status=%s first_half=%s betfair_ou_ft=%s betfair_ou_1h=%s",
        event_context.event_id,
        extraction.target_minute,
        status,
        extraction.first_half.status,
        extraction.exchange_ou.status,
        extraction.exchange_ou_1h.status,
    )
    return {
        **base,
        "P3_STATUS": status,
        "status": status,
        "P3_SIGNAL_PROFILE": profile,
        "modules": [module],
        "raw": raw,
    }


__all__ = ["ENGINE_VERSION", "calculate_pillar_3"]
