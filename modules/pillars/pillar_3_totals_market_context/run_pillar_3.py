"""Pillar 3 - Totals Market Context orchestrator."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext

from .periods import DEFAULT_P3_TOTALS_PERIOD_SCOPE, derived_metric_names, resolve_pillar_status
from .raw_engine import ENGINE_VERSION, calculate_p3_raw
from .snapshot_policy import extract_p3_market_snapshot


logger = logging.getLogger(__name__)


def _debug(message: str, *args: Any) -> None:
    logger.info("P3_TOTALS_MARKET DEBUG | " + message, *args)


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _mining_context(
    event_context: EventContext,
    target_minute: int | None,
    period: str | None,
    period_scope_token: str,
) -> dict[str, Any]:
    competition = getattr(event_context, "competition", None)
    return {
        "event_id": event_context.event_id,
        "sport": event_context.sport,
        "competition_id": getattr(competition, "competition_id", None),
        "competition": getattr(competition, "display_name", None),
        "season_id": getattr(event_context, "season_id", None),
        "season_name": getattr(event_context, "season_name", None),
        "season_year": getattr(event_context, "season_year", None),
        "market_group": "Over/Under",
        "period": period,
        "period_scope": period_scope_token,
        "minutes_to_start": event_context.minutes_until_start,
        "TARGET_MINUTE": target_minute,
    }


def calculate_pillar_3(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None = None,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Calculate P3 RAW with independent period gates over the registered scopes."""
    metric_names = derived_metric_names(DEFAULT_P3_TOTALS_PERIOD_SCOPE)
    context = odds_trajectory_context or getattr(
        event_context,
        "odds_trajectory_context",
        None,
    )
    if debug_mode:
        _debug("========== Inicio de Pillar 3 Totals Market Context RAW ==========")
        _debug(
            "Evento=%s (%s); PERIOD_SCOPE=%s; available=%s; trajectory_event_id=%s; minutos presentes=%s.",
            event_context.event_id,
            event_context.participants_label,
            DEFAULT_P3_TOTALS_PERIOD_SCOPE.metric_token,
            getattr(context, "available", False),
            getattr(context, "event_id", None),
            getattr(context, "target_minutes_present", []),
        )

    extraction = extract_p3_market_snapshot(event_context.event_id, context)
    period_diagnostics = extraction.period_diagnostics()
    base = {
        "pillar_id": "pillar_3_totals_market_context",
        "pillar_name": "Totals Market / Context RAW",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "EVENT_ID": event_context.event_id,
        "PERIOD_SCOPE": DEFAULT_P3_TOTALS_PERIOD_SCOPE.metric_token,
        "TARGET_MINUTE": extraction.target_minute,
        "PERIODS": period_diagnostics,
        "MISSING_INPUTS": list(extraction.missing_inputs),
        "INVALID_INPUTS": list(extraction.invalid_inputs),
        "AMBIGUOUS_INPUTS": list(extraction.ambiguous_inputs),
    }

    if extraction.snapshot is None:
        if debug_mode:
            _debug(
                "Full Time no superó su gate. razón=%s; target=%s; faltantes=%s; inválidos=%s; ambiguos=%s; diagnóstico=%s.",
                extraction.abort_reason,
                extraction.target_minute,
                extraction.missing_inputs,
                extraction.invalid_inputs,
                extraction.ambiguous_inputs,
                extraction.extraction_diagnostics,
            )
        logger.info(
            "P3 RAW aborted for event_id=%s target_minute=%s reason=%s missing=%s invalid=%s ambiguous=%s periods=%s",
            event_context.event_id,
            extraction.target_minute,
            extraction.abort_reason,
            extraction.missing_inputs,
            extraction.invalid_inputs,
            extraction.ambiguous_inputs,
            period_diagnostics,
        )
        return {
            **base,
            "PERIOD": None,
            "P3_STATUS": "INSUFFICIENT_DATA",
            "status": "INSUFFICIENT_DATA",
            "modules": [],
            "raw": {
                "reason": extraction.abort_reason,
                "mining_context": _mining_context(
                    event_context,
                    extraction.target_minute,
                    None,
                    DEFAULT_P3_TOTALS_PERIOD_SCOPE.metric_token,
                ),
                "periods": period_diagnostics,
                "extraction_diagnostics": extraction.extraction_diagnostics,
                "target_minutes_expected": list(
                    getattr(context, "target_minutes_expected", [])
                ),
                "target_minutes_present": list(
                    getattr(context, "target_minutes_present", [])
                ),
            },
        }

    snapshot = extraction.snapshot
    if debug_mode:
        _debug("TARGET_MINUTE=%s.", snapshot.target_minute)
        _debug("PERIOD resuelto=%s.", snapshot.full_time.period)
        values = snapshot.input_values()
        traces = snapshot.input_trace()
        for name, value in values.items():
            trace = traces.get(name)
            if trace:
                _debug(
                    "Asignación %s=%s desde bookie_id=%s, nombre=%s, source=%s, línea=%s, snapshot_id=%s, quote_id=%s, target=%s.",
                    name,
                    value,
                    trace.get("bookie_id"),
                    trace.get("bookie_name"),
                    trace.get("source"),
                    trace.get("choice_group"),
                    trace.get("snapshot_id"),
                    trace.get("quote_id"),
                    trace.get("target_minute"),
                )
            else:
                _debug("Asignación %s=%s; no existe quote trazable para este input.", name, value)

    metrics = calculate_p3_raw(snapshot, debug_mode=debug_mode)
    available_count = sum(value is not None for value in snapshot.input_values().values())
    status = resolve_pillar_status(
        required_complete=True,
        optional_complete=True,
        signal_present=metrics[metric_names.market_edge] is not None,
        available_input_count=available_count,
    )

    engine_raw = {
        "baseline_weights": {
            metric_names.pin_weight: metrics[metric_names.pin_weight],
            metric_names.b365_weight: metrics[metric_names.b365_weight],
        },
        "mining_context": _mining_context(
            event_context,
            snapshot.target_minute,
            snapshot.full_time.period,
            snapshot.full_time.period_scope.metric_token,
        ),
        "inputs": {
            name: _number(value) for name, value in snapshot.input_values().items()
        },
        "input_trace": snapshot.input_trace(),
        "periods": period_diagnostics,
        "extraction_diagnostics": extraction.extraction_diagnostics,
    }
    module = {
        "pillar_id": "pillar_3_totals_market_context",
        "module_id": "p3_raw_engine",
        "module_name": "Totals Market Context RAW Engine",
        "engine_version": ENGINE_VERSION,
        "P3_STATUS": status,
        "status": status,
        "EVENT_ID": event_context.event_id,
        "PERIOD": snapshot.full_time.period,
        "PERIOD_SCOPE": snapshot.full_time.period_scope.metric_token,
        "TARGET_MINUTE": snapshot.target_minute,
        "PERIODS": period_diagnostics,
        **metrics,
        "raw": engine_raw,
    }

    if debug_mode:
        _debug(
            "Estado final P3=%s; dirección=%s; contexto=%s; edge=%s; completitud=%s.",
            status,
            metrics[metric_names.p3_direction],
            metrics[metric_names.context_direction],
            metrics[metric_names.market_edge],
            metrics[metric_names.completeness],
        )

    logger.info(
        "P3 RAW calculated for event_id=%s target_minute=%s period_scope=%s period=%s status=%s direction=%s edge=%s completeness=%.3f debug_mode=%s",
        event_context.event_id,
        snapshot.target_minute,
        snapshot.full_time.period_scope.metric_token,
        snapshot.full_time.period,
        status,
        metrics[metric_names.p3_direction],
        metrics[metric_names.market_edge],
        metrics[metric_names.completeness],
        debug_mode,
    )
    return {
        **base,
        "PERIOD": snapshot.full_time.period,
        "P3_STATUS": status,
        "status": status,
        "modules": [module],
        **metrics,
        "raw": {
            "module_count": 1,
            "module_ids": ["p3_raw_engine"],
            "periods": period_diagnostics,
            "p3_raw_engine": engine_raw,
        },
    }


__all__ = ["ENGINE_VERSION", "calculate_pillar_3"]
