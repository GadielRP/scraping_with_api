"""Pillar 2 - Side Market orchestrator."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from modules.pillars.pillar_2_side_market.periods import (
    optional_metric_exclusion_reasons,
    resolve_pillar_status,
)
from modules.pillars.pillar_2_side_market.raw_engine import (
    ENGINE_VERSION,
    calculate_p2_raw,
)
from modules.pillars.pillar_2_side_market.snapshot_policy import (
    extract_p2_market_snapshot,
)


logger = logging.getLogger(__name__)


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


def _debug_section(title: str) -> None:
    logger.info("========== P2_SIDE_MARKET DEBUG | %s =========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("P2_SIDE_MARKET DEBUG | " + message, *args)


def _debug_input_assignments(snapshot) -> None:
    """Log every canonical P2 input and its selected quote lineage."""
    _debug_section("Asignación de inputs al snapshot canónico")
    values = snapshot.input_values()
    traces = snapshot.input_trace()
    for name, value in values.items():
        trace = traces.get(name, {})
        if not trace:
            _debug_line(
                "Asignación de input: %s = %s | excluido del período incompleto.",
                name,
                value,
            )
            continue
        source = trace.get("source") or "desconocido"
        bookie = trace.get("bookie_name") or "desconocido"
        market = trace.get("market_name") or "desconocido"
        period = trace.get("market_period") or "desconocido"
        choice = trace.get("choice_name") or "desconocido"
        exchange_side = trace.get("exchange_side") or "single"
        _debug_line(
            "Asignación de input: %s = %s | fuente=%s | bookie=%s | mercado=%s | período=%s | choice=%s | lado_exchange=%s | snapshot_id=%s | target_minute=%s.",
            name,
            value,
            source,
            bookie,
            market,
            period,
            choice,
            exchange_side,
            trace.get("snapshot_id"),
            trace.get("target_minute"),
        )


def _debug_metric_assignments(metrics: dict[str, Any]) -> None:
    _debug_section("Asignación de outputs RAW")
    for name, value in metrics.items():
        _debug_line("Asignación de output: %s = %s.", name, value)


def _mining_context(
    event_context: EventContext,
    target_minute: int | None,
) -> dict[str, Any]:
    return {
        "event_id": event_context.event_id,
        "sport": event_context.sport,
        "competition_id": getattr(event_context.competition, "competition_id", None),
        "competition": getattr(event_context.competition, "display_name", None),
        "market_type": "1X2",
        "minutes_to_start": event_context.minutes_until_start,
        "P2_TARGET_MINUTE": target_minute,
    }


def _json_inputs(values: dict[str, Decimal | None]) -> dict[str, float | None]:
    return {name: _number(value) for name, value in values.items()}


def calculate_pillar_2(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None = None,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Calculate P2 RAW with independent Full Time and First Half gates."""
    odds_trajectory_context = (
        odds_trajectory_context
        or getattr(event_context, "odds_trajectory_context", None)
    )

    if debug_mode:
        _debug_section("Inicio de Pillar 2 Side Market RAW")
        _debug_line(
            "Evento=%s (%s). Full Time es obligatorio; First Half se valida de forma independiente en el mismo target_minute.",
            event_context.event_id,
            event_context.participants_label,
        )
        _debug_line(
            "Minutos esperados=%s | presentes=%s | faltantes=%s.",
            getattr(odds_trajectory_context, "target_minutes_expected", []),
            getattr(odds_trajectory_context, "target_minutes_present", []),
            getattr(odds_trajectory_context, "missing_target_minutes", []),
        )

    extraction = extract_p2_market_snapshot(
        event_context.event_id,
        odds_trajectory_context,
    )
    period_diagnostics = extraction.period_diagnostics()
    if debug_mode:
        _debug_line(
            "Target minute seleccionado por P2 = %s.",
            extraction.target_minute,
        )
        _debug_line(
            "Gate Full Time=%s | gate First Half=%s.",
            extraction.full_time.status,
            extraction.first_half.status,
        )

    base = {
        "pillar_id": "pillar_2_side_market",
        "pillar_name": "Side Market RAW",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P2_TARGET_MINUTE": extraction.target_minute,
        "PERIODS": period_diagnostics,
        "MISSING_INPUTS": list(extraction.missing_inputs),
        "INVALID_INPUTS": list(extraction.invalid_inputs),
        "AMBIGUOUS_INPUTS": list(extraction.ambiguous_inputs),
    }

    if extraction.snapshot is None:
        first_half_technical: dict[str, Any] = {
            "status": extraction.first_half.status,
        }
        if extraction.first_half_snapshot is not None:
            first_half_technical.update(
                {
                    "note": "first_half_complete_but_unused_without_full_time",
                    "inputs": _json_inputs(
                        extraction.first_half_snapshot.input_values()
                    ),
                    "input_trace": extraction.first_half_snapshot.input_trace(),
                }
            )
        if debug_mode:
            _debug_line(
                "Full Time no superó su gate. Inputs faltantes=%s | inválidos=%s | ambiguos=%s | First Half=%s.",
                extraction.full_time.missing_inputs,
                extraction.full_time.invalid_inputs,
                extraction.full_time.ambiguous_inputs,
                extraction.first_half.status,
            )
        logger.info(
            "P2 RAW aborted for event_id=%s target_minute=%s reason=%s missing=%s invalid=%s ambiguous=%s periods=%s",
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
            "P2_STATUS": "INSUFFICIENT_DATA",
            "status": "INSUFFICIENT_DATA",
            "modules": [],
            "raw": {
                "reason": extraction.abort_reason or "full_time_completeness_gate_failed",
                "mining_context": _mining_context(event_context, extraction.target_minute),
                "periods": period_diagnostics,
                "first_half": first_half_technical,
                "target_minutes_expected": list(
                    getattr(odds_trajectory_context, "target_minutes_expected", [])
                ),
                "target_minutes_present": list(
                    getattr(odds_trajectory_context, "target_minutes_present", [])
                ),
            },
        }

    snapshot = extraction.snapshot
    first_half_complete = snapshot.first_half is not None
    excluded_metrics = (
        {} if first_half_complete else optional_metric_exclusion_reasons()
    )

    if debug_mode:
        _debug_line(
            "Full Time completo en target_minute=%s. First Half=%s.",
            extraction.target_minute,
            extraction.first_half.status,
        )
        _debug_input_assignments(snapshot)

    metrics = calculate_p2_raw(snapshot, debug_mode=debug_mode)
    status = resolve_pillar_status(
        required_complete=True,
        optional_complete=first_half_complete,
        signal_present=metrics.get("SIDE_MARKET_EDGE") is not None,
    )
    if debug_mode:
        _debug_metric_assignments(metrics)
        _debug_section("Asignación de pesos baseline RAW")
        _debug_line("Asignación W_PIN = 0.50.")
        _debug_line("Asignación W_B365 = 0.50.")
        _debug_line("Asignación W_BACK = 0.50.")
        _debug_line("Asignación W_LAY = 0.50.")
        _debug_line("Asignación W_BOOK = 0.50.")
        _debug_line("Asignación W_EXCHANGE = 0.50.")
        if first_half_complete:
            _debug_line("Asignación W_PIN_1H = 0.50.")
            _debug_line("Asignación W_B365_1H = 0.50.")
        else:
            _debug_line(
                "Pesos W_PIN_1H y W_B365_1H no se aplican porque First Half fue excluido."
            )

    engine_raw = {
        "baseline_weights": {
            "W_PIN": 0.5,
            "W_B365": 0.5,
            "W_PIN_1H": 0.5,
            "W_B365_1H": 0.5,
            "W_BACK": 0.5,
            "W_LAY": 0.5,
            "W_BOOK": 0.5,
            "W_EXCHANGE": 0.5,
        },
        "mining_context": _mining_context(event_context, extraction.target_minute),
        "inputs": _json_inputs(snapshot.input_values()),
        "input_trace": snapshot.input_trace(),
        "periods": period_diagnostics,
        "excluded_metrics": excluded_metrics,
    }
    module = {
        "pillar_id": "pillar_2_side_market",
        "module_id": "p2_raw_engine",
        "module_name": "Side Market RAW Engine",
        "engine_version": ENGINE_VERSION,
        "P2_STATUS": status,
        "status": status,
        "P2_TARGET_MINUTE": extraction.target_minute,
        "PERIODS": period_diagnostics,
        **metrics,
        "raw": engine_raw,
    }

    logger.info(
        "P2 RAW calculated for event_id=%s target_minute=%s status=%s direction=%s edge=%s first_half=%s debug_mode=%s",
        event_context.event_id,
        extraction.target_minute,
        status,
        metrics["P2_DIRECTION_RAW"],
        metrics["SIDE_MARKET_EDGE"],
        extraction.first_half.status,
        debug_mode,
    )
    return {
        **base,
        "P2_STATUS": status,
        "status": status,
        "modules": [module],
        **metrics,
        "raw": {
            "module_count": 1,
            "module_ids": ["p2_raw_engine"],
            "periods": period_diagnostics,
            "p2_raw_engine": engine_raw,
            **(
                {}
                if first_half_complete
                else {
                    "reason": "first_half_incomplete",
                    "excluded_metrics": excluded_metrics,
                }
            ),
        },
    }


__all__ = ["ENGINE_VERSION", "calculate_pillar_2"]
