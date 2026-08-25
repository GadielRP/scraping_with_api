"""Pillar 2 - Side Market orchestrator."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from modules.pillars.context import EventContext
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from modules.pillars.pillar_2_side_market.market_snapshot_extractor import (
    extract_p2_market_snapshot,
)
from modules.pillars.pillar_2_side_market.raw_engine import (
    ENGINE_VERSION,
    calculate_p2_raw,
)


logger = logging.getLogger(__name__)


def _number(value: Decimal) -> float:
    return float(value)


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


def calculate_pillar_2(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None = None,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Calculate the strict FT + 1H P2 RAW payload for one event."""
    odds_trajectory_context = (
        odds_trajectory_context
        or getattr(event_context, "odds_trajectory_context", None)
    )
    if odds_trajectory_context is None:
        raise ValueError("EventContext is missing odds_trajectory_context for P2")

    if debug_mode:
        _debug_section("Inicio de Pillar 2 Side Market RAW")
        _debug_line(
            "Evento=%s (%s). El cálculo utiliza un único target_minute para todos los inputs FT y 1H.",
            event_context.event_id,
            event_context.participants_label,
        )
        _debug_line(
            "Minutos esperados=%s | presentes=%s | faltantes=%s.",
            odds_trajectory_context.target_minutes_expected,
            odds_trajectory_context.target_minutes_present,
            odds_trajectory_context.missing_target_minutes,
        )

    extraction = extract_p2_market_snapshot(odds_trajectory_context)
    if debug_mode:
        _debug_line(
            "Target minute seleccionado por P2 = %s.",
            extraction.target_minute,
        )
    base = {
        "pillar_id": "pillar_2_side_market",
        "pillar_name": "Side Market RAW",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P2_TARGET_MINUTE": extraction.target_minute,
    }
    if extraction.snapshot is None:
        if debug_mode:
            _debug_line(
                "El gate universal de completitud no fue superado. Inputs faltantes=%s | inválidos=%s | ambiguos=%s.",
                extraction.missing_inputs,
                extraction.invalid_inputs,
                extraction.ambiguous_inputs,
            )
        logger.info(
            "P2 RAW aborted for event_id=%s target_minute=%s missing=%s invalid=%s ambiguous=%s",
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
            "modules": [],
            "MISSING_INPUTS": list(extraction.missing_inputs),
            "INVALID_INPUTS": list(extraction.invalid_inputs),
            "AMBIGUOUS_INPUTS": list(extraction.ambiguous_inputs),
            "raw": {
                "reason": (
                    "no_configured_target_minute"
                    if extraction.target_minute is None
                    else "universal_completeness_gate_failed"
                ),
                "mining_context": _mining_context(event_context, extraction.target_minute),
                "target_minutes_expected": list(
                    odds_trajectory_context.target_minutes_expected
                ),
                "target_minutes_present": list(
                    odds_trajectory_context.target_minutes_present
                ),
            },
        }

    if debug_mode:
        _debug_line(
            "El gate universal de completitud fue superado: se encontraron todos los inputs mínimos en target_minute=%s.",
            extraction.target_minute,
        )
        _debug_input_assignments(extraction.snapshot)

    metrics = calculate_p2_raw(extraction.snapshot, debug_mode=debug_mode)
    if debug_mode:
        _debug_metric_assignments(metrics)

    inputs = {
        name: _number(value)
        for name, value in extraction.snapshot.input_values().items()
    }
    if debug_mode:
        _debug_section("Asignación de pesos baseline RAW")
        _debug_line("Asignación W_PIN = 0.50.")
        _debug_line("Asignación W_B365 = 0.50.")
        _debug_line("Asignación W_PIN_1H = 0.50.")
        _debug_line("Asignación W_B365_1H = 0.50.")
        _debug_line("Asignación W_BACK = 0.50.")
        _debug_line("Asignación W_LAY = 0.50.")
        _debug_line("Asignación W_BOOK = 0.50.")
        _debug_line("Asignación W_EXCHANGE = 0.50.")

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
        "inputs": inputs,
        "input_trace": extraction.snapshot.input_trace(),
    }
    module = {
        "pillar_id": "pillar_2_side_market",
        "module_id": "p2_raw_engine",
        "module_name": "Side Market RAW Engine",
        "engine_version": ENGINE_VERSION,
        "P2_STATUS": "ACTIVE",
        "status": "ACTIVE",
        "P2_TARGET_MINUTE": extraction.target_minute,
        **metrics,
        "raw": engine_raw,
    }

    logger.info(
        "P2 RAW calculated for event_id=%s target_minute=%s direction=%s edge=%.6f debug_mode=%s",
        event_context.event_id,
        extraction.target_minute,
        metrics["P2_DIRECTION_RAW"],
        metrics["SIDE_MARKET_EDGE"],
        debug_mode,
    )
    return {
        **base,
        "P2_STATUS": "ACTIVE",
        "status": "ACTIVE",
        "modules": [module],
        **metrics,
        "raw": {
            "module_count": 1,
            "module_ids": ["p2_raw_engine"],
            "p2_raw_engine": engine_raw,
        },
    }
