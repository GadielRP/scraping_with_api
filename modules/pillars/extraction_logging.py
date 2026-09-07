"""Shared, period-scoped diagnostics for structural market pillars."""

from __future__ import annotations

import logging
from typing import Any


def log_extraction_diagnostics(
    logger: logging.Logger,
    *,
    pillar: str,
    event_id: int,
    target_minute: int | None,
    periods: dict[str, Any],
    full_time_requirement: str,
    debug_mode: bool,
) -> None:
    """Separate blocking required inputs from unavailable optional observations.

    The extraction DTO retains its aggregate fields for consumers. Logs use
    local diagnostics, with ambiguity/invalidity taking precedence over absence.
    """
    for period, diagnostics in periods.items():
        status = diagnostics["status"]
        if status == "COMPLETE" and not debug_mode:
            continue
        required = period == "full_time"
        ambiguous = set(diagnostics.get("ambiguous_inputs", ()))
        invalid = set(diagnostics.get("invalid_inputs", ())) - ambiguous
        missing = set(diagnostics.get("missing_inputs", ())) - ambiguous - invalid
        logger.info(
            "%s EXTRACTION | event_id=%s | target_minute=%s | period=%s | "
            "required=%s | blocks_profile=%s | status=%s | requirement=%s | "
            "missing_only=%s | invalid=%s | ambiguous=%s",
            pillar,
            event_id,
            target_minute,
            period,
            required,
            required and status not in {"COMPLETE", "PARTIAL"},
            status,
            full_time_requirement if required else "optional_observations",
            sorted(missing),
            sorted(invalid),
            sorted(ambiguous),
        )


def log_snapshot_inputs(logger: logging.Logger, snapshot: Any, *, pillar: str) -> None:
    """Log every selected input assignment and its available source lineage."""
    values = snapshot.input_values()
    traces = snapshot.input_trace()
    logger.info(f"{pillar} DEBUG | snapshot | target_minute=%s", snapshot.target_minute)
    for name, value in values.items():
        trace = traces.get(name)
        logger.info(
            f"{pillar} DEBUG | input assignment | name=%s | value=%s",
            name,
            value,
        )
        if not trace:
            logger.info(
                f"{pillar} DEBUG | input lineage | name=%s | unavailable_optional_or_not_selected=true",
                name,
            )
            continue
        logger.info(
            f"{pillar} DEBUG | input lineage | name=%s | target=%s | snapshot=%s | quote=%s",
            name,
            trace.get("target_minute"),
            trace.get("snapshot_id"),
            trace.get("quote_id"),
        )
        logger.info(
            f"{pillar} DEBUG | input lineage | name=%s | bookie_id=%s | bookie=%s | source=%s",
            name,
            trace.get("bookie_id"),
            trace.get("bookie_name"),
            trace.get("source"),
        )
        logger.info(
            f"{pillar} DEBUG | input lineage | name=%s | market_group=%s | period=%s | market_name=%s",
            name,
            trace.get("market_group"),
            trace.get("market_period"),
            trace.get("market_name"),
        )
        logger.info(
            f"{pillar} DEBUG | input lineage | name=%s | choice=%s | choice_group=%s | exchange_side=%s | level=%s",
            name,
            trace.get("choice_name"),
            trace.get("choice_group"),
            trace.get("exchange_side"),
            trace.get("exchange_level"),
        )
