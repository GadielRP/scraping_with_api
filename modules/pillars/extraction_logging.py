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
            required and status != "COMPLETE",
            status,
            full_time_requirement if required else "optional_observations",
            sorted(missing),
            sorted(invalid),
            sorted(ambiguous),
        )
