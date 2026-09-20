"""Orchestrator for Pillar 5 Exact Price Memory."""

from __future__ import annotations

import logging
from typing import Any

from infrastructure.settings import Config
from modules.pillars.context import EventContext
from modules.pillars.extraction_logging import log_extraction_diagnostics
from modules.pillars.market_snapshot_extractor import (
    TargetMinuteSelection,
    select_target_minute,
)
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext

from .periods import (
    FULL_TIME_PRICE_MEMORY_SCOPE,
    P5_PRICE_MEMORY_PERIOD_SCOPES,
    resolve_pillar_status,
)
from .snapshot_policy import extract_p5_market_snapshot

logger = logging.getLogger(__name__)

ENGINE_VERSION = "p5_price_memory_v3_0"


def _empty_inputs() -> dict[str, float | None]:
    return {
        name: None
        for scope in P5_PRICE_MEMORY_PERIOD_SCOPES
        for name in scope.input_names()
    }


def calculate_pillar_5(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext | None = None,
    *,
    target_selection: TargetMinuteSelection | None = None,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Calculate Pillar 5 snapshot and return a serializable pillar payload."""
    odds_context = (
        odds_trajectory_context
        or getattr(event_context, "ft_1x2_odds_trajectory_context", None)
        or getattr(event_context, "odds_trajectory_context", None)
    )

    if target_selection is None:
        target_selection = select_target_minute(
            odds_context,
            flow_id="pillar_5_price_memory",
            expected_event_id=event_context.event_id,
            allowed_target_minutes=getattr(Config, "PRE_START_ODDS_MOMENTS", None),
            evaluation_minute=getattr(event_context, "minutes_until_start", None),
        )

    logger.info(
        "P5 orchestrator start for event_id=%s participants=%s debug_mode=%s target_minute=%s",
        event_context.event_id,
        event_context.participants_label,
        debug_mode,
        target_selection.target_minute,
    )

    extraction = extract_p5_market_snapshot(
        event_context.event_id,
        odds_context,
        target_selection,
    )

    periods = extraction.period_diagnostics()
    log_extraction_diagnostics(
        logger,
        pillar="P5",
        event_id=event_context.event_id,
        target_minute=extraction.target_minute,
        periods=periods,
        full_time_requirement="any complete bookie: 1X2 or Home/Away",
        debug_mode=debug_mode,
    )

    pillar_status = resolve_pillar_status(
        required_complete=extraction.full_time.status == "COMPLETE",
        required_usable=extraction.full_time.usable,
        optional_complete=True,
    )

    snapshot = extraction.snapshot
    if snapshot is None:
        inputs = _empty_inputs()
        traces: dict[str, dict[str, Any]] = {}
        if extraction.full_time_snapshot is not None:
            inputs.update(extraction.full_time_snapshot.input_values())
            traces.update(extraction.full_time_snapshot.traces())
    else:
        inputs = snapshot.input_values()
        traces = snapshot.traces()

    logger.info(
        "P5 orchestrator done for %s: status=%s target_minute=%s",
        event_context.participants_label,
        pillar_status,
        extraction.target_minute,
    )

    return {
        "pillar_id": "pillar_5",
        "pillar_name": "Exact Price Memory",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P5_TARGET_MINUTE": extraction.target_minute,
        "P5_STATUS": pillar_status,
        "status": pillar_status,
        "PERIODS": periods,
        "MISSING_INPUTS": list(extraction.missing_inputs),
        "INVALID_INPUTS": list(extraction.invalid_inputs),
        "AMBIGUOUS_INPUTS": list(extraction.ambiguous_inputs),
        "raw": {
            "inputs": inputs,
            "traces": traces,
            "extraction_diagnostics": extraction.extraction_diagnostics,
            "abort_reason": extraction.abort_reason,
        },
    }


__all__ = ["ENGINE_VERSION", "calculate_pillar_5"]
