"""Thin orchestrator for the Pillar 4 temporal signal profile."""

from __future__ import annotations

import logging
from typing import Any

from modules.pillars.context import EventContext, EventIdentity
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext

from .debug_logging import log_p4_extraction, log_p4_signal_profile
from .periods import P4_MODULE_ID, P4_MODULE_NAME, P4_PILLAR_ID
from .raw_audit import build_raw_audit
from .signal_engine import ENGINE_VERSION, build_p4_signal_profile
from .trajectory_policy import extract_p4_trajectory_inputs


logger = logging.getLogger(__name__)


def calculate_pillar_4(
    event_context: EventIdentity | EventContext,
    odds_trajectory_context: OddsTrajectoryContext,
    *,
    target_minute: int,
    debug_mode: bool = False,
) -> dict[str, Any]:
    """Return P4's causal temporal profile for one exact operative target."""
    if odds_trajectory_context is None:
        raise ValueError("odds_trajectory_context is required for Pillar 4")
    extraction = extract_p4_trajectory_inputs(
        event_context,
        odds_trajectory_context,
        target_minute=target_minute,
    )
    if debug_mode:
        log_p4_extraction(logger, extraction)
    raw = build_raw_audit(extraction)
    base = {
        "pillar_id": P4_PILLAR_ID,
        "pillar_name": "Temporal Market Drift Signal Profile",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P4_TARGET_MINUTE": extraction.target_minute,
        "PERIODS": extraction.periods,
        "MISSING_INPUTS": list(extraction.missing_inputs),
        "INVALID_INPUTS": list(extraction.invalid_inputs),
        "AMBIGUOUS_INPUTS": list(extraction.ambiguous_inputs),
    }
    if not extraction.usable:
        logger.info(
            "P4 signal profile unavailable event_id=%s target_minute=%s reason=%s",
            event_context.event_id,
            extraction.target_minute,
            extraction.reason,
        )
        return {
            **base,
            "P4_STATUS": "INSUFFICIENT_DATA",
            "status": "INSUFFICIENT_DATA",
            "P4_SIGNAL_PROFILE": None,
            "modules": [],
            "raw": raw,
        }

    profile = build_p4_signal_profile(
        extraction,
        debug_mode=debug_mode,
    ).to_dict()
    status = str(profile["SUMMARY"]["STATUS"])
    module = {
        "pillar_id": P4_PILLAR_ID,
        "module_id": P4_MODULE_ID,
        "module_name": P4_MODULE_NAME,
        "engine_version": ENGINE_VERSION,
        "P4_STATUS": status,
        "status": status,
        "P4_TARGET_MINUTE": extraction.target_minute,
        "P4_SIGNAL_PROFILE": profile,
        "raw": raw,
    }
    if debug_mode:
        log_p4_signal_profile(logger, profile)
    logger.info(
        "P4 signal profile calculated event_id=%s target_minute=%s status=%s "
        "adaptive_series=%s checkpoint_series=%s",
        event_context.event_id,
        extraction.target_minute,
        status,
        profile["SUMMARY"]["ADAPTIVE_SERIES_COUNT"],
        profile["SUMMARY"]["CHECKPOINT_SERIES_COUNT"],
    )
    return {
        **base,
        "P4_STATUS": status,
        "status": status,
        "P4_SIGNAL_PROFILE": profile,
        "modules": [module],
        "raw": raw,
    }


__all__ = ["ENGINE_VERSION", "calculate_pillar_4"]
