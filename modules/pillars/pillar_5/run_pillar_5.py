"""Pillar 5 orchestrator."""

from __future__ import annotations

import logging
from typing import Any, Dict

from modules.pillars.context import EventContext
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from modules.pillars.pillar_5.exact_price_memory_engine.exact_price_memory_engine import (
    ENGINE_VERSION,
    calculate_p5_exact_price_memory_engine,
)

logger = logging.getLogger(__name__)


def calculate_pillar_5(
    event_context: EventContext,
    ft_1x2_odds_trajectory: OddsTrajectoryContext,
    debug_mode: bool = False,
) -> Dict[str, Any]:
    """Calculate Pillar 5 and return a serializable pillar payload."""
    logger.info(
        "P5 orchestrator start for event_id=%s participants=%s debug_mode=%s start_time=%s",
        event_context.event_id,
        event_context.participants_label,
        debug_mode,
        event_context.start_time_utc,
    )
    engine_result = calculate_p5_exact_price_memory_engine(
        event_context=event_context,
        ft_1x2_odds_trajectory=ft_1x2_odds_trajectory,
        debug_mode=debug_mode,
    )
    pillar_status = engine_result.get("P5_STATUS", "INSUFFICIENT_DATA")
    logger.info(
        "P5 orchestrator done for %s: status=%s direction=%s score=%.3f strength=%s sample_size=%s",
        event_context.participants_label,
        pillar_status,
        engine_result.get("P5_DIRECTION"),
        engine_result.get("P5", 0.0),
        engine_result.get("P5_STRENGTH"),
        engine_result.get("sample_size"),
    )

    return {
        "pillar_id": "pillar_5",
        "pillar_name": "Exact Price Memory",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P5_STATUS": pillar_status,
        "status": pillar_status,
        "modules": [engine_result],
        "P5_VALID": engine_result.get("P5_VALID"),
        "P5_DIRECTION": engine_result.get("P5_DIRECTION"),
        "P5": engine_result.get("P5"),
        "P5_STRENGTH": engine_result.get("P5_STRENGTH"),
        "raw": {
            "module_count": 1,
            "module_ids": [engine_result.get("module_id")],
            "exact_price_memory_engine": engine_result.get("raw", {}),
        },
    }
