"""Pillar 4 orchestrator."""

from __future__ import annotations

import logging
from typing import Any, Dict

from modules.pillars.context import EventContext
from modules.pillars.odds_trajectory_context import OddsTrajectoryContext
from modules.pillars.pillar_4.drift_engine.drift_engine import (
    ENGINE_VERSION,
    calculate_p4_drift_engine,
)

logger = logging.getLogger(__name__)


def calculate_pillar_4(
    event_context: EventContext,
    odds_trajectory_context: OddsTrajectoryContext,
    debug_mode: bool = False,
) -> Dict[str, Any]:
    """Calculate Pillar 4 and return a serializable pillar payload."""
    logger.info(
        "P4 orchestrator start for event_id=%s participants=%s debug_mode=%s",
        event_context.event_id,
        event_context.participants_label,
        debug_mode,
    )
    drift_engine_result = calculate_p4_drift_engine(
        event_context=event_context,
        odds_trajectory_context=odds_trajectory_context,
        debug_mode=debug_mode,
    )
    pillar_status = drift_engine_result.get("P4_STATUS", "INSUFFICIENT_DATA")
    logger.info(
        "P4 orchestrator done for %s: status=%s modules=%s market_period_count=%s",
        event_context.participants_label,
        pillar_status,
        [drift_engine_result.get("module_id")],
        drift_engine_result.get("market_period_count", 0),
    )

    return {
        "pillar_id": "pillar_4",
        "pillar_name": "Temporal Market Drift",
        "engine_version": ENGINE_VERSION,
        "event_id": event_context.event_id,
        "participants": event_context.participants_label,
        "P4_STATUS": pillar_status,
        "status": pillar_status,
        "modules": [drift_engine_result],
        "market_period_results": drift_engine_result.get("market_period_results", {}),
        "market_period_count": drift_engine_result.get("market_period_count", 0),
        "active_market_period_count": drift_engine_result.get("active_market_period_count", 0),
        "insufficient_market_period_count": drift_engine_result.get(
            "insufficient_market_period_count",
            0,
        ),
        "raw": {
            "module_count": 1,
            "module_ids": [drift_engine_result.get("module_id")],
            "drift_engine": drift_engine_result.get("raw", {}),
        },
    }
