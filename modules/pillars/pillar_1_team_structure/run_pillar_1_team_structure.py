"""Pillar 1 — Team Structure aggregator.

Currently contains only Module 1 (Base Strength).  When additional modules
are added, this aggregator will weight them to produce a single pillar value.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from modules.pillars.context import EventContext
from modules.pillars.pillar_1_team_structure.module_1.base_strength import (
    calculate_base_strength,
)

logger = logging.getLogger(__name__)


def calculate_pillar_1_team_structure(
    streak_analysis: Any,
    event_context: EventContext,
) -> Dict[str, Any]:
    """Calculate Pillar 1 — Team Structure for an event.

    Args:
        streak_analysis: A ``MatchupStreakContext`` produced by the streak
            analysis pipeline.

    Returns:
        A dictionary containing pillar metadata, the list of module results,
        and the aggregated pillar value.
    """
    m1_result = calculate_base_strength(
        streak_analysis,
        event_context=event_context,
    )

    # Temporary: pillar value equals M1 until more modules exist
    pillar_value = m1_result.value

    return {
        "pillar_id": "pillar_1_team_structure",
        "pillar_name": "Team Structure",
        "event_id": m1_result.event_id,
        "participants": m1_result.participants,
        "modules": [
            {
                "module_id": m1_result.module_id,
                "module_name": m1_result.module_name,
                "value": m1_result.value,
                "bias": m1_result.bias,
                "strength": m1_result.strength,
                "components": [
                    {
                        "name": c.name,
                        "edge": c.edge,
                        "bias": c.bias,
                        "strength": c.strength,
                        "weight": c.weight,
                        "weighted_edge": c.weighted_edge,
                    }
                    for c in m1_result.components
                ],
                "raw": m1_result.raw,
            }
        ],
        "value": pillar_value,
        "raw": {
            "temporary_weighting": "pillar_value_equals_m1_until_more_modules_exist",
        },
    }
