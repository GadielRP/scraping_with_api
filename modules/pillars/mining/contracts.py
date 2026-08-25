"""Persistence-neutral contracts for pillar mining observations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from shared.timezone_utils import get_local_now


@dataclass(frozen=True)
class PillarMiningObservation:
    event_id: int
    pillar_id: str
    result_scope: str
    module_id: str | None
    engine_version: str
    payload_schema_version: int
    evaluation_minute: int | None
    target_minute: int | None
    observation_slot: str
    sport: str
    competition_id: int | None
    market_type: str | None
    status: str
    is_successful: bool
    is_valid: bool | None
    score_name: str | None
    score: Decimal | None
    direction: str | None
    strength: str | None
    metrics: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    calculated_at: datetime = field(default_factory=get_local_now)
