"""Dependency-inversion ports for pillar mining."""

from __future__ import annotations

from typing import Any, Protocol

from modules.pillars.context import EventContext

from .contracts import PillarMiningRun


class PillarMiningWriter(Protocol):
    def replace_run(self, run: PillarMiningRun) -> None:
        """Atomically upsert a run and replace its complete child graph."""


class PillarMiningAdapter(Protocol):
    pillar_id: str

    def build(self, event_context: EventContext, result: dict[str, Any]) -> PillarMiningRun:
        """Translate one pillar-specific output into the common mining contract."""
