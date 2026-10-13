"""Dependency-inversion ports for pillar mining."""

from __future__ import annotations

from typing import Any, Protocol

from modules.pillars.context import EventContext, EventIdentity

from .contracts import EvaluationSnapshot, PillarMiningRun


class PillarMiningWriter(Protocol):
    def append_evaluation(self, snapshot: EvaluationSnapshot, *, session: Any) -> str:
        """Append the frozen inputs and configuration of one evaluation."""

    def append_run(self, run: PillarMiningRun, *, session: Any) -> int:
        """Append an immutable run in its owning evaluation transaction."""

    def require_complete_evaluation(self, evaluation_id: str, *, session: Any) -> None:
        """Reject incomplete evaluations before committing their transaction."""


class PillarMiningAdapter(Protocol):
    pillar_id: str

    def build(
        self, event_context: EventContext | EventIdentity, result: dict[str, Any]
    ) -> PillarMiningRun:
        """Translate one pillar-specific output into the common mining contract."""


class PillarMiningReader(Protocol):
    def get_result(self, run_id: int) -> dict[str, Any]:
        """Read the stored schema without changing historical status semantics."""

    def get_evaluation(self, evaluation_id: str) -> dict[str, Any]:
        """Reconstruct an evaluation from persisted inputs and results."""

    def get_mining_case(self, evaluation_id: str) -> dict[str, Any]:
        """Associate the original evaluation with a separate outcome observation."""
