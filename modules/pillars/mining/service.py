"""Application policy for deciding which pillar observations are persisted."""

from __future__ import annotations

from typing import Any

from modules.pillars.context import EventContext

from .pillar_2_adapter import build_p2_mining_observation
from .ports import PillarMiningWriter


class PillarMiningService:
    VALID_STATUS_MODES = frozenset({"all", "active_only"})

    def __init__(
        self,
        writer: PillarMiningWriter,
        *,
        enabled: bool = True,
        status_mode: str = "all",
    ) -> None:
        normalized_mode = str(status_mode).strip().lower()
        if normalized_mode not in self.VALID_STATUS_MODES:
            expected = ", ".join(sorted(self.VALID_STATUS_MODES))
            raise ValueError(
                f"status_mode must be one of: {expected}; got {status_mode!r}"
            )
        self._writer = writer
        self._enabled = bool(enabled)
        self._status_mode = normalized_mode

    def persist_p2(
        self,
        event_context: EventContext,
        p2_result: dict[str, Any],
    ) -> bool:
        """Persist P2 when enabled and allowed by the configured status policy."""
        if not self._enabled:
            return False

        observation = build_p2_mining_observation(event_context, p2_result)
        if self._status_mode == "active_only" and not observation.is_successful:
            return False

        self._writer.upsert(observation)
        return True
