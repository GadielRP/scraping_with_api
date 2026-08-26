"""Application service coordinating mining adapters and persistence policy."""

from __future__ import annotations

from typing import Any, Mapping

from modules.pillars.context import EventContext

from .contracts import validate_mining_run
from .ports import PillarMiningAdapter, PillarMiningWriter


class PillarMiningService:
    VALID_STATUS_MODES = frozenset({"all", "successful_only"})

    def __init__(
        self,
        writer: PillarMiningWriter,
        adapters: Mapping[str, PillarMiningAdapter],
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
        self._adapters = dict(adapters)
        self._enabled = bool(enabled)
        self._status_mode = normalized_mode

    def persist(
        self,
        pillar_id: str,
        event_context: EventContext,
        result: dict[str, Any],
    ) -> bool:
        if not self._enabled:
            return False

        try:
            adapter = self._adapters[pillar_id]
        except KeyError as exc:
            raise ValueError(f"no mining adapter registered for {pillar_id!r}") from exc

        run = adapter.build(event_context, result)
        validate_mining_run(run)
        if self._status_mode == "successful_only" and run.canonical_status != "SUCCESS":
            return False

        self._writer.replace_run(run)
        return True
