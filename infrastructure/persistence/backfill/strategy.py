"""Contracts shared by persisted-data backfill strategies."""

from __future__ import annotations

from typing import Any, Protocol


class BackfillConflict(RuntimeError):
    """Raised when a strategy cannot reconcile an event without guessing."""


class BackfillStrategy(Protocol):
    """A strategy selects persisted rows and mutates one event atomically."""

    strategy_name: str

    def audit(self) -> dict[str, Any]:
        """Return strategy, scope, parameters and deterministic event records."""

    def audit_metadata(self) -> dict[str, Any]:
        """Return small immutable metadata for a streaming manifest."""

    def audit_upper_bound(self) -> dict[str, Any] | None:
        """Capture the inclusive end of the deterministic audit population."""

    def audit_page(
        self,
        *,
        page_size: int,
        cursor: dict[str, Any] | None,
        upper_bound: dict[str, Any] | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
        """Read one bounded page and return its next keyset cursor."""

    def apply_event(
        self,
        event_id: int,
        *,
        expected_state: dict[str, Any],
    ) -> dict[str, Any]:
        """Apply one manifest event, or raise a fail-closed conflict."""
