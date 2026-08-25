"""Ports implemented by mining persistence infrastructure."""

from __future__ import annotations

from typing import Protocol

from .contracts import PillarMiningObservation


class PillarMiningWriter(Protocol):
    def upsert(self, observation: PillarMiningObservation) -> None:
        """Persist one canonical observation, replacing the same execution slot."""
