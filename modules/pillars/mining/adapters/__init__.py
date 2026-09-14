"""Pillar-specific adapters for the common mining contract."""

from .pillar_1 import P1SideMiningAdapter, P1TotalsMiningAdapter
from .pillar_2 import P2MiningAdapter
from .pillar_3 import P3MiningAdapter
from .pillar_4 import P4MiningAdapter

__all__ = [
    "P1SideMiningAdapter",
    "P1TotalsMiningAdapter",
    "P2MiningAdapter",
    "P3MiningAdapter",
    "P4MiningAdapter",
]
