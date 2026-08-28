"""Pillar-specific adapters for the common mining contract."""

from .pillar_2 import P2MiningAdapter
from .pillar_3 import P3MiningAdapter

__all__ = ["P2MiningAdapter", "P3MiningAdapter"]
