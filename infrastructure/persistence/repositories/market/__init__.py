"""Persistence collaborators for the odds market schema refactor.

See docs/refactors/db-schema-odds-refactor.md for the full design and the
phase each module belongs to. This subpackage exists to split
`market_repository.py` (market identity, choice writes, exchange quote
writes, snapshot writes, read queries) into single-responsibility units
instead of one monolithic repository class.
"""

from .exchange_quote_payload import ExchangeQuotePayload
from .market_choice_quote_writer import MarketChoiceQuoteWriter
from .market_choice_snapshot_writer import MarketChoiceSnapshotWriter
from .odds_movement import compute_movement

__all__ = [
    "ExchangeQuotePayload",
    "MarketChoiceQuoteWriter",
    "MarketChoiceSnapshotWriter",
    "compute_movement",
]
