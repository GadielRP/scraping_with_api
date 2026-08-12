"""Persistence collaborators for the odds market schema refactor.

See docs/refactors/db-schema-odds-refactor.md for the full design and the
phase each module belongs to. This subpackage exists to split
`market_repository.py` (market identity, choice writes, exchange quote
writes, snapshot writes, read queries) into single-responsibility units
instead of one monolithic repository class.
"""

from .exchange_quote_payload import ExchangeQuotePayload
from .market_choice_quote_backfill_repository import MarketChoiceQuoteBackfillRepository
from .market_choice_quote_merge_policy import (
    QuoteCandidateState,
    QuoteExistingState,
    QuoteMergeDecision,
    QuoteMergeMode,
    decide_quote_merge,
)
from .market_choice_quote_writer import MarketChoiceQuoteWriter, QuoteUpsertResult
from .market_choice_snapshot_writer import MarketChoiceSnapshotWriter
from .odds_movement import compute_movement
from .market_quote_read_policy import (
    QuoteFieldPriority,
    QuoteReadPriorityPolicy,
    load_quote_read_priority_policy,
)
from .market_read_models import (
    ExternalChoiceQuote,
    ExternalMarketQuoteBlock,
    ExternalMarketQuoteReadResult,
    MarketQuoteReadDiagnostic,
    QuoteFieldOrigin,
)
from .market_read_queries import MarketReadQueries
from .market_quote_readiness import (
    MarketQuoteReadinessAuditor,
    MarketQuoteReadinessIssue,
    MarketQuoteReadinessReport,
)

__all__ = [
    "ExchangeQuotePayload",
    "MarketChoiceQuoteBackfillRepository",
    "MarketChoiceQuoteWriter",
    "MarketChoiceSnapshotWriter",
    "QuoteCandidateState",
    "QuoteExistingState",
    "QuoteMergeDecision",
    "QuoteMergeMode",
    "QuoteUpsertResult",
    "compute_movement",
    "decide_quote_merge",
    "ExternalChoiceQuote",
    "ExternalMarketQuoteBlock",
    "ExternalMarketQuoteReadResult",
    "MarketQuoteReadDiagnostic",
    "MarketReadQueries",
    "MarketQuoteReadinessAuditor",
    "MarketQuoteReadinessIssue",
    "MarketQuoteReadinessReport",
    "QuoteFieldOrigin",
    "QuoteFieldPriority",
    "QuoteReadPriorityPolicy",
    "load_quote_read_priority_policy",
]
