"""Provider-specific ownership rules for canonical market writes.

The current schema intentionally collapses matching provider markets into one
``Market``/``MarketChoice`` row.  Until provider identity becomes part of that
schema, write ownership must be explicit so asynchronous providers cannot
silently replace each other's values.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MarketWritePolicy:
    """Describe which parts of a canonical choice one source may mutate."""

    name: str
    overwrite_initial_odds: bool = False
    persist_current_odds: bool = True
    persist_opening_snapshots: bool = True
    persist_current_snapshots: bool = True
    require_initial_odds: bool = False


DEFAULT_MARKET_WRITE_POLICY = MarketWritePolicy(name="standard")

# TEMPORARY ARCHITECTURE DECISION:
# OddsPortal is currently used only as the authoritative opening-odds source.
# It must not write current odds or snapshots because snapshots from different
# providers share the same canonical choice and downstream trajectory queries
# do not yet distinguish opening snapshots from current snapshots.  Keeping the
# rule here makes the compromise visible and removable when the schema becomes
# source-aware.
ODDSPORTAL_OPENING_ONLY_POLICY = MarketWritePolicy(
    name="oddsportal_opening_only",
    overwrite_initial_odds=True,
    persist_current_odds=False,
    persist_opening_snapshots=False,
    persist_current_snapshots=False,
    require_initial_odds=True,
)


def market_write_policy_for_source(source: str | None) -> MarketWritePolicy:
    """Return the centralized persistence policy for a provider source."""

    normalized = str(source or "").strip().lower()
    if normalized == "oddsportal" or normalized.startswith("oddsportal_"):
        return ODDSPORTAL_OPENING_ONLY_POLICY
    return DEFAULT_MARKET_WRITE_POLICY


__all__ = [
    "DEFAULT_MARKET_WRITE_POLICY",
    "MarketWritePolicy",
    "ODDSPORTAL_OPENING_ONLY_POLICY",
    "market_write_policy_for_source",
]
