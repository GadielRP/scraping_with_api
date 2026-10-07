"""Provider ownership rules for quotes within shared canonical markets.

Market and choice identity are shared; quote identity includes the provider,
exchange side and depth. A provider's write policy only changes its own quotes.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MarketWritePolicy:
    """Describe which parts of a provider quote one source may mutate."""

    name: str
    overwrite_initial_odds: bool = False
    persist_current_odds: bool = True
    persist_opening_snapshots: bool = True
    persist_current_snapshots: bool = True
    require_initial_odds: bool = False


DEFAULT_MARKET_WRITE_POLICY = MarketWritePolicy(name="standard")

# OddsPortal supplies authoritative openings. Its quotes do not write current
# prices or trajectory snapshots; readers resolve those fields independently.
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
