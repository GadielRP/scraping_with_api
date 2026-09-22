"""Resolve the single canonical moneyline contract used by Pillar 5.

This module deliberately contains no extraction or scoring logic.  Its only
responsibility is to inspect the canonical trajectory tree and decide whether
there is exactly one supported moneyline identity for the event.  A period is
part of that identity, so a regulation market and an including-overtime
market are treated as different contracts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from modules.pillars.market_snapshot_extractor import MarketIdentity
from modules.pillars.odds_trajectory_context import (
    MarketLineOddsTrajectory,
    OddsTrajectoryContext,
)

from .periods import P5_MONEYLINE_IDENTITIES


def _normalize(value: object) -> str:
    return " ".join(str(value or "").replace("-", " ").casefold().split())


def _identity_key(identity: MarketIdentity) -> tuple[str, str, str]:
    return (
        _normalize(identity.market_group),
        _normalize(identity.market_period),
        _normalize(identity.market_name),
    )


def _iter_market_lines(
    context: OddsTrajectoryContext,
) -> Iterable[MarketLineOddsTrajectory]:
    for periods in context.markets.values():
        for market_names in periods.values():
            for market_lines in market_names.values():
                yield from market_lines.values()


@dataclass(frozen=True, slots=True)
class P5MoneylineTarget:
    """One exact canonical market contract selected for an event."""

    market_group: str
    market_period: str
    market_name: str

    @property
    def identity(self) -> MarketIdentity:
        return MarketIdentity(
            self.market_group,
            self.market_period,
            self.market_name,
        )

    @property
    def is_two_way(self) -> bool:
        return self.market_group == "Home/Away"

    def to_dict(self) -> dict[str, Any]:
        return {
            "market_group": self.market_group,
            "market_period": self.market_period,
            "market_name": self.market_name,
            "market_shape": "TWO_WAY" if self.is_two_way else "THREE_WAY",
        }


@dataclass(frozen=True, slots=True)
class P5MoneylineSelection:
    """Result of moneyline target resolution, including safe diagnostics."""

    target: P5MoneylineTarget | None
    reason: str | None = None
    candidates: tuple[P5MoneylineTarget, ...] = ()

    @property
    def is_ambiguous(self) -> bool:
        return self.reason == "ambiguous_moneyline_market"

    def to_dict(self) -> dict[str, Any]:
        return {
            "reason": self.reason,
            "target": self.target.to_dict() if self.target else None,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


def select_p5_moneyline_target(
    context: OddsTrajectoryContext | None,
    *,
    supported_identities: tuple[MarketIdentity, ...] = P5_MONEYLINE_IDENTITIES,
) -> P5MoneylineSelection:
    """Select one moneyline identity without applying sport-based guessing.

    The trajectory context is already canonicalized.  If it contains no
    supported moneyline, P5 cannot build a snapshot.  If it contains more than
    one distinct identity, selecting one would mix settlement contracts, so we
    fail closed and expose the candidates for diagnostics.
    """
    if context is None:
        return P5MoneylineSelection(None, "missing_odds_trajectory_context")
    if not context.available:
        return P5MoneylineSelection(None, "odds_trajectory_unavailable")

    supported = {_identity_key(identity): identity for identity in supported_identities}
    found: dict[tuple[str, str, str], P5MoneylineTarget] = {}
    for market_line in _iter_market_lines(context):
        key = (
            _normalize(market_line.market_group),
            _normalize(market_line.market_period),
            _normalize(market_line.market_name),
        )
        identity = supported.get(key)
        if identity is None:
            continue
        found[key] = P5MoneylineTarget(
            market_group=identity.market_group,
            market_period=identity.market_period,
            market_name=identity.market_name,
        )

    candidates = tuple(
        sorted(
            found.values(),
            key=lambda candidate: (
                candidate.market_group,
                candidate.market_period,
                candidate.market_name,
            ),
        )
    )
    if not candidates:
        return P5MoneylineSelection(None, "moneyline_market_unavailable")
    if len(candidates) > 1:
        return P5MoneylineSelection(
            None,
            "ambiguous_moneyline_market",
            candidates,
        )
    return P5MoneylineSelection(candidates[0], candidates=candidates)


__all__ = [
    "P5MoneylineSelection",
    "P5MoneylineTarget",
    "select_p5_moneyline_target",
]
