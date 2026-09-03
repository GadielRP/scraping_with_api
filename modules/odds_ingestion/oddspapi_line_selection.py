"""Choose one current line per bookmaker/market/period, without I/O or mutation.

Both the /odds adapter and the mainline cache use this policy. Historical
responses use the cached selection instead of choosing again at closing prices.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Any

from infrastructure.persistence.repositories.market_mapping_repository import (
    CanonicalMarketResolution,
    MarketMappingIndex,
    MarketMappingRepository,
)
from modules.oddspapi.format_utils import normalize_source_id


def _finite_decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def _entries(value: Any) -> Iterable[tuple[str, dict]]:
    """Iterate the object and array shapes accepted by OddsPapi."""
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, dict):
                yield str(key), item
    elif isinstance(value, list):
        for index, item in enumerate(value):
            if isinstance(item, dict):
                key = item.get("slug") or item.get("marketId") or item.get("outcomeId") or index
                yield str(key), item


@dataclass(frozen=True)
class LineLiquidity:
    base_limit: Decimal
    consistency: Decimal
    effective_base_limit: Decimal


def line_liquidity(prices_and_limits: list[tuple[Decimal, Any]]) -> LineLiquidity | None:
    """Normalize every choice; missing/invalid limits make liquidity unavailable."""
    bases = []
    for price, raw_limit in prices_and_limits:
        limit = _finite_decimal(raw_limit)
        if limit is None or limit < 0 or not price.is_finite() or price <= 1:
            return None
        bases.append(limit * min(Decimal(1), price - 1))
    if not bases:
        return None
    base_limit = median(bases)
    consistency = min(bases) / max(bases) if max(bases) > 0 else Decimal(0)
    return LineLiquidity(base_limit, consistency, base_limit * consistency)


@dataclass(frozen=True)
class _Candidate:
    market_id: str
    line: Decimal
    mainline: bool
    price_gap: Decimal
    liquidity: LineLiquidity | None

    @property
    def rank(self) -> tuple:
        # A measured zero ranks above unknown liquidity. Remaining exact ties
        # use numeric line and source ID solely for deterministic selection.
        effective = (
            None if self.liquidity is None else self.liquidity.effective_base_limit
        )
        return (
            not self.mainline,
            self.price_gap,
            effective is None,
            -(effective or Decimal(0)),
            self.line,
            self.market_id,
        )

    def audit(self) -> dict:
        return {
            "sourceMarketId": self.market_id,
            "line": str(self.line),
            "providerMainLine": self.mainline,
            "priceGap": str(self.price_gap),
            "lineBaseLimit": (
                None if self.liquidity is None else str(self.liquidity.base_limit)
            ),
            "baseLimitConsistency": (
                None if self.liquidity is None else str(self.liquidity.consistency)
            ),
            "effectiveBaseLimit": (
                None if self.liquidity is None else str(self.liquidity.effective_base_limit)
            ),
        }


@dataclass(frozen=True)
class LineSelection:
    selected_market_ids: frozenset[str] = frozenset()
    excluded_market_ids: frozenset[str] = frozenset()
    diagnostics: tuple[dict, ...] = ()


def _candidate(
    market_id: str,
    market: dict,
    resolution: CanonicalMarketResolution,
    index: MarketMappingIndex,
    expected: set[str],
) -> tuple[_Candidate | None, str | None]:
    line = _finite_decimal(resolution.source_handicap)
    if line is None:
        return None, "invalid_line"
    if market.get("marketActive") is False:
        return None, "inactive_market"
    if len(expected) < 2:
        return None, "unknown_complete_choice_set"
    choices: dict[str, tuple[Decimal, dict]] = {}
    for outcome_id, outcome in _entries(market.get("outcomes", {})):
        outcome_resolution = MarketMappingRepository.resolve_outcome(
            index, market_source_mapping_id=resolution.mapping_id,
            source_outcome_id=outcome_id,
        )
        if not outcome_resolution.resolved:
            continue
        name = outcome_resolution.canonical_choice_name
        players = list(_entries(
            outcome.get("players") or ([outcome] if "price" in outcome else [])
        ))
        if not players:
            return None, "incomplete_choices"
        if len(players) != 1 or name in choices:
            return None, "ambiguous_choice"
        player = players[0][1]
        price = _finite_decimal(player.get("price"))
        if player.get("active") is False:
            return None, "inactive_choice"
        if price is None or price <= 1:
            return None, "invalid_choice_price"
        choices[name] = (price, player)
    if set(choices) != expected:
        return None, "incomplete_choices"
    prices = [price for price, _ in choices.values()]
    return _Candidate(
        market_id=market_id,
        line=line,
        mainline=all(player.get("mainLine") is True for _, player in choices.values()),
        price_gap=max(prices) - min(prices),
        liquidity=line_liquidity([
            (price, player.get("limit")) for price, player in choices.values()
        ]),
    ), None


def select_current_lines(
    markets: Any,
    *,
    market_mapping_index: MarketMappingIndex | None,
    source_sport_id: Any,
    source: str = "oddspapi",
    is_live: bool = False,
) -> LineSelection:
    """Select complete active lines independently for each bookmaker's payload.

    Prices are compared with Decimal at source precision, before persistence
    rounding. Non-line and live markets are outside this policy's scope.
    """
    if market_mapping_index is None:
        return LineSelection()
    expected_by_mapping: dict[int, set[str]] = {}
    for (mapping_id, _), outcome in market_mapping_index.outcome_mappings.items():
        if outcome.resolved and outcome.canonical_choice_name:
            expected_by_mapping.setdefault(mapping_id, set()).add(outcome.canonical_choice_name)

    groups: dict[tuple, list[_Candidate]] = {}
    handled: set[str] = set()
    diagnostics = []
    for raw_market_id, market in _entries(markets):
        market_id = normalize_source_id(raw_market_id)
        if market_id is None:
            continue
        resolution = MarketMappingRepository.resolve_market(
            market_mapping_index, source=source, source_sport_id=source_sport_id,
            source_market_id=market_id,
        )
        if (
            not resolution.resolved
            or not resolution.requires_choice_group
            or market.get("isLive", is_live)
        ):
            continue
        handled.add(market_id)
        candidate, reason = _candidate(
            market_id, market, resolution, market_mapping_index,
            expected_by_mapping.get(resolution.mapping_id, set()),
        )
        if candidate is None:
            diagnostics.append({
                "sourceMarketId": market_id,
                "canonicalMarketKey": resolution.canonical_market_key,
                "marketPeriod": resolution.canonical_market_period,
                "reason": reason,
            })
            continue
        key = (
            resolution.canonical_market_key, resolution.canonical_market_name,
            resolution.canonical_market_group, resolution.canonical_market_period,
        )
        groups.setdefault(key, []).append(candidate)

    selected = set()
    for key, candidates in groups.items():
        ranked = sorted(candidates, key=lambda candidate: candidate.rank)
        winner = ranked[0]
        selected.add(winner.market_id)
        diagnostics.append({
            "canonicalMarketKey": key[0],
            "marketPeriod": key[3],
            "reason": "selected_current_line",
            "selected": winner.audit(),
            "discarded": [candidate.audit() for candidate in ranked[1:]],
        })
    return LineSelection(frozenset(selected), frozenset(handled - selected), tuple(diagnostics))
