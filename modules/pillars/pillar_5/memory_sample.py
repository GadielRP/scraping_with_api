"""Build and validate exhaustive historical samples for Pillar 5."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from infrastructure.persistence.repositories.pillar_5_price_memory_repository import (
    HistoricalPriceMatch,
    Pillar5PriceMemoryRepository,
)
from modules.pillars.context import EventContext, EventIdentity
from shared.temporal import as_utc

from .calculation_models import MemoryQueryKey, MemorySample, PopulationFilters
from .models import ThreeWayMarketSnapshot

ODDS_QUANTUM = Decimal("0.001")


def canonical_price(value: object) -> Decimal:
    """Validate and canonize one positive decimal price to PostgreSQL 3dp."""
    if value is None or isinstance(value, bool):
        raise ValueError("odds price is missing")
    try:
        price = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("odds price is not decimal") from exc
    if not price.is_finite() or price <= Decimal("1"):
        raise ValueError("odds price must be finite and greater than 1")
    return price.quantize(ODDS_QUANTUM, rounding=ROUND_HALF_UP)


def build_memory_query_key(
    event_context: EventIdentity | EventContext,
    snapshot: ThreeWayMarketSnapshot,
    *,
    expected_bookie_id: int,
) -> MemoryQueryKey:
    """Create one exact semantic key from a coherent extracted snapshot."""
    points = [snapshot.home, snapshot.away]
    if snapshot.draw is not None:
        points.append(snapshot.draw)
    traces = [point.trace for point in points]
    reference = snapshot.home.trace

    if not str(event_context.sport or "").strip():
        raise ValueError("event sport is required")
    if any(trace.bookie_id != expected_bookie_id for trace in traces):
        raise ValueError("snapshot bookie identity mismatch")
    if any(trace.market_group != reference.market_group for trace in traces):
        raise ValueError("snapshot mixes market groups")
    if any(trace.market_period != reference.market_period for trace in traces):
        raise ValueError("snapshot mixes market periods")
    if any(trace.market_name != reference.market_name for trace in traces):
        raise ValueError("snapshot mixes market lines")

    market_group = reference.market_group
    if market_group == "1X2":
        if snapshot.draw is None:
            raise ValueError("1X2 snapshot is incomplete without DRAW")
        market_shape = "THREE_WAY"
        odds_draw = canonical_price(snapshot.draw.odds_price)
    elif market_group == "Home/Away":
        if snapshot.draw is not None:
            raise ValueError("Home/Away snapshot cannot contain DRAW")
        market_shape = "TWO_WAY"
        odds_draw = None
    else:
        raise ValueError(f"unsupported market group: {market_group!r}")

    return MemoryQueryKey(
        sport=str(event_context.sport).strip(),
        bookie_id=expected_bookie_id,
        market_group=market_group,
        market_period=reference.market_period,
        market_shape=market_shape,
        odds_home=canonical_price(snapshot.home.odds_price),
        odds_draw=odds_draw,
        odds_away=canonical_price(snapshot.away.odds_price),
    )


def _row_matches_key(row: HistoricalPriceMatch, key: MemoryQueryKey) -> bool:
    try:
        draw = canonical_price(row.odds_draw) if row.odds_draw is not None else None
        return (
            row.sport == key.sport
            and row.bookie_id == key.bookie_id
            and row.market_group == key.market_group
            and row.market_period == key.market_period
            and row.has_draw == key.has_draw
            and canonical_price(row.odds_home) == key.odds_home
            and draw == key.odds_draw
            and canonical_price(row.odds_away) == key.odds_away
        )
    except ValueError:
        return False


def _eligible_winner(value: object, *, has_draw: bool) -> str | None:
    normalized = str(value or "").strip().upper()
    if normalized in {"1", "HOME"}:
        return "HOME"
    if normalized in {"2", "AWAY"}:
        return "AWAY"
    if has_draw and normalized in {"X", "DRAW"}:
        return "DRAW"
    return None


def build_memory_sample(
    repository: Pillar5PriceMemoryRepository,
    *,
    key: MemoryQueryKey,
    current_event_id: int,
    current_starts_at: datetime,
    population_filters: PopulationFilters,
) -> MemorySample:
    """Load, validate, and de-duplicate the full exact-match population."""
    cutoff = as_utc(current_starts_at)
    rows = repository.find_exact_matches(
        sport=key.sport,
        bookie_id=key.bookie_id,
        market_group=key.market_group,
        market_period=key.market_period,
        odds_home=key.odds_home,
        odds_draw=key.odds_draw,
        odds_away=key.odds_away,
        has_draw=key.has_draw,
        competition_id=population_filters.competition_id,
        season_id=population_filters.season_id,
        country=population_filters.country,
        current_event_id=current_event_id,
        current_starts_at=cutoff,
        limit=None,
    )

    eligible: list[HistoricalPriceMatch] = []
    diagnostics: list[dict[str, Any]] = []
    seen_event_ids: set[int] = set()
    counts = {"HOME": 0, "DRAW": 0, "AWAY": 0}

    for row in rows:
        reason: str | None = None
        row_start = as_utc(row.starts_at)
        if row.event_id == current_event_id:
            reason = "current_event_excluded"
        elif row_start >= cutoff:
            reason = "non_causal_event_excluded"
        elif row.event_id in seen_event_ids:
            reason = "duplicate_event_excluded"
        elif not _row_matches_key(row, key):
            reason = "query_key_mismatch_excluded"

        winner = _eligible_winner(row.winner_side, has_draw=key.has_draw)
        if reason is None and winner is None:
            reason = "invalid_or_incompatible_result_excluded"

        if reason is not None:
            diagnostics.append({"event_id": row.event_id, "reason": reason})
            continue

        seen_event_ids.add(row.event_id)
        eligible.append(row)
        counts[winner] += 1

    return MemorySample(
        key=key,
        historical_matches=tuple(eligible),
        sample_size=len(eligible),
        wins_home=counts["HOME"],
        wins_draw=counts["DRAW"],
        wins_away=counts["AWAY"],
        eligibility_diagnostics=tuple(diagnostics),
    )


__all__ = [
    "ODDS_QUANTUM",
    "build_memory_query_key",
    "build_memory_sample",
    "canonical_price",
]
