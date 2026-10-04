"""Build and validate exhaustive historical samples for Pillar 5."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from modules.pillars.context import EventContext, EventIdentity
from shared.temporal import as_utc

from .calculation_models import MemoryQueryKey, MemorySample, PopulationFilters
from .models import ThreeWayMarketSnapshot
from .ports import PriceMemoryReader

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


def build_memory_sample(
    repository: PriceMemoryReader,
    *,
    key: MemoryQueryKey,
    current_event_id: int,
    current_starts_at: datetime,
    population_filters: PopulationFilters,
    debug_mode=False,
) -> MemorySample:
    return repository.summarize(
        key=key,
        current_event_id=current_event_id,
        current_starts_at=as_utc(current_starts_at),
        population_filters=population_filters,
    )
