"""Set-based reads of persisted odds for any bookmaker and provider."""
from __future__ import annotations

import logging
from sqlalchemy import Select, or_, select

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    Bookie, CanonicalMarketType, Event, Market, MarketChoice, MarketChoiceQuote,
)
from infrastructure.settings import Config
from .market_odds_read_models import MarketOddsReadResult
from .market_odds_resolver import resolve_market_odds_state
from .market_quote_read_policy import QuoteReadPriorityPolicy, load_quote_read_priority_policy

logger = logging.getLogger(__name__)


def _filter_quotes(
    query: Select, *, event_id: int,
    bookie_ids: tuple[int, ...] | None = None,
    sources: tuple[str, ...] | None = None,
    canonical_market_keys: tuple[str, ...] | None = None,
    is_live: bool | None = None,
) -> Select:
    query = query.where(Market.event_id == int(event_id))
    if bookie_ids is not None:
        query = query.where(Market.bookie_id.in_(bookie_ids))
    if sources is not None:
        query = query.where(MarketChoiceQuote.source.in_(
            tuple(source.strip().lower() for source in sources)
        ))
    if canonical_market_keys is not None:
        query = query.where(CanonicalMarketType.canonical_market_key.in_(canonical_market_keys))
    if is_live is not None:
        query = query.where(Market.is_live == is_live)
    return query.where(or_(
        MarketChoiceQuote.initial_odds.isnot(None),
        MarketChoiceQuote.current_odds.isnot(None),
    ))


class MarketOddsReadRepository:
    def __init__(self, priority_policy: QuoteReadPriorityPolicy | None = None) -> None:
        self.priority_policy = priority_policy or load_quote_read_priority_policy(Config.ODDS_READ_PRIORITY_CONFIG)

    def get_market_odds_state(
        self, event_id: int, *, bookie_ids: tuple[int, ...] | None = None,
        sources: tuple[str, ...] | None = None,
        canonical_market_keys: tuple[str, ...] | None = None, is_live: bool | None = None,
    ) -> MarketOddsReadResult:
        """Resolve opening/current quote state; this is not an as-of history read.

        None filters include all values; an empty tuple includes none. Sources
        limit eligible providers before applying field priority. Signed lines
        and exchange source/side identities remain separate.
        """
        query = (
            select(
                Event.id.label("event_id"),
                Event.sport.label("sport"),
                Market.market_id.label("market_id"),
                Market.market_type_id.label("market_type_id"),
                CanonicalMarketType.canonical_market_key.label("canonical_market_key"),
                CanonicalMarketType.market_family.label("market_family"),
                Market.bookie_id.label("bookie_id"),
                Bookie.name.label("bookie_name"),
                CanonicalMarketType.canonical_market_name.label("market_name"),
                CanonicalMarketType.canonical_market_group.label("market_group"),
                CanonicalMarketType.canonical_market_period.label("market_period"),
                Market.line_value.label("line_value"),
                Market.is_live.label("is_live"),
                MarketChoice.choice_id.label("choice_id"),
                MarketChoice.choice_name.label("choice_name"),
                MarketChoiceQuote.quote_id.label("quote_id"),
                MarketChoiceQuote.source.label("source"),
                MarketChoiceQuote.main_line.label("main_line"),
                MarketChoiceQuote.source_market_id.label("source_market_id"),
                MarketChoiceQuote.source_outcome_id.label("source_outcome_id"),
                MarketChoiceQuote.bookmaker_outcome_id.label("bookmaker_outcome_id"),
                MarketChoiceQuote.source_limit.label("source_limit"),
                MarketChoiceQuote.exchange_side.label("exchange_side"),
                MarketChoiceQuote.exchange_level.label("exchange_level"),
                MarketChoiceQuote.initial_odds.label("initial"),
                MarketChoiceQuote.initial_captured_at.label("initial_captured_at"),
                MarketChoiceQuote.current_odds.label("current"),
                MarketChoiceQuote.current_updated_at.label("current_updated_at"),
            )
            .join(Market, Market.event_id == Event.id)
            .outerjoin(
                CanonicalMarketType,
                CanonicalMarketType.market_type_id == Market.market_type_id,
            )
            .join(Bookie, Bookie.bookie_id == Market.bookie_id)
            .join(MarketChoice, MarketChoice.market_id == Market.market_id)
            .join(MarketChoiceQuote, MarketChoiceQuote.choice_id == MarketChoice.choice_id)
            .order_by(
                Market.market_id,
                MarketChoice.choice_id,
                MarketChoiceQuote.source,
                MarketChoiceQuote.exchange_side,
                MarketChoiceQuote.exchange_level,
                MarketChoiceQuote.quote_id,
            )
        )
        query = _filter_quotes(
            query, event_id=event_id, bookie_ids=bookie_ids, sources=sources,
            canonical_market_keys=canonical_market_keys, is_live=is_live,
        )
        with db_manager.get_session() as session:
            rows = session.execute(query).mappings().all()
        result = resolve_market_odds_state(event_id, rows, self.priority_policy)
        if result.has_blocking_diagnostics:
            logger.error(
                "Odds state read rejected invalid quotes event_id=%s codes=%s", event_id,
                [issue.code for issue in result.diagnostics if issue.blocking],
            )
        return result

    def has_quotes(
        self, event_id: int, *, sources: tuple[str, ...] | None = None,
        bookie_ids: tuple[int, ...] | None = None,
    ) -> bool:
        candidate = (
            select(MarketChoiceQuote.quote_id)
            .join(MarketChoice, MarketChoice.choice_id == MarketChoiceQuote.choice_id)
            .join(Market, Market.market_id == MarketChoice.market_id)
        )
        candidate = _filter_quotes(candidate, event_id=event_id, sources=sources, bookie_ids=bookie_ids)
        with db_manager.get_session() as session:
            return bool(session.execute(select(candidate.exists())).scalar_one())
