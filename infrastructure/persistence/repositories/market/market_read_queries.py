"""Set-based quote-aware market read queries and pure row projection."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Mapping, Optional, Sequence

from sqlalchemy import or_, select

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoice,
    MarketChoiceQuote,
)
from infrastructure.persistence.repositories.market.market_quote_read_policy import (
    QuoteReadPriorityPolicy,
    order_available_sources,
)
from infrastructure.persistence.repositories.market.market_read_models import (
    ExternalChoiceQuote,
    ExternalMarketQuoteBlock,
    ExternalMarketQuoteReadResult,
    MarketQuoteReadDiagnostic,
    QuoteFieldOrigin,
)


_CHOICE_ORDER = {
    "1": 1,
    "1x": 2,
    "x": 3,
    "x2": 4,
    "2": 5,
    "12": 6,
    "over": 7,
    "under": 8,
    "yes": 9,
    "no": 10,
}
_SIDE_ORDER = {None: 0, "back": 1, "lay": 2}


def _movement(initial: Optional[Decimal], current: Optional[Decimal]) -> Optional[int]:
    if initial is None or current is None:
        return None
    if current > initial:
        return 1
    if current < initial:
        return -1
    return 0


def _choice_sort_key(choice: ExternalChoiceQuote) -> tuple:
    name = choice.choice_name or ""
    return (_CHOICE_ORDER.get(name.casefold(), 999), name.casefold(), choice.choice_id)


def _block_sort_key(block: ExternalMarketQuoteBlock) -> tuple:
    return (
        0 if block.aggregation == "field_priority" else 1,
        block.source or "",
        block.market_group or "",
        block.market_period or "",
        block.market_name or "",
        block.choice_group is not None,
        block.choice_group or "",
        block.bookie_name.casefold(),
        _SIDE_ORDER.get(block.exchange_side, 99),
        block.market_id,
    )


def project_external_market_quote_rows(
    event_id: int,
    rows: Sequence[Mapping],
    priority_policy: QuoteReadPriorityPolicy,
) -> ExternalMarketQuoteReadResult:
    """Project flat quote rows into deterministic presentation blocks."""
    diagnostics: list[MarketQuoteReadDiagnostic] = []
    duplicates: dict[tuple, list[Mapping]] = defaultdict(list)
    for row in rows:
        duplicates[
            (
                row["choice_id"],
                row["source"],
                row["exchange_side"],
                row["exchange_level"],
            )
        ].append(row)
    invalid_quote_ids: set[int] = set()
    for identity, duplicate_rows in duplicates.items():
        if len(duplicate_rows) <= 1:
            continue
        quote_ids = tuple(sorted(int(row["quote_id"]) for row in duplicate_rows))
        invalid_quote_ids.update(quote_ids)
        diagnostics.append(
            MarketQuoteReadDiagnostic(
                code="unexpected_duplicate",
                blocking=True,
                market_id=int(duplicate_rows[0]["market_id"]),
                choice_id=int(identity[0]),
                quote_ids=quote_ids,
            )
        )

    by_market: dict[int, list[Mapping]] = defaultdict(list)
    for row in rows:
        if int(row["quote_id"]) not in invalid_quote_ids:
            by_market[int(row["market_id"])].append(row)

    blocks: list[ExternalMarketQuoteBlock] = []
    for market_id, market_rows in by_market.items():
        if not market_rows:
            continue
        metadata = market_rows[0]
        is_exchange = any(row["exchange_side"] in {"back", "lay"} for row in market_rows)

        selected_rows: list[Mapping] = []
        by_choice_source: dict[tuple[int, str], list[Mapping]] = defaultdict(list)
        for row in market_rows:
            by_choice_source[(int(row["choice_id"]), row["source"])].append(row)
        for (choice_id, source), source_rows in by_choice_source.items():
            explicit = [row for row in source_rows if row["exchange_side"] in {"back", "lay"}]
            candidates = source_rows
            if explicit:
                suppressed = [row for row in source_rows if row["exchange_side"] is None]
                if suppressed:
                    diagnostics.append(
                        MarketQuoteReadDiagnostic(
                            code="redundant_unsided_quote_suppressed",
                            blocking=False,
                            market_id=market_id,
                            choice_id=choice_id,
                            quote_ids=tuple(sorted(int(row["quote_id"]) for row in suppressed)),
                        )
                    )
                candidates = explicit

            by_side: dict[Optional[str], list[Mapping]] = defaultdict(list)
            for row in candidates:
                by_side[row["exchange_side"]].append(row)
            for side, side_rows in by_side.items():
                side_rows.sort(key=lambda item: (int(item["exchange_level"]), int(item["quote_id"])))
                if not is_exchange and side is None and any(int(item["exchange_level"]) != 0 for item in side_rows):
                    diagnostics.append(
                        MarketQuoteReadDiagnostic(
                            code="unexpected_level",
                            blocking=True,
                            market_id=market_id,
                            choice_id=choice_id,
                            quote_ids=tuple(int(item["quote_id"]) for item in side_rows),
                        )
                    )
                    continue
                selected_rows.append(side_rows[0])

        if not is_exchange:
            priority = priority_policy.resolve(
                sport=metadata["sport"], bookie_id=int(metadata["bookie_id"])
            )
            by_choice: dict[int, list[Mapping]] = defaultdict(list)
            for row in selected_rows:
                by_choice[int(row["choice_id"])].append(row)
            choices: list[ExternalChoiceQuote] = []
            contributing: set[str] = set()
            for choice_id, choice_rows in by_choice.items():
                available_sources = {row["source"] for row in choice_rows}
                initial_order, initial_unknown = order_available_sources(
                    available_sources, priority.initial
                )
                current_order, current_unknown = order_available_sources(
                    available_sources, priority.current
                )
                unknown = tuple(sorted(set(initial_unknown) | set(current_unknown)))
                if unknown:
                    diagnostics.append(
                        MarketQuoteReadDiagnostic(
                            code="unconfigured_source_fallback",
                            blocking=False,
                            market_id=market_id,
                            choice_id=choice_id,
                            quote_ids=tuple(sorted(int(row["quote_id"]) for row in choice_rows if row["source"] in unknown)),
                            detail=",".join(unknown),
                        )
                    )
                by_source = {row["source"]: row for row in choice_rows}
                initial_row = next(
                    (by_source[source] for source in initial_order if by_source[source]["initial"] is not None),
                    None,
                )
                current_row = next(
                    (by_source[source] for source in current_order if by_source[source]["current"] is not None),
                    None,
                )
                initial = initial_row["initial"] if initial_row else None
                current = current_row["current"] if current_row else None
                if initial_row:
                    contributing.add(initial_row["source"])
                if current_row:
                    contributing.add(current_row["source"])
                choices.append(
                    ExternalChoiceQuote(
                        choice_id=choice_id,
                        choice_name=choice_rows[0]["choice_name"],
                        exchange_level=None,
                        initial=initial,
                        current=current,
                        movement=_movement(initial, current),
                        initial_origin=(
                            QuoteFieldOrigin(
                                quote_id=int(initial_row["quote_id"]),
                                source=initial_row["source"],
                                captured_at=initial_row["initial_captured_at"],
                            )
                            if initial_row
                            else None
                        ),
                        current_origin=(
                            QuoteFieldOrigin(
                                quote_id=int(current_row["quote_id"]),
                                source=current_row["source"],
                                captured_at=current_row["current_updated_at"],
                            )
                            if current_row
                            else None
                        ),
                    )
                )
            if choices:
                choices.sort(key=_choice_sort_key)
                blocks.append(
                    ExternalMarketQuoteBlock(
                        market_id=market_id,
                        bookie_id=int(metadata["bookie_id"]),
                        bookie_name=metadata["bookie_name"],
                        market_name=metadata["market_name"],
                        market_group=metadata["market_group"],
                        market_period=metadata["market_period"],
                        choice_group=metadata["choice_group"],
                        is_live=bool(metadata["is_live"]),
                        aggregation="field_priority",
                        source=None,
                        exchange_side=None,
                        contributing_sources=tuple(sorted(contributing)),
                        choices=tuple(choices),
                    )
                )
            continue

        exchange_groups: dict[tuple[str, Optional[str]], list[Mapping]] = defaultdict(list)
        for row in selected_rows:
            exchange_groups[(row["source"], row["exchange_side"])].append(row)
        for (source, side), group_rows in exchange_groups.items():
            if side is None:
                diagnostics.append(
                    MarketQuoteReadDiagnostic(
                        code="unsided_quote_in_exchange_market",
                        blocking=False,
                        market_id=market_id,
                        quote_ids=tuple(sorted(int(row["quote_id"]) for row in group_rows)),
                    )
                )
            choices = []
            for row in group_rows:
                initial = row["initial"]
                current = row["current"]
                choices.append(
                    ExternalChoiceQuote(
                        choice_id=int(row["choice_id"]),
                        choice_name=row["choice_name"],
                        exchange_level=int(row["exchange_level"]),
                        initial=initial,
                        current=current,
                        movement=_movement(initial, current),
                        initial_origin=(
                            QuoteFieldOrigin(
                                int(row["quote_id"]), source, row["initial_captured_at"]
                            )
                            if initial is not None
                            else None
                        ),
                        current_origin=(
                            QuoteFieldOrigin(
                                int(row["quote_id"]), source, row["current_updated_at"]
                            )
                            if current is not None
                            else None
                        ),
                    )
                )
            choices.sort(key=_choice_sort_key)
            blocks.append(
                ExternalMarketQuoteBlock(
                    market_id=market_id,
                    bookie_id=int(metadata["bookie_id"]),
                    bookie_name=metadata["bookie_name"],
                    market_name=metadata["market_name"],
                    market_group=metadata["market_group"],
                    market_period=metadata["market_period"],
                    choice_group=metadata["choice_group"],
                    is_live=bool(metadata["is_live"]),
                    aggregation="exchange",
                    source=source,
                    exchange_side=side,
                    contributing_sources=(source,),
                    choices=tuple(choices),
                )
            )

    blocks.sort(key=_block_sort_key)
    return ExternalMarketQuoteReadResult(
        event_id=int(event_id), blocks=tuple(blocks), diagnostics=tuple(diagnostics)
    )


class MarketReadQueries:
    @staticmethod
    def get_external_market_quotes_for_event(
        event_id: int,
        priority_policy: QuoteReadPriorityPolicy,
    ) -> ExternalMarketQuoteReadResult:
        query = (
            select(
                Event.id.label("event_id"),
                Event.sport.label("sport"),
                Market.market_id.label("market_id"),
                Market.bookie_id.label("bookie_id"),
                Bookie.name.label("bookie_name"),
                Market.market_name.label("market_name"),
                Market.market_group.label("market_group"),
                Market.market_period.label("market_period"),
                Market.choice_group.label("choice_group"),
                Market.is_live.label("is_live"),
                MarketChoice.choice_id.label("choice_id"),
                MarketChoice.choice_name.label("choice_name"),
                MarketChoiceQuote.quote_id.label("quote_id"),
                MarketChoiceQuote.source.label("source"),
                MarketChoiceQuote.exchange_side.label("exchange_side"),
                MarketChoiceQuote.exchange_level.label("exchange_level"),
                MarketChoiceQuote.initial_odds.label("initial"),
                MarketChoiceQuote.initial_captured_at.label("initial_captured_at"),
                MarketChoiceQuote.current_odds.label("current"),
                MarketChoiceQuote.current_updated_at.label("current_updated_at"),
            )
            .join(Market, Market.event_id == Event.id)
            .join(Bookie, Bookie.bookie_id == Market.bookie_id)
            .join(MarketChoice, MarketChoice.market_id == Market.market_id)
            .join(MarketChoiceQuote, MarketChoiceQuote.choice_id == MarketChoice.choice_id)
            .where(
                Event.id == int(event_id),
                Market.bookie_id != 1,
                or_(
                    MarketChoiceQuote.initial_odds.isnot(None),
                    MarketChoiceQuote.current_odds.isnot(None),
                ),
            )
            .order_by(
                Market.market_id,
                MarketChoice.choice_id,
                MarketChoiceQuote.source,
                MarketChoiceQuote.exchange_side,
                MarketChoiceQuote.exchange_level,
                MarketChoiceQuote.quote_id,
            )
        )
        with db_manager.get_session() as session:
            rows = session.execute(query).mappings().all()
        return project_external_market_quote_rows(event_id, rows, priority_policy)

    @staticmethod
    def has_external_market_quotes_for_event(event_id: int) -> bool:
        candidate = (
            select(MarketChoiceQuote.quote_id)
            .join(MarketChoice, MarketChoice.choice_id == MarketChoiceQuote.choice_id)
            .join(Market, Market.market_id == MarketChoice.market_id)
            .where(
                Market.event_id == int(event_id),
                Market.bookie_id != 1,
                or_(
                    MarketChoiceQuote.initial_odds.isnot(None),
                    MarketChoiceQuote.current_odds.isnot(None),
                ),
            )
        )
        query = select(candidate.exists())
        with db_manager.get_session() as session:
            return bool(session.execute(query).scalar_one())


__all__ = ["MarketReadQueries", "project_external_market_quote_rows"]
