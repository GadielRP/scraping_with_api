"""Pure selection of prices and quote instruments from persisted rows."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping, Sequence

from .market_odds_read_models import (
    ChoiceOddsState,
    MarketOddsReadDiagnostic,
    MarketOddsReadResult,
    MarketOddsState,
    OddsPrice,
    QuotePriceOrigin,
)
from .market_quote_read_policy import QuoteFieldPriority, QuoteReadPriorityPolicy, order_available_sources

QuoteRow = Mapping[str, Any]
_CHOICE_ORDER = {name: index for index, name in enumerate(
    ("1", "1x", "x", "x2", "2", "12", "over", "under", "yes", "no")
)}


def _price(row: QuoteRow | None, field: str) -> OddsPrice | None:
    if row is None or row[field] is None:
        return None
    return OddsPrice(
        value=row[field],
        origin=QuotePriceOrigin(
            quote_id=int(row["quote_id"]),
            source=row["source"],
            exchange_side=row["exchange_side"],
            exchange_level=int(row["exchange_level"]),
            main_line=row.get("main_line"),
            source_market_id=row.get("source_market_id"),
            source_outcome_id=row.get("source_outcome_id"),
            bookmaker_outcome_id=row.get("bookmaker_outcome_id"),
            source_limit=row.get("source_limit"),
            initial_captured_at=row["initial_captured_at"] if field == "initial" else None,
            current_updated_at=row["current_updated_at"] if field == "current" else None,
        ),
    )


def _valid_quote_rows(
    rows: Sequence[QuoteRow], diagnostics: list[MarketOddsReadDiagnostic],
) -> list[QuoteRow]:
    by_identity = defaultdict(list)
    for row in rows:
        identity = (row["choice_id"], row["source"], row["exchange_side"], row["exchange_level"])
        by_identity[identity].append(row)
    valid = []
    for identity, candidates in by_identity.items():
        if len(candidates) == 1:
            valid.append(candidates[0])
        else:
            diagnostics.append(MarketOddsReadDiagnostic(
                code="unexpected_duplicate", blocking=True,
                market_id=int(candidates[0]["market_id"]), choice_id=int(identity[0]),
                quote_ids=tuple(sorted(int(row["quote_id"]) for row in candidates)),
            ))
    return valid


def _select_instruments(
    rows: Sequence[QuoteRow], is_exchange: bool, diagnostics: list[MarketOddsReadDiagnostic],
) -> list[QuoteRow]:
    """Suppress redundant unsided quotes and select the shallowest exchange level."""
    by_choice_source = defaultdict(list)
    for row in rows:
        by_choice_source[(row["choice_id"], row["source"])].append(row)
    selected = []
    for (choice_id, _source), candidates in by_choice_source.items():
        explicit = [row for row in candidates if row["exchange_side"] in {"back", "lay"}]
        if explicit:
            suppressed = [row for row in candidates if row["exchange_side"] is None]
            if suppressed:
                diagnostics.append(MarketOddsReadDiagnostic(
                    code="redundant_unsided_quote_suppressed", blocking=False,
                    market_id=int(candidates[0]["market_id"]), choice_id=int(choice_id),
                    quote_ids=tuple(sorted(int(row["quote_id"]) for row in suppressed)),
                ))
            candidates = explicit
        by_side = defaultdict(list)
        for row in candidates:
            by_side[row["exchange_side"]].append(row)
        for side, side_rows in by_side.items():
            if not is_exchange and side is None and any(row["exchange_level"] != 0 for row in side_rows):
                diagnostics.append(MarketOddsReadDiagnostic(
                    code="unexpected_level", blocking=True,
                    market_id=int(candidates[0]["market_id"]), choice_id=int(choice_id),
                    quote_ids=tuple(sorted(int(row["quote_id"]) for row in side_rows)),
                ))
                continue
            selected.append(min(side_rows, key=lambda row: (row["exchange_level"], row["quote_id"])))
    return selected


def _resolve_conventional_choices(
    rows: Sequence[QuoteRow], priority: QuoteFieldPriority, diagnostics: list[MarketOddsReadDiagnostic],
) -> list[ChoiceOddsState]:
    by_choice = defaultdict(list)
    for row in rows:
        by_choice[row["choice_id"]].append(row)
    choices = []
    for choice_id, candidates in by_choice.items():
        by_source = {row["source"]: row for row in candidates}
        opening_order, opening_unknown = order_available_sources(by_source, priority.initial)
        current_order, current_unknown = order_available_sources(by_source, priority.current)
        unknown = tuple(sorted(set(opening_unknown) | set(current_unknown)))
        if unknown:
            diagnostics.append(MarketOddsReadDiagnostic(
                code="unconfigured_source_fallback", blocking=False,
                market_id=int(candidates[0]["market_id"]), choice_id=int(choice_id),
                quote_ids=tuple(sorted(int(row["quote_id"]) for row in candidates if row["source"] in unknown)),
                detail=",".join(unknown),
            ))
        opening_row = next((by_source[s] for s in opening_order if by_source[s]["initial"] is not None), None)
        current_row = next((by_source[s] for s in current_order if by_source[s]["current"] is not None), None)
        choices.append(ChoiceOddsState(
            choice_id=int(choice_id), choice_name=candidates[0]["choice_name"],
            opening=_price(opening_row, "initial"), current=_price(current_row, "current"),
        ))
    return choices


def _market_state(
    event_id: int, metadata: QuoteRow, choices: Sequence[ChoiceOddsState],
    *, source: str | None = None, side: str | None = None,
) -> MarketOddsState:
    return MarketOddsState(
        event_id=int(event_id), market_id=int(metadata["market_id"]),
        bookie_id=int(metadata["bookie_id"]), bookie_name=metadata["bookie_name"],
        market_type_id=metadata.get("market_type_id"),
        canonical_market_key=metadata.get("canonical_market_key"),
        market_name=metadata["market_name"], market_group=metadata["market_group"],
        market_period=metadata["market_period"], market_family=metadata.get("market_family"),
        line_value=metadata["line_value"], is_live=bool(metadata["is_live"]),
        source=source, exchange_side=side,
        choices=tuple(sorted(choices, key=lambda choice: (
            _CHOICE_ORDER.get(choice.choice_name.casefold(), 999),
            choice.choice_name.casefold(), choice.choice_id,
        ))),
    )


def _resolve_exchange_states(
    event_id: int, metadata: QuoteRow, rows: Sequence[QuoteRow],
    diagnostics: list[MarketOddsReadDiagnostic],
) -> list[MarketOddsState]:
    by_source_side = defaultdict(list)
    for row in rows:
        by_source_side[(row["source"], row["exchange_side"])].append(row)
    states = []
    for (source, side), candidates in by_source_side.items():
        if side is None:
            diagnostics.append(MarketOddsReadDiagnostic(
                code="unsided_quote_in_exchange_market", blocking=False,
                market_id=int(metadata["market_id"]),
                quote_ids=tuple(sorted(int(row["quote_id"]) for row in candidates)),
            ))
        choices = [ChoiceOddsState(
            choice_id=int(row["choice_id"]), choice_name=row["choice_name"],
            opening=_price(row, "initial"), current=_price(row, "current"),
        ) for row in candidates]
        states.append(_market_state(event_id, metadata, choices, source=source, side=side))
    return states


def resolve_market_odds_state(
    event_id: int, rows: Sequence[QuoteRow], priority_policy: QuoteReadPriorityPolicy,
) -> MarketOddsReadResult:
    """Resolve prices within each signed market identity, preserving provenance.

    Conventional fields follow priority independently. Exchange quotes retain
    their provider and side; depth is selected per choice and preserved in origin.
    """
    diagnostics: list[MarketOddsReadDiagnostic] = []
    by_market = defaultdict(list)
    for row in _valid_quote_rows(rows, diagnostics):
        by_market[row["market_id"]].append(row)
    markets = []
    for rows_for_market in by_market.values():
        metadata = rows_for_market[0]
        is_exchange = any(row["exchange_side"] in {"back", "lay"} for row in rows_for_market)
        selected = _select_instruments(rows_for_market, is_exchange, diagnostics)
        if is_exchange:
            markets.extend(_resolve_exchange_states(event_id, metadata, selected, diagnostics))
        else:
            priority = priority_policy.resolve(sport=metadata["sport"], bookie_id=int(metadata["bookie_id"]))
            choices = _resolve_conventional_choices(selected, priority, diagnostics)
            if choices:
                markets.append(_market_state(event_id, metadata, choices))
    markets.sort(key=lambda market: (
        market.market_group or "", market.market_period, market.market_name,
        market.bookie_name.casefold(), market.line_value is not None, market.line_value or 0,
        market.source or "", market.exchange_side or "", market.market_id,
    ))
    return MarketOddsReadResult(int(event_id), tuple(markets), tuple(diagnostics))
