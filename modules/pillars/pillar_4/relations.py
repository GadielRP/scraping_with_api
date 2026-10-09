"""Checkpoint-only book/exchange relation changes for P4."""

from __future__ import annotations

import logging
from collections import defaultdict
from decimal import Decimal
from typing import Any, Iterable

logger = logging.getLogger(__name__)

def _edge_direction(value: Decimal) -> str:
    return "POSITIVE" if value > 0 else "NEGATIVE" if value < 0 else "ZERO"


def _relation(left: Decimal, right: Decimal) -> str:
    left_direction = _edge_direction(left)
    right_direction = _edge_direction(right)
    if left_direction == right_direction:
        return f"ALIGNED_{left_direction}"
    if "ZERO" in {left_direction, right_direction}:
        return "ONE_SIDE_ZERO"
    return "OPPOSED"


def _points(series: dict[str, Any]) -> dict[int, Decimal]:
    return {
        int(point["TARGET_MINUTE"]): Decimal(str(point["VALUE"]))
        for point in series.get("POINTS", [])
        if point.get("TARGET_MINUTE") is not None and point.get("VALUE") is not None
    }


def build_book_exchange_relation_changes(
    series_payloads: Iterable[dict[str, Any]],
    *,
    debug_mode: bool = False,
) -> dict[str, dict[str, Any]]:
    """Compare canonical representative edge series, never raw unrelated prices."""
    payloads = tuple(series_payloads)
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for series in payloads:
        market = series.get("MARKET") or {}
        if market.get("VIEW") != "CHECKPOINT_VIEW":
            continue
        key = (
            market.get("DOMAIN"),
            market.get("MARKET_ID"),
            market.get("MARKET_GROUP"),
            market.get("MARKET_PERIOD"),
            market.get("MARKET_NAME"),
            market.get("CHOICE_GROUP_KEY"),
        )
        groups[key].append(series)

    result: dict[str, dict[str, Any]] = {}
    for related in groups.values():
        by_type = {
            (series.get("MARKET") or {}).get("VALUE_TYPE"): series
            for series in related
        }
        book = by_type.get("BOOK_REP_EDGE")
        exchange = by_type.get("EXCHANGE_REP_EDGE")
        if book is None or exchange is None:
            continue
        book_points = _points(book)
        exchange_points = _points(exchange)
        common = sorted(set(book_points) & set(exchange_points), reverse=True)
        if len(common) < 2:
            continue
        first, last = common[0], common[-1]
        initial_book = book_points[first]
        initial_exchange = exchange_points[first]
        final_book = book_points[last]
        final_exchange = exchange_points[last]
        initial_gap = abs(initial_book - initial_exchange)
        final_gap = abs(final_book - final_exchange)
        gap_change = final_gap - initial_gap
        initial_relation = _relation(initial_book, initial_exchange)
        final_relation = _relation(final_book, final_exchange)
        if debug_mode:
            label = f"{book['SERIES_ID']}__{exchange['SERIES_ID']}"
            logger.info(
                "P4 FORMULA | %s | common_targets=%s | selected endpoints=%s -> %s",
                label, common, first, last,
            )
            logger.info(
                "P4 FORMULA | %s | initial_gap | formula=abs(book_edge - exchange_edge) | substitution=abs(%s - %s) | result=%s",
                label, initial_book, initial_exchange, initial_gap,
            )
            logger.info(
                "P4 FORMULA | %s | final_gap | formula=abs(book_edge - exchange_edge) | substitution=abs(%s - %s) | result=%s",
                label, final_book, final_exchange, final_gap,
            )
            logger.info(
                "P4 FORMULA | %s | gap_change | formula=final_gap - initial_gap | substitution=%s - %s | result=%s",
                label, final_gap, initial_gap, gap_change,
            )
            logger.info(
                "P4 FORMULA | %s | relation_state | initial=%s final=%s changed=%s",
                label, initial_relation, final_relation,
                initial_relation != final_relation,
            )
        relation = {
            "BOOK_SERIES_ID": book["SERIES_ID"],
            "EXCHANGE_SERIES_ID": exchange["SERIES_ID"],
            "FROM_TARGET_MINUTE": first,
            "TO_TARGET_MINUTE": last,
            "INITIAL_BOOK_EDGE_RAW": float(initial_book),
            "INITIAL_EXCHANGE_EDGE_RAW": float(initial_exchange),
            "FINAL_BOOK_EDGE_RAW": float(final_book),
            "FINAL_EXCHANGE_EDGE_RAW": float(final_exchange),
            "INITIAL_GAP_RAW": float(initial_gap),
            "FINAL_GAP_RAW": float(final_gap),
            "GAP_CHANGE_RAW": float(gap_change),
            "INITIAL_RELATION": initial_relation,
            "FINAL_RELATION": final_relation,
            "STATE_CHANGED_RAW": (
                initial_relation != final_relation
            ),
        }
        for series in related:
            result[series["SERIES_ID"]] = relation
    return result


__all__ = ["build_book_exchange_relation_changes"]
