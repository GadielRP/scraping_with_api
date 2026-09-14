"""Checkpoint-only book/exchange relation changes for P4."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Iterable


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
            "GAP_CHANGE_RAW": float(final_gap - initial_gap),
            "INITIAL_RELATION": _relation(initial_book, initial_exchange),
            "FINAL_RELATION": _relation(final_book, final_exchange),
            "STATE_CHANGED_RAW": (
                _relation(initial_book, initial_exchange)
                != _relation(final_book, final_exchange)
            ),
        }
        for series in related:
            result[series["SERIES_ID"]] = relation
    return result


__all__ = ["build_book_exchange_relation_changes"]
