"""Select best back/lay quotes from OddsPapi exchange payloads.

OddsPapi exchange outcomes expose a top-of-book price/limit on the player
row plus an optional ``exchangeMeta`` ladder. For persistence we only keep:

- back: player ``price`` + ``limit``
- lay: best (lowest) ``availableToLay`` price + size
"""

from __future__ import annotations

import math
from typing import Any


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def best_exchange_quotes(
    *,
    back_price: float,
    back_size: Any = None,
    exchange_meta: dict | None = None,
) -> list[dict]:
    """
    Build at most two exchange quotes for the repository snapshot contract.

    Returns a list of dicts with keys: side, level, price, size.
    """
    quotes = [
        {
            "side": "back",
            "level": 0,
            "price": float(back_price),
            "size": _finite_float(back_size),
        }
    ]

    if not isinstance(exchange_meta, dict):
        return quotes

    best_lay_price: float | None = None
    best_lay_size: float | None = None
    available_to_lay = exchange_meta.get("availableToLay")
    if isinstance(available_to_lay, list):
        for quote in available_to_lay:
            if not isinstance(quote, dict):
                continue
            price = _finite_float(quote.get("price"))
            if price is None:
                continue
            # Best lay for the layer is the lowest available lay price.
            if best_lay_price is None or price < best_lay_price:
                best_lay_price = price
                best_lay_size = _finite_float(quote.get("size"))

    if best_lay_price is not None:
        quotes.append(
            {
                "side": "lay",
                "level": 0,
                "price": best_lay_price,
                "size": best_lay_size,
            }
        )

    return quotes
