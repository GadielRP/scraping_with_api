"""Pure mathematical primitives for market readings."""

from decimal import Decimal


ONE = Decimal("1")
TWO = Decimal("2")


def side_edge(home_price: Decimal, away_price: Decimal) -> Decimal:
    home_raw = ONE / home_price
    away_raw = ONE / away_price
    return (home_raw - away_raw) / (home_raw + away_raw)


def absolute_gap(left: Decimal, right: Decimal) -> Decimal:
    return abs(left - right)


def pair_mean(left: Decimal, right: Decimal) -> Decimal:
    return (left + right) / TWO


def relative_spread(back_price: Decimal, lay_price: Decimal) -> Decimal:
    return (lay_price - back_price) / pair_mean(lay_price, back_price)


__all__ = ["absolute_gap", "pair_mean", "relative_spread", "side_edge"]
