"""Pure mathematical primitives for Pillar 3 Over/Under readings."""

from decimal import Decimal


ONE = Decimal("1")
TWO = Decimal("2")


def ou_edge(over_price: Decimal, under_price: Decimal) -> Decimal:
    over_raw = ONE / over_price
    under_raw = ONE / under_price
    return (over_raw - under_raw) / (over_raw + under_raw)


def absolute_gap(left: Decimal, right: Decimal) -> Decimal:
    return abs(left - right)


def pair_mean(left: Decimal, right: Decimal) -> Decimal:
    return (left + right) / TWO


__all__ = ["absolute_gap", "ou_edge", "pair_mean"]
