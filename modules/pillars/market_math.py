"""Pure numerical primitives shared by market engines."""

from decimal import Decimal


def side_edge(home_price: Decimal, away_price: Decimal) -> Decimal:
    home, away = Decimal(1) / home_price, Decimal(1) / away_price
    return (home - away) / (home + away)


def ou_edge(over_price: Decimal, under_price: Decimal) -> Decimal:
    return side_edge(over_price, under_price)


def absolute_gap(left: Decimal, right: Decimal) -> Decimal:
    return abs(left - right)


def pair_mean(left: Decimal, right: Decimal) -> Decimal:
    return (left + right) / Decimal(2)


def relative_spread(back_price: Decimal, lay_price: Decimal) -> Decimal:
    return (lay_price - back_price) / pair_mean(lay_price, back_price)
