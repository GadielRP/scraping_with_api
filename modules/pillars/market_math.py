"""Pure numerical primitives shared by market engines."""

from decimal import Decimal
from .instrumentation import record_calculation


def side_edge(home_price: Decimal, away_price: Decimal) -> Decimal:
    home, away = Decimal(1) / home_price, Decimal(1) / away_price
    value = (home - away) / (home + away)
    record_calculation("side_edge", home_price=home_price, away_price=away_price,
                       inverse_home=home, inverse_away=away,
                       numerator=home-away, denominator=home+away, value=value)
    return value


def ou_edge(over_price: Decimal, under_price: Decimal) -> Decimal:
    return side_edge(over_price, under_price)


def absolute_gap(left: Decimal, right: Decimal) -> Decimal:
    value = abs(left - right)
    record_calculation("absolute_gap", left=left, right=right, value=value)
    return value


def pair_mean(left: Decimal, right: Decimal) -> Decimal:
    value = (left + right) / Decimal(2)
    record_calculation("pair_mean", left=left, right=right, value=value)
    return value


def relative_spread(back_price: Decimal, lay_price: Decimal) -> Decimal:
    denominator = pair_mean(lay_price, back_price)
    value = (lay_price - back_price) / denominator
    record_calculation("relative_spread", back_price=back_price, lay_price=lay_price,
                       numerator=lay_price-back_price, denominator=denominator, value=value)
    return value
