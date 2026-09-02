"""Immutable DTOs for the canonical Pillar 2 signal profile."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .relations import Direction, Relation


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


@dataclass(frozen=True, slots=True)
class BookMarketSignal:
    pin_edge: Decimal | None
    pin_direction: Direction | None
    b365_edge: Decimal | None
    b365_direction: Direction | None
    book_relation: Relation | None
    book_gap: Decimal | None
    rep_edge: Decimal | None
    direction: Direction | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "PIN_EDGE": _number(self.pin_edge),
            "PIN_DIRECTION": self.pin_direction,
            "B365_EDGE": _number(self.b365_edge),
            "B365_DIRECTION": self.b365_direction,
            "BOOK_RELATION": self.book_relation,
            "BOOK_GAP": _number(self.book_gap),
            "REP_EDGE": _number(self.rep_edge),
            "DIRECTION": self.direction,
        }


@dataclass(frozen=True, slots=True)
class AsianHandicapSignal:
    pin_line: Decimal | None
    b365_line: Decimal | None
    pin_edge: Decimal | None
    pin_direction: Direction | None
    b365_edge: Decimal | None
    b365_direction: Direction | None
    book_relation: Relation | None
    line_gap: Decimal | None
    price_gap: Decimal | None
    rep_edge: Decimal | None
    direction: Direction | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "PIN_LINE": _number(self.pin_line),
            "B365_LINE": _number(self.b365_line),
            "PIN_EDGE": _number(self.pin_edge),
            "PIN_DIRECTION": self.pin_direction,
            "B365_EDGE": _number(self.b365_edge),
            "B365_DIRECTION": self.b365_direction,
            "BOOK_RELATION": self.book_relation,
            "LINE_GAP": _number(self.line_gap),
            "PRICE_GAP": _number(self.price_gap),
            "REP_EDGE": _number(self.rep_edge),
            "DIRECTION": self.direction,
        }


@dataclass(frozen=True, slots=True)
class CrossMarketSignal:
    relation: Relation | None
    gap: Decimal | None

    def to_dict(self, *, relation_key: str, gap_key: str) -> dict[str, Any]:
        return {relation_key: self.relation, gap_key: _number(self.gap)}


@dataclass(frozen=True, slots=True)
class PeriodSignal:
    one_x_two: BookMarketSignal
    asian_handicap: AsianHandicapSignal
    cross_market: CrossMarketSignal

    def to_dict(self, *, relation_key: str, gap_key: str) -> dict[str, Any]:
        return {
            "1X2": self.one_x_two.to_dict(),
            "AH": self.asian_handicap.to_dict(),
            "CROSS_MARKET": self.cross_market.to_dict(
                relation_key=relation_key,
                gap_key=gap_key,
            ),
        }


@dataclass(frozen=True, slots=True)
class ExchangeSignal:
    back_edge: Decimal
    back_direction: Direction
    lay_edge: Decimal
    lay_direction: Direction
    back_lay_relation: Relation
    exchange_internal_gap: Decimal
    rep_edge: Decimal
    direction: Direction
    home_spread: Decimal
    away_spread: Decimal
    side_spread: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "BACK_EDGE": _number(self.back_edge),
            "BACK_DIRECTION": self.back_direction,
            "LAY_EDGE": _number(self.lay_edge),
            "LAY_DIRECTION": self.lay_direction,
            "BACK_LAY_RELATION": self.back_lay_relation,
            "EXCHANGE_INTERNAL_GAP": _number(self.exchange_internal_gap),
            "REP_EDGE": _number(self.rep_edge),
            "DIRECTION": self.direction,
            "HOME_SPREAD": _number(self.home_spread),
            "AWAY_SPREAD": _number(self.away_spread),
            "SIDE_SPREAD": _number(self.side_spread),
        }


@dataclass(frozen=True, slots=True)
class BookExchangeSignal:
    book_direction: Direction
    exchange_direction: Direction
    relation: Relation
    gap: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "BOOK_DIRECTION": self.book_direction,
            "EXCHANGE_DIRECTION": self.exchange_direction,
            "RELATION": self.relation,
            "GAP": _number(self.gap),
        }


@dataclass(frozen=True, slots=True)
class AsianHandicapExchangeReading:
    home_odds: Decimal | None
    away_odds: Decimal | None
    home_size: Decimal | None
    away_size: Decimal | None
    edge: Decimal | None
    direction: Direction | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "HOME_ODDS": _number(self.home_odds),
            "AWAY_ODDS": _number(self.away_odds),
            "HOME_SIZE": _number(self.home_size),
            "AWAY_SIZE": _number(self.away_size),
            "EDGE": _number(self.edge),
            "DIRECTION": self.direction,
        }


@dataclass(frozen=True, slots=True)
class AsianHandicapExchangeSignal:
    line: Decimal | None
    back: AsianHandicapExchangeReading | None
    lay: AsianHandicapExchangeReading | None
    back_lay_relation: Relation | None
    internal_gap: Decimal | None
    rep_edge: Decimal | None
    direction: Direction | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "LINE": _number(self.line),
            "BACK_HOME_ODDS": None if self.back is None else _number(self.back.home_odds),
            "BACK_AWAY_ODDS": None if self.back is None else _number(self.back.away_odds),
            "BACK_HOME_SIZE": None if self.back is None else _number(self.back.home_size),
            "BACK_AWAY_SIZE": None if self.back is None else _number(self.back.away_size),
            "BACK_EDGE": None if self.back is None else _number(self.back.edge),
            "BACK_DIRECTION": None if self.back is None else self.back.direction,
            "LAY_HOME_ODDS": None if self.lay is None else _number(self.lay.home_odds),
            "LAY_AWAY_ODDS": None if self.lay is None else _number(self.lay.away_odds),
            "LAY_HOME_SIZE": None if self.lay is None else _number(self.lay.home_size),
            "LAY_AWAY_SIZE": None if self.lay is None else _number(self.lay.away_size),
            "LAY_EDGE": None if self.lay is None else _number(self.lay.edge),
            "LAY_DIRECTION": None if self.lay is None else self.lay.direction,
            "BACK_LAY_RELATION": self.back_lay_relation,
            "EXCHANGE_INTERNAL_GAP": _number(self.internal_gap),
            "REP_EDGE": _number(self.rep_edge),
            "DIRECTION": self.direction,
        }


@dataclass(frozen=True, slots=True)
class AsianHandicapBookExchangeSignal:
    line_diff_raw: Decimal | None
    line_gap: Decimal | None
    relation: Relation | None
    gap: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "LINE_DIFF_RAW": _number(self.line_diff_raw),
            "LINE_GAP": _number(self.line_gap),
            "RELATION": self.relation,
            "GAP": _number(self.gap),
        }


@dataclass(frozen=True, slots=True)
class FirstHalfRelationSignal:
    relation: Relation
    gap: Decimal
    ft_cross_market: Relation | None
    first_half_cross_market: Relation | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "FT_1H_1X2_RELATION": self.relation,
            "FT_1H_1X2_GAP": _number(self.gap),
            "FT_1H_STRUCTURE": {
                "FT_CROSS_MARKET": self.ft_cross_market,
                "FIRST_HALF_CROSS_MARKET": self.first_half_cross_market,
            },
        }


@dataclass(frozen=True, slots=True)
class P2SignalProfile:
    full_time: PeriodSignal
    first_half: PeriodSignal | None
    ft_1h: FirstHalfRelationSignal | None
    exchange: ExchangeSignal
    book_exchange: BookExchangeSignal
    exchange_ah: AsianHandicapExchangeSignal | None = None
    book_exchange_ah: AsianHandicapBookExchangeSignal | None = None
    exchange_ah_1h: AsianHandicapExchangeSignal | None = None
    book_exchange_ah_1h: AsianHandicapBookExchangeSignal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "FT": self.full_time.to_dict(
                relation_key="FT_1X2_AH_RELATION",
                gap_key="FT_CROSS_MARKET_GAP",
            ),
            "1H": (
                None
                if self.first_half is None
                else self.first_half.to_dict(
                    relation_key="1H_1X2_AH_RELATION",
                    gap_key="1H_CROSS_MARKET_GAP",
                )
            ),
            "FT_1H": None if self.ft_1h is None else self.ft_1h.to_dict(),
            "EXCHANGE": self.exchange.to_dict(),
            "BOOK_EXCHANGE": self.book_exchange.to_dict(),
            "BETFAIR_FT_AH": None if self.exchange_ah is None else self.exchange_ah.to_dict(),
            "BOOK_EXCHANGE_AH": None if self.book_exchange_ah is None else self.book_exchange_ah.to_dict(),
            "BETFAIR_1H_AH": None if self.exchange_ah_1h is None else self.exchange_ah_1h.to_dict(),
            "BOOK_EXCHANGE_1H_AH": None if self.book_exchange_ah_1h is None else self.book_exchange_ah_1h.to_dict(),
        }


__all__ = [
    "AsianHandicapSignal",
    "AsianHandicapExchangeReading",
    "AsianHandicapExchangeSignal",
    "AsianHandicapBookExchangeSignal",
    "BookExchangeSignal",
    "BookMarketSignal",
    "CrossMarketSignal",
    "ExchangeSignal",
    "FirstHalfRelationSignal",
    "P2SignalProfile",
    "PeriodSignal",
]
