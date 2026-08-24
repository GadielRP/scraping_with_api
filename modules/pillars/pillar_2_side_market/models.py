"""Typed contracts used by the Pillar 2 extractor and RAW engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class QuoteTrace:
    """Identity and temporal provenance of one selected odds point."""

    target_minute: int
    snapshot_id: int | None
    collected_at: datetime | None
    minutes_before_start: int | None
    market_group: str
    market_period: str
    market_name: str
    choice_group: str | None
    bookie_id: int | None
    bookie_name: str
    source: str | None
    exchange_side: str | None
    exchange_level: int
    choice_name: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "target_minute": self.target_minute,
            "snapshot_id": self.snapshot_id,
            "collected_at": (
                self.collected_at.isoformat() if self.collected_at is not None else None
            ),
            "minutes_before_start": self.minutes_before_start,
            "market_group": self.market_group,
            "market_period": self.market_period,
            "market_name": self.market_name,
            "choice_group": self.choice_group,
            "bookie_id": self.bookie_id,
            "bookie_name": self.bookie_name,
            "source": self.source,
            "exchange_side": self.exchange_side,
            "exchange_level": self.exchange_level,
            "choice_name": self.choice_name,
        }


@dataclass(frozen=True)
class QuotePoint:
    """A price and its optional exchange liquidity at the canonical minute."""

    odds_price: Decimal
    exchange_size: Decimal | None
    trace: QuoteTrace


@dataclass(frozen=True)
class TwoWayMarketSnapshot:
    home: QuotePoint
    away: QuotePoint


@dataclass(frozen=True)
class ThreeWayMarketSnapshot:
    home: QuotePoint
    draw: QuotePoint
    away: QuotePoint


@dataclass(frozen=True)
class AsianHandicapSnapshot(TwoWayMarketSnapshot):
    home_line: Decimal


@dataclass(frozen=True)
class ExchangeSnapshot:
    back: ThreeWayMarketSnapshot
    lay: ThreeWayMarketSnapshot


@dataclass(frozen=True)
class P2MarketSnapshot:
    """Complete, single-minute market snapshot accepted by P2 RAW."""

    target_minute: int
    pinnacle_ft_1x2: TwoWayMarketSnapshot
    bet365_ft_1x2: TwoWayMarketSnapshot
    pinnacle_ft_ah: AsianHandicapSnapshot
    bet365_ft_ah: AsianHandicapSnapshot
    betfair_ft_1x2: ExchangeSnapshot
    pinnacle_1h_1x2: TwoWayMarketSnapshot
    bet365_1h_1x2: TwoWayMarketSnapshot
    pinnacle_1h_ah: AsianHandicapSnapshot
    bet365_1h_ah: AsianHandicapSnapshot

    def input_values(self) -> dict[str, Decimal]:
        """Return every canonical minimum input under its blueprint name."""
        bf_back = self.betfair_ft_1x2.back
        bf_lay = self.betfair_ft_1x2.lay
        values = {
            "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE": self.pinnacle_ft_1x2.home.odds_price,
            "PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE": self.pinnacle_ft_1x2.away.odds_price,
            "B365_HOME_1X2_FULL_TIME_ODDS_PRICE": self.bet365_ft_1x2.home.odds_price,
            "B365_AWAY_1X2_FULL_TIME_ODDS_PRICE": self.bet365_ft_1x2.away.odds_price,
            "PIN_AH_FULL_TIME_LINE": self.pinnacle_ft_ah.home_line,
            "PIN_AH_HOME_FULL_TIME_ODDS_PRICE": self.pinnacle_ft_ah.home.odds_price,
            "PIN_AH_AWAY_FULL_TIME_ODDS_PRICE": self.pinnacle_ft_ah.away.odds_price,
            "B365_AH_FULL_TIME_LINE": self.bet365_ft_ah.home_line,
            "B365_AH_HOME_FULL_TIME_ODDS_PRICE": self.bet365_ft_ah.home.odds_price,
            "B365_AH_AWAY_FULL_TIME_ODDS_PRICE": self.bet365_ft_ah.away.odds_price,
            "BF_HOME_BACK_FULL_TIME_ODDS_PRICE": bf_back.home.odds_price,
            "BF_HOME_LAY_FULL_TIME_ODDS_PRICE": bf_lay.home.odds_price,
            "BF_DRAW_BACK_FULL_TIME_ODDS_PRICE": bf_back.draw.odds_price,
            "BF_DRAW_LAY_FULL_TIME_ODDS_PRICE": bf_lay.draw.odds_price,
            "BF_AWAY_BACK_FULL_TIME_ODDS_PRICE": bf_back.away.odds_price,
            "BF_AWAY_LAY_FULL_TIME_ODDS_PRICE": bf_lay.away.odds_price,
            "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.home.exchange_size,
            "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.home.exchange_size,
            "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.draw.exchange_size,
            "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.draw.exchange_size,
            "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.away.exchange_size,
            "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.away.exchange_size,
            "PIN_HOME_1X2_1H_ODDS_PRICE": self.pinnacle_1h_1x2.home.odds_price,
            "PIN_AWAY_1X2_1H_ODDS_PRICE": self.pinnacle_1h_1x2.away.odds_price,
            "B365_HOME_1X2_1H_ODDS_PRICE": self.bet365_1h_1x2.home.odds_price,
            "B365_AWAY_1X2_1H_ODDS_PRICE": self.bet365_1h_1x2.away.odds_price,
            "PIN_AH_1H_LINE": self.pinnacle_1h_ah.home_line,
            "PIN_AH_1H_HOME_PRICE": self.pinnacle_1h_ah.home.odds_price,
            "PIN_AH_1H_AWAY_PRICE": self.pinnacle_1h_ah.away.odds_price,
            "B365_AH_1H_LINE": self.bet365_1h_ah.home_line,
            "B365_AH_1H_HOME_PRICE": self.bet365_1h_ah.home.odds_price,
            "B365_AH_1H_AWAY_PRICE": self.bet365_1h_ah.away.odds_price,
        }
        # The six exchange sizes have passed the completeness gate and cannot
        # be None. The assertion narrows their type without changing runtime
        # behavior or replacing missing liquidity with an invented value.
        assert all(value is not None for value in values.values())
        return values  # type: ignore[return-value]

    def input_trace(self) -> dict[str, dict[str, Any]]:
        """Return point-level traceability for all price inputs."""
        bf_back = self.betfair_ft_1x2.back
        bf_lay = self.betfair_ft_1x2.lay
        points = {
            "PIN_HOME_1X2_FULL_TIME_ODDS_PRICE": self.pinnacle_ft_1x2.home,
            "PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE": self.pinnacle_ft_1x2.away,
            "B365_HOME_1X2_FULL_TIME_ODDS_PRICE": self.bet365_ft_1x2.home,
            "B365_AWAY_1X2_FULL_TIME_ODDS_PRICE": self.bet365_ft_1x2.away,
            "PIN_AH_FULL_TIME_LINE": self.pinnacle_ft_ah.home,
            "PIN_AH_HOME_FULL_TIME_ODDS_PRICE": self.pinnacle_ft_ah.home,
            "PIN_AH_AWAY_FULL_TIME_ODDS_PRICE": self.pinnacle_ft_ah.away,
            "B365_AH_FULL_TIME_LINE": self.bet365_ft_ah.home,
            "B365_AH_HOME_FULL_TIME_ODDS_PRICE": self.bet365_ft_ah.home,
            "B365_AH_AWAY_FULL_TIME_ODDS_PRICE": self.bet365_ft_ah.away,
            "BF_HOME_BACK_FULL_TIME_ODDS_PRICE": bf_back.home,
            "BF_HOME_LAY_FULL_TIME_ODDS_PRICE": bf_lay.home,
            "BF_DRAW_BACK_FULL_TIME_ODDS_PRICE": bf_back.draw,
            "BF_DRAW_LAY_FULL_TIME_ODDS_PRICE": bf_lay.draw,
            "BF_AWAY_BACK_FULL_TIME_ODDS_PRICE": bf_back.away,
            "BF_AWAY_LAY_FULL_TIME_ODDS_PRICE": bf_lay.away,
            "BF_HOME_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.home,
            "BF_HOME_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.home,
            "BF_DRAW_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.draw,
            "BF_DRAW_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.draw,
            "BF_AWAY_BACK_FULL_TIME_EXCHANGE_SIZE": bf_back.away,
            "BF_AWAY_LAY_FULL_TIME_EXCHANGE_SIZE": bf_lay.away,
            "PIN_HOME_1X2_1H_ODDS_PRICE": self.pinnacle_1h_1x2.home,
            "PIN_AWAY_1X2_1H_ODDS_PRICE": self.pinnacle_1h_1x2.away,
            "B365_HOME_1X2_1H_ODDS_PRICE": self.bet365_1h_1x2.home,
            "B365_AWAY_1X2_1H_ODDS_PRICE": self.bet365_1h_1x2.away,
            "PIN_AH_1H_LINE": self.pinnacle_1h_ah.home,
            "PIN_AH_1H_HOME_PRICE": self.pinnacle_1h_ah.home,
            "PIN_AH_1H_AWAY_PRICE": self.pinnacle_1h_ah.away,
            "B365_AH_1H_LINE": self.bet365_1h_ah.home,
            "B365_AH_1H_HOME_PRICE": self.bet365_1h_ah.home,
            "B365_AH_1H_AWAY_PRICE": self.bet365_1h_ah.away,
        }
        return {name: point.trace.to_dict() for name, point in points.items()}


@dataclass(frozen=True)
class P2ExtractionResult:
    """Outcome of the atomic P2 snapshot completeness gate."""

    snapshot: P2MarketSnapshot | None
    target_minute: int | None
    missing_inputs: tuple[str, ...] = ()
    invalid_inputs: tuple[str, ...] = ()
    ambiguous_inputs: tuple[str, ...] = ()
