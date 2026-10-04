"""Coherent current moneyline vector used by the memory calculator."""

from dataclasses import dataclass
from modules.pillars.market_snapshot_extractor import QuotePoint


@dataclass(frozen=True, slots=True)
class ThreeWayMarketSnapshot:
    home: QuotePoint
    draw: QuotePoint | None
    away: QuotePoint

    def is_complete(self) -> bool:
        if self.home is None or self.away is None:
            return False
        market_group = self.home.trace.market_group
        if market_group == "1X2":
            return self.draw is not None
        if market_group == "Home/Away":
            return self.draw is None
        return False
