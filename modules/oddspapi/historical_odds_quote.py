"""Transport-neutral historical observation shared by both odds reducers."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class HistoricalOddsAsOfQuote:
    """A fixed-moment reconstruction or an observation at its actual tick time."""

    bookmaker_slug: str
    source_market_id: str
    source_outcome_id: str
    player_id: str
    minutes_until_start: int | float
    price: float
    created_at: str
    collected_at: datetime
    limit: Any = None
    active: Any = None
