from dataclasses import dataclass, field
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class CacheQualityMetrics:
    total_count: int
    fresh_count: int
    stale_count: int
    freshness_ratio: float
    homogeneity: float
    score: float

    @property
    def comparison_key(self) -> Tuple[int, float, float, int]:
        return (
            self.fresh_count,
            self.freshness_ratio,
            self.homogeneity,
            self.total_count,
        )


@dataclass
class BookieOdds:
    """Odds from a single bookmaker."""

    name: str
    odds_1: str = "-"
    odds_x: str = "-"
    odds_2: str = "-"
    payout: str = "-"
    initial_odds_1: Optional[str] = None
    initial_odds_x: Optional[str] = None
    initial_odds_2: Optional[str] = None
    movement_odds_time: Optional[str] = None
    handicap: Optional[str] = None


@dataclass
class BetfairExchangeOdds:
    """Betfair exchange Back/Lay odds."""

    back_1: str = "-"
    back_x: str = "-"
    back_2: str = "-"
    back_1_vol: str = ""
    back_x_vol: str = ""
    back_2_vol: str = ""
    lay_1: str = "-"
    lay_x: str = "-"
    lay_2: str = "-"
    lay_1_vol: str = ""
    lay_x_vol: str = ""
    lay_2_vol: str = ""
    payout: str = "-"
    initial_back_1: Optional[str] = None
    initial_back_x: Optional[str] = None
    initial_back_2: Optional[str] = None
    initial_lay_1: Optional[str] = None
    initial_lay_x: Optional[str] = None
    initial_lay_2: Optional[str] = None
    movement_odds_time: Optional[str] = None
    handicap: Optional[str] = None


@dataclass
class MarketExtraction:
    """Odds extracted for a specific market_group + market_period combination."""

    market_group: str = ""
    market_period: str = ""
    market_name: str = ""
    bookie_odds: List[BookieOdds] = field(default_factory=list)
    betfair: Optional[BetfairExchangeOdds] = None


@dataclass
class MatchOddsData:
    """Complete structured odds for a match across all scraped periods."""

    match_url: str = ""
    home_team: str = "Unknown"
    away_team: str = "Unknown"
    sport: str = ""
    extractions: List[MarketExtraction] = field(default_factory=list)
    extraction_time_ms: float = 0
    bookie_odds: List[BookieOdds] = field(default_factory=list)
    betfair: Optional[BetfairExchangeOdds] = None


@dataclass
class ScrapeAttemptResult:
    """Detailed result for one scraping attempt, including resume metadata."""

    data: Optional[MatchOddsData]
    resume_state: Optional[dict]
    partial_match_data: Optional[MatchOddsData]
    failed_reason: Optional[str] = None
    failed_step_idx: Optional[int] = None


@dataclass
class GroupSeedResult:
    cache_warmed: bool
    candidate_count: int
    season_id: Optional[int] = None
    league_url: Optional[str] = None
    error: Optional[str] = None

