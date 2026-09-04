"""Versioned Oddspapi pre-start policy owned by the provider package.

Edit this file when changing product/policy defaults for Oddspapi pre-start
ingestion. Deployment concerns stay in environment-backed Config:

- API keys / base URL / timeouts / endpoint cooldowns
- feature activation (ENABLE_ODDSPAPI_*)
- worker count / max events per run
- bookmaker allowlists chosen per environment
- operational toggles that operators flip without a code deploy
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from typing import Sequence


@dataclass(frozen=True)
class OddspapiPreStartSettings:
    """Complete versioned policy for Oddspapi pre-start odds acquisition."""

    # Dual-endpoint acquisition no longer needs an endpoint env switch; this is
    # retained only as the default label/fallback for legacy callers.
    default_endpoint: str = "odds"

    # Minutes-until-start key moments where both /odds and /historical-odds
    # are fetched so opening prices can be merged from the historical
    # response. Also used as the exchange historical moment list.
    opening_historical_moments: tuple[int, ...] = (5,)

    # Historical opening quotes must span at least this many minutes to count.
    initial_odds_min_span_minutes: float = 60.0

    significant_change_min_magnitude_pct: float = 15.0
    significant_change_min_history_hours: float = 20.0
    significant_change_flash_reversal_minutes: float = 3.0
    significant_change_min_price: float = 1.01

    # Exchange historical request planning.
    exchange_market_keys: tuple[str, ...] = (
        "1x2_full_time",
        "over_under_full_time",
        "asian_handicap_full_time",
        "home_away_full_time",
        "home_away_full_time_including_overtime",
        "over_under_full_time_including_overtime",
        "asian_handicap_full_time_including_overtime",
        "handicap_full_time_including_overtime",
        "handicap_first_to_fifth_inning",
        "home_away_first_to_fifth_inning",
        "1x2_first_to_fifth_inning",
    )

    exchange_main_line_only: bool = True
    exchange_include_player_props: bool = False
    exchange_max_outcomes_per_event: int = 8
    exchange_max_requests_per_run: int = 40

    # Persistence/normalization policy.
    # When False, player active=false ticks and marketActive=false markets are
    # still eligible (adapter, mainline cache extract, historical reader,
    # exchange selector). Driven by Config.ODDSPAPI_PRE_START_REQUIRE_ACTIVE_QUOTES.
    require_active_quotes: bool = True
    persist_main_line_only: bool = True

    # Donor order when a bookmaker has no cached mainLine outcome ids of its
    # own. Historical persist uses the first donor that has rows. Own-cache
    # always wins. Empty after this walk skips that bookmaker.
    mainline_cache_fallback_bookmakers: tuple[str, ...] = (
        "pinnacle",
        "bet365",
        "betfair-ex",
    )

    # Operational toggles for reconstructing key-moment prices from live
    # /historical-odds (ENABLE_ODDSPAPI_HISTORICAL_AS_OF_*) and restricting
    # paid /odds to T-1 (ODDSPAPI_PRE_START_CLOSING_ONLY) live in Config.
    # TEMPORARY: ODDSPAPI_PRE_START_ALLOWED_MOMENTS also lives in Config and
    # currently limits Oddspapi pre-start fetches to T-5.

    # Optional allowlists; empty means "no extra filter".
    allowed_market_keys: tuple[str, ...] = ()
    allowed_market_groups: tuple[str, ...] = ()
    allowed_market_periods: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in (
            "significant_change_min_magnitude_pct",
            "significant_change_min_history_hours",
            "significant_change_flash_reversal_minutes",
            "significant_change_min_price",
        ):
            value = getattr(self, name)
            if not isfinite(value):
                raise ValueError(f"{name} must be finite")
        if self.significant_change_min_magnitude_pct <= 0:
            raise ValueError("significant_change_min_magnitude_pct must be positive")
        if self.significant_change_min_history_hours < 0:
            raise ValueError("significant_change_min_history_hours must be non-negative")
        if self.significant_change_flash_reversal_minutes < 0:
            raise ValueError("significant_change_flash_reversal_minutes must be non-negative")
        if self.significant_change_min_price < 1.01:
            raise ValueError("significant_change_min_price must be at least 1.01")
        if self.initial_odds_min_span_minutes < 0:
            raise ValueError("initial_odds_min_span_minutes must be non-negative")
        if self.exchange_max_outcomes_per_event < 0:
            raise ValueError("exchange_max_outcomes_per_event must be non-negative")
        if self.exchange_max_requests_per_run < 0:
            raise ValueError("exchange_max_requests_per_run must be non-negative")
        if not self.opening_historical_moments:
            raise ValueError("opening_historical_moments cannot be empty")

    def resolved_opening_historical_moments(
        self,
        moments: Sequence[int] | None = None,
    ) -> list[int]:
        """Return an explicit override, otherwise the versioned setting."""
        source = moments or self.opening_historical_moments
        return [int(moment) for moment in source if moment is not None]

    def as_list(self, values: Sequence[str] | None) -> list[str] | None:
        cleaned = [str(value).strip() for value in (values or ()) if str(value).strip()]
        return cleaned or None


ODDSPAPI_PRE_START_SETTINGS = OddspapiPreStartSettings()
