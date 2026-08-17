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
from typing import Sequence


@dataclass(frozen=True)
class OddspapiPreStartSettings:
    """Complete versioned policy for Oddspapi pre-start odds acquisition."""

    # Dual-endpoint acquisition no longer needs an endpoint env switch; this is
    # retained only as the default label/fallback for legacy callers.
    default_endpoint: str = "odds"

    # Regular-bookmaker opening enrichment uses historical quotes only when the
    # key moment is at/above this opening mark (also used as exchange moment).
    opening_historical_moments: tuple[int, ...] = (120,)

    # Historical opening quotes must span at least this many minutes to count.
    initial_odds_min_span_minutes: float = 60.0

    # Exchange historical request planning.
    exchange_market_keys: tuple[str, ...] = (
        "1x2_full_time",
        "over_under_full_time",
        "asian_handicap_full_time",
        "home_away_full_time",
        "home_away_full_time_including_overtime",
        "over_under_full_time_including_overtime",
        "asian_handicap_full_time_including_overtime",
    )
    exchange_main_line_only: bool = True
    exchange_include_player_props: bool = False
    exchange_max_outcomes_per_event: int = 8
    exchange_max_requests_per_run: int = 40

    # Persistence/normalization policy.
    # When False, quotes with active=false are still eligible for current and
    # historical opening/current selection (needed when a bookmaker marks
    # suspended/stale lines inactive but still publishes prices). Driven by
    # Config.ODDSPAPI_PRE_START_REQUIRE_ACTIVE_QUOTES at runtime.
    require_active_quotes: bool = True
    persist_main_line_only: bool = False

    # Operational toggles for reconstructing key-moment prices from live
    # /historical-odds (ENABLE_ODDSPAPI_HISTORICAL_AS_OF_*) and restricting
    # paid /odds to T-1 (ODDSPAPI_PRE_START_CLOSING_ONLY) live in Config.

    # Optional allowlists; empty means "no extra filter".
    allowed_market_keys: tuple[str, ...] = ()
    allowed_market_groups: tuple[str, ...] = ()
    allowed_market_periods: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.initial_odds_min_span_minutes < 0:
            raise ValueError("initial_odds_min_span_minutes must be non-negative")
        if self.exchange_max_outcomes_per_event < 0:
            raise ValueError("exchange_max_outcomes_per_event must be non-negative")
        if self.exchange_max_requests_per_run < 0:
            raise ValueError("exchange_max_requests_per_run must be non-negative")
        if not self.opening_historical_moments:
            raise ValueError("opening_historical_moments cannot be empty")

    def as_list(self, values: Sequence[str] | None) -> list[str] | None:
        cleaned = [str(value).strip() for value in (values or ()) if str(value).strip()]
        return cleaned or None


ODDSPAPI_PRE_START_SETTINGS = OddspapiPreStartSettings()
