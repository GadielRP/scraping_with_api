"""Versioned, package-local policy for the OddsPortal scraper.

Edit this file when changing what or how OddsPortal is scraped. Deployment
concerns such as worker activation, proxy credentials, browser concurrency,
and cross-job coordination timeouts intentionally remain in environment-backed
application settings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Tuple


@dataclass(frozen=True)
class OddsPortalBookmakerPolicy:
    """Bookmaker persistence and tooltip-hover policy per route step."""

    # This is also the persistence allowlist. Unlisted regular bookmaker odds
    # are not extracted from the page or materialized in Python.
    hover_names: Tuple[str, ...] = (
        "bet365",
    )
    # Ordered selection cap; zero disables regular extraction and persistence.
    hover_limit: int = 1

    persist_betfair: bool = True
    hover_betfair: bool = True

    def __post_init__(self) -> None:
        if self.hover_limit < 0:
            raise ValueError("hover_limit must be non-negative")


@dataclass(frozen=True)
class OddsPortalBrowserPolicy:
    """Provider-specific Playwright behavior and timeout policy."""

    block_resources: bool = True
    block_service_workers: bool = True
    clear_state_before_navigation: bool = True
    ignore_https_errors: bool = True
    fresh_context_per_event: bool = True

    match_goto_timeout_ms: int = 30_000
    fast_fail_empty_timeout_ms: int = 15_000
    market_render_timeout_ms: int = 60_000
    shell_grace_timeout_ms: int = 8_000
    tab_wait_timeout_s: int = 20
    league_goto_timeout_ms: int = 21_000
    league_rows_timeout_ms: int = 18_000
    session_restart_attempts: int = 2

    save_debug_on_goto_timeout: bool = True
    enable_shell_grace: bool = True
    debug_timing: bool = False
    debug_dir: str = "logs/debug/oddsportal"

    def __post_init__(self) -> None:
        numeric_timeouts = (
            self.match_goto_timeout_ms,
            self.fast_fail_empty_timeout_ms,
            self.market_render_timeout_ms,
            self.shell_grace_timeout_ms,
            self.tab_wait_timeout_s,
            self.league_goto_timeout_ms,
            self.league_rows_timeout_ms,
            self.session_restart_attempts,
        )
        if any(timeout <= 0 for timeout in numeric_timeouts):
            raise ValueError("OddsPortal browser timeouts must be positive")
        if not self.debug_dir.strip():
            raise ValueError("debug_dir cannot be empty")


@dataclass(frozen=True)
class OddsPortalScrapingSettings:
    """Complete versioned policy owned by the OddsPortal package."""

    ui_language: str = "en"
    bookmakers: OddsPortalBookmakerPolicy = field(
        default_factory=OddsPortalBookmakerPolicy
    )
    browser: OddsPortalBrowserPolicy = field(
        default_factory=OddsPortalBrowserPolicy
    )

    def __post_init__(self) -> None:
        if self.ui_language not in {"en", "es"}:
            raise ValueError("ui_language must be 'en' or 'es'")

    @property
    def domain(self) -> str:
        return "oddsportal.com" if self.ui_language == "en" else "cuotasahora.com"


ODDSPORTAL_SCRAPING_SETTINGS = OddsPortalScrapingSettings()
