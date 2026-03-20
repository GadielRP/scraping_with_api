import asyncio
import logging
import random
import time
import os
import uuid
import re
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Condition, local
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# Import configuration
try:
    from config import Config
    from oddsportal_config import (
        SEASON_ODDSPORTAL_MAP, BOOKIE_ALIASES, TEAM_ALIASES, PRIORITY_BOOKIES,
        OP_GROUPS, OP_GROUPS_DISPLAY, OP_PERIODS, SPORT_SCRAPING_ROUTES,
        build_op_fragment, build_match_url_with_fragment, flatten_sport_scraping_route,
        INSTITUTIONAL_NOISE
    )
    from team_matcher import TeamMatcher
except ImportError:
    # Fallback/Mock for standalone testing
    class MockConfig:
        PROXY_ENABLED = False
        PROXY_ENDPOINT = None
        PROXY_USERNAME = None
        PROXY_PASSWORD = None
        ODDSPORTAL_MATCH_GOTO_TIMEOUT_MS = 30000
        ODDSPORTAL_FAST_FAIL_EMPTY_TIMEOUT_MS = 15000
        ODDSPORTAL_MARKET_RENDER_TIMEOUT_MS = 60000
        ODDSPORTAL_SHELL_GRACE_TIMEOUT_MS = 8000
        ODDSPORTAL_TAB_WAIT_TIMEOUT = 20
        ODDSPORTAL_SAVE_DEBUG_ON_GOTO_TIMEOUT = True
        ODDSPORTAL_ENABLE_SHELL_GRACE = True
    Config = MockConfig()
    SEASON_ODDSPORTAL_MAP = {}
    BOOKIE_ALIASES = {}
    TEAM_ALIASES = {}
    PRIORITY_BOOKIES = ["bet365", "Pinnacle", "BettingAsia", "Megapari", "1xBet"]
    OP_GROUPS = {"1X2": "1X2", "HOME_AWAY": "home-away"}
    OP_PERIODS = {"FT_INC_OT": 1, "FULL_TIME": 2, "1ST_HALF": 3}
    SPORT_SCRAPING_ROUTES = {}
    def build_op_fragment(group_key: Optional[str], period_key: Optional[str]) -> Optional[str]:
        if not group_key or not period_key:
            return None
        g = OP_GROUPS.get(group_key)
        p = OP_PERIODS.get(period_key)
        if g is None or p is None:
            return None
        return f"#{g};{p}"

    def build_match_url_with_fragment(match_url: str, group_key: Optional[str], period_key: Optional[str]) -> str:
        base_url = (match_url or "").split("#", 1)[0].rstrip("/")
        fragment = build_op_fragment(group_key, period_key)
        if not fragment:
            return base_url
        return f"{base_url}/{fragment}"

    def flatten_sport_scraping_route(sport: Optional[str]) -> List[Dict[str, Any]]:
        return [{
            "step_idx": 0,
            "step_key": "None:FULL_TIME",
            "group_idx": 0,
            "period_idx": 0,
            "group_key": None,
            "group_display": "1X2",
            "db_market_group": "1X2",
            "period_key": "FULL_TIME",
            "period_display": "Full Time",
            "db_market_period": "Full Time",
            "db_market_name": "Full time",
            "extract_fn": "standard",
            "betfair_period_index": 0,
            "betfair_enabled": True,
            "fragment": None,
        }]

logger = logging.getLogger(__name__)
_LOG_CONTEXT = local()

DEBUG_TIMING = os.getenv("DEBUG_TIMING", "false").lower() == "true"
ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS = int(os.getenv("ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS", "21000"))
ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS = int(os.getenv("ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS", "18000"))
ODDSPORTAL_SESSION_RESTART_ATTEMPTS = int(os.getenv("ODDSPORTAL_SESSION_RESTART_ATTEMPTS", "2"))
EN_DASH = "\u2013"
TEAM_SEPARATOR_PATTERN = rf"\s+(?:vs|[{EN_DASH}-])\s+"
LEGACY_CACHE_MATCH_PATTERN = rf"([^\n{EN_DASH}\-]+)[\s\n]+(?:vs|[{EN_DASH}v\-])[\s\n]+([^\n\d]+)"
TEAM_PREFIX_CLEAN_PATTERN = rf"(^.*?\d{{2}}:\d{{2}}\s+|^\w+,\s+\d{{1,2}}\s+\w+\s+[{EN_DASH}-]\s+|^.*?\d{{1,2}}:\d{{2}}\s+)"

def log_timing(msg):
    if DEBUG_TIMING:
        print(f"⏱️ [Timing] {msg}")

def _normalize_league_url(league_url: Optional[str]) -> Optional[str]:
    """Normalize league URLs so grouping and cache keys stay stable."""
    if not league_url:
        return None
    normalized = league_url.strip()
    if not normalized:
        return None
    return normalized.rstrip("/")


def _build_league_group_key(season_id: Optional[int], league_url: Optional[str]) -> Optional[Tuple[int, str]]:
    normalized_league_url = _normalize_league_url(league_url)
    if not season_id or not normalized_league_url:
        return None
    return (season_id, normalized_league_url)


def _build_structured_league_cache(candidates: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    return {
        candidate["href"]: {
            "home": candidate["home"],
            "away": candidate["away"],
            "raw_text": candidate.get("raw_text", ""),
        }
        for candidate in candidates
        if candidate.get("href")
    }


def _format_group_key(group_key: Optional[Tuple[int, str]]) -> str:
    if not group_key:
        return "(non-primable)"
    season_id, league_url = group_key
    return f"(season_id={season_id}, league_url={league_url})"


class _OddsPortalLogPrefixFilter(logging.Filter):
    """Prefix this module's logs with the active worker/priming label."""

    def filter(self, record: logging.LogRecord) -> bool:
        prefix = getattr(_LOG_CONTEXT, "prefix", None)
        if prefix and not getattr(record, "_op_prefix_applied", False):
            record.msg = f"{prefix} {record.msg}"
            record._op_prefix_applied = True
        return True


if not any(isinstance(existing_filter, _OddsPortalLogPrefixFilter) for existing_filter in logger.filters):
    logger.addFilter(_OddsPortalLogPrefixFilter())


@contextmanager
def _log_prefix(prefix: Optional[str]):
    previous_prefix = getattr(_LOG_CONTEXT, "prefix", None)
    _LOG_CONTEXT.prefix = prefix
    try:
        yield
    finally:
        if previous_prefix is None:
            try:
                delattr(_LOG_CONTEXT, "prefix")
            except AttributeError:
                pass
        else:
            _LOG_CONTEXT.prefix = previous_prefix


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------



@dataclass
class BookieOdds:
    """Odds from a single bookmaker."""
    name: str
    odds_1: str = "-"
    odds_x: str = "-"
    odds_2: str = "-"
    payout: str = "-"
    # Opening/initial odds from hover tooltip (None if not extracted)
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
    # Opening/initial back odds from hover tooltip (None if not extracted)
    initial_back_1: Optional[str] = None
    initial_back_x: Optional[str] = None
    initial_back_2: Optional[str] = None
    # Opening/initial lay odds from hover tooltip (None if not extracted)
    initial_lay_1: Optional[str] = None
    initial_lay_x: Optional[str] = None
    initial_lay_2: Optional[str] = None
    movement_odds_time: Optional[str] = None
    handicap: Optional[str] = None

@dataclass
class MarketExtraction:
    """Odds extracted for a specific market_group + market_period combination."""
    market_group: str = ""       # DB value, e.g. "1X2", "Home/Away"
    market_period: str = ""      # DB value, e.g. "Full-time", "1st half"
    market_name: str = ""        # DB value, e.g. "Full time", "1st half"
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
    # Legacy compat: populated from first extraction for backward compatibility
    bookie_odds: List[BookieOdds] = field(default_factory=list)
    betfair: Optional[BetfairExchangeOdds] = None


@dataclass
class ScrapeAttemptResult:
    """Detailed result for one scraping attempt, including resume metadata."""
    data: Optional[MatchOddsData]
    resume_state: Optional[Dict[str, Any]]
    partial_match_data: Optional[MatchOddsData]
    failed_reason: Optional[str] = None
    failed_step_idx: Optional[int] = None


@dataclass
class GroupSeedResult:
    """Result of a resolver-seed task that only warms the league cache."""
    success: bool
    cache_warmed: bool
    candidate_count: int
    season_id: Optional[int] = None
    league_url: Optional[str] = None
    error: Optional[str] = None


class OddsPortalScraper:
    """
    Scraper for OddsPortal match pages.
    Uses Playwright with anti-detection measures.
    """
    
    def __init__(self, headless: bool = True, debug_dir: Optional[str] = None, testing_mode: bool = False):
        self.headless = headless
        self.debug_dir = debug_dir
        self.testing_mode = testing_mode
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.context: Optional[BrowserContext] = None
        # Safe default so retry logging never crashes when proxy is disabled.
        self._session_id = "no-proxy"
        
        # Context lifecycle config flags
        self._ignore_https_errors = getattr(Config, 'ODDSPORTAL_IGNORE_HTTPS_ERRORS', True)
        self._fresh_context_per_event = getattr(Config, 'ODDSPORTAL_FRESH_CONTEXT_PER_EVENT', True)
        
        # Initialize Team Matcher
        self.team_matcher = TeamMatcher(
            team_aliases=TEAM_ALIASES, 
            noise_list=INSTITUTIONAL_NOISE
        )
        
        if self.debug_dir:
            import os
            os.makedirs(self.debug_dir, exist_ok=True)


    async def start(self):
        """Start the browser session."""
        if self.browser:
            return
            
        self.playwright = await async_playwright().start()
        
        launch_args = {
            "headless": self.headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--window-size=1920,1080",
            ],
        }

        if getattr(Config, 'PROXY_ENABLED', False) and getattr(Config, 'PROXY_ENDPOINT', None):
            launch_args["proxy"] = {
                "server": f"http://{Config.PROXY_ENDPOINT}",
            }
            if getattr(Config, 'PROXY_USERNAME', None) and getattr(Config, 'PROXY_PASSWORD', None):
                launch_args["proxy"]["username"] = Config.PROXY_USERNAME
                launch_args["proxy"]["password"] = Config.PROXY_PASSWORD
            # Track session restarts for retry logging
            self._session_id = uuid.uuid4().hex[:8]
            logger.info(f"🛡️ OddsPortalScraper: Launching Playwright with PROXY (session-{self._session_id})")

        self.browser = await self.playwright.chromium.launch(**launch_args)
        
        # Browser is ready — context creation is deferred to _create_fresh_context()
        self.context = None
        
        logger.info(
            f"✅ OddsPortalScraper: Browser started (no context yet — "
            f"fresh_context_per_event={self._fresh_context_per_event}, "
            f"ignore_https_errors={self._ignore_https_errors})"
        )

    async def _intercept_route(self, route):
        """Intercept network requests to block heavy assets and known trackers to save proxy bandwidth."""
        request = route.request
        url = request.url.lower()
        
        # 1. Block heavy unused resource types. 
        # Only block image to be safe. Sometimes fonts/media blocking can stall modern JS frameworks from firing DOMContentLoaded.
        if request.resource_type in ["image", "media"]:
            await route.abort()
            return
            
        # 2. Block known tracking / 3rd party domains
        blocked_domains = [
            "cookielaw.org",
            "googletagmanager.com",
            "google-analytics.com",
            "clarity.ms",
            "surveygizmo.eu",
            "onetrust.com",
            "googlesyndication.com"
        ]
        
        if any(d in url for d in blocked_domains):
            await route.abort()
            return
            
        await route.continue_()

    # ---------------------------------------------------------------------------
    # Context Factory Helpers
    # ---------------------------------------------------------------------------

    def _build_context_options(self) -> Dict[str, Any]:
        """Centralize all options for browser.new_context()."""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        ]
        return {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": random.choice(user_agents),
            "locale": "en-US",
            "timezone_id": "America/Mexico_City",
            "java_script_enabled": True,
            "ignore_https_errors": self._ignore_https_errors,
        }

    def _get_evasion_init_script(self) -> str:
        """Return the anti-detection init script as a reusable string."""
        return """
            // Overwrite webdriver property
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            
            // Mock permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
            );
            
            // Mock plugins and languages
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            
            // Mock platform
            Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
            
            // Mock chrome object
            window.chrome = { runtime: {} };
        """

    async def _install_context_features(self, context: BrowserContext) -> None:
        """Apply init script and resource-blocking route to a context."""
        await context.add_init_script(self._get_evasion_init_script())
        if getattr(Config, 'ODDSPORTAL_BLOCK_RESOURCES', True):
            await context.route("**/*", self._intercept_route)

    async def _create_fresh_context(self) -> BrowserContext:
        """Create a brand-new BrowserContext from the already-open browser."""
        if not self.browser:
            raise RuntimeError("Cannot create fresh context: browser is not started. Call start() first.")
        ctx = await self.browser.new_context(**self._build_context_options())
        await self._install_context_features(ctx)
        return ctx

    # ---------------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------------

    async def stop(self):
        """Stop the browser session."""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        except Exception:
            # Browser might already be closed (TargetClosedError)
            pass
        finally:
            if self.playwright:
                await self.playwright.stop()
            
            self.browser = None
            self.context = None
            self.playwright = None
            logger.info("🛑 OddsPortalScraper: Browser stopped")

    async def _goto_fresh(self, page: Page, url: str, **kwargs) -> object:
        """Cache-defeating wrapper around page.goto().

        1. Sets no-cache/no-store HTTP headers for this navigation only.
        2. Appends a _t=<timestamp> cache-buster to the URL (before the fragment).
        """
        # --- Build cache-busted URL ---
        # Split on '#' first so the fragment stays at the end.
        if '#' in url:
            base_part, fragment = url.split('#', 1)
        else:
            base_part, fragment = url, None

        cache_buster = f"_t={int(time.time())}"
        if '?' in base_part:
            base_part = f"{base_part}&{cache_buster}"
        else:
            base_part = f"{base_part}?{cache_buster}"

        fresh_url = f"{base_part}#{fragment}" if fragment else base_part
        logger.debug(f"🔄 _goto_fresh: {fresh_url}")

        # --- Apply anti-cache headers for this navigation only ---
        try:
            await page.set_extra_http_headers({
                "Cache-Control": "no-cache, no-store",
                "Pragma": "no-cache",
            })
            response = await page.goto(fresh_url, **kwargs)
            return response
        finally:
            # Reset headers so they don't bleed into subsequent XHRs/fetches.
            await page.set_extra_http_headers({})

    async def _clear_browser_state(self) -> None:
        """Clear cookies, web storage, cache storage, and service workers from the active context."""
        if not self.context:
            logger.debug("⚠️ _clear_browser_state: no active context, skipping")
            return

        try:
            # 1. Clear cookies
            await self.context.clear_cookies()

            # 2. Clear localStorage / sessionStorage via a temporary page
            try:
                tmp_page = await self.context.new_page()
                await tmp_page.goto("about:blank")
                await tmp_page.evaluate("() => { localStorage.clear(); sessionStorage.clear(); }")

                # 3. Clear Cache Storage (best-effort)
                try:
                    await tmp_page.evaluate("""
                        async () => {
                            if (typeof caches !== 'undefined') {
                                const names = await caches.keys();
                                await Promise.all(names.map(n => caches.delete(n)));
                            }
                        }
                    """)
                except Exception:
                    pass

                # 4. Unregister service workers (best-effort)
                try:
                    await tmp_page.evaluate("""
                        async () => {
                            if (navigator.serviceWorker) {
                                const regs = await navigator.serviceWorker.getRegistrations();
                                await Promise.all(regs.map(r => r.unregister()));
                            }
                        }
                    """)
                except Exception:
                    pass

                await tmp_page.close()
            except Exception as storage_err:
                logger.debug(f"⚠️ Storage/cache clear on about:blank failed (non-fatal): {storage_err}")

            logger.info("🧹 OddsPortalScraper: Browser state cleared (cookies + storage + cache storage + service workers)")
        except Exception as e:
            logger.warning(f"⚠️ _clear_browser_state failed (non-fatal): {e}")


    def _normalize_base_match_url(self, match_url: str) -> str:
        """Remove fragment/trailing slash and return canonical match base URL."""
        return (match_url or "").split("#")[0].rstrip("/")

    async def _remove_interaction_overlays(self, page: Page):
        """Best-effort cleanup of UI elements that can block clicks/hovers."""
        try:
            await page.evaluate("""
                () => {
                    document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());
                    const onetrust = document.getElementById('onetrust-banner-sdk');
                    if (onetrust) onetrust.remove();
                    const shade = document.querySelector('.onetrust-pc-dark-filter');
                    if (shade) shade.remove();
                }
            """)
        except Exception:
            pass

    async def _has_market_content(self, page: Page) -> bool:
        """Return True when any known market data container is present."""
        try:
            return await page.evaluate(r"""
                () => {
                    const hasRows = document.querySelectorAll('div.border-black-borders.flex.h-9').length > 0;
                    const hasOUCollapsed = document.querySelectorAll('div[data-testid="over-under-collapsed-row"]').length > 0;
                    const hasAHCollapsed = document.querySelectorAll('div[data-testid="asian-handicap-collapsed-row"]').length > 0;
                    const hasOUExpanded = document.querySelectorAll('div[data-testid="over-under-expanded-row"]').length > 0;
                    return hasRows || hasOUCollapsed || hasAHCollapsed || hasOUExpanded;
                }
            """)
        except Exception:
            return False

    async def _get_active_period_labels(self, page: Page) -> List[str]:
        """Collect active period labels from all kickoff-events-nav blocks."""
        try:
            labels = await page.evaluate(r"""
                () => {
                    const out = [];
                    const navs = Array.from(document.querySelectorAll('div[data-testid="kickoff-events-nav"]'));
                    for (const nav of navs) {
                        const active = nav.querySelector('div[data-testid="sub-nav-active-tab"]');
                        const txt = active ? active.textContent.trim() : '';
                        if (txt) out.push(txt);
                    }
                    return out;
                }
            """)
            return labels or []
        except Exception:
            return []

    async def _is_target_period_active(self, page: Page, period_display_name: str) -> bool:
        target = (period_display_name or "").strip().lower()
        if not target:
            return False
        labels = await self._get_active_period_labels(page)
        return any((lbl or "").strip().lower() == target for lbl in labels)

    async def _get_active_group_label(self, page: Page) -> str:
        """Return active market group tab label (e.g. '1X2', 'Over/Under')."""
        try:
            return (await page.evaluate(r"""
                () => {
                    const active = document.querySelector('ul.visible-links.odds-tabs li.active-odds')
                        || document.querySelector('li[data-testid="navigation-active-tab"]');
                    return active ? active.textContent.trim() : '';
                }
            """)) or ""
        except Exception:
            return ""

    async def _is_target_group_active(self, page: Page, group_display_name: str) -> bool:
        target = (group_display_name or "").strip().lower()
        if not target:
            return False
        active_label = (await self._get_active_group_label(page)).strip().lower()
        return active_label == target

    async def _collect_match_page_state(self, page: Page) -> Dict[str, Any]:
        """Gather detailed page state metrics to diagnose partial or empty loads.

        Uses selectors validated against actual OddsPortal match-page DOM.
        """
        try:
            return await page.evaluate(r"""
                () => {
                    const getCount = (sel) => document.querySelectorAll(sel).length;

                    // --- Shell markers ---
                    const event_container_count = getCount('div.event-container');
                    const group_tab_count = getCount('ul.visible-links.odds-tabs li');
                    const period_nav_count = getCount('div[data-testid="kickoff-events-nav"]');

                    // --- Data markers ---
                    const bookmaker_row_count = getCount('div.border-black-borders.flex.h-9');
                    const odds_cell_count = getCount('div.odds-cell');
                    const ou_collapsed_count = getCount('div[data-testid="over-under-collapsed-row"]');
                    const ah_collapsed_count = getCount('div[data-testid="asian-handicap-collapsed-row"]');
                    const ou_expanded_count = getCount('div[data-testid="over-under-expanded-row"]');

                    // --- Skeleton / placeholder markers ---
                    const skeleton_row_count = getCount('div.animate-pulse.bg-gray-light')
                        + getCount('[class*="skeleton"]');

                    // --- Event container child count ---
                    const firstContainer = document.querySelector('div.event-container');
                    const event_container_child_count = firstContainer ? firstContainer.children.length : 0;

                    // --- Active group label ---
                    const activeGroupEl = document.querySelector('ul.visible-links.odds-tabs li.active-odds')
                        || document.querySelector('li[data-testid="navigation-active-tab"]');
                    const active_group_label = activeGroupEl ? activeGroupEl.textContent.trim() : '';

                    // --- Active period labels ---
                    const active_period_labels = [];
                    const navs = document.querySelectorAll('div[data-testid="kickoff-events-nav"]');
                    navs.forEach(nav => {
                        const active = nav.querySelector('div[data-testid="sub-nav-active-tab"]');
                        if (active) {
                            const txt = active.textContent.trim();
                            if (txt) active_period_labels.push(txt);
                        }
                    });

                    // --- Derived booleans ---
                    const has_shell_markers = event_container_count > 0
                        || group_tab_count > 0
                        || period_nav_count > 0;

                    const has_data_markers = bookmaker_row_count > 0
                        || odds_cell_count > 0
                        || ou_collapsed_count > 0
                        || ah_collapsed_count > 0
                        || ou_expanded_count > 0;

                    return {
                        url: window.location.href,
                        title: document.title,
                        ready_state: document.readyState,
                        event_container_count,
                        event_container_child_count,
                        group_tab_count,
                        period_nav_count,
                        active_group_label,
                        active_period_labels,
                        bookmaker_row_count,
                        odds_cell_count,
                        ou_collapsed_count,
                        ah_collapsed_count,
                        ou_expanded_count,
                        skeleton_row_count,
                        has_shell_markers,
                        has_data_markers,
                    };
                }
            """)
        except Exception:
            return {
                "url": "", "title": "", "ready_state": "",
                "event_container_count": 0, "event_container_child_count": 0,
                "group_tab_count": 0, "period_nav_count": 0,
                "active_group_label": "", "active_period_labels": [],
                "bookmaker_row_count": 0, "odds_cell_count": 0,
                "ou_collapsed_count": 0, "ah_collapsed_count": 0,
                "ou_expanded_count": 0, "skeleton_row_count": 0,
                "has_shell_markers": False, "has_data_markers": False,
            }

    def _classify_match_page_state(self, state: Dict[str, Any]) -> str:
        """Classify the dictionary returned by _collect_match_page_state.

        Rules are ordered so that data-present pages never false-fail,
        and MISSING_EVENT_CONTAINER only fires when shell markers exist
        but the event container itself is absent.
        """
        # 1. Data already rendered → success
        if state.get('has_data_markers', False):
            return "DATA_RENDERED"

        # 2. No shell markers at all → blank page
        if not state.get('has_shell_markers', False):
            return "NO_SHELL"

        # 3. Shell markers exist, but event container is missing
        if state.get('event_container_count', 0) == 0:
            return "MISSING_EVENT_CONTAINER"

        # 4. Event container exists, but is empty
        if state.get('event_container_count', 0) > 0 and state.get('event_container_child_count', 0) == 0:
            return "EMPTY_EVENT_CONTAINER"

        # 5. Shell + skeleton, no data
        if state.get('skeleton_row_count', 0) > 0 and not state.get('has_data_markers', False):
            return "SHELL_WITH_SKELETON_NO_DATA"

        # 6. Shell + nav tabs, no skeleton, no data
        if (state.get('group_tab_count', 0) > 0 or state.get('period_nav_count', 0) > 0) \
                and state.get('skeleton_row_count', 0) == 0 \
                and not state.get('has_data_markers', False):
            return "SHELL_WITH_NAV_NO_DATA"

        return "UNKNOWN_PARTIAL_STATE"

    def _format_page_state_summary(self, state: Dict[str, Any], classification: str) -> str:
        """Create a compact log line for the state."""
        return (
            f"[{classification}] "
            f"title='{state.get('title', '')}' "
            f"url={state.get('url', '')} "
            f"evtC={state.get('event_container_count', 0)} "
            f"grpTab={state.get('group_tab_count', 0)} "
            f"periodNav={state.get('period_nav_count', 0)} "
            f"skel={state.get('skeleton_row_count', 0)} "
            f"rows={state.get('bookmaker_row_count', 0)} "
            f"odds={state.get('odds_cell_count', 0)} "
            f"ou={state.get('ou_collapsed_count', 0)}/{state.get('ou_expanded_count', 0)} "
            f"ah={state.get('ah_collapsed_count', 0)}"
        )

    def _classify_goto_exception(self, error: Exception) -> Tuple[str, str]:
        """Map Playwright goto exceptions into clearer log/debug reasons."""
        error_text = str(error or "")
        upper_text = error_text.upper()

        if "ERR_TUNNEL_CONNECTION_FAILED" in upper_text:
            return "GOTO_PROXY_TUNNEL_FAILED", "proxy tunnel connection failed before page load"
        if "ERR_CONNECTION_RESET" in upper_text:
            return "GOTO_CONNECTION_RESET", "connection reset before page load"
        if "ERR_CONNECTION_CLOSED" in upper_text:
            return "GOTO_CONNECTION_CLOSED", "connection closed before page load"
        if "ERR_CONNECTION_REFUSED" in upper_text:
            return "GOTO_CONNECTION_REFUSED", "connection refused before page load"
        if "ERR_NAME_NOT_RESOLVED" in upper_text:
            return "GOTO_DNS_RESOLUTION_FAILED", "DNS resolution failed before page load"
        if "ERR_CERT_AUTHORITY_INVALID" in upper_text:
            return "GOTO_CERT_AUTHORITY_INVALID", "certificate authority not trusted"
        if "ERR_CERT_COMMON_NAME_INVALID" in upper_text:
            return "GOTO_CERT_COMMON_NAME_INVALID", "TLS certificate common name mismatch"
        if "ERR_CERT_DATE_INVALID" in upper_text:
            return "GOTO_CERT_DATE_INVALID", "TLS certificate date invalid"
        if "ERR_CERT_INVALID" in upper_text:
            return "GOTO_CERT_INVALID", "TLS certificate invalid"
        if "ERR_TIMED_OUT" in upper_text or "TIMEOUT" in upper_text:
            return "GOTO_TIMEOUT", "navigation timed out before usable content loaded"
        return "GOTO_FAILED", "navigation failed before usable content loaded"

    async def _save_debug_artifacts(self, page: Page, reason: str, extra: Dict[str, Any] = None):
        """Save screenshot, HTML, and JSON manifest on failure if debug_dir is set."""
        if not self.debug_dir:
            return
            
        try:
            import datetime
            import json
            import os
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            safe_reason = reason.replace('/', '_').replace(' ', '_').lower()
            base_name = f"op_fail_{timestamp}_{safe_reason}"
            
            os.makedirs(self.debug_dir, exist_ok=True)
            
            png_path = os.path.join(self.debug_dir, f"{base_name}.png")
            await page.screenshot(path=png_path, full_page=True)
            
            html_path = os.path.join(self.debug_dir, f"{base_name}.html")
            html_content = await page.content()
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_content)
                
            state = await self._collect_match_page_state(page)
            classification = self._classify_match_page_state(state)
            
            extras_payload = extra or {}
            resume_manifest = {}
            for key in [
                "completed_step_keys",
                "next_step_idx",
                "failed_step_key",
                "failed_reason",
                "resume_fragment",
                "partial_extraction_count",
            ]:
                if key in extras_payload:
                    resume_manifest[key] = extras_payload.get(key)
            if not resume_manifest and isinstance(extras_payload.get("resume_state"), dict):
                nested_resume = extras_payload["resume_state"]
                for key in [
                    "completed_step_keys",
                    "next_step_idx",
                    "failed_step_key",
                    "failed_reason",
                    "resume_fragment",
                    "partial_extraction_count",
                ]:
                    if key in nested_resume:
                        resume_manifest[key] = nested_resume.get(key)

            manifest = {
                "timestamp": timestamp,
                "reason": reason,
                "url": page.url,
                "title": await page.title(),
                "session_id": getattr(self, '_session_id', 'unknown'),
                "state": state,
                "classification": classification,
                "config": {
                    "goto_timeout": getattr(Config, 'ODDSPORTAL_MATCH_GOTO_TIMEOUT_MS', 30000),
                    "empty_timeout": getattr(Config, 'ODDSPORTAL_FAST_FAIL_EMPTY_TIMEOUT_MS', 15000),
                    "render_timeout": getattr(Config, 'ODDSPORTAL_MARKET_RENDER_TIMEOUT_MS', 60000),
                    "shell_grace": getattr(Config, 'ODDSPORTAL_SHELL_GRACE_TIMEOUT_MS', 8000),
                },
                "extras": extras_payload,
                **resume_manifest,
            }
            
            json_path = os.path.join(self.debug_dir, f"{base_name}.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=4)
                
            logger.info(f"💾 Saved debug artifacts for {reason} at {self.debug_dir}/{base_name}.*")
        except Exception as e:
            logger.error(f"Failed to save debug artifacts: {e}")


    def _make_initial_resume_state(
        self,
        match_url: str,
        sport: Optional[str],
        route_steps: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        next_fragment = route_steps[0].get("fragment") if route_steps else None
        return {
            "sport": sport,
            "route_step_count": len(route_steps),
            "next_step_idx": 0,
            "completed_step_keys": [],
            "failed_step_key": None,
            "failed_group_key": None,
            "failed_period_key": None,
            "failed_reason": None,
            "resume_fragment": next_fragment,
            "last_completed_fragment": None,
            "partial_extraction_count": 0,
        }

    def _normalize_resume_state(
        self,
        resume_state: Optional[Dict[str, Any]],
        sport: Optional[str],
        route_steps: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        initial_state = self._make_initial_resume_state("", sport, route_steps)
        if not isinstance(resume_state, dict):
            return initial_state

        expected_step_count = len(route_steps)
        if (
            resume_state.get("sport") != sport
            or resume_state.get("route_step_count") != expected_step_count
        ):
            return initial_state

        step_key_to_idx = {
            step.get("step_key"): step.get("step_idx", idx)
            for idx, step in enumerate(route_steps)
            if step.get("step_key")
        }
        ordered_completed: List[str] = []
        for key in resume_state.get("completed_step_keys", []) or []:
            if key in step_key_to_idx and key not in ordered_completed:
                ordered_completed.append(key)

        max_completed_idx = -1
        if ordered_completed:
            max_completed_idx = max(step_key_to_idx[key] for key in ordered_completed)

        raw_next_idx = resume_state.get("next_step_idx", 0)
        next_step_idx = raw_next_idx if isinstance(raw_next_idx, int) else 0
        next_step_idx = max(next_step_idx, max_completed_idx + 1)
        next_step_idx = min(max(next_step_idx, 0), expected_step_count)

        failed_step_key = resume_state.get("failed_step_key")
        if failed_step_key not in step_key_to_idx:
            failed_step_key = None

        return {
            "sport": sport,
            "route_step_count": expected_step_count,
            "next_step_idx": next_step_idx,
            "completed_step_keys": ordered_completed,
            "failed_step_key": failed_step_key,
            "failed_group_key": resume_state.get("failed_group_key") if failed_step_key else None,
            "failed_period_key": resume_state.get("failed_period_key") if failed_step_key else None,
            "failed_reason": resume_state.get("failed_reason") if failed_step_key else None,
            "resume_fragment": (
                route_steps[next_step_idx].get("fragment")
                if next_step_idx < expected_step_count
                else None
            ),
            "last_completed_fragment": resume_state.get("last_completed_fragment"),
            "partial_extraction_count": int(resume_state.get("partial_extraction_count", 0) or 0),
        }

    def _mark_step_completed(
        self,
        resume_state: Dict[str, Any],
        step: Dict[str, Any],
        match_data: MatchOddsData,
    ) -> None:
        completed = resume_state.setdefault("completed_step_keys", [])
        step_key = step.get("step_key")
        if step_key and step_key not in completed:
            completed.append(step_key)

        route_count = int(resume_state.get("route_step_count", 0) or 0)
        next_step_idx = step.get("step_idx", 0) + 1
        if route_count > 0:
            next_step_idx = min(next_step_idx, route_count)

        resume_state["next_step_idx"] = next_step_idx
        resume_state["failed_step_key"] = None
        resume_state["failed_group_key"] = None
        resume_state["failed_period_key"] = None
        resume_state["failed_reason"] = None
        resume_state["resume_fragment"] = None
        resume_state["last_completed_fragment"] = step.get("fragment")
        resume_state["partial_extraction_count"] = len(match_data.extractions)

    def _mark_step_failed(self, resume_state: Dict[str, Any], step: Dict[str, Any], reason: str) -> None:
        resume_state["failed_step_key"] = step.get("step_key")
        resume_state["failed_group_key"] = step.get("group_key")
        resume_state["failed_period_key"] = step.get("period_key")
        resume_state["failed_reason"] = reason
        resume_state["resume_fragment"] = step.get("fragment")
        resume_state["next_step_idx"] = step.get("step_idx", 0)
        resume_state["partial_extraction_count"] = int(resume_state.get("partial_extraction_count", 0) or 0)

    def _restore_partial_match_data(
        self,
        partial_match_data: Optional[MatchOddsData],
        match_url: str,
        sport: Optional[str],
    ) -> MatchOddsData:
        if isinstance(partial_match_data, MatchOddsData):
            match_data = partial_match_data
            match_data.match_url = match_url
            if sport is not None:
                match_data.sport = sport
            if match_data.extractions is None:
                match_data.extractions = []
        else:
            match_data = MatchOddsData(match_url=match_url, sport=sport or "")
        self._ensure_legacy_match_level_fields(match_data)
        return match_data

    def _ensure_legacy_match_level_fields(self, match_data: MatchOddsData) -> None:
        if match_data.extractions:
            first = match_data.extractions[0]
            match_data.bookie_odds = first.bookie_odds
            match_data.betfair = first.betfair
        else:
            match_data.bookie_odds = []
            match_data.betfair = None

    def _log_structured_recap(self, match_data: MatchOddsData) -> None:
        """Emit a readable recap of all extracted markets for debugging."""
        if not match_data.extractions:
            return

        match_label = f"{match_data.home_team} vs {match_data.away_team}"
        logger.info(f"[RECAP] -- ODDS RECAP: {match_label} --")

        for ext in match_data.extractions:
            handicap_str = ""
            if ext.bookie_odds and getattr(ext.bookie_odds[0], "handicap", None):
                handicap_str = f" [{ext.bookie_odds[0].handicap}]"
            logger.info(
                f"   [MARKET] {ext.market_group} | {ext.market_period} | "
                f"{ext.market_name}{handicap_str}"
            )

            is_ou = ext.market_group == "Over/Under"
            is_ah = ext.market_group == "Asian Handicap"
            if is_ou:
                col_labels = ("Over", "Under")
            elif is_ah:
                col_labels = ("1", "2")
            else:
                col_labels = ("1", "X", "2")

            for b in ext.bookie_odds:
                if is_ou or is_ah:
                    current = f"{col_labels[0]}={b.odds_1 or '-'} {col_labels[-1]}={b.odds_2 or '-'}"
                    opening = f"{col_labels[0]}={b.initial_odds_1 or '-'} {col_labels[-1]}={b.initial_odds_2 or '-'}"
                else:
                    current = f"1={b.odds_1 or '-'} X={b.odds_x or '-'} 2={b.odds_2 or '-'}"
                    opening = f"1={b.initial_odds_1 or '-'} X={b.initial_odds_x or '-'} 2={b.initial_odds_2 or '-'}"
                logger.info(f"      {b.name}: {current} (open: {opening})")

            if ext.betfair:
                bf = ext.betfair
                if is_ou or is_ah:
                    back_str = f"{col_labels[0]}={bf.back_1 or '-'} {col_labels[-1]}={bf.back_2 or '-'}"
                    lay_str = f"{col_labels[0]}={bf.lay_1 or '-'} {col_labels[-1]}={bf.lay_2 or '-'}"
                else:
                    back_str = f"1={bf.back_1 or '-'} X={bf.back_x or '-'} 2={bf.back_2 or '-'}"
                    lay_str = f"1={bf.lay_1 or '-'} X={bf.lay_x or '-'} 2={bf.lay_2 or '-'}"
                logger.info(f"      Betfair Back: {back_str} | Lay: {lay_str}")

        logger.info(f"[RECAP] -- END RECAP: {match_label} --")

    def _resume_state_for_debug(self, resume_state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(resume_state, dict):
            return {}
        return {
            "completed_step_keys": list(resume_state.get("completed_step_keys", []) or []),
            "next_step_idx": resume_state.get("next_step_idx"),
            "failed_step_key": resume_state.get("failed_step_key"),
            "failed_reason": resume_state.get("failed_reason"),
            "resume_fragment": resume_state.get("resume_fragment"),
            "partial_extraction_count": resume_state.get("partial_extraction_count"),
        }



    async def _wait_for_market_render(self, page: Page, extract_fn: str, timeout_ms: int = 15000) -> bool:
        """
        Wait for selectors appropriate for the extraction mode.
        standard: bookmaker rows
        over_under/asian_handicap: collapsed/expanded accordion rows (or standard fallback)
        """
        try:
            if extract_fn == "standard":
                await page.wait_for_selector("div.border-black-borders.flex.h-9", state="visible", timeout=timeout_ms)
            else:
                await page.wait_for_selector(
                    "div[data-testid='over-under-collapsed-row'], "
                    "div[data-testid='asian-handicap-collapsed-row'], "
                    "div[data-testid='over-under-expanded-row'], "
                    "div.border-black-borders.flex.h-9",
                    state="visible",
                    timeout=timeout_ms,
                )
            return True
        except Exception as e:
            try:
                state = await self._collect_match_page_state(page)
                classification = self._classify_match_page_state(state)
                summary = self._format_page_state_summary(state, classification)
                logger.warning(f"⚠️ Wait for market render ({extract_fn}) failed. Final state: {summary} - {e}")
            except Exception as log_e:
                logger.debug(f"Failed to log state during _wait_for_market_render timeout: {log_e}")
            return False
    async def _click_period_tab(self, page: Page, period_display_name: str) -> bool:
        """
        Click a market period sub-tab and wait for the odds table to update.

        Success criteria are strict to prevent stale-data extraction:
        - target period tab must be active, and
        - market content must be present, and
        - either odds changed OR active-tab state transitioned to target.
        """
        try:
            # Snapshot reference odds value before clicking
            ref_value = await page.evaluate(r"""
                () => {
                    const getFirstVisibleText = (selector) => {
                        const el = Array.from(document.querySelectorAll(selector)).find(
                            e => e.getBoundingClientRect().width > 0 && e.getBoundingClientRect().height > 0
                        );
                        return el ? el.innerText.trim() : null;
                    };
                    const oddsVal = getFirstVisibleText('div.odds-cell');
                    if (oddsVal) return oddsVal;
                    const ouVal = getFirstVisibleText('div[data-testid="over-under-collapsed-row"] p');
                    if (ouVal) return ouVal;
                    const ahVal = getFirstVisibleText('div[data-testid="asian-handicap-collapsed-row"] p');
                    if (ahVal) return ahVal;
                    return null;
                }
            """)
            logger.info(f"📸 Reference odds value before period switch: {ref_value}")

            before_active_labels = await self._get_active_period_labels(page)
            period_target_norm = (period_display_name or "").strip().lower()
            before_active_has_target = any((lbl or "").strip().lower() == period_target_norm for lbl in before_active_labels)

            # Find target period tab
            period_navs = await page.query_selector_all('div[data-testid="kickoff-events-nav"]')
            if not period_navs:
                logger.warning("⚠️ Period sub-nav not found (data-testid='kickoff-events-nav')")
                return False

            target_tab = None
            available_tabs = []
            for nav in period_navs:
                tabs = await nav.query_selector_all('div[data-testid="sub-nav-active-tab"], div[data-testid="sub-nav-inactive-tab"]')
                for tab in tabs:
                    text = await tab.text_content()
                    text_stripped = text.strip() if text else ""
                    if text_stripped:
                        available_tabs.append(text_stripped)
                        if period_target_norm == text_stripped.lower().strip():
                            target_tab = tab
                            break
                if target_tab:
                    break

            if not target_tab:
                logger.warning(f"⚠️ Period tab '{period_display_name}' not found in any kickoff-events-nav block")
                logger.warning(f"   Available tabs: {available_tabs}")
                return False

            testid = await target_tab.get_attribute('data-testid')
            is_active = (testid == 'sub-nav-active-tab')

            if is_active:
                logger.info(f"ℹ️ Period tab '{period_display_name}' is already active. Verifying content...")
                if await self._has_market_content(page):
                    return True
                logger.info("  ⏳ Period tab is active but content is missing. Waiting for render...")

            if not is_active:
                await self._remove_interaction_overlays(page)
                logger.info(f"🖱️ Clicking period tab: '{period_display_name}'")
                try:
                    await target_tab.scroll_into_view_if_needed()
                    await target_tab.click(timeout=3000)
                except Exception:
                    await target_tab.click(force=True, timeout=3000)

            max_wait_s = Config.ODDSPORTAL_TAB_WAIT_TIMEOUT
            poll_interval_ms = 300
            elapsed = 0
            table_changed = False

            while elapsed < max_wait_s:
                await page.wait_for_timeout(poll_interval_ms)
                elapsed += poll_interval_ms / 1000

                new_value = await page.evaluate(r"""
                    () => {
                        const getFirstVisibleText = (selector) => {
                            const el = Array.from(document.querySelectorAll(selector)).find(
                                e => e.getBoundingClientRect().width > 0 && e.getBoundingClientRect().height > 0
                            );
                            return el ? el.innerText.trim() : null;
                        };
                        const oddsVal = getFirstVisibleText('div.odds-cell');
                        if (oddsVal) return oddsVal;
                        const ouVal = getFirstVisibleText('div[data-testid="over-under-collapsed-row"] p');
                        if (ouVal) return ouVal;
                        const ahVal = getFirstVisibleText('div[data-testid="asian-handicap-collapsed-row"] p');
                        if (ahVal) return ahVal;
                        return null;
                    }
                """)

                active_now = await self._is_target_period_active(page, period_display_name)
                if active_now and new_value and (ref_value is None or new_value != ref_value):
                    logger.info(f"✅ Odds table updated after {elapsed:.1f}s: {ref_value} → {new_value}")
                    table_changed = True
                    break

            if not table_changed:
                content_present = await self._has_market_content(page)
                active_now = await self._is_target_period_active(page, period_display_name)

                # Identical-odds edge case: accept only if the target tab is truly active.
                if active_now and content_present and (not before_active_has_target or is_active):
                    logger.info(
                        f"ℹ️ No odds-text delta after {max_wait_s}s, but target period is active and content is present — treating as success."
                    )
                    table_changed = True
                else:
                    logger.warning(
                        f"⚠️ Period switch validation failed for '{period_display_name}' "
                        f"(active_now={active_now}, content_present={content_present}, before_active_has_target={before_active_has_target}, ref={ref_value})"
                    )

            await asyncio.sleep(0.5)
            return table_changed

        except Exception as e:
            logger.error(f"❌ Error clicking period tab '{period_display_name}': {e}")
            return False
    async def _click_market_group_tab(self, page: Page, group_display_name: str) -> bool:
        """
        Click a market group tab (e.g. "Over/Under", "1X2") and wait for the table to update.

        Success criteria are strict to avoid false positives that can cause stale extractions.
        """
        try:
            ref_value = await page.evaluate(r"""
                () => {
                    const getFirstVisibleText = (selector) => {
                        const el = Array.from(document.querySelectorAll(selector)).find(
                            e => e.getBoundingClientRect().width > 0 && e.getBoundingClientRect().height > 0
                        );
                        return el ? el.innerText.trim() : null;
                    };
                    const oddsVal = getFirstVisibleText('div.odds-cell');
                    if (oddsVal) return oddsVal;
                    const ouVal = getFirstVisibleText('div[data-testid="over-under-collapsed-row"] p');
                    if (ouVal) return ouVal;
                    const ahVal = getFirstVisibleText('div[data-testid="asian-handicap-collapsed-row"] p');
                    if (ahVal) return ahVal;
                    return null;
                }
            """)
            logger.info(f"📸 Reference value before group switch: {ref_value}")

            before_active_group = await self._get_active_group_label(page)
            group_target_norm = (group_display_name or "").strip().lower()
            before_active_is_target = before_active_group.strip().lower() == group_target_norm

            tabs = await page.query_selector_all('ul.visible-links.odds-tabs li')
            if not tabs:
                logger.warning("⚠️ Market group tabs not found")
                return False

            target_tab = None
            available_tabs = []
            for tab in tabs:
                text = await tab.text_content()
                text_stripped = text.strip() if text else ""
                if text_stripped:
                    available_tabs.append(text_stripped)
                    if group_target_norm == text_stripped.lower().strip():
                        target_tab = tab
                        break

            if not target_tab:
                logger.warning(f"⚠️ Market group tab '{group_display_name}' not found")
                logger.warning(f"   Available tabs: {available_tabs}")
                return False

            is_active = await target_tab.evaluate("el => el.classList.contains('active-odds')")

            if is_active:
                logger.info(f"ℹ️ Market group tab '{group_display_name}' is already active. Verifying content...")
                if await self._has_market_content(page):
                    return True
                logger.info("  ⏳ Tab is active but content is missing. Waiting for render...")

            if not is_active:
                await self._remove_interaction_overlays(page)
                logger.info(f"🖱️ Clicking market group tab: '{group_display_name}'")
                try:
                    await target_tab.scroll_into_view_if_needed()
                    await target_tab.click(timeout=3000)
                except Exception:
                    await target_tab.click(force=True, timeout=3000)

            max_wait_s = Config.ODDSPORTAL_TAB_WAIT_TIMEOUT
            poll_interval_ms = 300
            elapsed = 0
            table_changed = False

            while elapsed < max_wait_s:
                await page.wait_for_timeout(poll_interval_ms)
                elapsed += poll_interval_ms / 1000

                new_value = await page.evaluate(r"""
                    () => {
                        const getFirstVisibleText = (selector) => {
                            const el = Array.from(document.querySelectorAll(selector)).find(
                                e => e.getBoundingClientRect().width > 0 && e.getBoundingClientRect().height > 0
                            );
                            return el ? el.innerText.trim() : null;
                        };
                        const oddsVal = getFirstVisibleText('div.odds-cell');
                        if (oddsVal) return oddsVal;
                        const ouVal = getFirstVisibleText('div[data-testid="over-under-collapsed-row"] p');
                        if (ouVal) return ouVal;
                        const ahVal = getFirstVisibleText('div[data-testid="asian-handicap-collapsed-row"] p');
                        if (ahVal) return ahVal;
                        return null;
                    }
                """)

                active_now = await self._is_target_group_active(page, group_display_name)
                if active_now and new_value and (ref_value is None or new_value != ref_value):
                    logger.info(f"✅ Odds table updated after {elapsed:.1f}s: {ref_value} → {new_value}")
                    table_changed = True
                    break

            if not table_changed:
                content_present = await self._has_market_content(page)
                active_now = await self._is_target_group_active(page, group_display_name)

                # Identical-odds edge case: accept only if target group is active.
                if active_now and content_present and (not before_active_is_target or is_active):
                    logger.info(
                        f"ℹ️ No odds-text delta after {max_wait_s}s, but target group is active and content is present — treating as success."
                    )
                    table_changed = True
                else:
                    logger.warning(
                        f"⚠️ Group switch validation failed for '{group_display_name}' "
                        f"(active_now={active_now}, content_present={content_present}, before_active_is_target={before_active_is_target}, ref={ref_value})"
                    )

            await asyncio.sleep(0.5)
            return table_changed

        except Exception as e:
            logger.error(f"❌ Error clicking market group tab '{group_display_name}': {e}")
            return False
            
    def _load_cached_candidates(
        self,
        season_id: int,
        league_url: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """Load structured league candidates from the DB cache."""
        from repository import OddsPortalCacheRepository
        cached = OddsPortalCacheRepository.get_league_cache(season_id)

        if not cached:
            return []

        candidates = []
        for href, data in cached.items():
            if not href:
                continue

            if isinstance(data, dict):
                candidates.append({
                    "home": data.get("home", ""),
                    "away": data.get("away", ""),
                    "href": href,
                    "raw_text": data.get("raw_text", ""),
                })
                continue

            row_text = data or ""
            parts = re.split(TEAM_SEPARATOR_PATTERN, row_text)
            if len(parts) >= 2:
                p1, p2 = parts[0], parts[1]
            else:
                match = re.search(LEGACY_CACHE_MATCH_PATTERN, row_text)
                if match:
                    p1, p2 = match.group(1), match.group(2)
                else:
                    p1, p2 = None, None

            if p1 and p2:
                home = p1.split("\n")[-1].strip()
                away = p2.split("\n")[0].strip()
                home = re.sub(TEAM_PREFIX_CLEAN_PATTERN, '', home)
                away = re.sub(r'\s+\d+\.\d+.*', '', away)
                if home and away:
                    candidates.append({"home": home, "away": away, "href": href})
                    continue

            candidates.append({"home": row_text, "away": "", "href": href})

        return candidates

    async def _extract_league_candidates(self, league_url: str, season_id: Optional[int]) -> List[Dict[str, str]]:
        """
        Navigate to a league page, extract candidates once, and populate cache if available.
        This shared path is reused by both priming and cache-miss discovery.
        """
        if not self.browser:
            await self.start()

        # If no active context (normal when fresh_context_per_event is on),
        # create a temporary one for this league navigation.
        _temp_ctx = None
        ctx = self.context
        if not ctx:
            _temp_ctx = await self._create_fresh_context()
            ctx = _temp_ctx

        page = await ctx.new_page()
        navigation_league_url = league_url
        normalized_league_url = _normalize_league_url(league_url) or league_url
        try:
            logger.info(f"🌐 Navigating to league: {navigation_league_url}")
            t0 = time.perf_counter()
            try:
                response = await page.goto(
                    navigation_league_url,
                    wait_until="domcontentloaded",
                    timeout=ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS,
                )
            except Exception as e:
                error_str = str(e).lower()
                if "timeout" in error_str or "err_" in error_str or "net::" in error_str:
                    logger.error(
                        "🔄 FAST FAIL (League goto): navigation failed quickly "
                        f"after {ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS}ms: {str(e).split(chr(10))[0]}"
                    )
                    return []
                raise

            t_goto = time.perf_counter()
            log_timing(f"League page load ({navigation_league_url}) took {t_goto - t0:.2f}s")

            if not response or response.status != 200:
                logger.error(f"âŒ Failed to load league page. Status: {response.status if response else 'N/A'}")
                return []

            try:
                page_title = await page.title()
                if any(blocked in page_title for blocked in ["Access Denied", "Just a moment...", "Attention Required!", "Security check", "Cloudflare"]):
                    logger.error(f"🔄 FAST FAIL (League title): Proxy IP blocked. Title: '{page_title}'")
                    return []
            except Exception:
                pass

            try:
                await page.wait_for_selector("div.eventRow", timeout=ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS)
                t_wait = time.perf_counter()
                log_timing(f"Waiting for 'div.eventRow' selector took {t_wait - t_goto:.2f}s")
            except Exception:
                logger.error(
                    "🔄 FAST FAIL (League rows): no event rows loaded "
                    f"within {ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS}ms on {navigation_league_url}"
                )
                return []

            try:
                accept_btn = await page.query_selector("button:has-text('I Accept'), button:has-text('Accept All')")
                if accept_btn:
                    await accept_btn.click()
                    await asyncio.sleep(0.5)
            except Exception:
                pass

            t_js_league = time.perf_counter()
            rows_data = await page.evaluate("""() => {
                const rows = Array.from(document.querySelectorAll("div.eventRow"));
                return rows.map(row => {
                    const text = row.innerText;
                    const links = Array.from(row.querySelectorAll("a[href]")).map(a => ({
                        href: a.getAttribute("href"),
                        text: a.innerText.trim()
                    }));
                    return { text, links };
                });
            }""")
            log_timing(f"Extracting league rows via JS evaluating took {time.perf_counter() - t_js_league:.2f}s")

            if not rows_data:
                logger.warning(f"âš ï¸ No event rows found on {navigation_league_url}")
                return []

            candidates = []
            league_path = navigation_league_url.replace("https://www.oddsportal.com", "").rstrip("/")

            for row in rows_data:
                row_text = row.get("text", "")
                links = row.get("links", [])
                p1, p2, href = None, None, None

                for link in links:
                    l_href = link.get("href", "")
                    if not l_href or "-" not in l_href:
                        continue
                    if l_href.rstrip("/") in league_path:
                        continue

                    link_text = link.get("text", "")
                    parts = re.split(TEAM_SEPARATOR_PATTERN, link_text)
                    if len(parts) >= 2:
                        p1 = parts[0].strip()
                        p2 = parts[1].strip()
                        p1 = re.sub(r'^\d{2}:\d{2}\s+', '', p1)
                        p2 = re.sub(r'\s+\d+\.\d+.*', '', p2)
                        href = l_href
                        break

                if not (p1 and p2):
                    lines = [line.strip() for line in row_text.split('\n') if line.strip()]
                    for line in lines:
                        if any(k in line for k in ["Today", "Tomorrow", "Mar", "Apr", "Play Offs", "Play Out"]):
                            continue
                        if f" {EN_DASH} " in line or " - " in line:
                            parts = re.split(TEAM_SEPARATOR_PATTERN, line)
                            if len(parts) >= 2:
                                p1 = parts[0].strip()
                                p2 = parts[1].strip()
                                p1 = re.sub(r'^\d{2}:\d{2}\s+', '', p1)
                                p2 = re.sub(r'\s+\d+\.\d+.*', '', p2)
                                if not href and links:
                                    href = links[0].get("href")
                                break

                if p1 and p2 and href:
                    candidates.append({
                        "home": p1,
                        "away": p2,
                        "href": href,
                        "raw_text": row_text,
                    })

            if season_id and candidates:
                try:
                    from repository import OddsPortalCacheRepository
                    cache_dict = _build_structured_league_cache(candidates)
                    if cache_dict and OddsPortalCacheRepository.save_league_cache(season_id, cache_dict):
                        logger.info(f"⚡¦ Cached {len(cache_dict)} match URLs for season {season_id}")
                    elif cache_dict:
                        logger.warning(f"âš ï¸ Cache save returned False for season {season_id}")
                except Exception as cache_err:
                    logger.warning(f"âš ï¸ Cache save failed: {cache_err}")

            return candidates
        finally:
            await page.close()
            if _temp_ctx:
                try:
                    await _temp_ctx.close()
                except Exception:
                    pass


    async def find_match_url(self, league_url: str, home_team: str, away_team: str, season_id: int = None) -> Optional[str]:
        """
        Resolve a match URL by team names.
        Cache hits return immediately; cache misses reuse the shared league extraction path.
        """
        if season_id:
            cached_url = self.find_match_url_from_cache(
                season_id,
                home_team,
                away_team,
                league_url=league_url,
            )
            if cached_url:
                logger.info(f"Cache hit (internal): {cached_url}")
                return cached_url

        try:
            candidates = await self._extract_league_candidates(league_url, season_id)
            if not candidates:
                logger.warning(f"OddsPortal discovery returned no candidates for {league_url}")
                return None

            logger.info(f"Scanning {len(candidates)} candidates for {home_team} vs {away_team}...")
            best_match = self.team_matcher.find_best_match(home_team, away_team, candidates)
            if best_match:
                logger.info(
                    f"Match found: {best_match['home']} vs {best_match['away']} "
                    f"(Score: {best_match['max_score']:.1f}, Reversed: {best_match['is_reversed']})"
                )
                return f"https://www.oddsportal.com{best_match['href']}"

            logger.warning(f"Match not found: {home_team} vs {away_team}")
            return None
        except Exception as e:
            logger.error(f"Error finding match on {league_url}: {e}")
            return None

    def find_match_url_from_cache(
        self,
        season_id: int,
        home_team: str,
        away_team: str,
        league_url: Optional[str] = None,
    ) -> Optional[str]:
        """Try to find a match URL from the DB-backed league cache."""
        try:
            candidates = self._load_cached_candidates(season_id, league_url=league_url)
            if not candidates:
                logger.debug(f"No valid candidates parsed from cache for season {season_id}")
                return None

            logger.debug(f"Scanning {len(candidates)} cached candidates for {home_team} vs {away_team}...")
            best_match = self.team_matcher.find_best_match(home_team, away_team, candidates)
            if best_match and best_match["max_score"] >= 80:
                logger.info(
                    f"Cache hit: {best_match['home']} vs {best_match['away']} "
                    f"(Score: {best_match['max_score']:.1f})"
                )
                return f"https://www.oddsportal.com{best_match['href']}"
            return None
        except Exception as e:
            logger.debug(f"Cache lookup failed: {e}")
            return None

    async def scrape_match(self, match_url: str, sport: str = None, clear_state: bool = False) -> Optional[MatchOddsData]:
        """Compatibility wrapper for callers expecting only MatchOddsData."""
        attempt = await self.scrape_match_attempt(
            match_url,
            sport=sport,
            clear_state=clear_state,
        )
        return attempt.data

    async def scrape_match_attempt(
        self,
        match_url: str,
        sport: str = None,
        clear_state: bool = False,
        resume_state: Optional[Dict[str, Any]] = None,
        partial_match_data: Optional[MatchOddsData] = None,
    ) -> ScrapeAttemptResult:
        """
        Scrape a match while preserving route-step progress so retries can resume
        from the failed `group_key + period_key` fragment.
        """
        route_steps = flatten_sport_scraping_route(sport)
        route_step_count = len(route_steps)
        state_is_consistent = (
            isinstance(resume_state, dict)
            and resume_state.get("sport") == sport
            and resume_state.get("route_step_count") == route_step_count
        )

        if not state_is_consistent:
            # Resume state is source of truth for avoiding duplicates.
            partial_match_data = None

        normalized_resume_state = self._normalize_resume_state(resume_state, sport, route_steps)
        match_data = self._restore_partial_match_data(partial_match_data, match_url, sport)
        normalized_resume_state["partial_extraction_count"] = len(match_data.extractions)

        if route_step_count == 0:
            self._ensure_legacy_match_level_fields(match_data)
            self._log_structured_recap(match_data)

            return ScrapeAttemptResult(
                data=match_data,
                resume_state=normalized_resume_state,
                partial_match_data=match_data,
            )

        start_step_idx = normalized_resume_state.get("next_step_idx", 0)
        if not isinstance(start_step_idx, int):
            start_step_idx = 0
        start_step_idx = min(max(start_step_idx, 0), route_step_count)
        normalized_resume_state["next_step_idx"] = start_step_idx

        if start_step_idx >= route_step_count:
            normalized_resume_state["resume_fragment"] = None
            self._ensure_legacy_match_level_fields(match_data)
            self._log_structured_recap(match_data)
            return ScrapeAttemptResult(
                data=match_data,
                resume_state=normalized_resume_state,
                partial_match_data=match_data,
            )

        start_step = route_steps[start_step_idx]
        normalized_resume_state["resume_fragment"] = start_step.get("fragment")
        initial_url = build_match_url_with_fragment(
            match_url,
            start_step.get("group_key"),
            start_step.get("period_key"),
        )

        if not self.browser:
            await self.start()

        previous_context = self.context
        fresh_context = None
        page = None

        try:
            if self._fresh_context_per_event:
                try:
                    fresh_context = await self._create_fresh_context()
                except Exception as ctx_err:
                    logger.error(f"❌ Failed to create fresh context for {match_url}: {ctx_err}")
                    return ScrapeAttemptResult(
                        data=None,
                        resume_state=normalized_resume_state,
                        partial_match_data=match_data,
                        failed_reason="CONTEXT_CREATE_FAILED",
                        failed_step_idx=start_step_idx,
                    )
                self.context = fresh_context
                logger.info(
                    f"🔒 Fresh context created (session-{self._session_id}, "
                    f"ignore_https_errors={self._ignore_https_errors})"
                )
            elif not self.context:
                self.context = await self._create_fresh_context()

            if clear_state and self.context:
                await self._clear_browser_state()

            page = await self.context.new_page()
        except Exception as setup_err:
            logger.error(f"❌ Failed to set up page for {match_url}: {setup_err}")
            if fresh_context:
                try:
                    await fresh_context.close()
                except Exception:
                    pass
                self.context = previous_context
            return ScrapeAttemptResult(
                data=None,
                resume_state=normalized_resume_state,
                partial_match_data=match_data,
                failed_reason="PAGE_SETUP_FAILED",
                failed_step_idx=start_step_idx,
            )

        try:
            t0 = time.perf_counter()
            logger.info(
                f"🗺️ Scraping route for '{sport}': {route_step_count} steps "
                f"(resume step={start_step_idx}, fragment={normalized_resume_state.get('resume_fragment')})"
            )
            logger.info(f"🌐 Navigating to match: {initial_url}")

            self._original_debug_dir = self.debug_dir
            self._event_debug_dir_created = False

            response = None
            e_goto = None
            goto_error_code = None
            goto_error_summary = None
            try:
                response = await self._goto_fresh(
                    page,
                    initial_url,
                    wait_until="domcontentloaded",
                    timeout=Config.ODDSPORTAL_MATCH_GOTO_TIMEOUT_MS,
                )
            except Exception as e:
                e_goto = e
                goto_error_code, goto_error_summary = self._classify_goto_exception(e)
                logger.warning(
                    f"Navigation exception before page load: {goto_error_summary} "
                    f"({goto_error_code})."
                )
                logger.warning(f"Inspecting page state after navigation exception: {e}")

            t_goto = time.perf_counter()
            log_timing(f"Match page load ({initial_url}) took {t_goto - t0:.2f}s")

            if e_goto is not None:
                state = await self._collect_match_page_state(page)
                classification = self._classify_match_page_state(state)
                state_summary = self._format_page_state_summary(state, classification)
                logger.info(f"Page state after navigation exception: {state_summary}")

                if classification == "NO_SHELL":
                    reason_prefix = goto_error_code or "GOTO_FAILED"
                    reason = f"{reason_prefix}_NO_SHELL"
                    logger.error(f"FAST FAIL: {reason}. {state_summary}")
                    if getattr(Config, 'ODDSPORTAL_SAVE_DEBUG_ON_GOTO_TIMEOUT', True):
                        await self._save_debug_artifacts(
                            page,
                            reason,
                            {"error": str(e_goto), **self._resume_state_for_debug(normalized_resume_state)},
                        )
                    return ScrapeAttemptResult(
                        data=None,
                        resume_state=normalized_resume_state,
                        partial_match_data=match_data,
                        failed_reason=reason,
                        failed_step_idx=start_step_idx,
                    )
                elif classification == "DATA_RENDERED":
                    logger.info("Navigation threw, but data is already rendered. Continuing.")
                else:
                    logger.info(
                        f"Navigation exception left page in {classification}. "
                        "Falling through to smart-wait/render checks."
                    )

            try:
                page_title = await page.title()
                if any(blocked in page_title for blocked in ["Access Denied", "Just a moment...", "Attention Required!", "Security check", "Cloudflare"]):
                    reason = "CLOUDFLARE_BLOCK"
                    logger.error(f"FAST FAIL: {reason}")
                    await self._save_debug_artifacts(page, reason, self._resume_state_for_debug(normalized_resume_state))
                    return ScrapeAttemptResult(
                        data=None,
                        resume_state=normalized_resume_state,
                        partial_match_data=match_data,
                        failed_reason=reason,
                        failed_step_idx=start_step_idx,
                    )
            except Exception:
                pass

            if response is not None and response.status >= 400:
                reason = f"HTTP_{response.status}"
                logger.error(f"FAST FAIL: {reason}")
                await self._save_debug_artifacts(page, reason, self._resume_state_for_debug(normalized_resume_state))
                return ScrapeAttemptResult(
                    data=None,
                    resume_state=normalized_resume_state,
                    partial_match_data=match_data,
                    failed_reason=reason,
                    failed_step_idx=start_step_idx,
                )

            first_extract_fn = start_step.get("extract_fn", "standard")
            js_timeout = getattr(Config, 'ODDSPORTAL_FAST_FAIL_EMPTY_TIMEOUT_MS', 15000)
            js_observer = f"""
            () => new Promise(resolve => {{
                setTimeout(() => {{
                    const shell1 = document.querySelector('div.event-container');
                    const shell2 = document.querySelector('ul.visible-links.odds-tabs li');
                    const shell3 = document.querySelector('div[data-testid="kickoff-events-nav"]');
                    const has_shell = shell1 || shell2 || shell3;

                    const skeleton = document.querySelector('div.animate-pulse.bg-gray-light');
                    const data1 = document.querySelector('div.border-black-borders.flex.h-9');
                    const data2 = document.querySelector('div[data-testid="over-under-collapsed-row"]');
                    const data3 = document.querySelector('div[data-testid="asian-handicap-collapsed-row"]');
                    const data4 = document.querySelector('div[data-testid="over-under-expanded-row"]');
                    const data5 = document.querySelector('div.odds-cell');
                    const has_data = data1 || data2 || data3 || data4 || data5;

                    if (!has_data) {{
                        if (!shell1) {{
                            resolve('Missing event-container');
                        }} else if (shell1.children.length === 0) {{
                            resolve('Event container stayed empty');
                        }} else if (has_shell && skeleton) {{
                            resolve('Shell loaded, skeleton persisted, no data rows');
                        }} else if (has_shell && !skeleton) {{
                            resolve('Shell loaded, no data rows');
                        }} else {{
                            resolve('Unknown partial page state');
                        }}
                    }} else {{
                        resolve(null);
                    }}
                }}, {js_timeout});
            }})
            """

            fast_fail_task = asyncio.create_task(page.evaluate(js_observer))
            render_task = asyncio.create_task(
                self._wait_for_market_render(
                    page,
                    first_extract_fn,
                    timeout_ms=getattr(Config, 'ODDSPORTAL_MARKET_RENDER_TIMEOUT_MS', 60000),
                )
            )

            done, pending = await asyncio.wait([fast_fail_task, render_task], return_when=asyncio.FIRST_COMPLETED)
            for pending_task in pending:
                pending_task.cancel()

            if fast_fail_task in done:
                ff_reason = fast_fail_task.result()
                if ff_reason is not None:
                    if ff_reason in ["Shell loaded, skeleton persisted, no data rows", "Shell loaded, no data rows"]:
                        logger.info(f"⏳ JS Observer detected '{ff_reason}'. Routing to shell-grace logic.")
                        if getattr(Config, 'ODDSPORTAL_ENABLE_SHELL_GRACE', True):
                            rendered = await self._wait_for_market_render(
                                page,
                                first_extract_fn,
                                timeout_ms=getattr(Config, 'ODDSPORTAL_SHELL_GRACE_TIMEOUT_MS', 8000),
                            )
                            if not rendered:
                                reason_code = "SHELL_WITH_SKELETON_NO_DATA" if "skeleton persisted" in ff_reason else "SHELL_WITH_NAV_NO_DATA"
                                logger.error(f"FAST FAIL: {reason_code} (after shell grace).")
                                await self._save_debug_artifacts(page, reason_code, self._resume_state_for_debug(normalized_resume_state))
                                return ScrapeAttemptResult(
                                    data=None,
                                    resume_state=normalized_resume_state,
                                    partial_match_data=match_data,
                                    failed_reason=reason_code,
                                    failed_step_idx=start_step_idx,
                                )
                            logger.info("✅ Shell-grace successful.")
                        else:
                            reason_code = "SHELL_WITH_SKELETON_NO_DATA" if "skeleton persisted" in ff_reason else "SHELL_WITH_NAV_NO_DATA"
                            logger.error(f"FAST FAIL: {reason_code} (shell-grace disabled).")
                            await self._save_debug_artifacts(page, reason_code, self._resume_state_for_debug(normalized_resume_state))
                            return ScrapeAttemptResult(
                                data=None,
                                resume_state=normalized_resume_state,
                                partial_match_data=match_data,
                                failed_reason=reason_code,
                                failed_step_idx=start_step_idx,
                            )
                    else:
                        reason_code = "FAST_FAIL_" + ff_reason.replace(" ", "_").upper()
                        state = await self._collect_match_page_state(page)
                        summary = self._format_page_state_summary(state, self._classify_match_page_state(state))
                        logger.error(f"FAST FAIL: {reason_code}. {summary}")
                        await self._save_debug_artifacts(page, reason_code, self._resume_state_for_debug(normalized_resume_state))
                        return ScrapeAttemptResult(
                            data=None,
                            resume_state=normalized_resume_state,
                            partial_match_data=match_data,
                            failed_reason=reason_code,
                            failed_step_idx=start_step_idx,
                        )

            if render_task in done:
                rendered = render_task.result()
                if not rendered:
                    state = await self._collect_match_page_state(page)
                    classification = self._classify_match_page_state(state)
                    state_summary = self._format_page_state_summary(state, classification)
                    reason = f"MATCH_RENDER_TIMEOUT_{classification}"
                    logger.error(f"❌ Match page failure: {reason}. {state_summary}")
                    await self._save_debug_artifacts(page, reason, self._resume_state_for_debug(normalized_resume_state))
                    return ScrapeAttemptResult(
                        data=None,
                        resume_state=normalized_resume_state,
                        partial_match_data=match_data,
                        failed_reason=reason,
                        failed_step_idx=start_step_idx,
                    )

            log_timing(f"Primary rendering + wait race took {time.perf_counter() - t_goto:.2f}s")

            t_cookie = time.perf_counter()
            for btn_sel in [
                "#onetrust-accept-btn-handler",
                "button.onetrust-close-btn-handler",
                "button:has-text('I Accept')",
                "button:has-text('Accept All')",
                "button:has-text('Accept')",
            ]:
                try:
                    btn = await page.query_selector(btn_sel)
                    if btn:
                        await btn.click()
                        logger.debug(f"🍪 Dismissed consent via: {btn_sel}")
                        await asyncio.sleep(0.5)
                        break
                except Exception:
                    continue
            log_timing(f"Dismissing cookie banners took {time.perf_counter() - t_cookie:.2f}s")

            await page.evaluate("window.scrollTo(0, 500)")
            await asyncio.sleep(1.0)

            completed_step_keys = set(normalized_resume_state.get("completed_step_keys", []))
            for step in route_steps[start_step_idx:]:
                step_idx = step.get("step_idx", 0)
                step_key = step.get("step_key")
                group_key = step.get("group_key")
                period_key = step.get("period_key")
                db_market_group = step.get("db_market_group", "1X2")
                db_market_period = step.get("db_market_period", "Full Time")
                db_market_name = step.get("db_market_name", db_market_period)

                if step_key in completed_step_keys:
                    normalized_resume_state["next_step_idx"] = max(
                        normalized_resume_state.get("next_step_idx", 0),
                        step_idx + 1,
                    )
                    continue

                t_period = time.perf_counter()
                logger.info(
                    f"📊 [step {step_idx + 1}/{route_step_count}] Extracting: "
                    f"{db_market_group} / {db_market_period}"
                )

                is_first_step_in_attempt = step_idx == start_step_idx
                prev_step = route_steps[step_idx - 1] if step_idx > 0 else None

                if not is_first_step_in_attempt:
                    if group_key and (not prev_step or prev_step.get("group_key") != group_key):
                        tab_label = step.get("group_display") or OP_GROUPS_DISPLAY.get(group_key, group_key)
                        switched_group = await self._click_market_group_tab(page, tab_label)
                        if not switched_group:
                            reason_code = f"GROUP_SWITCH_FAILED_{group_key}"
                            self._mark_step_failed(normalized_resume_state, step, reason_code)
                            debug_extra = {"step": step, **self._resume_state_for_debug(normalized_resume_state)}
                            await self._save_debug_artifacts(page, reason_code, debug_extra)
                            return ScrapeAttemptResult(
                                data=None,
                                resume_state=normalized_resume_state,
                                partial_match_data=match_data,
                                failed_reason=reason_code,
                                failed_step_idx=step_idx,
                            )

                    should_click_period = False
                    if group_key:
                        if prev_step and prev_step.get("group_key") == group_key:
                            should_click_period = step.get("period_key") != prev_step.get("period_key")
                        elif step.get("period_idx", 0) > 0:
                            should_click_period = True

                    if should_click_period:
                        logger.info(f"🔀 Switching to period: {db_market_period} (tab click)")
                        t_frag = time.perf_counter()
                        tab_clicked = await self._click_period_tab(page, step.get("period_display", db_market_period))
                        if not tab_clicked:
                            reason_code = f"PERIOD_SWITCH_FAILED_{period_key}"
                            self._mark_step_failed(normalized_resume_state, step, reason_code)
                            debug_extra = {"step": step, **self._resume_state_for_debug(normalized_resume_state)}
                            await self._save_debug_artifacts(page, reason_code, debug_extra)
                            return ScrapeAttemptResult(
                                data=None,
                                resume_state=normalized_resume_state,
                                partial_match_data=match_data,
                                failed_reason=reason_code,
                                failed_step_idx=step_idx,
                            )
                        log_timing(f"Period tab-click navigation to {period_key} took {time.perf_counter() - t_frag:.2f}s")

                extract_fn = step.get("extract_fn", "standard")
                t_extract = time.perf_counter()
                if extract_fn == "over_under":
                    period_data = await self._extract_data_over_under(page, match_url)
                elif extract_fn == "asian_handicap":
                    period_data = await self._extract_data_asian_handicap(page, match_url)
                else:
                    period_data = await self._extract_data(page, match_url)
                log_timing(f"JS extraction for {db_market_period} took {time.perf_counter() - t_extract:.2f}s")

                if not period_data:
                    reason_code = f"PERIOD_DATA_EMPTY_{period_key}"
                    self._mark_step_failed(normalized_resume_state, step, reason_code)
                    debug_extra = {"step": step, **self._resume_state_for_debug(normalized_resume_state)}
                    await self._save_debug_artifacts(page, reason_code, debug_extra)
                    return ScrapeAttemptResult(
                        data=None,
                        resume_state=normalized_resume_state,
                        partial_match_data=match_data,
                        failed_reason=reason_code,
                        failed_step_idx=step_idx,
                    )

                if match_data.home_team in (None, "", "Unknown"):
                    match_data.home_team = period_data.home_team
                if match_data.away_team in (None, "", "Unknown"):
                    match_data.away_team = period_data.away_team

                if (
                    self.debug_dir
                    and not self._event_debug_dir_created
                    and match_data.home_team
                    and match_data.away_team
                ):
                    slug = f"{match_data.home_team}-vs-{match_data.away_team}".lower().replace(" ", "-").replace("/", "-")
                    event_debug_dir = os.path.join(self.debug_dir, f"debug_{slug}")
                    try:
                        os.makedirs(event_debug_dir, exist_ok=True)
                        self.debug_dir = event_debug_dir
                        self._event_debug_dir_created = True
                        logger.info(f"📂 Event debug directory: {self.debug_dir}")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to create event debug directory: {e}")

                logger.info(f"✅ Extracted {len(period_data.bookie_odds)} bookies for {db_market_period}")

                target_bookie_obj = None
                for priority_name in PRIORITY_BOOKIES:
                    for b in period_data.bookie_odds:
                        if priority_name.lower() in b.name.lower() or b.name.lower() in priority_name.lower():
                            target_bookie_obj = b
                            break
                    if target_bookie_obj:
                        break

                if target_bookie_obj:
                    logger.info(f"🎯 Extracting opening odds via hover for: {target_bookie_obj.name} ({db_market_period})")
                    t_hover = time.perf_counter()
                    opening = await self._extract_opening_odds_for_bookie(page, target_bookie_obj.name)
                    log_timing(f"Hover extraction for {target_bookie_obj.name} ({db_market_period}) took {time.perf_counter() - t_hover:.2f}s")

                    is_ou = db_market_group == "Over/Under"
                    lbl_1 = "Over" if is_ou else "1"
                    lbl_2 = "Under" if is_ou else "2"

                    # Determine expected keys based on the same 2-way / 3-way signal used by extraction.
                    # In 2-way markets, odds_x is stored as None (or empty / "-"), so only expect 1 and 2.
                    _three_way_market = (not is_ou) and (getattr(target_bookie_obj, 'odds_x', None) not in (None, '', '-'))

                    if is_ou or not _three_way_market:
                        expected_keys = ['1', '2']
                    else:
                        expected_keys = ['1', 'X', '2']

                    if opening:
                        # Assign only keys with real values
                        if opening.get('1'):
                            target_bookie_obj.initial_odds_1 = opening['1'][0]
                            target_bookie_obj.movement_odds_time = opening['1'][1]
                        if opening.get('X'):
                            target_bookie_obj.initial_odds_x = opening['X'][0]
                            if not getattr(target_bookie_obj, 'movement_odds_time', None) and opening['X'][1]:
                                target_bookie_obj.movement_odds_time = opening['X'][1]
                        if opening.get('2'):
                            target_bookie_obj.initial_odds_2 = opening['2'][0]
                            if not getattr(target_bookie_obj, 'movement_odds_time', None) and opening['2'][1]:
                                target_bookie_obj.movement_odds_time = opening['2'][1]

                        extracted_keys = [k for k in expected_keys if opening.get(k)]
                        missing_keys = [k for k in expected_keys if not opening.get(k)]

                        if not missing_keys:
                            lbl_x_part = f" X={target_bookie_obj.initial_odds_x}" if 'X' in expected_keys else ""
                            logger.info(
                                f"✅ FULL_SUCCESS Opening odds ({db_market_period}): "
                                f"{lbl_1}={target_bookie_obj.initial_odds_1}{lbl_x_part} "
                                f"{lbl_2}={target_bookie_obj.initial_odds_2} "
                                f"(Time: {target_bookie_obj.movement_odds_time})"
                            )
                        else:
                            lbl_x_part = f" X={target_bookie_obj.initial_odds_x}" if 'X' in expected_keys else ""
                            logger.warning(
                                f"⚠️ PARTIAL_SUCCESS Opening odds ({db_market_period}): "
                                f"{lbl_1}={target_bookie_obj.initial_odds_1}{lbl_x_part} "
                                f"{lbl_2}={target_bookie_obj.initial_odds_2} "
                                f"(missing: {missing_keys})"
                            )
                    else:
                        logger.warning(
                            f"⚠️ TOTAL_FAIL Opening odds ({db_market_period}): "
                            f"could not extract any opening odds for {target_bookie_obj.name}"
                        )
                else:
                    logger.info(f"ℹ️ No priority bookie found for {db_market_period}, skipping opening odds hover")

                extraction_betfair = None
                if step.get("betfair_enabled") and period_data.betfair:
                    logger.info(f"🎯 Extracting Betfair Exchange opening odds via hover ({db_market_period})")
                    t_bf = time.perf_counter()
                    bf_opening = await self._extract_opening_odds_betfair(page)
                    log_timing(f"Betfair hover extraction ({db_market_period}) took {time.perf_counter() - t_bf:.2f}s")

                    if bf_opening:
                        # Assign only keys with real values
                        if bf_opening.get('back_1'):
                            period_data.betfair.initial_back_1 = bf_opening['back_1'][0]
                            period_data.betfair.movement_odds_time = bf_opening['back_1'][1]
                        if bf_opening.get('back_x'):
                            period_data.betfair.initial_back_x = bf_opening['back_x'][0]
                        if bf_opening.get('back_2'):
                            period_data.betfair.initial_back_2 = bf_opening['back_2'][0]
                        if bf_opening.get('lay_1'):
                            period_data.betfair.initial_lay_1 = bf_opening['lay_1'][0]
                        if bf_opening.get('lay_x'):
                            period_data.betfair.initial_lay_x = bf_opening['lay_x'][0]
                        if bf_opening.get('lay_2'):
                            period_data.betfair.initial_lay_2 = bf_opening['lay_2'][0]

                        # Determine expected keys from layout already detected
                        _bf_three_way = any(k in bf_opening for k in ('back_x', 'lay_x')) or (
                            period_data.betfair.back_x not in (None, "-", "")
                        )
                        if _bf_three_way:
                            bf_expected_back = ['back_1', 'back_x', 'back_2']
                            bf_expected_lay  = ['lay_1', 'lay_x', 'lay_2']
                        else:
                            bf_expected_back = ['back_1', 'back_2']
                            bf_expected_lay  = ['lay_1', 'lay_2']
                        bf_expected = bf_expected_back + bf_expected_lay

                        bf_extracted = [k for k in bf_expected if bf_opening.get(k)]
                        bf_missing   = [k for k in bf_expected if not bf_opening.get(k)]

                        if not bf_missing:
                            logger.info(
                                f"✅ FULL_SUCCESS Betfair opening odds ({db_market_period}): "
                                f"Back 1={period_data.betfair.initial_back_1} "
                                f"X={period_data.betfair.initial_back_x} "
                                f"2={period_data.betfair.initial_back_2} | "
                                f"Lay 1={period_data.betfair.initial_lay_1} "
                                f"X={period_data.betfair.initial_lay_x} "
                                f"2={period_data.betfair.initial_lay_2}"
                            )
                        else:
                            logger.warning(
                                f"⚠️ PARTIAL_SUCCESS Betfair opening odds ({db_market_period}): "
                                f"Back 1={period_data.betfair.initial_back_1} "
                                f"X={period_data.betfair.initial_back_x} "
                                f"2={period_data.betfair.initial_back_2} | "
                                f"Lay 1={period_data.betfair.initial_lay_1} "
                                f"X={period_data.betfair.initial_lay_x} "
                                f"2={period_data.betfair.initial_lay_2} "
                                f"(missing: {bf_missing})"
                            )
                    else:
                        logger.warning(
                            f"⚠️ TOTAL_FAIL Betfair opening odds ({db_market_period}): "
                            f"could not extract any opening odds from Betfair Exchange"
                        )
                    extraction_betfair = period_data.betfair

                extraction = MarketExtraction(
                    market_group=db_market_group,
                    market_period=db_market_period,
                    market_name=db_market_name,
                    bookie_odds=period_data.bookie_odds,
                    betfair=extraction_betfair,
                )
                match_data.extractions.append(extraction)
                self._mark_step_completed(normalized_resume_state, step, match_data)
                completed_step_keys.add(step_key)
                next_idx = normalized_resume_state.get("next_step_idx", 0)
                normalized_resume_state["resume_fragment"] = (
                    route_steps[next_idx].get("fragment")
                    if next_idx < route_step_count
                    else None
                )
                normalized_resume_state["partial_extraction_count"] = len(match_data.extractions)
                log_timing(f"Total period extraction for {db_market_period} took {time.perf_counter() - t_period:.2f}s")

            self._ensure_legacy_match_level_fields(match_data)
            total_duration = time.perf_counter() - t0
            match_data.extraction_time_ms = total_duration * 1000
            log_timing(f"Total match scraping process (scrape_match_attempt) took {total_duration:.2f}s")

            total_bookies = sum(len(e.bookie_odds) for e in match_data.extractions)
            logger.info(f"✅ Completed scraping {match_data.home_team} vs {match_data.away_team}: {len(match_data.extractions)} periods, {total_bookies} bookie entries total")
            self._log_structured_recap(match_data)
            return ScrapeAttemptResult(
                data=match_data,
                resume_state=normalized_resume_state,
                partial_match_data=match_data,
            )

        except Exception as e:
            logger.error(f"❌ Error scraping match {match_url}: {e}")
            return ScrapeAttemptResult(
                data=None,
                resume_state=normalized_resume_state,
                partial_match_data=match_data,
                failed_reason=f"SCRAPE_EXCEPTION_{type(e).__name__}",
                failed_step_idx=normalized_resume_state.get("next_step_idx"),
            )
        finally:
            try:
                if page:
                    await page.close()
            except Exception:
                pass
            if fresh_context:
                try:
                    await fresh_context.close()
                except Exception:
                    pass
                self.context = previous_context

            if hasattr(self, '_original_debug_dir'):
                self.debug_dir = self._original_debug_dir


    def _parse_opening_odds_from_modal_html(self, modal_html: str, label: str = "") -> Optional[Tuple[str, str]]:
        """
        Parse the opening odds value from the tooltip modal HTML.

        Contract: returns (opening_val, movement_time) only when a real opening odd was found.
        Never returns (None, movement_time). Returns None on any failure.

        The modal HTML contains:
          - movement time in '<div class="text-[10px] font-normal">'
          - an 'Opening odds:' section with a flex row: [date/time div] [font-bold value div]
          Betfair adds a third div with volume like "(0)" which must be ignored.
        """
        try:
            movement_time = None

            # Save modal HTML if testing mode and debug dir are active
            if self.testing_mode and self.debug_dir:
                try:
                    debug_filename = f"modal_{label}.html" if label else "modal_unknown.html"
                    debug_filename = "".join([c if c.isalnum() or c in "._-" else "_" for c in debug_filename])
                    debug_path = os.path.join(self.debug_dir, debug_filename)
                    with open(debug_path, "w", encoding="utf-8") as f:
                        f.write(modal_html)
                    logger.debug(f"💾 Saved modal HTML: {debug_filename}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to save modal HTML for {label}: {e}")

            # Extract movement time — anchored to the top of the tooltip (before Opening odds block)
            idx_opening_anchor = modal_html.find('Opening odds')
            pre_section = modal_html[:idx_opening_anchor] if idx_opening_anchor != -1 else modal_html

            time_matches = re.findall(
                r'<div[^>]*text-\[10px\][^>]*font-normal[^>]*>\s*([^<]+)\s*</div>', pre_section
            )
            if time_matches:
                movement_time = time_matches[0].strip()
            else:
                date_matches = re.findall(
                    r'(?:>|^\s*)(\d{1,2}\s+[A-Za-z]{3},\s+\d{2}:\d{2})(?:<|\s*$)', pre_section
                )
                if date_matches:
                    movement_time = date_matches[0].strip()

            # Must have Opening odds section to proceed
            if 'Opening odds' not in modal_html:
                return None

            idx = modal_html.find('Opening odds')
            section = modal_html[idx:idx + 600]

            # Strategy 1: find flex row pattern — [any div] followed by [font-bold div with number]
            # Captures (datetime_text, bold_val) pairs; ignore volume entries like "(28)" or "(0)"
            matches = re.findall(
                r'<div[^>]*>\s*([^<]+)\s*</div>\s*<div[^>]*font-bold[^>]*>([\d.]+)</div>',
                section
            )
            extracted_val = None
            for _, val in matches:
                val = val.strip()
                try:
                    f = float(val)
                    if 1.0 <= f <= 1001.0:
                        extracted_val = val
                        break
                except ValueError:
                    continue

            # Strategy 2: any font-bold numeric in opening section (fallback)
            if not extracted_val:
                bold_matches = re.findall(r'<div[^>]*font-bold[^>]*>([\d.]+)</div>', section)
                for val in bold_matches:
                    val = val.strip()
                    try:
                        f = float(val)
                        if 1.0 <= f <= 1001.0:
                            extracted_val = val
                            break
                    except ValueError:
                        continue

            # Only succeed when we have a real opening odd value
            if extracted_val:
                return (extracted_val, movement_time)

            # No valid opening odd found — return None (never (None, time))
            return None

        except Exception as e:
            logger.warning(f"Error parsing opening odds from modal: {e}")
            return None

    async def _dismiss_odds_movement_tooltip(self, page: Page) -> None:
        """
        Move the mouse off the current hover target so Vue.js dismisses the tooltip,
        then wait briefly for the tooltip to detach from the DOM.
        Errors are swallowed — this helper is never fatal.
        """
        try:
            await page.mouse.move(0, 0)
            await page.wait_for_timeout(300)
            try:
                await page.wait_for_selector(
                    "h3:has-text('Odds movement')", state="detached", timeout=1500
                )
            except Exception:
                pass
        except Exception:
            pass

    async def _wait_for_scoped_tooltip_html(self, page: Page, timeout_ms: int = 4000) -> Optional[str]:
        """
        Old tooltip lookup behavior:
        after hover, find the visible 'Odds movement' tooltip globally on the page,
        then return the inner_html of its parent element.
        """
        try:
            odds_movement_h3 = await page.wait_for_selector(
                "h3:has-text('Odds movement')",
                state="visible",
                timeout=timeout_ms
            )

            is_visible = await page.is_visible("h3:has-text('Odds movement')")
            if not is_visible:
                return None

            modal_wrapper = await odds_movement_h3.evaluate_handle(
                "node => node.parentElement"
            )
            modal_el = modal_wrapper.as_element()
            if modal_el:
                return await modal_el.inner_html()

        except Exception:
            return None

        return None

    async def _get_hover_target_from_container(self, container):
        """
        Resolve the hover target from an odd-container: prefers the inner
        font-bold flex div, falls back to the container itself.
        """
        try:
            inner = await container.query_selector("div.flex-center.flex-col.font-bold")
            return inner if inner else container
        except Exception:
            return container

    async def _find_bookie_row(self, page: Page, bookie_name: str):
        """
        Locate the bookie row element for *bookie_name*, checking standard rows first
        then any expanded-context rows.  Returns the element or None.
        """
        # Standard rows (h-9)
        target_row = await page.query_selector(
            f"div.border-black-borders.flex.h-9:has(a[title*='{bookie_name}'])"
        )
        if not target_row:
            target_row = await page.query_selector(
                f"div.border-black-borders.flex.h-9:has(img[alt*='{bookie_name}'])"
            )
        if not target_row:
            rows = await page.query_selector_all("div.border-black-borders.flex.h-9")
            for row in rows:
                name_link = await row.query_selector("a[title]")
                if name_link:
                    title = await name_link.get_attribute("title")
                    if title and bookie_name.lower() in title.lower():
                        target_row = row
                        break
                img = await row.query_selector("img[alt]")
                if img:
                    alt = await img.get_attribute("alt")
                    if alt and bookie_name.lower() in alt.lower():
                        target_row = row
                        break
        # Fallback: expanded-context rows (any height variant)
        if not target_row:
            rows = await page.query_selector_all("div.border-black-borders.flex")
            for row in rows:
                name_link = await row.query_selector("a[title]")
                if name_link:
                    title = await name_link.get_attribute("title")
                    if title and bookie_name.lower() in title.lower():
                        target_row = row
                        break
                img = await row.query_selector("img[alt]")
                if img:
                    alt = await img.get_attribute("alt")
                    if alt and bookie_name.lower() in alt.lower():
                        target_row = row
                        break
        return target_row

    async def _extract_opening_odds_for_bookie(
        self, page: Page, bookie_name: str
    ) -> Optional[Dict[str, Optional[Tuple[str, str]]]]:
        """
        Hover over each odds cell for a specific bookie to trigger the tooltip,
        then extract the opening odds from the scoped 'Odds movement' tooltip.

        Key changes vs original:
          - row, odd-containers and hover target are re-resolved on every retry
            (no stale handles between attempts).
          - tooltip is captured from within the same odd-container that was hovered
            (scoped, not global page selector).
          - result dict only contains keys with a real opening value; None is never stored.
          - returns None when no key has a real value.
        """
        try:
            await page.wait_for_timeout(500)

            # Dismiss overlay-bookie-modal (it intercepts pointer events and blocks hover)
            await page.evaluate("""
                () => { document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove()); }
            """)

            # Initial row lookup to determine choice count — re-resolved on each retry below
            initial_row = await self._find_bookie_row(page, bookie_name)
            if not initial_row:
                logger.warning(f"⚠️ Bookie row not found for: {bookie_name}")
                return None

            initial_containers = await initial_row.query_selector_all("div[data-testid='odd-container']")
            if not initial_containers:
                logger.warning(f"⚠️ No odd containers found in row for: {bookie_name}")
                return None

            is_three_way = len(initial_containers) >= 3
            choice_keys = ['1', 'X', '2'] if is_three_way else ['1', '2']
            logger.info(f"🖱️ Hovering {len(choice_keys)} odds cells for {bookie_name}")

            result: Dict[str, Tuple[str, str]] = {}  # only real values stored

            for i, choice in enumerate(choice_keys):
                max_retries = 3
                t_hover_cell = time.perf_counter()

                # Dismiss any lingering tooltip before starting this choice
                await self._dismiss_odds_movement_tooltip(page)

                for attempt in range(max_retries):
                    try:
                        # Re-resolve row and containers on every attempt
                        target_row = await self._find_bookie_row(page, bookie_name)
                        if not target_row:
                            logger.debug(f"  Cell {choice}: row not found (attempt {attempt + 1})")
                            await asyncio.sleep(0.4)
                            continue

                        containers = await target_row.query_selector_all("div[data-testid='odd-container']")
                        if not containers or i >= len(containers):
                            logger.debug(f"  Cell {choice}: container index {i} out of range (attempt {attempt + 1})")
                            await asyncio.sleep(0.4)
                            continue

                        current_container = containers[i]

                        # Validate geometry — skip attempt if not rendered
                        bbox_check = await current_container.bounding_box()
                        if not bbox_check:
                            logger.debug(f"  Cell {choice}: bounding_box is None (attempt {attempt + 1}), skipping")
                            await asyncio.sleep(0.4)
                            continue

                        hover_target = await self._get_hover_target_from_container(current_container)

                        await hover_target.scroll_into_view_if_needed()
                        await page.evaluate("window.scrollBy(0, -150)")
                        await page.wait_for_timeout(200)

                        # Remove overlays before every attempt
                        await page.evaluate("""
                            () => {
                                document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());
                                const onetrust = document.getElementById('onetrust-banner-sdk');
                                if (onetrust) onetrust.remove();
                                const shade = document.querySelector('.onetrust-pc-dark-filter');
                                if (shade) shade.remove();
                            }
                        """)

                        # Humanized mouse movement (wiggle) to trigger Vue.js mouseenter
                        bbox = await hover_target.bounding_box()
                        if bbox:
                            cx = bbox['x'] + bbox['width'] / 2
                            cy = bbox['y'] + bbox['height'] / 2
                            await page.mouse.move(cx - 15, cy - 15)
                            await page.wait_for_timeout(50)
                            await page.mouse.move(cx, cy)
                            await page.wait_for_timeout(50)

                        try:
                            await hover_target.hover(force=True, timeout=1500)
                        except Exception:
                            pass

                        # Fire JS hover events to guarantee Vue.js tooltip trigger
                        await page.evaluate("""
                            (el) => {
                                el.dispatchEvent(new PointerEvent('pointerover', {bubbles: true, cancelable: true, pointerId: 1}));
                                el.dispatchEvent(new PointerEvent('pointerenter', {bubbles: true, cancelable: true, pointerId: 1}));
                                el.dispatchEvent(new MouseEvent('mouseover',  {bubbles: true, cancelable: true}));
                                el.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true, cancelable: true}));
                                el.dispatchEvent(new MouseEvent('mousemove',  {bubbles: true, cancelable: true}));
                            }
                        """, hover_target)

                        # Wait for tooltip using the old global page-level lookup
                        wait_ms = 3000 + (attempt * 1000)
                        html = await self._wait_for_scoped_tooltip_html(page, timeout_ms=wait_ms)

                        if not html:
                            logger.debug(f"  Cell {choice}: global tooltip not found (attempt {attempt + 1}/{max_retries})")
                            await self._dismiss_odds_movement_tooltip(page)
                            continue

                        label = f"{bookie_name}_{choice}"
                        parsed = self._parse_opening_odds_from_modal_html(html, label=label)

                        if parsed:
                            opening_val, opening_time = parsed
                            result[choice] = (opening_val, opening_time)
                            logger.debug(f"  Cell {choice}: opening={opening_val} at {opening_time} (attempt {attempt + 1})")
                        else:
                            logger.debug(f"  Cell {choice}: tooltip found but no opening odd parsed (attempt {attempt + 1})")

                        # Dismiss tooltip and break retry loop regardless of parse outcome
                        await self._dismiss_odds_movement_tooltip(page)
                        break

                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.debug(f"  Cell {choice}: hover error (attempt {attempt + 1}), retrying: {e}")
                            await self._dismiss_odds_movement_tooltip(page)
                            continue
                        logger.warning(f"  Error hovering cell {choice} for {bookie_name}: {e}")

                # Dismiss after all retries for this choice
                await self._dismiss_odds_movement_tooltip(page)

                if choice in result:
                    log_timing(f"Hovering and extracting '{choice}' opening odd for {bookie_name} took {time.perf_counter() - t_hover_cell:.2f}s")
                else:
                    log_timing(f"Failed to extract '{choice}' opening odd for {bookie_name} after {time.perf_counter() - t_hover_cell:.2f}s")

            return result if result else None

        except Exception as e:
            logger.error(f"Error in _extract_opening_odds_for_bookie({bookie_name}): {e}")
            return None


    async def _extract_opening_odds_betfair(self, page: Page) -> Optional[Dict[str, Optional[Tuple[str, str]]]]:
        """
        Extract opening/initial odds for Betfair Exchange by hovering over its odds cells.

        Key changes vs original:
          - exchange section, containers and hover target are re-resolved on every retry.
          - tooltip is captured scoped to the hovered odd-container, not via global h3 selector.
          - visibility is not inferred from CSS classes (Betfair keeps 'hidden' on tooltip root).
          - result dict only contains keys with a real opening value; None is never stored.
          - returns None when no key has a real value.
        """
        try:
            # Initial section lookup to determine layout — re-resolved per retry below
            exchange_section = await page.query_selector("div[data-testid='betting-exchanges-section']")
            if not exchange_section:
                logger.warning("⚠️ Betfair Exchange section not found for hover extraction")
                return None

            # Give Vue.js time to fully render all columns (incl. X/Draw) before committing to layout
            await page.wait_for_timeout(500)

            odd_containers_init = await exchange_section.query_selector_all("div[data-testid='odd-container']")
            if not odd_containers_init:
                logger.warning("⚠️ No odd containers found in Betfair Exchange section")
                return None

            def _build_betfair_choice_to_index(container_count: int) -> Dict[str, int]:
                if container_count >= 6:
                    return {
                        'back_1': 0, 'back_x': 1, 'back_2': 2,
                        'lay_1': 3, 'lay_x': 4, 'lay_2': 5,
                    }
                if container_count >= 4:
                    return {
                        'back_1': 0, 'back_2': 1,
                        'lay_1': 2, 'lay_2': 3,
                    }
                return {}

            initial_mapping = _build_betfair_choice_to_index(len(odd_containers_init))
            if not initial_mapping:
                logger.warning(f"⚠️ Unexpected Betfair container count: {len(odd_containers_init)}")
                return None

            logger.debug(
                f"  Betfair: {len(odd_containers_init)} containers detected -> "
                f"{'3-way' if len(odd_containers_init) >= 6 else '2-way'}"
            )
            logger.info("🖱️ Hovering Betfair cells (Back & Lay) with live layout remap")

            # Tracks which logical cells have already been processed.
            # This allows X/Draw to be picked up later if the layout finishes rendering mid-loop.
            processed_choices = set()

            # Dismiss overlays before starting the hover loop
            await page.evaluate("""
                () => {
                    document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());
                    const onetrust = document.getElementById('onetrust-banner-sdk');
                    if (onetrust) onetrust.remove();
                    const shade = document.querySelector('.onetrust-pc-dark-filter');
                    if (shade) shade.remove();
                }
            """)

            result: Dict[str, Tuple[str, str]] = {}  # only real values stored

            while True:
                ex_sec_now = await page.query_selector("div[data-testid='betting-exchanges-section']")
                if not ex_sec_now:
                    logger.warning("⚠️ Betfair Exchange section disappeared before hover extraction")
                    break

                containers_now = await ex_sec_now.query_selector_all("div[data-testid='odd-container']")
                live_mapping = _build_betfair_choice_to_index(len(containers_now))
                if not live_mapping:
                    logger.warning(
                        f"⚠️ Unexpected Betfair container count during hover extraction: {len(containers_now)}"
                    )
                    break

                pending_choices = [k for k in live_mapping.keys() if k not in processed_choices]
                if not pending_choices:
                    break

                choice = pending_choices[0]
                t_hover_bf = time.perf_counter()
                max_retries = 3

                # Dismiss any lingering tooltip before starting this choice
                await self._dismiss_odds_movement_tooltip(page)

                for attempt in range(max_retries):
                    try:
                        # Re-resolve exchange section and containers on every attempt
                        ex_sec = await page.query_selector("div[data-testid='betting-exchanges-section']")
                        if not ex_sec:
                            logger.debug(f"  Betfair {choice}: exchange section not found (attempt {attempt + 1})")
                            await asyncio.sleep(0.4)
                            continue

                        containers = await ex_sec.query_selector_all("div[data-testid='odd-container']")
                        current_mapping = _build_betfair_choice_to_index(len(containers))
                        if not current_mapping:
                            logger.debug(
                                f"  Betfair {choice}: unexpected container count {len(containers)} "
                                f"(attempt {attempt + 1})"
                            )
                            await asyncio.sleep(0.4)
                            continue

                        if choice not in current_mapping:
                            logger.debug(
                                f"  Betfair {choice}: choice not present in current "
                                f"{'3-way' if len(containers) >= 6 else '2-way'} layout "
                                f"(attempt {attempt + 1})"
                            )
                            break

                        current_container = containers[current_mapping[choice]]

                        # Validate geometry
                        bbox_check = await current_container.bounding_box()
                        if not bbox_check:
                            logger.debug(f"  Betfair {choice}: bounding_box is None (attempt {attempt + 1}), skipping")
                            await asyncio.sleep(0.4)
                            continue

                        hover_target = await self._get_hover_target_from_container(current_container)

                        await hover_target.scroll_into_view_if_needed()
                        await page.evaluate("window.scrollBy(0, -150)")
                        await page.wait_for_timeout(200)

                        # Clear overlays before every attempt
                        await page.evaluate("""
                            () => {
                                document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());
                                const onetrust = document.getElementById('onetrust-banner-sdk');
                                if (onetrust) onetrust.remove();
                                const shade = document.querySelector('.onetrust-pc-dark-filter');
                                if (shade) shade.remove();
                            }
                        """)

                        # Humanized mouse movement
                        bbox = await hover_target.bounding_box()
                        if bbox:
                            cx = bbox['x'] + bbox['width'] / 2
                            cy = bbox['y'] + bbox['height'] / 2
                            await page.mouse.move(cx - 15, cy - 15)
                            await page.wait_for_timeout(50)
                            await page.mouse.move(cx, cy)
                            await page.wait_for_timeout(50)

                        try:
                            await hover_target.hover(force=True, timeout=2000)
                        except Exception:
                            pass

                        # Fire JS hover events to guarantee Vue.js tooltip trigger
                        await page.evaluate("""
                            (el) => {
                                el.dispatchEvent(new PointerEvent('pointerover', {bubbles: true, cancelable: true, pointerId: 1}));
                                el.dispatchEvent(new PointerEvent('pointerenter', {bubbles: true, cancelable: true, pointerId: 1}));
                                el.dispatchEvent(new MouseEvent('mouseover',  {bubbles: true, cancelable: true}));
                                el.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true, cancelable: true}));
                                el.dispatchEvent(new MouseEvent('mousemove',  {bubbles: true, cancelable: true}));
                            }
                        """, hover_target)

                        # Wait for tooltip using the old global page-level lookup
                        wait_ms = 3000 + (attempt * 1000)
                        html = await self._wait_for_scoped_tooltip_html(page, timeout_ms=wait_ms)

                        if not html:
                            logger.debug(f"  Betfair {choice}: global tooltip not found (attempt {attempt + 1}/{max_retries})")
                            await self._dismiss_odds_movement_tooltip(page)
                            continue

                        label = f"Betfair_{choice}"
                        parsed = self._parse_opening_odds_from_modal_html(html, label=label)

                        if parsed:
                            opening_val, opening_time = parsed
                            result[choice] = (opening_val, opening_time)
                            logger.debug(f"  Betfair {choice}: opening={opening_val} at {opening_time} (attempt {attempt + 1})")
                        else:
                            logger.debug(f"  Betfair {choice}: tooltip found but no opening odd parsed (attempt {attempt + 1})")

                        await self._dismiss_odds_movement_tooltip(page)
                        break

                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.debug(f"  Betfair {choice}: modal not found (attempt {attempt + 1}/{max_retries}), retrying...")
                            await self._dismiss_odds_movement_tooltip(page)
                            continue
                        logger.warning(f"  Betfair {choice}: all retries failed: {e}")

                await self._dismiss_odds_movement_tooltip(page)

                if choice in result:
                    log_timing(f"Hovering and extracting Betfair '{choice}' opening odd took {time.perf_counter() - t_hover_bf:.2f}s")
                else:
                    log_timing(f"Failed to extract Betfair '{choice}' opening odd after {time.perf_counter() - t_hover_bf:.2f}s")

                processed_choices.add(choice)

            return result if result else None

        except Exception as e:
            logger.error(f"Error in _extract_opening_odds_betfair: {e}")
            return None

    async def _extract_data(self, page: Page, match_url: str) -> MatchOddsData:
        """Execute JS to extract structured data."""
        
        # JS Function (Same as validation script)
        raw_data = await page.evaluate(r"""
            () => {
                const result = {
                    homeTeam: '', awayTeam: '', bookies: [], 
                    betfairBack: null, betfairLay: null, betfairPayout: null
                };
                
                // --- Team Names ---
                const h1 = document.querySelector('h1');
                if (h1) {
                    let h1Text = h1.textContent.trim();
                    const dashIdx = h1Text.indexOf(' - ');
                    if (dashIdx > 0) h1Text = h1Text.substring(0, dashIdx);
                    const vsSplit = h1Text.split(' vs ');
                    if (vsSplit.length >= 2) {
                        result.homeTeam = vsSplit[0].trim();
                        result.awayTeam = vsSplit[1].trim();
                    }
                }
                
                // --- Bookmakers ---
                const allDivs = document.querySelectorAll('div.flex.h-9');
                for (const row of allDivs) {
                    if (!row.className.includes('border-black-borders')) continue;
                    
                    const oddsCells = row.querySelectorAll('div.odds-cell');
                    if (oddsCells.length < 2) continue;
                    
                    let bookieName = null;
                    const nameLink = row.querySelector('a[title]');
                    if (nameLink) bookieName = nameLink.getAttribute('title');
                    if (!bookieName) {
                        const img = row.querySelector('img[alt]');
                        if (img) bookieName = img.getAttribute('alt');
                    }
                    if (!bookieName || ['Oddsportal', 'Search'].includes(bookieName)) continue;
                    
                    const odds = Array.from(oddsCells).map(c => c.textContent.trim());
                    
                    // Payout
                    const lastChild = row.children[row.children.length - 1];
                    let payout = '-';
                    if (lastChild && lastChild.textContent.includes('%')) {
                        payout = lastChild.textContent.trim();
                    }
                    
                    const isThreeWay = odds.length >= 3;
                    result.bookies.push({
                        name: bookieName,
                        odds1: odds[0] || '-',
                        oddsX: isThreeWay ? (odds[1] || '-') : null,
                        odds2: isThreeWay ? (odds[2] || '-') : (odds[1] || '-'),
                        payout: payout,
                    });
                }
                
                // --- Extract Betfair Exchange ---
                // Search for the section directly
                const exchangeSection = document.querySelector('div[data-testid="betting-exchanges-section"]');

                if (exchangeSection) {
                    const allOddContainers = exchangeSection.querySelectorAll('div[data-testid="odd-container"]');
                    
                    const extractOddFromContainer = (container) => {
                        const ps = container.querySelectorAll('p');
                        for (const p of ps) {
                            const txt = p.textContent.trim();
                            if (!txt || txt === '-') continue;
                            if (/^\d+(\.\d+)?$/.test(txt)) return txt;
                        }
                        return null;
                    };

                    if (allOddContainers.length >= 6) {
                        // 3-Way Market (1X2)
                        // Back Odds (Indices 0, 1, 2)
                        const back1 = extractOddFromContainer(allOddContainers[0]);
                        const backX = extractOddFromContainer(allOddContainers[1]);
                        const back2 = extractOddFromContainer(allOddContainers[2]);
                        
                        // Lay Odds (Indices 3, 4, 5)
                        const lay1 = extractOddFromContainer(allOddContainers[3]);
                        const layX = extractOddFromContainer(allOddContainers[4]);
                        const lay2 = extractOddFromContainer(allOddContainers[5]);
                        
                        if (back1 || backX || back2) {
                            result.betfairBack = {
                                odds1: back1 || '-',
                                oddsX: backX || '-',
                                odds2: back2 || '-'
                            };
                        }
                        
                        if (lay1 || layX || lay2) {
                            result.betfairLay = {
                                odds1: lay1 || '-',
                                oddsX: layX || '-',
                                odds2: lay2 || '-'
                            };
                        }
                    } else if (allOddContainers.length >= 4) {
                        // 2-Way Market (Home/Away)
                        // Back Odds (Indices 0, 1)
                        const back1 = extractOddFromContainer(allOddContainers[0]);
                        const back2 = extractOddFromContainer(allOddContainers[1]);
                        
                        // Lay Odds (Indices 2, 3)
                        const lay1 = extractOddFromContainer(allOddContainers[2]);
                        const lay2 = extractOddFromContainer(allOddContainers[3]);
                        
                        if (back1 || back2) {
                            result.betfairBack = {
                                odds1: back1 || '-',
                                oddsX: '-',
                                odds2: back2 || '-'
                            };
                        }
                        
                        if (lay1 || lay2) {
                            result.betfairLay = {
                                odds1: lay1 || '-',
                                oddsX: '-',
                                odds2: lay2 || '-'
                            };
                        }
                    }
                        
                    // Extract payout from section text if available
                    const sectionText = exchangeSection.innerText || '';
                    const payMatch = sectionText.match(/(\d{2,3}\.\d)%/);
                    if (payMatch) result.betfairPayout = payMatch[0];
                }
                
                return result;
            }
        """)

        # Convert to Python Objects
        match_data = MatchOddsData(match_url=match_url)
        match_data.home_team = raw_data.get('homeTeam', 'Unknown')
        match_data.away_team = raw_data.get('awayTeam', 'Unknown')
        
        # Store ALL scraped bookies with their final/current odds.
        # Opening odds (initial_odds_*) are left as None here; they will be
        # populated later in scrape_match() via hover — but only for the single
        # top-priority bookie (and Betfair). All others remain None.
        for b in raw_data.get('bookies', []):
            match_data.bookie_odds.append(BookieOdds(
                name=b['name'],
                odds_1=b['odds1'],
                odds_x=b['oddsX'],
                odds_2=b['odds2'],
                payout=b['payout']
            ))
            
        bf_back = raw_data.get('betfairBack')
        if bf_back:
            bf_lay = raw_data.get('betfairLay') or {}
            match_data.betfair = BetfairExchangeOdds(
                back_1=bf_back.get('odds1'), back_1_vol=bf_back.get('vol1'),
                back_x=bf_back.get('oddsX'), back_x_vol=bf_back.get('volX'),
                back_2=bf_back.get('odds2'), back_2_vol=bf_back.get('vol2'),
                lay_1=bf_lay.get('odds1'), lay_1_vol=bf_lay.get('vol1'),
                lay_x=bf_lay.get('oddsX'), lay_x_vol=bf_lay.get('volX'),
                lay_2=bf_lay.get('odds2'), lay_2_vol=bf_lay.get('vol2'),
                payout=raw_data.get('betfairPayout', '-')
            )
            
        return match_data

    async def _extract_data_over_under(self, page: Page, match_url: str) -> Optional[MatchOddsData]:
        """
        Extracts Over/Under market data from the current page.
        Identifies the handicap value, Home (Over), and Away (Under) odds for each bookmaker.
        """
        logger.info("  🔄 Finding main Over/Under line (closest odds)...")
        # Extract all row values to Python for debugging and identifying the main line
        rows_data = await page.evaluate("""
            () => {
                const rows = Array.from(document.querySelectorAll('div[data-testid="over-under-collapsed-row"]'));
                return rows.map((row, index) => {
                    let over = null;
                    let under = null;
                    let handicapText = "";
                    
                    const optionBox = row.querySelector('div[data-testid="over-under-collapsed-option-box"] p');
                    if (optionBox) {
                        handicapText = optionBox.innerText.trim();
                    }

                    const containers = row.querySelectorAll('.flex-center.border-black-main');
                    if (containers.length >= 2) {
                        over = parseFloat(containers[0].innerText.trim());
                        under = parseFloat(containers[1].innerText.trim());
                    }
                    return { index, handicapText, over, under };
                });
            }
        """)

        min_diff = float('inf')
        target_index = -1
        target_handicap = None
        
        logger.info(f"  📊 Evaluating {len(rows_data)} Over/Under rows for the closest odds...")
        for row in rows_data:
            idx = row.get('index')
            hc = row.get('handicapText', 'Unknown')
            over = row.get('over')
            under = row.get('under')
            
            if isinstance(over, (int, float)) and isinstance(under, (int, float)):
                diff = abs(over - under)
                #logger.info(f"    - Row {idx} ({hc}): Over={over}, Under={under} => Diff={diff:.2f}")
                if diff < min_diff:
                    min_diff = diff
                    target_index = idx
                    target_handicap = hc
            else:
                #logger.debug(f"    - Row {idx} ({hc}): Invalid odds Over={over}, Under={under}")
                pass

        if target_index != -1:
            logger.info(f"  👉 Selecting row {target_index} ({target_handicap}) with min difference {min_diff:.2f}")
            # Click the target row using Nth match
            await page.locator('div[data-testid="over-under-collapsed-row"]').nth(target_index).click()
        else:
            logger.warning("  ⚠️ Could not determine main line Over/Under row")
            return None
        # Wait for the row to expand
        await page.wait_for_timeout(1500)
        
        raw_data = await page.evaluate("""
            (hc) => {
                const result = {
                    homeTeam: 'Unknown',
                    awayTeam: 'Unknown',
                    bookies: [],
                    handicap: hc
                };
                
                // --- Team Names ---
                const h1 = document.querySelector('h1');
                if (h1) {
                    let h1Text = h1.textContent.trim();
                    const dashIdx = h1Text.indexOf(' - ');
                    if (dashIdx > 0) h1Text = h1Text.substring(0, dashIdx);
                    const vsSplit = h1Text.split(' vs ');
                    if (vsSplit.length >= 2) {
                        result.homeTeam = vsSplit[0].trim();
                        result.awayTeam = vsSplit[1].trim();
                    }
                }
                
                // --- Bookmakers ---
                const allRows = document.querySelectorAll('div[data-testid="over-under-expanded-row"]');
                for (const row of allRows) {
                    let bookieName = null;
                    const namePara = row.querySelector('[data-testid="outrights-expanded-bookmaker-name"]');
                    if (namePara) bookieName = namePara.textContent.trim();
                    if (!bookieName) {
                        const img = row.querySelector('[data-testid="outrights-expanded-bookmaker-logo"] img');
                        if (img) bookieName = img.getAttribute('alt') || img.getAttribute('title');
                    }
                    if (!bookieName || ['Oddsportal', 'Search'].includes(bookieName)) continue;
                    
                    // Handicap (from arguments since it's not visible in the expanded row)
                    let handicap = hc;
                    
                    // Odds extractor
                    const cleanOdd = (container) => {
                        const p = container.querySelector('p.odds-text') || container.querySelector('p');
                        if (p) return p.textContent.trim();
                        return container.textContent.trim(); // fallback
                    };
                    
                    // Odds
                    const oddContainers = row.querySelectorAll('[data-testid="odd-container"]');
                    let odds1 = '-', odds2 = '-';
                    if (oddContainers.length >= 2) {
                        // Over is first, Under is second
                        odds1 = cleanOdd(oddContainers[0]);
                        odds2 = cleanOdd(oddContainers[1]);
                    }
                    
                    // Payout
                    let payout = '-';
                    const payoutContainer = row.querySelector('[data-testid="payout-container"]');
                    if (payoutContainer) payout = payoutContainer.textContent.trim();
                    else {
                         // Backup payout finder
                         const lastChild = row.children[row.children.length - 1];
                         if (lastChild && lastChild.textContent.includes('%')) {
                             payout = lastChild.textContent.trim();
                         }
                    }
                    
                    result.bookies.push({
                        name: bookieName,
                        handicap: handicap,
                        odds1: odds1 || '-',
                        oddsX: '-',
                        odds2: odds2 || '-',
                        payout: payout,
                    });
                }
                
                return result;
            }
        """, target_handicap)

        # Convert to Python Objects
        match_data = MatchOddsData(match_url=match_url)
        match_data.home_team = raw_data.get('homeTeam', 'Unknown')
        match_data.away_team = raw_data.get('awayTeam', 'Unknown')
        
        # Clean up handicap string (e.g. "Over/Under +3" -> "3", "O/U +0.5" -> "0.5")
        clean_hc = raw_data.get('handicap')
        if clean_hc:
            clean_hc = clean_hc.replace('Over/Under', '').replace('O/U', '').replace('+', '').strip()
            
        for b in raw_data.get('bookies', []):
            match_data.bookie_odds.append(BookieOdds(
                name=b['name'],
                odds_1=b['odds1'],
                odds_x=b['oddsX'],
                odds_2=b['odds2'],
                payout=b['payout'],
                handicap=clean_hc
            ))
            
        return match_data

    async def _extract_data_asian_handicap(self, page: Page, match_url: str) -> Optional[MatchOddsData]:
        """
        Extracts Asian Handicap market data from the current page.
        Identifies the handicap value, Home (1) and Away (2) odds for each bookmaker.
        """
        logger.info("  🔄 Finding main Asian Handicap line (closest odds)...")
        # Extract all row values to Python for debugging and identifying the main line
        rows_data = await page.evaluate("""
            () => {
                const rows = Array.from(document.querySelectorAll('div[data-testid="over-under-collapsed-row"]'));
                return rows.map((row, index) => {
                    let odd1 = null;
                    let odd2 = null;
                    let handicapText = "";
                    
                    const optionBox = row.querySelector('div[data-testid="over-under-collapsed-option-box"] p');
                    if (optionBox) {
                        handicapText = optionBox.innerText.trim();
                    }

                    const containers = row.querySelectorAll('.flex-center.border-black-main');
                    if (containers.length >= 2) {
                        odd1 = parseFloat(containers[0].innerText.trim());
                        odd2 = parseFloat(containers[1].innerText.trim());
                    }
                    return { index, handicapText, odd1, odd2 };
                });
            }
        """)

        min_diff = float('inf')
        target_index = -1
        target_handicap = None
        
        logger.info(f"  📊 Evaluating {len(rows_data)} Asian Handicap rows for the closest odds...")
        for row in rows_data:
            idx = row.get('index')
            hc = row.get('handicapText', 'Unknown')
            odd1 = row.get('odd1')
            odd2 = row.get('odd2')
            
            if isinstance(odd1, (int, float)) and isinstance(odd2, (int, float)):
                diff = abs(odd1 - odd2)
                #logger.info(f"    - Row {idx} ({hc}): 1={odd1}, 2={odd2} => Diff={diff:.2f}")
                if diff < min_diff:
                    min_diff = diff
                    target_index = idx
                    target_handicap = hc
            else:
                #logger.debug(f"    - Row {idx} ({hc}): Invalid odds 1={odd1}, 2={odd2}")
                pass

        if target_index != -1:
            logger.info(f"  👉 Selecting row {target_index} ({target_handicap}) with min difference {min_diff:.2f}")
            # Click the target row using Nth match
            await page.locator('div[data-testid="over-under-collapsed-row"]').nth(target_index).click()
        else:
            logger.warning("  ⚠️ Could not determine main line Asian Handicap row")
            return None
        # Wait for the row to expand
        await page.wait_for_timeout(1500)
        
        raw_data = await page.evaluate("""
            (hc) => {
                const result = {
                    homeTeam: 'Unknown',
                    awayTeam: 'Unknown',
                    bookies: [],
                    handicap: hc
                };
                
                // --- Team Names ---
                const h1 = document.querySelector('h1');
                if (h1) {
                    let h1Text = h1.textContent.trim();
                    const dashIdx = h1Text.indexOf(' - ');
                    if (dashIdx > 0) h1Text = h1Text.substring(0, dashIdx);
                    const vsSplit = h1Text.split(' vs ');
                    if (vsSplit.length >= 2) {
                        result.homeTeam = vsSplit[0].trim();
                        result.awayTeam = vsSplit[1].trim();
                    }
                }
                
                // --- Bookmakers ---
                const allRows = document.querySelectorAll('div[data-testid="over-under-expanded-row"]');
                for (const row of allRows) {
                    let bookieName = null;
                    const namePara = row.querySelector('[data-testid="outrights-expanded-bookmaker-name"]');
                    if (namePara) bookieName = namePara.textContent.trim();
                    if (!bookieName) {
                        const img = row.querySelector('[data-testid="outrights-expanded-bookmaker-logo"] img');
                        if (img) bookieName = img.getAttribute('alt') || img.getAttribute('title');
                    }
                    if (!bookieName || ['Oddsportal', 'Search'].includes(bookieName)) continue;
                    
                    // Handicap (from arguments since it's not visible in the expanded row)
                    let handicap = hc;
                    
                    // Odds extractor
                    const cleanOdd = (container) => {
                        const p = container.querySelector('p.odds-text') || container.querySelector('p');
                        if (p) return p.textContent.trim();
                        return container.textContent.trim(); // fallback
                    };
                    
                    // Odds
                    const oddContainers = row.querySelectorAll('[data-testid="odd-container"]');
                    let odds1 = '-', odds2 = '-';
                    if (oddContainers.length >= 2) {
                        // 1 is first, 2 is second
                        odds1 = cleanOdd(oddContainers[0]);
                        odds2 = cleanOdd(oddContainers[1]);
                    }
                    
                    // Payout
                    let payout = '-';
                    const payoutContainer = row.querySelector('[data-testid="payout-container"]');
                    if (payoutContainer) payout = payoutContainer.textContent.trim();
                    else {
                         // Backup payout finder
                         const lastChild = row.children[row.children.length - 1];
                         if (lastChild && lastChild.textContent.includes('%')) {
                             payout = lastChild.textContent.trim();
                         }
                    }
                    
                    result.bookies.push({
                        name: bookieName,
                        handicap: handicap,
                        odds1: odds1 || '-',
                        oddsX: '-',
                        odds2: odds2 || '-',
                        payout: payout,
                    });
                }
                
                return result;
            }
        """, target_handicap)

        # Convert to Python Objects
        match_data = MatchOddsData(match_url=match_url)
        match_data.home_team = raw_data.get('homeTeam', 'Unknown')
        match_data.away_team = raw_data.get('awayTeam', 'Unknown')
        
        # Clean up handicap string (e.g. "Asian Handicap -2.5" -> "-2.5", "AH -1.5" -> "-1.5")
        clean_hc = raw_data.get('handicap')
        if clean_hc:
            clean_hc = clean_hc.replace('Asian Handicap', '').replace('AH', '').strip()
            
        for b in raw_data.get('bookies', []):
            match_data.bookie_odds.append(BookieOdds(
                name=b['name'],
                odds_1=b['odds1'],
                odds_x=b['oddsX'],
                odds_2=b['odds2'],
                payout=b['payout'],
                handicap=clean_hc
            ))
            
        return match_data

# Singleton instance for simple usage
_scraper = None

async def get_scaler():
    global _scraper
    if _scraper is None:
        _scraper = OddsPortalScraper()
        await _scraper.start()
    return _scraper

async def scrape_match_odds(match_url: str) -> Optional[MatchOddsData]:
    """Helper function to scrape a single match using shared scraper."""
    scraper = await get_scaler()
    return await scraper.scrape_match(match_url)

if __name__ == "__main__":
    # Test run
    async def main():
        url = "https://www.oddsportal.com/football/england/premier-league/wolves-arsenal-rXDVZ0h4/"  # Example from validation
        print(f"Testing scraper with {url}...")
        data = await scrape_match_odds(url)
        if data:
            print(f"Success! Found {len(data.bookie_odds)} bookies.")
            if data.betfair:
                print(f"Betfair Back 1: {data.betfair.back_1} (Vol: {data.betfair.back_1_vol})")
        else:
            print("Failed to scrape.")
            
        if _scraper:
            await _scraper.stop()

    asyncio.run(main())

def scrape_match_sync(match_url: str = None, league_url: str = None, 
                      home_team: str = None, away_team: str = None,
                      sport: str = None) -> Optional[MatchOddsData]:
    """
    Synchronous wrapper for scraping a single match.
    Can provide either match_url OR (league_url, home_team, away_team).
    creates a fresh scraper instance to ensure event loop safety.
    
    Args:
        match_url: Direct match URL (optional if league_url + teams provided)
        league_url: League page URL for finding the match
        home_team: Home team name
        away_team: Away team name
        sport: Sport string for scraping route (e.g. "football", "basketball")
    """
    try:
        async def _run():
            scraper = OddsPortalScraper()
            await scraper.start()
            try:
                target_url = match_url
                if not target_url and league_url and home_team and away_team:
                    target_url = await scraper.find_match_url(league_url, home_team, away_team)
                    
                data = None
                resume_state: Optional[Dict[str, Any]] = None
                partial_match_data: Optional[MatchOddsData] = None
                if target_url:
                    attempt = await scraper.scrape_match_attempt(
                        target_url,
                        sport=sport,
                        resume_state=resume_state,
                        partial_match_data=partial_match_data,
                    )
                    data = attempt.data
                    resume_state = attempt.resume_state
                    partial_match_data = attempt.partial_match_data
                    
                # Session-aware retry: if scrape OR discovery failed (likely bad proxy/IP),
                # restart browser with a fresh proxy session and retry once.
                if data is None:
                    logger.warning(f"🔄 scrape_match_sync: No data (or match discovery failed) — restarting browser with new proxy session and retrying...")
                    await scraper.stop()
                    await scraper.start()  # Fresh session ID = new IP
                    
                    # Re-find match URL if needed
                    retry_url = match_url
                    if not retry_url and league_url and home_team and away_team:
                        retry_url = await scraper.find_match_url(league_url, home_team, away_team)
                    if retry_url:
                        attempt = await scraper.scrape_match_attempt(
                            retry_url,
                            sport=sport,
                            resume_state=resume_state,
                            partial_match_data=partial_match_data,
                        )
                        data = attempt.data
                        resume_state = attempt.resume_state
                        partial_match_data = attempt.partial_match_data
                        if data:
                            logger.info(f"✅ scrape_match_sync: RETRY SUCCEEDED with new session-{scraper._session_id}")
                        else:
                            logger.warning("⚠️ scrape_match_sync: Retry also returned no data")
                
                return data
            finally:
                await scraper.stop()
                
        # Use new_event_loop() instead of asyncio.run() for thread-safety
        # asyncio.run() manages signal handlers that only work on the main thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_run())
        finally:
            loop.close()
    except Exception as e:
        import traceback
        logger.error(f"Error in scrape_match_sync: {e}\n{traceback.format_exc()}")
        return None



def _resolve_task_sport(task: Dict[str, Any]) -> Optional[str]:
    """Resolve sport directly from the task, falling back to season metadata."""
    task_sport = task.get("sport")
    season_id = task.get("season_id")
    if not task_sport and season_id:
        op_info = SEASON_ODDSPORTAL_MAP.get(season_id)
        if op_info:
            task_sport = op_info.get("sport")
    return task_sport


async def _resolve_task_match_url(
    scraper: OddsPortalScraper,
    task: Dict[str, Any],
    *,
    allow_live_lookup: bool = True,
) -> Optional[str]:
    """Resolve a task's match URL with cache-first semantics."""
    match_url = task.get("match_url")
    if match_url:
        return match_url

    season_id = task.get("season_id")
    league_url = task.get("league_url")
    home_team = task.get("home_team")
    away_team = task.get("away_team")

    if season_id and league_url and home_team and away_team:
        cached_url = scraper.find_match_url_from_cache(
            season_id,
            home_team,
            away_team,
            league_url=league_url,
        )
        if cached_url:
            return cached_url

    if allow_live_lookup and league_url and home_team and away_team:
        return await scraper.find_match_url(
            league_url,
            home_team,
            away_team,
            season_id=season_id,
        )

    return None


async def _scrape_task_with_recovery(
    scraper: OddsPortalScraper,
    task: Dict[str, Any],
    task_label: str,
) -> Tuple[Optional[MatchOddsData], Optional[str]]:
    """
    Scrape one task with inline recovery.

    Recovery ladder:
      1. Fresh context on the current browser session (cache/live discovery + scrape)
      2. Another fresh context on the same browser session (clear_state=True for compat)
      3. Full browser restart(s) with a fresh session and full re-resolution
    """
    sport = _resolve_task_sport(task)
    event_id = task.get("event_id")
    clear_state = bool(task.get("clear_state", False))
    resume_state = task.get("_oddsportal_resume_state")
    partial_match_data = task.get("_oddsportal_partial_match_data")
    match_url = await _resolve_task_match_url(scraper, task, allow_live_lookup=True)

    if not match_url:
        logger.warning(f"{task_label}: could not resolve match URL on the current session for event_id {event_id}")

    data = None
    attempt: Optional[ScrapeAttemptResult] = None
    if match_url:
        attempt = await scraper.scrape_match_attempt(
            match_url,
            sport=sport,
            clear_state=clear_state,
            resume_state=resume_state,
            partial_match_data=partial_match_data,
        )
        data = attempt.data
        if data:
            task.pop("_oddsportal_resume_state", None)
            task.pop("_oddsportal_partial_match_data", None)
            return data, match_url
        resume_state = attempt.resume_state
        partial_match_data = attempt.partial_match_data
        task["_oddsportal_resume_state"] = resume_state
        task["_oddsportal_partial_match_data"] = partial_match_data

    if match_url:
        resume_meta = resume_state if isinstance(resume_state, dict) else {}
        logger.info(
            f"{task_label}: resuming from step={resume_meta.get('next_step_idx')} "
            f"failed_step_key={resume_meta.get('failed_step_key')} "
            f"resume_fragment={resume_meta.get('resume_fragment')}"
        )
        logger.info(
            f"{task_label}: attempt 1 returned no data — fresh-context retry on "
            f"existing browser session-{scraper._session_id} for event_id {event_id}"
        )
        try:
            attempt = await scraper.scrape_match_attempt(
                match_url,
                sport=sport,
                clear_state=True,
                resume_state=resume_state,
                partial_match_data=partial_match_data,
            )
            data = attempt.data
        except Exception as e:
            logger.error(f"{task_label}: fresh-context retry failed for event_id {event_id}: {e}")
            data = None
            attempt = None
        if data:
            logger.info(
                f"{task_label}: fresh-context retry succeeded for event_id {event_id} "
                f"with the existing browser session-{scraper._session_id}"
            )
            task.pop("_oddsportal_resume_state", None)
            task.pop("_oddsportal_partial_match_data", None)
            return data, match_url
        if attempt:
            resume_state = attempt.resume_state
            partial_match_data = attempt.partial_match_data
            task["_oddsportal_resume_state"] = resume_state
            task["_oddsportal_partial_match_data"] = partial_match_data

    restart_attempts = max(1, ODDSPORTAL_SESSION_RESTART_ATTEMPTS)
    for restart_idx in range(1, restart_attempts + 1):
        resume_meta = resume_state if isinstance(resume_state, dict) else {}
        logger.warning(
            f"{task_label}: restarting browser session ({restart_idx}/{restart_attempts}) "
            f"for event_id {event_id} after fast-fail/empty scrape"
        )
        logger.warning(
            f"{task_label}: restart resume point step={resume_meta.get('next_step_idx')} "
            f"failed_step_key={resume_meta.get('failed_step_key')} "
            f"resume_fragment={resume_meta.get('resume_fragment')}"
        )
        try:
            await scraper.stop()
            await scraper.start()
        except Exception as e:
            logger.error(f"{task_label}: browser restart failed for event_id {event_id}: {e}")
            continue

        retry_url = await _resolve_task_match_url(scraper, task, allow_live_lookup=True)
        if not retry_url:
            logger.warning(
                f"{task_label}: match URL still unresolved after restart "
                f"({restart_idx}/{restart_attempts}) for event_id {event_id}"
            )
            continue

        match_url = retry_url
        try:
            attempt = await scraper.scrape_match_attempt(
                match_url,
                sport=sport,
                clear_state=False,
                resume_state=resume_state,
                partial_match_data=partial_match_data,
            )
            data = attempt.data
        except Exception as e:
            logger.error(f"{task_label}: restart attempt {restart_idx} failed for event_id {event_id}: {e}")
            data = None
            attempt = None

        if data:
            logger.info(
                f"{task_label}: restart attempt {restart_idx} succeeded for event_id {event_id} "
                f"with session-{scraper._session_id}"
            )
            task.pop("_oddsportal_resume_state", None)
            task.pop("_oddsportal_partial_match_data", None)
            return data, match_url
        if attempt:
            resume_state = attempt.resume_state
            partial_match_data = attempt.partial_match_data
            task["_oddsportal_resume_state"] = resume_state
            task["_oddsportal_partial_match_data"] = partial_match_data

    return None, match_url


# ---------------------------------------------------------------------------
# Seed-only resolver
# ---------------------------------------------------------------------------

async def _seed_group_cache_only(
    scraper: OddsPortalScraper,
    task: Dict[str, Any],
    task_label: str,
) -> GroupSeedResult:
    """Seed the league cache for a group without scraping any match.

    Recovery ladder (reduced version of the full scrape ladder):
      1. Normal attempt on the current browser session.
      2. If it fails or returns 0 candidates, restart the browser session
         up to ODDSPORTAL_SESSION_RESTART_ATTEMPTS times.
    """
    season_id = task.get("season_id")
    league_url = task.get("league_url")
    event_id = task.get("event_id")

    if not league_url:
        msg = f"No league_url in seed task for event_id {event_id}"
        logger.warning(f"{task_label}: {msg}")
        return GroupSeedResult(success=False, cache_warmed=False, candidate_count=0,
                               season_id=season_id, league_url=league_url, error=msg)

    logger.info(f"{task_label}: Resolver seed start for event_id {event_id} "
                f"(season_id={season_id}, league_url={league_url})")

    # --- Attempt 1: current session ---
    try:
        candidates = await scraper._extract_league_candidates(league_url, season_id)
    except Exception as e:
        logger.error(f"{task_label}: league extraction failed for seed: {e}")
        candidates = []

    if candidates:
        logger.info(f"{task_label}: Resolver seed finished — cache_warmed=True, "
                     f"candidate_count={len(candidates)}")
        return GroupSeedResult(success=True, cache_warmed=True,
                               candidate_count=len(candidates),
                               season_id=season_id, league_url=league_url)

    # --- Session restart attempts ---
    restart_attempts = max(1, ODDSPORTAL_SESSION_RESTART_ATTEMPTS)
    for restart_idx in range(1, restart_attempts + 1):
        logger.warning(
            f"{task_label}: seed returned 0 candidates — restarting browser session "
            f"({restart_idx}/{restart_attempts}) for seed of event_id {event_id}"
        )
        try:
            await scraper.stop()
            await scraper.start()
        except Exception as e:
            logger.error(f"{task_label}: browser restart failed during seed: {e}")
            continue

        try:
            candidates = await scraper._extract_league_candidates(league_url, season_id)
        except Exception as e:
            logger.error(f"{task_label}: league extraction failed on restart {restart_idx}: {e}")
            candidates = []

        if candidates:
            logger.info(
                f"{task_label}: Resolver seed finished after restart {restart_idx} — "
                f"cache_warmed=True, candidate_count={len(candidates)}"
            )
            return GroupSeedResult(success=True, cache_warmed=True,
                                   candidate_count=len(candidates),
                                   season_id=season_id, league_url=league_url)

    logger.warning(f"{task_label}: Resolver seed finished — cache_warmed=False "
                    f"(all {restart_attempts} restart(s) exhausted)")
    return GroupSeedResult(success=False, cache_warmed=False, candidate_count=0,
                           season_id=season_id, league_url=league_url,
                           error="all seed attempts exhausted")


# ---------------------------------------------------------------------------
# Active dispatcher overrides
# ---------------------------------------------------------------------------

def _build_dispatch_groups(tasks: List[Dict[str, Any]]) -> Tuple[Dict[Tuple[int, str], List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """Group tasks by normalized season/league key; keep non-groupable tasks separate."""
    dispatch_groups: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
    standalone_tasks: List[Dict[str, Any]] = []

    for task in tasks:
        task_copy = dict(task)
        group_key = _build_league_group_key(task_copy.get("season_id"), task_copy.get("league_url"))
        if group_key:
            dispatch_groups.setdefault(group_key, []).append(task_copy)
        else:
            standalone_tasks.append(task_copy)

    return dispatch_groups, standalone_tasks


def _attach_cached_match_urls(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Refresh task match URLs from the DB-backed cache without opening a browser."""
    if not tasks:
        return []

    scraper = OddsPortalScraper()
    refreshed_tasks: List[Dict[str, Any]] = []
    for task in tasks:
        refreshed_task = dict(task)
        if (
            not refreshed_task.get("match_url")
            and refreshed_task.get("season_id")
            and refreshed_task.get("league_url")
            and refreshed_task.get("home_team")
            and refreshed_task.get("away_team")
        ):
            cached_url = scraper.find_match_url_from_cache(
                refreshed_task["season_id"],
                refreshed_task["home_team"],
                refreshed_task["away_team"],
                league_url=refreshed_task["league_url"],
            )
            if cached_url:
                refreshed_task["match_url"] = cached_url
        refreshed_tasks.append(refreshed_task)
    return refreshed_tasks


def scrape_multiple_matches_sync(tasks: List[Dict], debug_dir: Optional[str] = None, on_result=None) -> Dict[int, Optional[MatchOddsData]]:
    """Scrape multiple matches sequentially using one shared browser session."""
    results: Dict[int, Optional[MatchOddsData]] = {}

    if not tasks:
        return results

    logger.info(f"OddsPortal batch: scraping {len(tasks)} matches with shared browser")

    try:
        async def _run():
            scraper = OddsPortalScraper(debug_dir=debug_dir)
            await scraper.start()
            try:
                sequential_tasks = _attach_cached_match_urls(tasks)
                for i, task in enumerate(sequential_tasks, start=1):
                    event_id = task["event_id"]
                    task_label = f"OddsPortal [{i}/{len(sequential_tasks)}]"
                    try:
                        data, match_url = await _scrape_task_with_recovery(scraper, task, task_label)
                        results[event_id] = data

                        if data:
                            logger.info(
                                f"{task_label}: got {len(data.extractions)} period(s), "
                                f"{len(data.bookie_odds)} bookies"
                            )
                        else:
                            logger.warning(
                                f"{task_label}: scrape failed "
                                f"(match_url={match_url or 'unresolved'})"
                            )

                        if on_result:
                            try:
                                on_result(event_id, data)
                            except Exception as cb_err:
                                logger.error(f"on_result callback error for event {event_id}: {cb_err}")

                        if i < len(sequential_tasks):
                            await asyncio.sleep(1)
                    except Exception as e:
                        logger.error(f"OddsPortal scrape failed for event {event_id}: {e}")
                        results[event_id] = None
                        if on_result:
                            try:
                                on_result(event_id, None)
                            except Exception as cb_err:
                                logger.error(f"on_result callback error for event {event_id}: {cb_err}")
            finally:
                await scraper.stop()
            return results

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_run())
        finally:
            loop.close()

    except Exception as e:
        import traceback
        logger.error(f"Error in scrape_multiple_matches_sync: {e}\n{traceback.format_exc()}")
        return results


def scrape_multiple_matches_parallel_sync(tasks: List[Dict], num_browsers: int = 1, debug_dir: Optional[str] = None, on_result=None) -> Dict[int, Optional[MatchOddsData]]:
    """
    Event-driven dispatcher with decoupled seeding.

    The dispatcher separates league-cache seeding from event scraping:
      - resolver_seed: lightweight task that navigates to the league page,
        extracts candidates, and warms the cache.  Does NOT scrape any match.
      - ready_event: event with a resolved match_url, ready for scraping.

    Siblings are released immediately after the seed finishes (not after the
    resolver finishes scraping its match).  The event that originated the
    resolver re-enters the ready queue and competes for any available browser.
    """
    if not tasks:
        return {}

    if num_browsers <= 1 or len(tasks) == 1:
        return scrape_multiple_matches_sync(tasks, debug_dir=debug_dir, on_result=on_result)

    # --- Phase 1: attach warm-cache URLs and classify tasks ---
    tasks_with_cache = _attach_cached_match_urls(tasks)
    dispatch_groups, standalone_tasks = _build_dispatch_groups(tasks_with_cache)
    sorted_group_items = sorted(
        dispatch_groups.items(),
        key=lambda item: len(item[1]),
        reverse=True,
    )

    worker_count = max(1, min(num_browsers, len(tasks_with_cache)))
    logger.info(f"OddsPortal Parallel: distributing {len(tasks_with_cache)} tasks across {worker_count} dynamic browser(s)")

    dispatcher_condition = Condition()
    ready_event_queue: deque = deque()
    resolver_queue: deque = deque()
    # group_state now tracks ALL pending (unscraped) events per group
    group_state: Dict[Tuple[int, str], Dict[str, Any]] = {}
    # remaining_event_count = real events pending completion (excludes seed tasks)
    remaining_event_count = len(tasks_with_cache)
    all_results: Dict[int, Optional[MatchOddsData]] = {}

    # Max seed retries per group (separate from per-event browser restart attempts)
    MAX_SEED_ATTEMPTS = max(1, ODDSPORTAL_SESSION_RESTART_ATTEMPTS + 1)

    ready_task_count = 0
    resolver_seed_count = 0
    pending_in_groups = 0
    standalone_ready_count = 0
    standalone_resolver_count = 0

    # --- Phase 2: populate queues and group_state ---
    for group_key, group_tasks in sorted_group_items:
        resolved_group_tasks: List[Dict[str, Any]] = []
        unresolved_group_tasks: List[Dict[str, Any]] = []

        for task in group_tasks:
            task_copy = dict(task)
            task_copy["_dispatch_group_key"] = group_key
            if task_copy.get("match_url"):
                task_copy["_dispatch_role"] = "ready_event"
                resolved_group_tasks.append(task_copy)
            else:
                unresolved_group_tasks.append(task_copy)

        # Tasks that already have a match_url go straight to the ready queue
        for ready_task in resolved_group_tasks:
            ready_event_queue.append(ready_task)
            ready_task_count += 1

        # All unresolved tasks stay in pending_tasks — including the one used
        # as basis for the resolver_seed task.
        group_state[group_key] = {
            "pending_tasks": list(unresolved_group_tasks),
            "resolver_active": bool(unresolved_group_tasks),
            "seed_attempts": 0,
            "cache_warmed": False,
        }
        pending_in_groups += len(unresolved_group_tasks)

        # Create exactly ONE resolver_seed task per group if there are pending tasks
        if unresolved_group_tasks:
            seed_basis = unresolved_group_tasks[0]
            seed_task: Dict[str, Any] = {
                "event_id": seed_basis.get("event_id"),
                "season_id": seed_basis.get("season_id"),
                "league_url": seed_basis.get("league_url"),
                "home_team": seed_basis.get("home_team"),
                "away_team": seed_basis.get("away_team"),
                "_dispatch_role": "resolver_seed",
                "_dispatch_group_key": group_key,
            }
            resolver_queue.append(seed_task)
            resolver_seed_count += 1

    # Standalone tasks (no group key) — treat as ready or direct events
    for task in standalone_tasks:
        task_copy = dict(task)
        if task_copy.get("match_url"):
            task_copy["_dispatch_role"] = "ready_event"
            ready_event_queue.append(task_copy)
            ready_task_count += 1
            standalone_ready_count += 1
        else:
            # No group key = cannot seed a league, go straight to live lookup
            task_copy["_dispatch_role"] = "ready_event"
            ready_event_queue.append(task_copy)
            ready_task_count += 1
            standalone_resolver_count += 1

    logger.info(
        f"OddsPortal Parallel: total_events={len(tasks_with_cache)}, dispatch_groups={len(dispatch_groups)}, "
        f"ready_events={ready_task_count}, resolver_seeds={resolver_seed_count}, "
        f"pending_in_groups={pending_in_groups}, standalone_ready={standalone_ready_count}, "
        f"standalone_no_url={standalone_resolver_count}"
    )

    # ------------------------------------------------------------------
    # _release_group_after_seed — liberates pending tasks after seed
    # ------------------------------------------------------------------
    def _release_group_after_seed(
        group_key: Tuple[int, str],
        seed_task: Dict[str, Any],
        seed_result: GroupSeedResult,
    ) -> None:
        nonlocal remaining_event_count

        seed_event_id = seed_task.get("event_id")

        with dispatcher_condition:
            state = group_state.get(group_key)
            if not state:
                dispatcher_condition.notify_all()
                return

            state["seed_attempts"] += 1
            state["resolver_active"] = False

            pending_tasks = state["pending_tasks"]
            if not pending_tasks:
                state["cache_warmed"] = seed_result.cache_warmed
                dispatcher_condition.notify_all()
                return

        # Re-check cache for every pending task (outside lock to avoid holding
        # the lock during DB queries)
        refreshed = _attach_cached_match_urls(pending_tasks)

        cache_hit_ids: List[int] = []
        cache_miss_ids: List[int] = []
        tasks_with_url: List[Dict[str, Any]] = []
        tasks_without_url: List[Dict[str, Any]] = []

        for t in refreshed:
            t_copy = dict(t)
            t_copy["_dispatch_group_key"] = group_key
            if t_copy.get("match_url"):
                tasks_with_url.append(t_copy)
                cache_hit_ids.append(t_copy.get("event_id"))
            else:
                tasks_without_url.append(t_copy)
                cache_miss_ids.append(t_copy.get("event_id"))

        with dispatcher_condition:
            state = group_state[group_key]

            if seed_result.cache_warmed:
                # Cache was successfully warmed — release ALL tasks immediately
                state["cache_warmed"] = True
                state["pending_tasks"] = []

                for t in tasks_with_url:
                    t["_dispatch_role"] = "ready_event"
                    t["_released_by_resolver"] = True
                    t["_resolver_event_id"] = seed_event_id
                    t["_release_reason"] = "cache_warmed"
                    ready_event_queue.append(t)

                for t in tasks_without_url:
                    # Even without match_url, release for live lookup
                    t["_dispatch_role"] = "ready_event"
                    t["_released_by_resolver"] = True
                    t["_resolver_event_id"] = seed_event_id
                    t["_release_reason"] = "cache_warmed"
                    ready_event_queue.append(t)

            elif state["seed_attempts"] < MAX_SEED_ATTEMPTS:
                # Seed failed but we have retries left — release cache hits,
                # keep misses pending, and re-enqueue a new resolver_seed.
                state["pending_tasks"] = tasks_without_url

                for t in tasks_with_url:
                    t["_dispatch_role"] = "ready_event"
                    t["_released_by_resolver"] = True
                    t["_resolver_event_id"] = seed_event_id
                    t["_release_reason"] = "partial_seed"
                    ready_event_queue.append(t)

                if tasks_without_url:
                    re_seed_basis = tasks_without_url[0]
                    re_seed_task: Dict[str, Any] = {
                        "event_id": re_seed_basis.get("event_id"),
                        "season_id": re_seed_basis.get("season_id"),
                        "league_url": re_seed_basis.get("league_url"),
                        "home_team": re_seed_basis.get("home_team"),
                        "away_team": re_seed_basis.get("away_team"),
                        "_dispatch_role": "resolver_seed",
                        "_dispatch_group_key": group_key,
                    }
                    resolver_queue.append(re_seed_task)
                    state["resolver_active"] = True

            else:
                # Seed exhausted all retries — release everyone for live lookup
                state["pending_tasks"] = []
                state["cache_warmed"] = False

                for t in tasks_with_url:
                    t["_dispatch_role"] = "ready_event"
                    t["_released_by_resolver"] = True
                    t["_resolver_event_id"] = seed_event_id
                    t["_release_reason"] = "seed_failed_fallback"
                    ready_event_queue.append(t)

                for t in tasks_without_url:
                    t["_dispatch_role"] = "ready_event"
                    t["_released_by_resolver"] = True
                    t["_resolver_event_id"] = seed_event_id
                    t["_release_reason"] = "seed_failed_fallback"
                    ready_event_queue.append(t)

            dispatcher_condition.notify_all()

        # --- Logging (outside lock) ---
        released_total = len(cache_hit_ids) + len(cache_miss_ids) if seed_result.cache_warmed or state["seed_attempts"] >= MAX_SEED_ATTEMPTS else len(cache_hit_ids)
        if released_total > 0:
            logger.info(
                f"Dispatcher: resolver seed for event_id {seed_event_id} unlocked "
                f"{released_total} sibling ready task(s) for {_format_group_key(group_key)} "
                f"(cache_hits={len(cache_hit_ids)}, live_lookups={len(cache_miss_ids) if seed_result.cache_warmed or state['seed_attempts'] >= MAX_SEED_ATTEMPTS else 0})"
            )
        if not seed_result.cache_warmed and state["seed_attempts"] < MAX_SEED_ATTEMPTS and tasks_without_url:
            logger.info(
                f"Dispatcher: re-enqueuing resolver seed for {_format_group_key(group_key)} "
                f"(attempt {state['seed_attempts']}/{MAX_SEED_ATTEMPTS}, "
                f"remaining_pending={len(tasks_without_url)})"
            )
        if not seed_result.cache_warmed and state["seed_attempts"] >= MAX_SEED_ATTEMPTS:
            logger.warning(
                f"Dispatcher: all seed attempts exhausted for {_format_group_key(group_key)} — "
                f"releasing {len(cache_miss_ids)} tasks for live lookup fallback"
            )

    # ------------------------------------------------------------------
    # _complete_task — only for real events, NOT for resolver_seed
    # ------------------------------------------------------------------
    def _complete_task(task: Dict[str, Any], data: Optional[MatchOddsData], resolved_match_url: Optional[str]) -> None:
        nonlocal remaining_event_count

        event_id = task.get("event_id")
        with dispatcher_condition:
            if event_id is not None:
                all_results[event_id] = data
            remaining_event_count -= 1
            dispatcher_condition.notify_all()

    # ------------------------------------------------------------------
    # _claim_next_task
    # ------------------------------------------------------------------
    def _claim_next_task() -> Optional[Dict[str, Any]]:
        with dispatcher_condition:
            while True:
                if ready_event_queue:
                    return ready_event_queue.popleft()
                if resolver_queue:
                    return resolver_queue.popleft()
                if remaining_event_count == 0:
                    return None
                dispatcher_condition.wait()

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------
    def _run_dynamic_worker_sync(worker_index: int) -> None:
        worker_label = f"[Browser {worker_index}/{worker_count}]"

        async def _run() -> None:
            scraper = OddsPortalScraper(debug_dir=debug_dir)
            await scraper.start()
            try:
                while True:
                    task = _claim_next_task()
                    if task is None:
                        return

                    role = task.get("_dispatch_role")
                    group_key = task.get("_dispatch_group_key")
                    event_id = task.get("event_id")

                    # --- Branch: resolver_seed ---
                    if role == "resolver_seed" and group_key:
                        task_label = (
                            f"{worker_label} OddsPortal [SEED "
                            f"{task.get('home_team', '?')} vs {task.get('away_team', '?')}]"
                        )
                        logger.info(
                            f"{worker_label} Resolver seed start for event_id {event_id} "
                            f"in {_format_group_key(group_key)}"
                        )
                        seed_result = await _seed_group_cache_only(scraper, task, task_label)
                        _release_group_after_seed(group_key, task, seed_result)
                        # Do NOT call _complete_task or on_result for seed tasks
                        continue

                    # --- Branch: real event ---
                    task_label = (
                        f"{worker_label} OddsPortal "
                        f"[{task.get('home_team', 'Unknown')} vs {task.get('away_team', 'Unknown')}]"
                    )
                    data: Optional[MatchOddsData] = None
                    resolved_match_url: Optional[str] = None

                    if role == "ready_event" and task.get("_released_by_resolver") and group_key:
                        logger.info(
                            f"{worker_label} Starting resolver-released sibling event_id {event_id} "
                            f"for {_format_group_key(group_key)} "
                            f"(reason={task.get('_release_reason', 'unknown')})"
                        )

                    try:
                        data, resolved_match_url = await _scrape_task_with_recovery(scraper, task, task_label)
                        if data:
                            logger.info(
                                f"{worker_label} event_id {event_id} scraped successfully "
                                f"(match_url={resolved_match_url or 'unresolved'})"
                            )
                        else:
                            logger.warning(
                                f"{worker_label} event_id {event_id} finished without data "
                                f"(match_url={resolved_match_url or 'unresolved'})"
                            )
                    except Exception as e:
                        logger.error(f"{worker_label} event_id {event_id} failed with exception: {e}")
                        data = None
                    finally:
                        _complete_task(task, data, resolved_match_url)

                    if on_result:
                        try:
                            on_result(event_id, data)
                        except Exception as cb_err:
                            logger.error(f"{worker_label} on_result callback error for event {event_id}: {cb_err}")
            finally:
                await scraper.stop()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            with _log_prefix(worker_label):
                loop.run_until_complete(_run())
        finally:
            loop.close()

    # ------------------------------------------------------------------
    # Launch workers
    # ------------------------------------------------------------------
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(_run_dynamic_worker_sync, idx) for idx in range(1, worker_count + 1)]
        for future in as_completed(futures):
            future.result()

    total_success = sum(1 for v in all_results.values() if v is not None)
    logger.info(
        f"OddsPortal Parallel summary: Total scraped={total_success}/{len(tasks_with_cache)} "
        f"(entries in results dict={len(all_results)})"
    )
    return all_results