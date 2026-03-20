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
    
    def __init__(self, headless: bool = True, debug_dir: Optional[str] = None):
        self.headless = headless
        self.debug_dir = debug_dir
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
                "extras": extra or {}
            }
            
            json_path = os.path.join(self.debug_dir, f"{base_name}.json")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f, indent=4)
                
            logger.info(f"💾 Saved debug artifacts for {reason} at {self.debug_dir}/{base_name}.*")
        except Exception as e:
            logger.error(f"Failed to save debug artifacts: {e}")



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
        """
        Navigate to a match page and extract odds for all configured market periods.
        
        Creates a fresh BrowserContext per event when fresh_context_per_event is enabled,
        ensuring complete isolation between events within the same browser.
        
        Args:
            match_url: Full OddsPortal match URL
            sport: Sport string from SEASON_ODDSPORTAL_MAP (e.g. "football", "basketball")
                   Used to determine scraping route from SPORT_SCRAPING_ROUTES.
                   If None, falls back to legacy single-extraction behavior.
        """
        if not self.browser:
            await self.start()

        # --- Fresh context lifecycle ---
        previous_context = self.context
        fresh_context = None
        page = None

        try:
            if self._fresh_context_per_event:
                try:
                    fresh_context = await self._create_fresh_context()
                except Exception as ctx_err:
                    logger.error(f"❌ Failed to create fresh context for {match_url}: {ctx_err}")
                    return None
                self.context = fresh_context
                logger.info(
                    f"🔒 Fresh context created (session-{self._session_id}, "
                    f"ignore_https_errors={self._ignore_https_errors})"
                )
            elif not self.context:
                # Fallback: create a persistent context if fresh mode is off and none exists
                self.context = await self._create_fresh_context()

            if clear_state and self.context:
                await self._clear_browser_state()

            page = await self.context.new_page()
        except Exception as setup_err:
            logger.error(f"❌ Failed to set up page for {match_url}: {setup_err}")
            # Clean up fresh context if page creation failed
            if fresh_context:
                try:
                    await fresh_context.close()
                except Exception:
                    pass
                self.context = previous_context
            return None

        try:
            t0 = time.perf_counter()
            
            # Resolve scraping route for this sport
            route = SPORT_SCRAPING_ROUTES.get(sport) if sport else None
            groups = []
            if route and "groups" in route:
                groups = route["groups"]
                logger.info(f"🗺️ Scraping route for '{sport}': {len(groups)} groups")
            elif route:
                # Legacy fallback for old config format
                group_key = route.get("primary_group")
                groups = [{
                    "group_key": group_key,
                    "db_market_group": route.get("db_market_group", "1X2"),
                    "periods": route.get("periods", [("FULL_TIME", "Full-time", "Full time")]),
                    "betfair_period_index": route.get("betfair_period_index", 0),
                    "extract_fn": "standard"
                }]
                logger.info(f"🗺️ Legacy scraping route for '{sport}'")
            else:
                groups = [{
                    "group_key": None,
                    "db_market_group": "1X2",
                    "periods": [("FULL_TIME", "Full-time", "Full time")],
                    "betfair_period_index": 0,
                    "extract_fn": "standard"
                }]
                logger.info(f"⚠️ No scraping route for sport='{sport}', using legacy single-extraction mode")
            
            # Build initial URL with fragment for the first period of the FIRST group
            first_group = groups[0] if groups else None
            if first_group and first_group.get("group_key") and first_group.get("periods"):
                period_key = first_group["periods"][0][0]
                fragment = f"#{OP_GROUPS[first_group['group_key']]};{OP_PERIODS[period_key]}"
                # Robustly strip any existing fragment or trailing slash from base URL
                base_url = match_url.split('#')[0].rstrip('/')
                initial_url = f"{base_url}/{fragment}"
            else:
                initial_url = match_url
            
            logger.info(f"🌐 Navigating to match: {initial_url}")
            
            # Navigate to match page
            response = None
            e_goto = None
            goto_error_code = None
            goto_error_summary = None
            try:
                response = await self._goto_fresh(page, initial_url, wait_until="domcontentloaded", timeout=Config.ODDSPORTAL_MATCH_GOTO_TIMEOUT_MS)
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
            
            # --- Fast Fail & Smart Wait Implementation ---
            # Step 1: If goto threw, inspect the page before deciding whether to bail.
            # Only a truly blank page (NO_SHELL) fails immediately.
            # Everything else (including MISSING_EVENT_CONTAINER, which can be
            # transient during SPA hydration) falls through to the smart-wait race.
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
                        await self._save_debug_artifacts(page, reason, {"error": str(e_goto)})
                    await page.close()
                    return None
                elif classification == "DATA_RENDERED":
                    logger.info("Navigation threw, but data is already rendered. Continuing.")
                else:
                    # Shell is present (possibly hydrating). Don't fail — let the
                    # smart-wait race below be the final arbiter.
                    logger.info(
                        f"Navigation exception left page in {classification}. "
                        f"Falling through to smart-wait/render checks."
                    )
            
            # Step 2: Cloudflare / WAF block check
            try:
                page_title = await page.title()
                if any(blocked in page_title for blocked in ["Access Denied", "Just a moment...", "Attention Required!", "Security check", "Cloudflare"]):
                    reason = "CLOUDFLARE_BLOCK"
                    logger.error(f"FAST FAIL: {reason}")
                    await self._save_debug_artifacts(page, reason)
                    await page.close()
                    return None
            except Exception:
                pass

            # Step 3: HTTP response status check (only when goto succeeded)
            if response is not None and response.status >= 400:
                reason = f"HTTP_{response.status}"
                logger.error(f"FAST FAIL: {reason}")
                await self._save_debug_artifacts(page, reason)
                await page.close()
                return None

            # Step 4: Smart Wait Race — JS fast-fail observer vs market render
            first_extract_fn = (first_group or {}).get("extract_fn", "standard")
            
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
            render_task = asyncio.create_task(self._wait_for_market_render(page, first_extract_fn, timeout_ms=getattr(Config, 'ODDSPORTAL_MARKET_RENDER_TIMEOUT_MS', 60000)))
            
            done, pending = await asyncio.wait([fast_fail_task, render_task], return_when=asyncio.FIRST_COMPLETED)
            
            for p_task in pending:
                p_task.cancel()
                
            if fast_fail_task in done:
                ff_reason = fast_fail_task.result()
                if ff_reason is not None:
                    # Fast fail JS triggered — shell-grace recoverable states get one more chance
                    if ff_reason in ["Shell loaded, skeleton persisted, no data rows", "Shell loaded, no data rows"]:
                        logger.info(f"⏳ JS Observer detected '{ff_reason}'. Routing to shell-grace logic.")
                        if getattr(Config, 'ODDSPORTAL_ENABLE_SHELL_GRACE', True):
                            rendered = await self._wait_for_market_render(page, first_extract_fn, timeout_ms=getattr(Config, 'ODDSPORTAL_SHELL_GRACE_TIMEOUT_MS', 8000))
                            if not rendered:
                                reason_code = "SHELL_WITH_SKELETON_NO_DATA" if "skeleton persisted" in ff_reason else "SHELL_WITH_NAV_NO_DATA"
                                logger.error(f"FAST FAIL: {reason_code} (after shell grace).")
                                await self._save_debug_artifacts(page, reason_code)
                                await page.close()
                                return None
                            else:
                                logger.info("✅ Shell-grace successful.")
                        else:
                            reason_code = "SHELL_WITH_SKELETON_NO_DATA" if "skeleton persisted" in ff_reason else "SHELL_WITH_NAV_NO_DATA"
                            logger.error(f"FAST FAIL: {reason_code} (shell-grace disabled).")
                            await self._save_debug_artifacts(page, reason_code)
                            await page.close()
                            return None
                    else:
                        # Hard fast fails (Missing event-container, Event container stayed empty, etc.)
                        reason_code = "FAST_FAIL_" + ff_reason.replace(" ", "_").upper()
                        state = await self._collect_match_page_state(page)
                        summary = self._format_page_state_summary(state, self._classify_match_page_state(state))
                        logger.error(f"FAST FAIL: {reason_code}. {summary}")
                        await self._save_debug_artifacts(page, reason_code)
                        await page.close()
                        return None
            
            # If render_task finished first, check its success
            if render_task in done:
                rendered = render_task.result()
                if not rendered:
                    state = await self._collect_match_page_state(page)
                    classification = self._classify_match_page_state(state)
                    state_summary = self._format_page_state_summary(state, classification)
                    reason = f"MATCH_RENDER_TIMEOUT_{classification}"
                    logger.error(f"❌ Match page failure: {reason}. {state_summary}")
                    await self._save_debug_artifacts(page, reason)
                    await page.close()
                    return None

            log_timing(f"Primary rendering + wait race took {time.perf_counter() - t_goto:.2f}s")
            # --- End Fast Fail & Smart Wait ---
            
            t_wait = time.perf_counter()
            log_timing(f"Waiting for bookmaker rows selector took {t_wait - t_goto:.2f}s")
            
            # Handle cookie/consent banner
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
            
            # Scroll to load lazy elements
            await page.evaluate("window.scrollTo(0, 500)")
            await asyncio.sleep(1.0)
            

            
            # ========================================
            # MULTI-GROUP & MULTI-PERIOD EXTRACTION
            # ========================================
            match_data = MatchOddsData(match_url=match_url, sport=sport or "")
            
            for group_idx, group_cfg in enumerate(groups):
                group_key = group_cfg.get("group_key")
                db_market_group = group_cfg.get("db_market_group", "1X2")
                periods = group_cfg.get("periods", [])
                betfair_period_idx = group_cfg.get("betfair_period_index")
                extract_fn = group_cfg.get("extract_fn", "standard")
                
                logger.info(f"📑 Starting extraction for group: {db_market_group} ({len(periods)} periods)")
                
                # Switch to market group tab (skip for the first group, it is already loaded via initial URL fragment)
                if group_idx > 0 and group_key:
                    tab_label = OP_GROUPS_DISPLAY.get(group_key, group_key)
                    success = await self._click_market_group_tab(page, tab_label)
                    
                    if not success:
                        logger.error(f"Group switch failed for {group_key}; aborting match so the caller can restart with a fresh browser session")
                        await self._save_debug_artifacts(page, f"GROUP_SWITCH_FAILED_{group_key}")
                        return None
                    if not success:
                        logger.warning(f"⚠️ Could not switch to group {group_key}, skipping")
                        continue
                
                for period_idx, (period_key, db_market_period, db_market_name) in enumerate(periods):
                    t_period = time.perf_counter()
                    logger.info(f"📊 [{period_idx+1}/{len(periods)}] Extracting: {db_market_group} / {db_market_period}")
                
                    # Navigate to this period's tab (skip for first period since we already loaded it)
                    if period_idx > 0 and group_key:
                        logger.info(f"🔀 Switching to period: {db_market_period} (tab click)")
                        t_frag = time.perf_counter()
                    
                        tab_clicked = await self._click_period_tab(page, db_market_period)
                    
                        if not tab_clicked:
                            logger.error(f"Period switch failed for {db_market_period}; aborting match so the caller can restart with a fresh browser session")
                            await self._save_debug_artifacts(page, f"PERIOD_SWITCH_FAILED_{period_key}")
                            return None
                    
                        if not tab_clicked:
                            logger.warning(f"⚠️ Could not switch to period {db_market_period}, skipping")
                            continue
                    
                        log_timing(f"Period tab-click navigation to {period_key} took {time.perf_counter() - t_frag:.2f}s")
                
                    # Extract current/final odds via JS
                    t_extract = time.perf_counter()
                    if extract_fn == "over_under":
                        period_data = await self._extract_data_over_under(page, match_url)
                    elif extract_fn == "asian_handicap":
                        period_data = await self._extract_data_asian_handicap(page, match_url)
                    else:
                        period_data = await self._extract_data(page, match_url)
                    log_timing(f"JS extraction for {db_market_period} took {time.perf_counter() - t_extract:.2f}s")
                
                    if not period_data:
                        logger.error(f"No data extracted for period {db_market_period}; aborting match so the caller can restart with a fresh browser session")
                        await self._save_debug_artifacts(page, f"PERIOD_DATA_EMPTY_{period_key}")
                        return None
                
                    # Populate match-level team names from first extraction
                    if period_idx == 0:
                        match_data.home_team = period_data.home_team
                        match_data.away_team = period_data.away_team
                
                    logger.info(f"✅ Extracted {len(period_data.bookie_odds)} bookies for {db_market_period}")
                
                    # --- Opening odds via hover ---
                    # Find highest-priority bookie for hover extraction
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
                        lbl_x = "X=" + str(opening.get('X')) + " " if opening and not is_ou else ""
                        
                        if opening:
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
                            
                            logger.info(f"✅ Opening odds ({db_market_period}): {lbl_1}={target_bookie_obj.initial_odds_1} {lbl_x}{lbl_2}={target_bookie_obj.initial_odds_2} (Time: {target_bookie_obj.movement_odds_time})")
                        else:
                            logger.warning(f"⚠️ Could not extract opening odds for {target_bookie_obj.name} ({db_market_period})")
                    else:
                        logger.info(f"ℹ️ No priority bookie found for {db_market_period}, skipping opening odds hover")
                
                    # Betfair extraction — only on the designated period
                    extraction_betfair = None
                    if period_idx == betfair_period_idx and period_data.betfair:
                        logger.info(f"🎯 Extracting Betfair Exchange opening odds via hover ({db_market_period})")
                        t_bf = time.perf_counter()
                        bf_opening = await self._extract_opening_odds_betfair(page)
                        log_timing(f"Betfair hover extraction ({db_market_period}) took {time.perf_counter() - t_bf:.2f}s")
                        if bf_opening:
                            # Update back odds
                            if bf_opening.get('back_1'):
                                period_data.betfair.initial_back_1 = bf_opening['back_1'][0]
                                period_data.betfair.movement_odds_time = bf_opening['back_1'][1]
                            if bf_opening.get('back_x'):
                                period_data.betfair.initial_back_x = bf_opening['back_x'][0]
                            if bf_opening.get('back_2'):
                                period_data.betfair.initial_back_2 = bf_opening['back_2'][0]
                                
                            # Update lay odds
                            if bf_opening.get('lay_1'):
                                period_data.betfair.initial_lay_1 = bf_opening['lay_1'][0]
                            if bf_opening.get('lay_x'):
                                period_data.betfair.initial_lay_x = bf_opening['lay_x'][0]
                            if bf_opening.get('lay_2'):
                                period_data.betfair.initial_lay_2 = bf_opening['lay_2'][0]
                                
                            logger.info(f"✅ Betfair opening odds ({db_market_period}):")
                            logger.info(f"   Back: 1={period_data.betfair.initial_back_1} X={period_data.betfair.initial_back_x} 2={period_data.betfair.initial_back_2} (Time: {period_data.betfair.movement_odds_time})")
                            logger.info(f"   Lay:  1={period_data.betfair.initial_lay_1} X={period_data.betfair.initial_lay_x} 2={period_data.betfair.initial_lay_2}")
                        else:
                            logger.warning(f"⚠️ Could not extract Betfair Exchange opening odds ({db_market_period})")
                        extraction_betfair = period_data.betfair
                
                    # Wrap into MarketExtraction
                    extraction = MarketExtraction(
                        market_group=db_market_group,
                        market_period=db_market_period,
                        market_name=db_market_name,
                        bookie_odds=period_data.bookie_odds,
                        betfair=extraction_betfair,
                    )
                    match_data.extractions.append(extraction)
                    log_timing(f"Total period extraction for {db_market_period} took {time.perf_counter() - t_period:.2f}s")
            
            # ========================================
            # LEGACY COMPAT: populate top-level fields from first extraction
            # ========================================
            if match_data.extractions:
                first = match_data.extractions[0]
                match_data.bookie_odds = first.bookie_odds
                match_data.betfair = first.betfair
            
            total_duration = time.perf_counter() - t0
            match_data.extraction_time_ms = total_duration * 1000
            log_timing(f"Total match scraping process (scrape_match) took {total_duration:.2f}s")
            
            total_bookies = sum(len(e.bookie_odds) for e in match_data.extractions)
            logger.info(f"✅ Completed scraping {match_data.home_team} vs {match_data.away_team}: {len(match_data.extractions)} periods, {total_bookies} bookie entries total")
            
            # ── Structured Recap Log ──────────────────────────────────────
            match_label = f"{match_data.home_team} vs {match_data.away_team}"
            logger.info(f"📋 ── ODDS RECAP: {match_label} ──")
            for ext in match_data.extractions:
                handicap_str = f" [{ext.bookie_odds[0].handicap}]" if ext.bookie_odds and getattr(ext.bookie_odds[0], 'handicap', None) else ""
                logger.info(f"   📌 {ext.market_group} | {ext.market_period} | {ext.market_name}{handicap_str}")
                
                # Determine column labels based on market group
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
                
                # Betfair recap
                if ext.betfair:
                    bf = ext.betfair
                    if is_ou or is_ah:
                        back_str = f"{col_labels[0]}={bf.back_1 or '-'} {col_labels[-1]}={bf.back_2 or '-'}"
                        lay_str  = f"{col_labels[0]}={bf.lay_1 or '-'} {col_labels[-1]}={bf.lay_2 or '-'}"
                    else:
                        back_str = f"1={bf.back_1 or '-'} X={bf.back_x or '-'} 2={bf.back_2 or '-'}"
                        lay_str  = f"1={bf.lay_1 or '-'} X={bf.lay_x or '-'} 2={bf.lay_2 or '-'}"
                    logger.info(f"      Betfair Back: {back_str} | Lay: {lay_str}")
            logger.info(f"📋 ── END RECAP: {match_label} ──")
            
            return match_data
            
        except Exception as e:
            logger.error(f"❌ Error scraping match {match_url}: {e}")
            return None
        finally:
            try:
                if page:
                    await page.close()
            except Exception:
                pass
            # Close the fresh context and restore previous reference
            if fresh_context:
                try:
                    await fresh_context.close()
                except Exception:
                    pass
                self.context = previous_context


    def _parse_opening_odds_from_modal_html(self, modal_html: str) -> Optional[Tuple[str, str]]:
        """
        Parse the opening odds value from the tooltip modal HTML.
        
        The modal HTML contains a movement odds time in the '<div class="text-[10px] font-normal">' 
        and the opening odds in the 'Opening odds:' section.
        """
        try:
            import re
            movement_time = None
            
            # Extract movement time (final/current odds time)
            time_matches = re.findall(r'<div[^>]*text-\[10px\][^>]*font-normal[^>]*>\s*([^<]+)\s*</div>', modal_html)
            if time_matches:
                movement_time = time_matches[0].strip()
            else:
                idx_opening = modal_html.find('Opening odds')
                if idx_opening != -1:
                    pre_section = modal_html[:idx_opening]
                    date_matches = re.findall(r'(?:>|^\s*)(\d{1,2}\s+[A-Za-z]{3},\s+\d{2}:\d{2})(?:<|\s*$)', pre_section)
                    if date_matches:
                        movement_time = date_matches[0].strip()

            if 'Opening odds' not in modal_html:
                return (None, movement_time) if movement_time else None

            idx = modal_html.find('Opening odds')
            section = modal_html[idx:idx + 500]
            
            # Find opening odds value (the bold number in 'Opening odds' section)
            # e.g.: <div>15 Feb, 19:47</div><div class="font-bold">1.69</div>
            matches = re.findall(r'<div[^>]*>\s*([^<]+)\s*</div>\s*<div[^>]*font-bold[^>]*>([\d.]+)</div>', section)
            extracted_val = None
            for _, val in matches:
                try:
                    f = float(val)
                    if 1.0 <= f <= 1001.0:
                        extracted_val = val
                        break
                except ValueError:
                    continue
                    
            if not extracted_val:
                matches = re.findall(r'<div[^>]*font-bold[^>]*>([\d.]+)</div>', section)
                for val in matches:
                    try:
                        f = float(val)
                        if 1.0 <= f <= 1001.0:
                            extracted_val = val
                            break
                    except ValueError:
                        continue
                        
            if extracted_val:
                return (extracted_val, movement_time)
            
            return (None, movement_time) if movement_time else None
            
        except Exception as e:
            logger.warning(f"Error parsing opening odds from modal: {e}")
            return None

    async def _extract_opening_odds_for_bookie(
        self, page: Page, bookie_name: str
    ) -> Optional[Dict[str, Optional[Tuple[str, str]]]]:
        """
        Hover over each odds cell for a specific bookie to trigger the tooltip,
        then extract the opening odds from the 'Odds movement' modal.
        
        Uses the OddsHarvester pattern:
          1. Find the bookie row by matching img title or a[title]
          2. Hover each div.flex-center.flex-col.font-bold inside the row
          3. Wait 2s for tooltip to appear
          4. Capture the modal HTML via h3:text('Odds movement') parent
          5. Parse opening odds from the modal
        
        Returns:
            Dict with keys '1', 'X', '2' mapping to tuples (opening_odds, timestamp) or None on failure.
        """
        try:
            await page.wait_for_timeout(500)

            # Dismiss overlay-bookie-modal (it intercepts pointer events and blocks hover)
            await page.evaluate("""
                () => { document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove()); }
            """)

            # Use CSS :has() to find the bookie row directly
            target_row = await page.query_selector(
                f"div.border-black-borders.flex.h-9:has(a[title*='{bookie_name}'])"
            )
            if not target_row:
                target_row = await page.query_selector(
                    f"div.border-black-borders.flex.h-9:has(img[alt*='{bookie_name}'])"
                )
            if not target_row:
                # Case-insensitive fallback: iterate rows
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

            if not target_row:
                logger.warning(f"⚠️ Bookie row not found for: {bookie_name}")
                return None

            # Get the odds cells to hover — inner wrappers that trigger the tooltip
            odds_blocks = await target_row.query_selector_all("div[data-testid='odd-container'] div.flex-center.flex-col.font-bold")
            if not odds_blocks:
                odds_blocks = await target_row.query_selector_all("div[data-testid='odd-container']")

            if not odds_blocks:
                logger.warning(f"⚠️ No odds blocks found in row for: {bookie_name}")
                return None

            logger.info(f"🖱️ Hovering {len(odds_blocks)} odds cells for {bookie_name}")

            is_three_way = len(odds_blocks) >= 3
            if is_three_way:
                choice_keys = ['1', 'X', '2']
            else:
                choice_keys = ['1', '2']
                
            result: Dict[str, Optional[Tuple[str, str]]] = {}

            for i, odds_block in enumerate(odds_blocks[:len(choice_keys)]):  # Respect the length of dynamic keys
                choice = choice_keys[i] if i < len(choice_keys) else str(i)
                max_retries = 3
                got_value = False
                
                t_hover_cell = time.perf_counter()
                for attempt in range(max_retries):
                    try:
                        await odds_block.scroll_into_view_if_needed()
                        # Scroll window up to avoid sticky header
                        await page.evaluate("window.scrollBy(0, -150)")
                        await page.wait_for_timeout(200)

                        # Remove overlays that block hover (bookie-modal + consent)
                        await page.evaluate("""
                            () => {
                                document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());
                                const onetrust = document.getElementById('onetrust-banner-sdk');
                                if (onetrust) onetrust.remove();
                                const shade = document.querySelector('.onetrust-pc-dark-filter');
                                if (shade) shade.remove();
                            }
                        """)
                        
                        # Humanized mouse movement (wiggle)
                        bbox = await odds_block.bounding_box()
                        if bbox:
                            cx = bbox['x'] + bbox['width'] / 2
                            cy = bbox['y'] + bbox['height'] / 2
                            await page.mouse.move(cx - 15, cy - 15)
                            await page.wait_for_timeout(50)
                            await page.mouse.move(cx, cy)
                            await page.wait_for_timeout(50)

                        # Hover with force=True; fallback to mouse.move() + JS dispatch
                        try:
                            # Reduced hover timeout to 1500ms
                            await odds_block.hover(force=True, timeout=1500)
                        except Exception:
                            pass

                        # Also fire JS hover events to trigger Vue.js tooltip component
                        await page.evaluate("""
                            (el) => {
                                el.dispatchEvent(new PointerEvent('pointerover', {bubbles: true, cancelable: true, pointerId: 1}));
                                el.dispatchEvent(new PointerEvent('pointerenter', {bubbles: true, cancelable: true, pointerId: 1}));
                                el.dispatchEvent(new MouseEvent('mouseover', {bubbles: true, cancelable: true}));
                                el.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true, cancelable: true}));
                                el.dispatchEvent(new MouseEvent('mousemove', {bubbles: true, cancelable: true}));
                            }
                        """, odds_block)
                        
                        # Find the modal via 'Odds movement' heading with dynamic wait instead of hard sleep
                        wait_ms = 3000 + (attempt * 1000)
                        try:
                            odds_movement_h3 = await page.wait_for_selector(
                                "h3:has-text('Odds movement')", state="visible", timeout=wait_ms
                            )
                            # Verify tooltip is actually visible before parsing
                            is_visible = await page.is_visible("h3:has-text('Odds movement')")
                            if not is_visible:
                                logger.warning(f"  ⚠️ Cell {choice}: tooltip found in DOM but NOT visible (attempt {attempt + 1})")
                                if attempt < max_retries - 1:
                                    await page.mouse.move(0, 0)
                                    await page.wait_for_timeout(500)
                                    continue
                                result[choice] = None
                                break
                            modal_wrapper = await odds_movement_h3.evaluate_handle(
                                "node => node.parentElement"
                            )
                            modal_el = modal_wrapper.as_element()
                            if modal_el:
                                html = await modal_el.inner_html()
                                parsed = self._parse_opening_odds_from_modal_html(html)
                                if parsed:
                                    opening_val, opening_time = parsed
                                    result[choice] = (opening_val, opening_time)
                                    logger.debug(f"  Cell {choice}: opening={opening_val} at {opening_time} (attempt {attempt + 1})")
                                else:
                                    result[choice] = None
                                got_value = True
                            else:
                                result[choice] = None
                        except Exception:
                            if attempt < max_retries - 1:
                                logger.debug(f"  Cell {choice}: modal not found (attempt {attempt + 1}/{max_retries}), retrying...")
                                # Move away and wait before retry
                                await page.mouse.move(0, 0)
                                await page.wait_for_timeout(500)
                                continue
                            result[choice] = None
                        
                        # Move mouse away to dismiss tooltip before next cell
                        await page.mouse.move(0, 0)
                        await page.wait_for_timeout(500)
                        # Wait for modal to fully detach before hovering the next cell
                        try:
                            await page.wait_for_selector(
                                "h3:has-text('Odds movement')",
                                state="detached", timeout=2000
                            )
                        except Exception:
                            pass
                        break  # Exit retry loop (success or final attempt done)
                        
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.debug(f"  Cell {choice}: hover error (attempt {attempt + 1}), retrying: {e}")
                            await page.mouse.move(0, 0)
                            await page.wait_for_timeout(500)
                            continue
                        logger.warning(f"  Error hovering cell {choice} for {bookie_name}: {e}")
                        result[choice] = None
                
                if result.get(choice) is not None:
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

        Uses the same retry pattern as _extract_opening_odds_for_bookie:
          - 3 attempts per cell with escalating wait (3s → 4s → 5s)
          - Mouse reset to (0, 0) between retries so Vue.js sees a fresh mouseenter
          - Tooltip detach wait between cells to avoid stale DOM interference

        A 500ms pre-wait is applied after finding the exchange section to let Vue.js
        fully render all columns (including the X/Draw column) before committing to a
        2-way or 3-way container count.

        Returns:
            Dict with keys 'back_1'/'back_x'/'back_2' and 'lay_1'/'lay_x'/'lay_2'
            mapping to (opening_odds, timestamp) tuples, or None on failure.
        """
        try:
            exchange_section = await page.query_selector("div[data-testid='betting-exchanges-section']")
            if not exchange_section:
                logger.warning("⚠️ Betfair Exchange section not found for hover extraction")
                return None

            # FIX 2: Give Vue.js time to fully render all columns (incl. the X/Draw column)
            # before counting containers, to avoid a race condition that silently drops back_x/lay_x.
            await page.wait_for_timeout(500)

            # Get all odd containers in the exchange section
            odd_containers = await exchange_section.query_selector_all("div[data-testid='odd-container']")
            if not odd_containers:
                logger.warning("⚠️ No odd containers found in Betfair Exchange section")
                return None

            # Determine if 2-way or 3-way market based on rendered container count
            is_three_way = len(odd_containers) >= 6
            logger.debug(f"  Betfair: {len(odd_containers)} containers detected -> {'3-way' if is_three_way else '2-way'}")

            if is_three_way:
                # Indices 0,1,2 = Back; 3,4,5 = Lay
                back_containers = odd_containers[:3]
                lay_containers = odd_containers[3:6]
                choice_keys_back = ['back_1', 'back_x', 'back_2']
                choice_keys_lay  = ['lay_1', 'lay_x', 'lay_2']
            else:
                # Indices 0,1 = Back; 2,3 = Lay
                back_containers = odd_containers[:2]
                lay_containers  = odd_containers[2:4]
                choice_keys_back = ['back_1', 'back_2']
                choice_keys_lay  = ['lay_1', 'lay_2']

            result: Dict[str, Optional[Tuple[str, str]]] = {}

            all_targets = []
            for i, c in enumerate(back_containers):
                all_targets.append((choice_keys_back[i], c))
            for i, c in enumerate(lay_containers):
                all_targets.append((choice_keys_lay[i], c))

            logger.info(f"🖱️ Hovering {len(all_targets)} Betfair cells (Back & Lay)")

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

            for choice, container in all_targets:
                t_hover_bf = time.perf_counter()
                max_retries = 3
                got_value = False

                for attempt in range(max_retries):
                    try:
                        hover_target = await container.query_selector("div.flex-center.flex-col.font-bold")
                        if not hover_target:
                            hover_target = container

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

                        # FIX 1: Escalating wait per attempt (3s → 4s → 5s)
                        wait_ms = 3000 + (attempt * 1000)
                        odds_movement_h3 = await page.wait_for_selector(
                            "h3:has-text('Odds movement')", state="visible", timeout=wait_ms
                        )

                        # Extra visibility check — tooltip can be in DOM but still animating in
                        is_visible = await page.is_visible("h3:has-text('Odds movement')")
                        if not is_visible:
                            logger.warning(f"  ⚠️ Betfair {choice}: tooltip in DOM but NOT visible (attempt {attempt + 1})")
                            if attempt < max_retries - 1:
                                await page.mouse.move(0, 0)
                                await page.wait_for_timeout(500)
                                continue
                            result[choice] = None
                            break

                        modal_wrapper = await odds_movement_h3.evaluate_handle("node => node.parentElement")
                        modal_el = modal_wrapper.as_element()
                        if modal_el:
                            html = await modal_el.inner_html()
                            parsed = self._parse_opening_odds_from_modal_html(html)

                            if self.debug_dir:
                                try:
                                    debug_path = os.path.join(self.debug_dir, f"modal_Betfair_{choice}.html")
                                    with open(debug_path, "w", encoding="utf-8") as f:
                                        f.write(html)
                                except Exception as e:
                                    logger.warning(f"⚠️ Failed to save modal HTML: {e}")

                            if parsed:
                                opening_val, opening_time = parsed
                                result[choice] = (opening_val, opening_time)
                                logger.debug(f"  Betfair {choice}: opening={opening_val} at {opening_time} (attempt {attempt + 1})")
                            else:
                                result[choice] = None
                            got_value = True
                        else:
                            result[choice] = None

                    except Exception:
                        if attempt < max_retries - 1:
                            logger.debug(f"  Betfair {choice}: modal not found (attempt {attempt + 1}/{max_retries}), retrying...")
                            await page.mouse.move(0, 0)
                            await page.wait_for_timeout(500)
                            continue
                        result[choice] = None

                    # Dismiss tooltip and wait for it to fully detach before the next cell
                    await page.mouse.move(0, 0)
                    await page.wait_for_timeout(300)
                    try:
                        await page.wait_for_selector(
                            "h3:has-text('Odds movement')", state="detached", timeout=2000
                        )
                    except Exception:
                        pass
                    break  # exit retry loop (success or final attempt done)

                if got_value:
                    log_timing(f"Hovering and extracting Betfair '{choice}' opening odd took {time.perf_counter() - t_hover_bf:.2f}s")
                else:
                    log_timing(f"Failed to extract Betfair '{choice}' opening odd after {time.perf_counter() - t_hover_bf:.2f}s")

            # Log summary
            if result:
                back_str = f"Back: 1={result.get('back_1', (None,))[0]} X={result.get('back_x', (None,))[0]} 2={result.get('back_2', (None,))[0]}"
                lay_str  = f"Lay: 1={result.get('lay_1', (None,))[0]} X={result.get('lay_x', (None,))[0]} 2={result.get('lay_2', (None,))[0]}"
                logger.info(f"✅ Betfair opening odds: {back_str} | {lay_str}")

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
                if target_url:
                    data = await scraper.scrape_match(target_url, sport=sport)
                    
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
                        data = await scraper.scrape_match(retry_url, sport=sport)
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
    match_url = await _resolve_task_match_url(scraper, task, allow_live_lookup=True)

    if not match_url:
        logger.warning(f"{task_label}: could not resolve match URL on the current session for event_id {event_id}")

    data = None
    if match_url:
        data = await scraper.scrape_match(match_url, sport=sport, clear_state=clear_state)
        if data:
            return data, match_url

    if match_url:
        logger.info(
            f"{task_label}: attempt 1 returned no data — fresh-context retry on "
            f"existing browser session-{scraper._session_id} for event_id {event_id}"
        )
        try:
            data = await scraper.scrape_match(match_url, sport=sport, clear_state=True)
        except Exception as e:
            logger.error(f"{task_label}: fresh-context retry failed for event_id {event_id}: {e}")
            data = None
        if data:
            logger.info(
                f"{task_label}: fresh-context retry succeeded for event_id {event_id} "
                f"with the existing browser session-{scraper._session_id}"
            )
            return data, match_url

    restart_attempts = max(1, ODDSPORTAL_SESSION_RESTART_ATTEMPTS)
    for restart_idx in range(1, restart_attempts + 1):
        logger.warning(
            f"{task_label}: restarting browser session ({restart_idx}/{restart_attempts}) "
            f"for event_id {event_id} after fast-fail/empty scrape"
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
            data = await scraper.scrape_match(match_url, sport=sport, clear_state=False)
        except Exception as e:
            logger.error(f"{task_label}: restart attempt {restart_idx} failed for event_id {event_id}: {e}")
            data = None

        if data:
            logger.info(
                f"{task_label}: restart attempt {restart_idx} succeeded for event_id {event_id} "
                f"with session-{scraper._session_id}"
            )
            return data, match_url

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
