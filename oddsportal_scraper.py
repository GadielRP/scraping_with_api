import asyncio
import logging
import random
import time
import os
import uuid
from dataclasses import dataclass, field
from typing import List, Optional, Dict

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# Import configuration
try:
    from config import Config
    from oddsportal_config import (
        SEASON_ODDSPORTAL_MAP, BOOKIE_ALIASES, TEAM_ALIASES, PRIORITY_BOOKIES,
        OP_GROUPS, OP_GROUPS_DISPLAY, OP_PERIODS, SPORT_SCRAPING_ROUTES,
    )
except ImportError:
    # Fallback/Mock for standalone testing
    class MockConfig:
        PROXY_ENABLED = False
        PROXY_ENDPOINT = None
        PROXY_USERNAME = None
        PROXY_PASSWORD = None
    Config = MockConfig()
    SEASON_ODDSPORTAL_MAP = {}
    BOOKIE_ALIASES = {}
    TEAM_ALIASES = {}
    PRIORITY_BOOKIES = ["bet365", "Pinnacle", "BettingAsia", "Megapari", "1xBet"]
    OP_GROUPS = {"1X2": "1X2", "HOME_AWAY": "home-away"}
    OP_PERIODS = {"FT_INC_OT": 1, "FULL_TIME": 2, "1ST_HALF": 3}
    SPORT_SCRAPING_ROUTES = {}

logger = logging.getLogger(__name__)

DEBUG_TIMING = os.getenv("DEBUG_TIMING", "false").lower() == "true"
ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS = int(os.getenv("ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS", "15000"))
ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS = int(os.getenv("ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS", "10000"))

def log_timing(msg):
    if DEBUG_TIMING:
        print(f"⏱️ [Timing] {msg}")

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
        if self.debug_dir:
            import os
            os.makedirs(self.debug_dir, exist_ok=True)

    def _normalize_match_text(self, s: str) -> str:
        """
        Normalize team or row text for matching.
        Strips accents, 'fc/cf/ud', non-alphanumeric noise, and collapses whitespace.
        """
        if not s:
            return ""
        import unicodedata
        import re
        # Strip accents/diacritics (e.g., Montréal -> Montreal)
        s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('utf-8')
        s = s.lower()
        # Remove team suffixes/prefixes that vary between Sofa and OP
        for noise in ["fc", "cf", "ud", "afc", "rc", "as", "sc"]:
            if f" {noise}" in s or s.startswith(f"{noise} "):
                s = s.replace(noise, "")
        # Remove all non-alphanumeric characters, keep space
        s = re.sub(r'[^a-z0-9\s]', ' ', s)
        # Collapse spaces and strip
        return " ".join(s.split())
        
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
        
        # Common modern User-Agents to rotate
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        ]
        
        # Create context with anti-detection
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=random.choice(user_agents),
            locale="en-US",
            timezone_id="America/Chicago",
            java_script_enabled=True,
        )
        
        # Inject evasion scripts
        await self.context.add_init_script("""
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
        """)
        
        # Intercept and block unnecessary resources (toggleable via .env)
        if getattr(Config, 'ODDSPORTAL_BLOCK_RESOURCES', True):
            await self.context.route("**/*", self._intercept_route)
            logger.info("🚫 Resource blocking ENABLED (ODDSPORTAL_BLOCK_RESOURCES=true)")
        else:
            logger.info("✅ Resource blocking DISABLED (ODDSPORTAL_BLOCK_RESOURCES=false)")
        
        logger.info("✅ OddsPortalScraper: Browser started")

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
        except Exception:
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

            max_wait_s = int(os.environ.get("ODDSPORTAL_TAB_WAIT_TIMEOUT", 20))
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

            max_wait_s = int(os.environ.get("ODDSPORTAL_TAB_WAIT_TIMEOUT", 20))
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
    async def find_match_url(self, league_url: str, home_team: str, away_team: str, season_id: int = None) -> Optional[str]:
        """
        Navigate to league page and find match URL by team names.
        Also populates the DB cache with all match URLs found on the page.
        """
        if not self.context:
            await self.start()
            
        page = await self.context.new_page()
        try:
            logger.info(f"🌐 Navigating to league: {league_url}")
            t0 = time.perf_counter()
            try:
                response = await page.goto(
                    league_url,
                    wait_until="domcontentloaded",
                    timeout=ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS,
                )
            except Exception as e:
                error_str = str(e).lower()
                if "timeout" in error_str or "err_" in error_str or "net::" in error_str:
                    logger.error(
                        "🚨 FAST FAIL (League goto): navigation failed quickly "
                        f"after {ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS}ms: {str(e).split(chr(10))[0]}"
                    )
                    return None
                raise
            t_goto = time.perf_counter()
            log_timing(f"League page load ({league_url}) took {t_goto - t0:.2f}s")
            
            if not response or response.status != 200:
                logger.error(f"❌ Failed to load league page. Status: {response.status if response else 'N/A'}")
                return None

            # Quick anti-bot/block detection to avoid lingering on a poisoned session.
            try:
                page_title = await page.title()
                if any(blocked in page_title for blocked in ["Access Denied", "Just a moment...", "Attention Required!", "Security check", "Cloudflare"]):
                    logger.error(f"🚨 FAST FAIL (League title): Proxy IP blocked. Title: '{page_title}'")
                    return None
            except Exception:
                pass
            
            # Wait for event rows to appear (faster than networkidle which waits for all ads/trackers)
            try:
                await page.wait_for_selector("div.eventRow", timeout=ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS)
                t_wait = time.perf_counter()
                log_timing(f"Waiting for 'div.eventRow' selector took {t_wait - t_goto:.2f}s")
            except Exception:
                logger.error(
                    "🚨 FAST FAIL (League rows): no event rows loaded "
                    f"within {ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS}ms on {league_url}"
                )
                return None
                
            # Handle cookie banner
            try:
                accept_btn = await page.query_selector("button:has-text('I Accept'), button:has-text('Accept All')")
                if accept_btn:
                    await accept_btn.click()
                    await asyncio.sleep(0.5)
            except Exception:
                pass
                
            # Search for match
            home_norm = self._normalize_match_text(home_team)
            away_norm = self._normalize_match_text(away_team)
            
            # Check aliases if available
            home_alias = TEAM_ALIASES.get(home_team, home_team)
            away_alias = TEAM_ALIASES.get(away_team, away_team)
            home_alias_norm = self._normalize_match_text(home_alias)
            away_alias_norm = self._normalize_match_text(away_alias)
            
            # --- OPTIMIZED SEARCH ALGORITHM ---
            # Instead of iterating DOM elements (slow), we extract all rows' text and links in ONE payload.
            # Complexity: O(1) network round-trip + O(N) string process in Python (negligible)
            
            t_js_league = time.perf_counter()
            rows_data = await page.evaluate("""() => {
                const rows = Array.from(document.querySelectorAll("div.eventRow"));
                return rows.map(row => {
                    const text = row.innerText;
                    // Get all links in the row
                    const links = Array.from(row.querySelectorAll("a[href]")).map(a => a.getAttribute("href"));
                    return { text, links };
                });
            }""")
            log_timing(f"Extracting league rows via JS evaluating took {time.perf_counter() - t_js_league:.2f}s")
            
            if not rows_data:
                logger.warning(f"⚠️ No event rows found on {league_url}")
                return None
            
            # --- CACHE POPULATION: Save all match URLs for this league ---
            if season_id:
                cache_dict = {}
                league_path = league_url.rstrip('/')
                for row_data in rows_data:
                    for href in row_data.get('links', []):
                        if not href:
                            continue
                        # Only store genuine match slugs: must have ≥ 4 path segments
                        # (e.g. /football/italy/serie-a/pisa-bologna-Qy7EzcEL/)
                        # This excludes league URLs (/football/italy/serie-a/) and nav links.
                        parts = [p for p in href.strip('/').split('/') if p]
                        if len(parts) < 4:
                            continue
                        # Also exclude the league URL itself as a safety net
                        if href.rstrip('/') == league_path.replace('https://www.oddsportal.com', ''):
                            continue
                        cache_dict[href] = row_data.get('text', '')
                if cache_dict:
                    try:
                        from repository import OddsPortalCacheRepository
                        OddsPortalCacheRepository.save_league_cache(season_id, cache_dict)
                        logger.info(f"💾 Cached {len(cache_dict)} match URLs for season {season_id}")
                    except Exception as e:
                        logger.warning(f"⚠️ Failed to cache league URLs: {e}")
                
            logger.info(f"🔎 Scanning {len(rows_data)} rows for {home_team} vs {away_team} (Batch Mode)...")
            
            for row_data in rows_data:
                text_norm = self._normalize_match_text(row_data['text'] or "")
                
                # Check for both teams
                h_found = home_norm in text_norm or home_alias_norm in text_norm
                a_found = away_norm in text_norm or away_alias_norm in text_norm
                
                if h_found and a_found:
                    # Found match row, finding correct link
                    found_link = None
                    for href in row_data['links']:
                        if not href: continue
                        
                        # Rule 1: Must contain hyphen (slug structure)
                        if "-" not in href: continue
                        
                        # Rule 2: Must NOT be the league URL itself
                        if href.rstrip('/') in league_url.rstrip('/'): continue
                        
                        found_link = href
                        break
                        
                    if found_link:
                        logger.info(f"✅ Found match link: {found_link}")
                        return f"https://www.oddsportal.com{found_link}"

            logger.warning(f"❌ Match not found: {home_team} vs {away_team}")
            return None
            
        except Exception as e:
            logger.error(f"Error finding match on {league_url}: {e}")
            return None
        finally:
            await page.close()

    def find_match_url_from_cache(self, season_id: int, home_team: str, away_team: str) -> Optional[str]:
        """
        Try to find match URL from DB cache (no browser navigation needed).
        Uses the same normalize + alias matching algorithm as find_match_url.
        
        Returns:
            Full match URL or None if not found in cache
        """
        try:
            from repository import OddsPortalCacheRepository
            cached = OddsPortalCacheRepository.get_league_cache(season_id)
            if not cached:
                return None
            
            home_norm = self._normalize_match_text(home_team)
            away_norm = self._normalize_match_text(away_team)
            
            home_alias = TEAM_ALIASES.get(home_team, home_team)
            away_alias = TEAM_ALIASES.get(away_team, away_team)
            home_alias_norm = self._normalize_match_text(home_alias)
            away_alias_norm = self._normalize_match_text(away_alias)
            
            for href, display_text in cached.items():
                text_norm = self._normalize_match_text(display_text or "")
                
                h_found = home_norm in text_norm or home_alias_norm in text_norm
                a_found = away_norm in text_norm or away_alias_norm in text_norm
                
                if h_found and a_found:
                    # Verify href is a genuine match slug: must have ≥ 4 path segments
                    # (e.g. /football/italy/serie-a/pisa-bologna-Qy7EzcEL/)
                    # This prevents league URLs (/football/italy/serie-a/) from matching.
                    parts = [p for p in href.strip('/').split('/') if p]
                    if len(parts) >= 4:
                        logger.info(f"⚡ Cache hit! Found match URL for {home_team} vs {away_team} (season {season_id})")
                        return f"https://www.oddsportal.com{href}"
            
            logger.debug(f"Cache miss: {home_team} vs {away_team} not in cache for season {season_id} ({len(cached)} URLs checked)")
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Error checking league cache: {e}")
            return None

    async def scrape_match(self, match_url: str, sport: str = None) -> Optional[MatchOddsData]:
        """
        Navigate to a match page and extract odds for all configured market periods.
        
        Uses URL fragment identifiers (#{group};{period}) to switch between
        market groups/periods without reloading the page.
        
        Args:
            match_url: Full OddsPortal match URL
            sport: Sport string from SEASON_ODDSPORTAL_MAP (e.g. "football", "basketball")
                   Used to determine scraping route from SPORT_SCRAPING_ROUTES.
                   If None, falls back to legacy single-extraction behavior.
        """
        if not self.context:
            await self.start()
            
        page = await self.context.new_page()
        
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
            try:
                response = await page.goto(initial_url, wait_until="domcontentloaded", timeout=30000)
            except Exception as e:
                error_str = str(e).lower()

                # Immediate network/proxy failures should short-circuit quickly.
                immediate_network_markers = [
                    "err_timed_out",
                    "err_connection",
                    "err_name_not_resolved",
                    "err_tunnel_connection_failed",
                    "err_proxy_connection_failed",
                    "err_socks_connection_failed",
                    "net::err_failed",
                    "timeout 30000ms exceeded",
                    "navigation timeout",
                ]
                if any(err in error_str for err in immediate_network_markers):
                    logger.error(f"🚨 FAST FAIL (Network): Page navigation completely failed: {str(e).split(chr(10))[0]}")
                    await page.close()
                    return None

                # Non-fatal: domcontentloaded might have partially loaded; continue to fast-fail checks.
                logger.warning(f"⚠️ Initial goto struggled, but continuing to check for odds: {e}")

            # If we received a hard HTTP error, fail fast.
            if response and response.status >= 400:
                logger.error(f"🚨 FAST FAIL (HTTP): Match page returned status {response.status} for {initial_url}")
                await page.close()
                return None

            # Blank-shell safety: if navigation yielded no response and no useful page context, fail early.
            if response is None:
                try:
                    title_now = (await page.title() or "").strip()
                    url_now = (page.url or "").strip().lower()
                    if not title_now and (not url_now or url_now == "about:blank"):
                        logger.error("🚨 FAST FAIL (Blank shell): navigation produced empty page context")
                        await page.close()
                        return None
                except Exception:
                    pass
            t_goto = time.perf_counter()
            log_timing(f"Match page load ({initial_url}) took {t_goto - t0:.2f}s")
            
            # --- Fast Fail & Smart Wait Implementation ---
            fast_fail_event = asyncio.Event()
            fast_fail_reason = "Unknown"

            async def on_fast_fail_signal(reason: str):
                nonlocal fast_fail_reason
                fast_fail_reason = reason
                fast_fail_event.set()

            # Fast Fail Check 1: Quickly detect Cloudflare blocks via Title
            try:
                page_title = await page.title()
                if any(blocked in page_title for blocked in ["Access Denied", "Just a moment...", "Attention Required!", "Security check", "Cloudflare"]):
                    logger.error(f"🚨 FAST FAIL (Title): Proxy IP blocked by Cloudflare. Title: '{page_title}'")
                    await page.close()
                    return None
            except Exception:
                pass

            # Fast Fail Check 2: Event-driven detection of unrendered or shell-only SPA pages
            # Expose a function for the browser to signal failure back to Python
            await page.expose_function('__signalFastFail', on_fast_fail_signal)

            empty_timeout_ms = int(os.environ.get("ODDSPORTAL_FAST_FAIL_EMPTY_TIMEOUT_MS", "15000"))

            # Inject observer-based fast-fail logic
            await page.evaluate(r"""
                (EMPTY_TIMEOUT_MS) => {
                    if (window.__opFastFailInstalled) return;
                    window.__opFastFailInstalled = true;

                    let signaled = false;
                    const signalOnce = (reason) => {
                        if (signaled) return;
                        signaled = true;
                        window.__signalFastFail(reason);
                    };

                    const hasOddsMarkers = () => {
                        return !!(
                            document.querySelector('div.border-black-borders.flex.h-9') ||
                            document.querySelector('div[data-testid="over-under-collapsed-row"]') ||
                            document.querySelector('div[data-testid="asian-handicap-collapsed-row"]') ||
                            document.querySelector('div[data-testid="over-under-expanded-row"]') ||
                            document.querySelector('ul.visible-links.odds-tabs li') ||
                            document.querySelector('div[data-testid="kickoff-events-nav"]')
                        );
                    };

                    const container = document.querySelector('div.event-container');
                    const isContainerEmpty = () => {
                        if (!container) return true;
                        return container.children.length === 0 ||
                            (container.children.length === 1 && container.innerHTML.trim() === '<!---->');
                    };

                    if (hasOddsMarkers()) {
                        return;
                    }

                    let failTimer = setTimeout(() => {
                        if (hasOddsMarkers()) return;
                        if (!container) {
                            signalOnce('Missing event-container');
                        } else if (isContainerEmpty()) {
                            signalOnce('Event container stayed empty (SPA render failure)');
                        } else {
                            signalOnce('No odds UI markers loaded');
                        }
                    }, EMPTY_TIMEOUT_MS);

                    const observer = new MutationObserver(() => {
                        if (hasOddsMarkers()) {
                            clearTimeout(failTimer);
                            observer.disconnect();
                        }
                    });

                    const target = document.body || document.documentElement;
                    if (target) {
                        observer.observe(target, { childList: true, subtree: true });
                    }
                }
            """, empty_timeout_ms)

            # Race: wait for initial market render (success) vs fast-fail event.
            first_extract_fn = (first_group or {}).get("extract_fn", "standard")

            async def wait_for_rows():
                try:
                    rendered = await self._wait_for_market_render(page, first_extract_fn, timeout_ms=60000)
                    return "success" if rendered else "timeout"
                except Exception as e:
                    return f"error: {str(e)}"

            async def wait_for_fail():
                await fast_fail_event.wait()
                return "fast_fail"

            # Create tasks
            rows_task = asyncio.create_task(wait_for_rows())
            fail_task = asyncio.create_task(wait_for_fail())

            done, pending = await asyncio.wait(
                [rows_task, fail_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel whichever task is still running
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Determine results
            finished_task = done.pop()
            task_result = finished_task.result()

            if task_result != "success":
                # Failure path (either fast fail or 60s timeout)
                page_title = "Unknown"
                try: page_title = await page.title()
                except: pass
                
                if task_result == "fast_fail":
                    logger.error(f"🚨 FAST FAIL: {fast_fail_reason}")
                else:
                    logger.error(f"❌ Match page failure: {task_result}")
                
                logger.error(f"   URL: {initial_url}")
                logger.error(f"   Title: {page_title}")
                
                if self.debug_dir:
                    try:
                        fail_path = os.path.join(self.debug_dir, "match_load_failure.png")
                        await page.screenshot(path=fail_path, full_page=True)
                        fail_html = os.path.join(self.debug_dir, "match_load_failure.html")
                        with open(fail_html, "w", encoding="utf-8") as f:
                            f.write(await page.content())
                        logger.info(f"💾 Saved failure debug info to {self.debug_dir}")
                    except Exception as se:
                        logger.warning(f"⚠️ Failed to save failure debug info: {se}")
                
                await page.close()
                return None
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
            
            # Save debug screenshot for the initial page load
            if self.debug_dir:
                try:
                    screenshot_path = os.path.join(self.debug_dir, "match_page_loaded.png")
                    await page.screenshot(path=screenshot_path, full_page=True)
                    html_path = os.path.join(self.debug_dir, "match_page_loaded.html")
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(await page.content())
                    logger.info(f"💾 Saved debug info to {self.debug_dir}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to save debug info: {e}")
            
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
                        # Last resort: full page reload to the target group's URL fragment
                        logger.warning(f"⚠️ Tab click failed for {group_key}. Attempting full page reload recovery...")
                        try:
                            fragment = OP_GROUPS.get(group_key, group_key)
                            first_period_key = periods[0][0] if periods else "FULL_TIME"
                            period_code = OP_PERIODS.get(first_period_key, 2)
                            base_url = self._normalize_base_match_url(match_url)
                            reload_url = f"{base_url}/#{fragment};{period_code}"
                            await page.goto(reload_url, wait_until="domcontentloaded", timeout=30000)
                            rendered = await self._wait_for_market_render(page, extract_fn, timeout_ms=15000)
                            if not rendered:
                                raise TimeoutError("market render wait timed out after group reload")
                            await asyncio.sleep(1)
                            success = True
                            logger.info(f"✅ Page reload recovery succeeded for {group_key}")
                        except Exception as re:
                            logger.error(f"❌ Page reload recovery failed for {group_key}: {re}")
                    
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
                            # Fallback: full page reload to the exact group+period fragment URL
                            logger.warning(f"⚠️ Period tab click failed for {db_market_period}. Attempting page reload recovery...")
                            try:
                                fragment = OP_GROUPS.get(group_key, group_key)
                                period_code = OP_PERIODS.get(period_key, 2)
                                base_url = self._normalize_base_match_url(match_url)
                                reload_url = f"{base_url}/#{fragment};{period_code}"
                                await page.goto(reload_url, wait_until="domcontentloaded", timeout=30000)
                                rendered = await self._wait_for_market_render(page, extract_fn, timeout_ms=15000)
                                if not rendered:
                                    raise TimeoutError("market render wait timed out after period reload")
                                await asyncio.sleep(1)
                                tab_clicked = True
                                logger.info(f"✅ Page reload recovery succeeded for period {db_market_period}")
                            except Exception as re:
                                logger.error(f"❌ Page reload recovery failed for period {db_market_period}: {re}")
                    
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
                        logger.warning(f"⚠️ No data extracted for period {db_market_period}")
                        continue
                
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
                            target_bookie_obj.initial_odds_1 = opening.get('1')
                            target_bookie_obj.initial_odds_x = opening.get('X')
                            target_bookie_obj.initial_odds_2 = opening.get('2')
                            logger.info(f"✅ Opening odds ({db_market_period}): {lbl_1}={opening.get('1')} {lbl_x}{lbl_2}={opening.get('2')}")
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
                            period_data.betfair.initial_back_1 = bf_opening.get('back_1')
                            period_data.betfair.initial_back_x = bf_opening.get('back_x')
                            period_data.betfair.initial_back_2 = bf_opening.get('back_2')
                            period_data.betfair.initial_lay_1 = bf_opening.get('lay_1')
                            period_data.betfair.initial_lay_x = bf_opening.get('lay_x')
                            period_data.betfair.initial_lay_2 = bf_opening.get('lay_2')
                            logger.info(f"✅ Betfair opening odds ({db_market_period}):")
                            logger.info(f"   Back: 1={bf_opening.get('back_1')} X={bf_opening.get('back_x')} 2={bf_opening.get('back_2')}")
                            logger.info(f"   Lay:  1={bf_opening.get('lay_1')} X={bf_opening.get('lay_x')} 2={bf_opening.get('lay_2')}")
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
            await page.close()


    def _parse_opening_odds_from_modal_html(self, modal_html: str) -> Optional[str]:
        """
        Parse the opening odds value from the tooltip modal HTML.
        
        The modal HTML (from OddsHarvester hover pattern) contains:
          <div class="mt-2 gap-1">
            <div class="font-bold">Opening odds:</div>
            <div class="flex gap-1">
              <div>15 Feb, 19:47</div>
              <div class="font-bold">1.69</div>   ← this is the opening odds
            </div>
          </div>
        """
        try:
            # Look for 'Opening odds:' text, then find the bold value after the date
            if 'Opening odds' not in modal_html:
                return None
            
            # Find the section after 'Opening odds:'
            idx = modal_html.find('Opening odds')
            section = modal_html[idx:idx + 500]  # Take a reasonable chunk
            
            # Find all font-bold divs in this section and get the numeric one
            import re
            # Match <div class="font-bold">NUMBER</div> or <div class="font-bold" ...>NUMBER</div>
            matches = re.findall(r'<div[^>]*font-bold[^>]*>([\d.]+)</div>', section)
            for val in matches:
                try:
                    f = float(val)
                    if 1.0 <= f <= 1001.0:  # Sanity check: valid odds range (Lay odds can be high)
                        return val
                except ValueError:
                    continue
            return None
        except Exception as e:
            logger.warning(f"Error parsing opening odds from modal: {e}")
            return None

    async def _extract_opening_odds_for_bookie(
        self, page: Page, bookie_name: str
    ) -> Optional[Dict[str, Optional[str]]]:
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
            Dict with keys '1', 'X', '2' mapping to opening odds strings, or None on failure.
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
                
            result: Dict[str, Optional[str]] = {}

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
                                opening_val = self._parse_opening_odds_from_modal_html(html)
                                result[choice] = opening_val
                                logger.debug(f"  Cell {choice}: opening={opening_val} (attempt {attempt + 1})")
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


    async def _extract_opening_odds_betfair(self, page: Page) -> Optional[Dict[str, Optional[str]]]:
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
            mapping to opening odds strings, or None on failure.
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

            result: Dict[str, Optional[str]] = {}

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
                            opening_val = self._parse_opening_odds_from_modal_html(html)

                            if self.debug_dir:
                                try:
                                    debug_path = os.path.join(self.debug_dir, f"modal_Betfair_{choice}.html")
                                    with open(debug_path, "w", encoding="utf-8") as f:
                                        f.write(html)
                                except Exception as e:
                                    logger.warning(f"⚠️ Failed to save modal HTML: {e}")

                            result[choice] = opening_val
                            logger.debug(f"  Betfair {choice}: opening={opening_val} (attempt {attempt + 1})")
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
                back_str = f"Back: 1={result.get('back_1')} X={result.get('back_x')} 2={result.get('back_2')}"
                lay_str  = f"Lay: 1={result.get('lay_1')} X={result.get('lay_x')} 2={result.get('lay_2')}"
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


def scrape_multiple_matches_sync(tasks: List[Dict], debug_dir: Optional[str] = None, on_result=None) -> Dict[int, Optional[MatchOddsData]]:
    """
    Scrape multiple matches using ONE browser session (browser reuse).
    
    Tries DB cache first for each match URL. On cache hit, skips league page
    navigation entirely (~14s saved). On cache miss, navigates to the league
    page (which also populates the cache for subsequent events).
    
    Args:
        tasks: List of dicts with keys: event_id, league_url, home_team, away_team, season_id
        debug_dir: Optional directory to save screenshots/HTML on failure
    
    Returns:
        Dict mapping event_id -> MatchOddsData (or None if scrape failed)
    """
    results = {}
    
    if not tasks:
        return results
    
    logger.info(f"🔍 OddsPortal batch: scraping {len(tasks)} matches with shared browser")
    
    try:
        async def _run():
            scraper = OddsPortalScraper(debug_dir=debug_dir)
            await scraper.start()  # Browser opens ONCE
            try:
                for i, task in enumerate(tasks):
                    event_id = task['event_id']
                    season_id = task.get('season_id')
                    try:
                        logger.info(f"🔍 OddsPortal [{i+1}/{len(tasks)}]: {task['home_team']} vs {task['away_team']}")
                        
                        # Step 1: Try cache first (no browser navigation needed)
                        match_url = None
                        if season_id:
                            match_url = scraper.find_match_url_from_cache(
                                season_id, task['home_team'], task['away_team']
                            )
                        
                        # Step 2: Fall back to live league page navigation (also populates cache)
                        if not match_url:
                            match_url = await scraper.find_match_url(
                                task['league_url'], task['home_team'], task['away_team'],
                                season_id=season_id
                            )
                        
                        # Resolve sport from season_id for scraping route
                        task_sport = task.get('sport')
                        if not task_sport and season_id:
                            op_info = SEASON_ODDSPORTAL_MAP.get(season_id)
                            if op_info:
                                task_sport = op_info.get('sport')
                        
                        data = None
                        if match_url:
                            data = await scraper.scrape_match(match_url, sport=task_sport)
                        
                        # Session-aware retry: if scrape OR discovery failed (likely bad proxy/IP),
                        # restart browser with a fresh proxy session and retry once.
                        if data is None:
                            logger.warning(f"🔄 OddsPortal [{i+1}/{len(tasks)}]: No data (or match discovery failed) — restarting browser with new proxy session and retrying...")
                            await scraper.stop()
                            await scraper.start()  # Generates a fresh session ID = new IP
                            
                            # Re-find match URL (cache may still have it)
                            retry_url = match_url
                            if not retry_url and season_id:
                                retry_url = scraper.find_match_url_from_cache(
                                    season_id, task['home_team'], task['away_team']
                                )
                            if not retry_url:
                                retry_url = await scraper.find_match_url(
                                    task['league_url'], task['home_team'], task['away_team'],
                                    season_id=season_id
                                )
                            if retry_url:
                                data = await scraper.scrape_match(retry_url, sport=task_sport)
                                if data:
                                    logger.info(f"✅ OddsPortal [{i+1}/{len(tasks)}]: RETRY SUCCEEDED with new session-{scraper._session_id}")
                                else:
                                    logger.warning(f"⚠️ OddsPortal [{i+1}/{len(tasks)}]: Retry also returned no data")
                        
                        results[event_id] = data
                        if data:
                            logger.info(f"✅ OddsPortal [{i+1}/{len(tasks)}]: Got {len(data.extractions)} period(s), {len(data.bookie_odds)} bookies")
                        else:
                            logger.warning(f"⚠️ OddsPortal [{i+1}/{len(tasks)}]: Scrape failed (Match not found or navigation error)")
                        
                        # Invoke callback immediately so caller can save/signal per event
                        if on_result:
                            try:
                                on_result(event_id, data)
                            except Exception as cb_err:
                                logger.error(f"❌ on_result callback error for event {event_id}: {cb_err}")
                        
                        # Small delay between scrapes to be respectful to OddsPortal
                        if i < len(tasks) - 1:
                            await asyncio.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"❌ OddsPortal scrape failed for event {event_id}: {e}")
                        results[event_id] = None
                        if on_result:
                            try:
                                on_result(event_id, None)
                            except Exception as cb_err:
                                logger.error(f"❌ on_result callback error for event {event_id}: {cb_err}")
            finally:
                await scraper.stop()  # Browser closes ONCE
            return results
        
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
        logger.error(f"❌ Error in scrape_multiple_matches_sync: {e}\n{traceback.format_exc()}")
        return results

def scrape_multiple_matches_parallel_sync(tasks: List[Dict], num_browsers: int = 1, debug_dir: Optional[str] = None, on_result=None) -> Dict[int, Optional[MatchOddsData]]:
    """
    Distribute scrape tasks across multiple concurrent Playwright browsers.
    If num_browsers == 1, delegates directly to scrape_multiple_matches_sync.
    Otherwise, splits tasks and processes them in a ThreadPoolExecutor.
    """
    if not tasks:
        return {}
        
    if num_browsers <= 1 or len(tasks) == 1:
        return scrape_multiple_matches_sync(tasks, debug_dir=debug_dir, on_result=on_result)
        
    logger.info(f"🚀 OddsPortal Parallel: Splitting {len(tasks)} tasks across {num_browsers} browsers")
    
    # Determine chunks (simple round-robin distribution to balance leagues)
    chunks = [[] for _ in range(num_browsers)]
    for i, task in enumerate(tasks):
        chunks[i % num_browsers].append(task)
        
    # Remove empty chunks if tasks < num_browsers
    chunks = [c for c in chunks if c]
    
    all_results = {}
    
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        future_to_chunk = {
            executor.submit(scrape_multiple_matches_sync, chunk, debug_dir, on_result): i 
            for i, chunk in enumerate(chunks)
        }
        
        for future in as_completed(future_to_chunk):
            chunk_idx = future_to_chunk[future]
            try:
                chunk_result = future.result()
                all_results.update(chunk_result)
                logger.info(f"✅ OddsPortal Parallel: Browser {chunk_idx + 1}/{len(chunks)} completed successfully")
            except Exception as e:
                import traceback
                logger.error(f"❌ OddsPortal Parallel: Browser {chunk_idx + 1} failed: {e}\n{traceback.format_exc()}")
                
    return all_results
