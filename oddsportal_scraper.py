import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# Import configuration
try:
    from config import Config
    from oddsportal_config import SEASON_ODDSPORTAL_MAP, BOOKIE_ALIASES, TEAM_ALIASES, PRIORITY_BOOKIES
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

logger = logging.getLogger(__name__)

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

@dataclass
class MatchOddsData:
    """Complete structured odds for a match."""
    match_url: str = ""
    home_team: str = "Unknown"
    away_team: str = "Unknown"
    bookie_odds: List[BookieOdds] = field(default_factory=list)
    betfair: Optional[BetfairExchangeOdds] = None
    extraction_time_ms: float = 0


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
            logger.info("🛡️ OddsPortalScraper: Launching Playwright with PROXY configuration")

        self.browser = await self.playwright.chromium.launch(**launch_args)
        
        # Create context with anti-detection
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/Chicago",
            java_script_enabled=True,
        )
        
        # Inject evasion scripts
        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
            );
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        """)
        
        logger.info("✅ OddsPortalScraper: Browser started")

    async def stop(self):
        """Stop the browser session."""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        
        self.browser = None
        self.context = None
        self.playwright = None
        logger.info("🛑 OddsPortalScraper: Browser stopped")

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
            response = await page.goto(league_url, wait_until="domcontentloaded", timeout=60000)
            if not response or response.status != 200:
                logger.error(f"❌ Failed to load league page. Status: {response.status if response else 'N/A'}")
                return None
            
            # Wait for event rows to appear (faster than networkidle which waits for all ads/trackers)
            try:
                await page.wait_for_selector("div.eventRow", timeout=30000)
            except Exception:
                pass
                
            # Handle cookie banner
            try:
                accept_btn = await page.query_selector("button:has-text('I Accept'), button:has-text('Accept All')")
                if accept_btn:
                    await accept_btn.click()
                    await asyncio.sleep(0.5)
            except Exception:
                pass
                
            # Search for match
            # Simple normalization for matching
            def normalize(s):
                return s.lower().replace("fc", "").replace("cf", "").replace("ud", "").strip()
                
            home_norm = normalize(home_team)
            away_norm = normalize(away_team)
            
            # Check aliases if available
            home_alias = TEAM_ALIASES.get(home_team, home_team)
            away_alias = TEAM_ALIASES.get(away_team, away_team)
            home_alias_norm = normalize(home_alias)
            away_alias_norm = normalize(away_alias)
            
            # --- OPTIMIZED SEARCH ALGORITHM ---
            # Instead of iterating DOM elements (slow), we extract all rows' text and links in ONE payload.
            # Complexity: O(1) network round-trip + O(N) string process in Python (negligible)
            
            rows_data = await page.evaluate("""() => {
                const rows = Array.from(document.querySelectorAll("div.eventRow"));
                return rows.map(row => {
                    const text = row.innerText;
                    // Get all links in the row
                    const links = Array.from(row.querySelectorAll("a[href]")).map(a => a.getAttribute("href"));
                    return { text, links };
                });
            }""")
            
            if not rows_data:
                logger.warning(f"⚠️ No event rows found on {league_url}")
                return None
            
            # --- CACHE POPULATION: Save all match URLs for this league ---
            if season_id:
                cache_dict = {}
                for row_data in rows_data:
                    for href in row_data.get('links', []):
                        if href and '-' in href:
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
                text_norm = normalize(row_data['text'] or "")
                
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
            
            def normalize(s):
                return s.lower().replace("fc", "").replace("cf", "").replace("ud", "").strip()
            
            home_norm = normalize(home_team)
            away_norm = normalize(away_team)
            
            home_alias = TEAM_ALIASES.get(home_team, home_team)
            away_alias = TEAM_ALIASES.get(away_team, away_team)
            home_alias_norm = normalize(home_alias)
            away_alias_norm = normalize(away_alias)
            
            for href, display_text in cached.items():
                text_norm = normalize(display_text or "")
                
                h_found = home_norm in text_norm or home_alias_norm in text_norm
                a_found = away_norm in text_norm or away_alias_norm in text_norm
                
                if h_found and a_found:
                    # Verify href has slug structure (contains hyphen, not just the league URL)
                    if '-' in href:
                        logger.info(f"⚡ Cache hit! Found match URL for {home_team} vs {away_team} (season {season_id})")
                        return f"https://www.oddsportal.com{href}"
            
            logger.debug(f"Cache miss: {home_team} vs {away_team} not in cache for season {season_id} ({len(cached)} URLs checked)")
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Error checking league cache: {e}")
            return None

    async def scrape_match(self, match_url: str) -> Optional[MatchOddsData]:

        """
        Navigate to a match page and extract odds.
        Also extracts opening/initial odds for the highest-priority available bookie
        and for Betfair Exchange via hover interactions.
        """
        if not self.context:
            await self.start()
            
        page = await self.context.new_page()
        
        try:
            logger.info(f"🌐 Navigating to match: {match_url}")
            t0 = time.perf_counter()
            
            # Navigate
            response = await page.goto(match_url, wait_until="domcontentloaded", timeout=60000)
            if not response or response.status != 200:
                logger.error(f"❌ Failed to load page. Status: {response.status if response else 'N/A'}")
                return None
            
            # Wait for content
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass  # Proceed even if network doesn't fully idle
                
            # Handle cookie/consent banner — try multiple selectors
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
            
            # Wait for bookie rows to ensure content is loaded
            try:
                # 'div.flex.h-9' is the row container for bookies
                await page.wait_for_selector('div.flex.h-9', timeout=10000)
            except Exception:
                logger.warning("⚠️ Bookie rows not found within timeout (page might be empty or blocked)")
            
            # Scroll to load lazy elements
            await page.evaluate("window.scrollTo(0, 500)")
            await asyncio.sleep(1.0)
            
            # Extract final odds via JS
            t_extract_start = time.perf_counter()
            data = await self._extract_data(page, match_url)
            extract_duration = time.perf_counter() - t_extract_start
            
            if self.debug_dir:
                try:
                    import os
                    screenshot_path = os.path.join(self.debug_dir, "match_page_loaded.png")
                    await page.screenshot(path=screenshot_path, full_page=True)
                    html_path = os.path.join(self.debug_dir, "match_page_loaded.html")
                    with open(html_path, "w", encoding="utf-8") as f:
                        f.write(await page.content())
                    logger.info(f"💾 Saved debug info to {self.debug_dir}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to save debug info: {e}")

            if data:
                data.extraction_time_ms = extract_duration * 1000
                logger.info(f"✅ Extracted odds for {data.home_team} vs {data.away_team} ({len(data.bookie_odds)} bookies)")
                
                # --- Extract opening/initial odds via hover ---
                # Step 1: Find the highest-priority bookie available in the scraped data
                target_bookie_obj = None
                for priority_name in PRIORITY_BOOKIES:
                    norm = priority_name.lower().strip()
                    # Try exact or partial match
                    for b in data.bookie_odds:
                        if priority_name.lower() in b.name.lower() or b.name.lower() in priority_name.lower():
                            target_bookie_obj = b
                            break
                    if target_bookie_obj:
                        break
                
                if target_bookie_obj:
                    logger.info(f"🎯 Extracting opening odds via hover for: {target_bookie_obj.name}")
                    opening = await self._extract_opening_odds_for_bookie(page, target_bookie_obj.name)
                    if opening:
                        target_bookie_obj.initial_odds_1 = opening.get('1')
                        target_bookie_obj.initial_odds_x = opening.get('X')
                        target_bookie_obj.initial_odds_2 = opening.get('2')
                        logger.info(f"✅ Opening odds for {target_bookie_obj.name}: 1={opening.get('1')} X={opening.get('X')} 2={opening.get('2')}")
                    else:
                        logger.warning(f"⚠️ Could not extract opening odds for {target_bookie_obj.name}")
                else:
                    logger.info("ℹ️ No priority bookie found in scraped data, skipping opening odds hover")
                
                # Step 2: Extract opening odds for Betfair Exchange
                if data.betfair:
                    logger.info("🎯 Extracting Betfair Exchange opening odds via hover")
                    bf_opening = await self._extract_opening_odds_betfair(page)
                    if bf_opening:
                        data.betfair.initial_back_1 = bf_opening.get('back_1')
                        data.betfair.initial_back_x = bf_opening.get('back_x')
                        data.betfair.initial_back_2 = bf_opening.get('back_2')
                        data.betfair.initial_lay_1 = bf_opening.get('lay_1')
                        data.betfair.initial_lay_x = bf_opening.get('lay_x')
                        data.betfair.initial_lay_2 = bf_opening.get('lay_2')
                        
                        logger.info(f"✅ Betfair opening odds:")
                        logger.info(f"   Back: 1={bf_opening.get('back_1')} X={bf_opening.get('back_x')} 2={bf_opening.get('back_2')}")
                        logger.info(f"   Lay:  1={bf_opening.get('lay_1')} X={bf_opening.get('lay_x')} 2={bf_opening.get('lay_2')}")
                    else:
                        logger.warning("⚠️ Could not extract Betfair Exchange opening odds")
            
            return data
            
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
            odds_blocks = await target_row.query_selector_all("div.flex-center.flex-col.font-bold")
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
                
                for attempt in range(max_retries):
                    try:
                        await odds_block.scroll_into_view_if_needed()
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
                        # Hover with force=True; fallback to mouse.move() + JS dispatch
                        try:
                            await odds_block.hover(force=True, timeout=5000)
                        except Exception:
                            bbox = await odds_block.bounding_box()
                            if bbox:
                                await page.mouse.move(
                                    bbox['x'] + bbox['width'] / 2,
                                    bbox['y'] + bbox['height'] / 2
                                )
                        # Also fire JS hover events to trigger Vue.js tooltip component
                        await page.evaluate("""
                            (el) => {
                                el.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true, cancelable: true}));
                                el.dispatchEvent(new MouseEvent('mouseover', {bubbles: true, cancelable: true}));
                            }
                        """, odds_block)
                        # Increasing wait: 2s, 3s, 4s per attempt
                        wait_ms = 2000 + (attempt * 1000)
                        await page.wait_for_timeout(wait_ms)
                        
                        # Find the modal via 'Odds movement' heading
                        try:
                            odds_movement_h3 = await page.wait_for_selector(
                                "h3:has-text('Odds movement')", timeout=3000
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
            
            return result if result else None
            
        except Exception as e:
            logger.error(f"Error in _extract_opening_odds_for_bookie({bookie_name}): {e}")
            return None

    async def _extract_opening_odds_betfair(self, page: Page) -> Optional[Dict[str, Optional[str]]]:
        """
        Extract opening/initial odds for Betfair Exchange by hovering over its odds cells.
        
        Betfair Exchange cells use a different structure (data-v-1580f19d) but the same
        tooltip pattern. The section is identified by data-testid='betting-exchanges-section'.
        
        Returns:
            Dict with keys 'back_1', 'back_x', 'back_2' for opening back odds.
        """
        try:
            exchange_section = await page.query_selector("div[data-testid='betting-exchanges-section']")
            if not exchange_section:
                logger.warning("⚠️ Betfair Exchange section not found for hover extraction")
                return None
            
            # Get all odd containers in the exchange section
            # Back odds are the first 2 (2-way) or 3 (3-way) containers
            odd_containers = await exchange_section.query_selector_all("div[data-testid='odd-container']")
            if not odd_containers:
                logger.warning("⚠️ No odd containers found in Betfair Exchange section")
                return None
            
            # Determine if 2-way or 3-way market
            is_three_way = len(odd_containers) >= 6
            back_count = 3 if is_three_way else 2
            
            # Identify Back and Lay containers
            if is_three_way:
                # Indices 0,1,2 = Back; 3,4,5 = Lay
                back_containers = odd_containers[:3]
                lay_containers = odd_containers[3:6]
                choice_keys_back = ['back_1', 'back_x', 'back_2']
                choice_keys_lay = ['lay_1', 'lay_x', 'lay_2']
            else:
                # Indices 0,1 = Back; 2,3 = Lay
                back_containers = odd_containers[:2]
                lay_containers = odd_containers[2:4]
                choice_keys_back = ['back_1', 'back_2']
                choice_keys_lay = ['lay_1', 'lay_2']
            
            result: Dict[str, Optional[str]] = {}
            
            # Combine for iteration
            all_targets = []
            for i, c in enumerate(back_containers):
                all_targets.append((choice_keys_back[i], c))
            for i, c in enumerate(lay_containers):
                all_targets.append((choice_keys_lay[i], c))
            
            logger.info(f"🖱️ Hovering {len(all_targets)} Betfair cells (Back & Lay)")

            # Dismiss overlays before starting
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
                try:
                    hover_target = await container.query_selector("div.flex-center.flex-col.font-bold")
                    if not hover_target:
                        hover_target = container

                    await hover_target.scroll_into_view_if_needed()
                    await page.evaluate("""
                        () => {
                            document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());
                            const onetrust = document.getElementById('onetrust-banner-sdk');
                            if (onetrust) onetrust.remove();
                        }
                    """)
                    try:
                        await hover_target.hover(force=True, timeout=5000)
                    except Exception:
                        bbox = await hover_target.bounding_box()
                        if bbox:
                            await page.mouse.move(
                                bbox['x'] + bbox['width'] / 2,
                                bbox['y'] + bbox['height'] / 2
                            )
                    # Fire JS hover events to trigger Vue.js tooltip
                    await page.evaluate("""
                        (el) => {
                            el.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true, cancelable: true}));
                            el.dispatchEvent(new MouseEvent('mouseover', {bubbles: true, cancelable: true}));
                        }
                    """, hover_target)
                    await page.wait_for_timeout(2000)
                    
                    try:
                        odds_movement_h3 = await page.wait_for_selector(
                            "h3:has-text('Odds movement')", timeout=3000
                        )
                        # Verify tooltip is actually visible before parsing
                        is_visible = await page.is_visible("h3:has-text('Odds movement')")
                        if not is_visible:
                            logger.warning(f"  ⚠️ Betfair {choice}: tooltip found in DOM but NOT visible")
                            result[choice] = None
                            await page.mouse.move(0, 0)
                            await page.wait_for_timeout(300)
                            continue
                        modal_wrapper = await odds_movement_h3.evaluate_handle(
                            "node => node.parentElement"
                        )
                        modal_el = modal_wrapper.as_element()
                        if modal_el:
                            html = await modal_el.inner_html()
                            opening_val = self._parse_opening_odds_from_modal_html(html)
                            
                            if self.debug_dir:
                                try:
                                    import os
                                    debug_path = os.path.join(self.debug_dir, f"modal_Betfair_{choice}.html")
                                    with open(debug_path, "w", encoding="utf-8") as f:
                                        f.write(html)
                                except Exception as e:
                                    logger.warning(f"⚠️ Failed to save modal HTML: {e}")
                            result[choice] = opening_val
                            logger.debug(f"  Betfair {choice}: opening={opening_val}")
                        else:
                            result[choice] = None
                    except Exception:
                        result[choice] = None
                    
                    await page.mouse.move(0, 0)
                    await page.wait_for_timeout(300)
                    
                except Exception as e:
                    logger.warning(f"  Error hovering Betfair cell {choice}: {e}")
                    result[choice] = None
            
            # Log summary
            if result:
                back_str = f"Back: 1={result.get('back_1')} X={result.get('back_x')} 2={result.get('back_2')}"
                lay_str = f"Lay: 1={result.get('lay_1')} X={result.get('lay_x')} 2={result.get('lay_2')}"
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
        
        # FILTER: Only include the FIRST matching bookie from the PRIORITY_BOOKIES list
        # This ensures we only store ONE bookie (plus potentially Betfair Exchange below)
        found_bookie = False
        for priority_name in PRIORITY_BOOKIES:
            # Check if this priority bookie exists in the raw data
            for b in raw_data.get('bookies', []):
                if priority_name.lower() in b['name'].lower() or b['name'].lower() in priority_name.lower():
                    # Found the highest priority bookie available
                    match_data.bookie_odds.append(BookieOdds(
                        name=b['name'],
                        odds_1=b['odds1'],
                        odds_x=b['oddsX'],
                        odds_2=b['odds2'],
                        payout=b['payout']
                    ))
                    found_bookie = True
                    break
            
            if found_bookie:
                break
            
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
                      home_team: str = None, away_team: str = None) -> Optional[MatchOddsData]:
    """
    Synchronous wrapper for scraping a single match.
    Can provide either match_url OR (league_url, home_team, away_team).
    creates a fresh scraper instance to ensure event loop safety.
    """
    try:
        async def _run():
            scraper = OddsPortalScraper()
            await scraper.start()
            try:
                target_url = match_url
                if not target_url and league_url and home_team and away_team:
                    target_url = await scraper.find_match_url(league_url, home_team, away_team)
                    
                if target_url:
                    return await scraper.scrape_match(target_url)
                return None
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


def scrape_multiple_matches_sync(tasks: List[Dict]) -> Dict[int, Optional[MatchOddsData]]:
    """
    Scrape multiple matches using ONE browser session (browser reuse).
    
    Tries DB cache first for each match URL. On cache hit, skips league page
    navigation entirely (~14s saved). On cache miss, navigates to the league
    page (which also populates the cache for subsequent events).
    
    Args:
        tasks: List of dicts with keys: event_id, league_url, home_team, away_team, season_id
    
    Returns:
        Dict mapping event_id -> MatchOddsData (or None if scrape failed)
    """
    results = {}
    
    if not tasks:
        return results
    
    logger.info(f"🔍 OddsPortal batch: scraping {len(tasks)} matches with shared browser")
    
    try:
        async def _run():
            scraper = OddsPortalScraper()
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
                        
                        if match_url:
                            data = await scraper.scrape_match(match_url)
                            results[event_id] = data
                            if data:
                                logger.info(f"✅ OddsPortal [{i+1}/{len(tasks)}]: Got {len(data.bookie_odds)} bookies")
                            else:
                                logger.warning(f"⚠️ OddsPortal [{i+1}/{len(tasks)}]: Scrape returned no data")
                        else:
                            logger.warning(f"⚠️ OddsPortal [{i+1}/{len(tasks)}]: Match not found on league page")
                            results[event_id] = None
                        
                        # Small delay between scrapes to be respectful to OddsPortal
                        if i < len(tasks) - 1:
                            await asyncio.sleep(2)
                        
                    except Exception as e:
                        logger.error(f"❌ OddsPortal scrape failed for event {event_id}: {e}")
                        results[event_id] = None
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
