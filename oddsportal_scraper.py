import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict

from playwright.async_api import async_playwright, Page, Browser, BrowserContext

# Import configuration
try:
    from oddsportal_config import SEASON_ODDSPORTAL_MAP, BOOKIE_ALIASES, TEAM_ALIASES
except ImportError:
    # Fallback/Mock for standalone testing
    SEASON_ODDSPORTAL_MAP = {}
    BOOKIE_ALIASES = {}
    TEAM_ALIASES = {}

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
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.context: Optional[BrowserContext] = None
        
    async def start(self):
        """Start the browser session."""
        if self.browser:
            return
            
        self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--window-size=1920,1080",
            ]
        )
        
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

    async def find_match_url(self, league_url: str, home_team: str, away_team: str) -> Optional[str]:
        """
        Navigate to league page and find match URL by team names.
        """
        if not self.context:
            await self.start()
            
        page = await self.context.new_page()
        try:
            logger.info(f"🌐 Navigating to league: {league_url}")
            t0 = time.perf_counter()
            response = await page.goto(league_url, wait_until="domcontentloaded", timeout=30000)
            if not response or response.status != 200:
                logger.error(f"❌ Failed to load league page. Status: {response.status if response else 'N/A'}")
                return None
            
            # Wait for content
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
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
                return s.lower().replace("fc", "").replace("cf", "").strip()
                
            home_norm = normalize(home_team)
            away_norm = normalize(away_team)
            
            # Check aliases if available
            home_alias = TEAM_ALIASES.get(home_team, home_team)
            away_alias = TEAM_ALIASES.get(away_team, away_team)
            home_alias_norm = normalize(home_alias)
            away_alias_norm = normalize(away_alias)
            
            event_rows = await page.query_selector_all("div.eventRow")
            if not event_rows:
                logger.warning(f"⚠️ No event rows found on {league_url}")
                return None
                
            logger.info(f"🔎 Scanning {len(event_rows)} rows for {home_team} vs {away_team}...")
            
            for row in event_rows:
                text = await row.inner_text()
                text_norm = normalize(text)
                
                # Check for both teams
                # Logic: Check if (Home OR HomeAlias) AND (Away OR AwayAlias) are in text
                h_found = home_norm in text_norm or home_alias_norm in text_norm
                a_found = away_norm in text_norm or away_alias_norm in text_norm
                
                if h_found and a_found:
                    # Found match row, extract link
                    link = await row.query_selector("a[href^='/'][href*='-']")
                    if not link:
                         # Fallback for any link
                         link = await row.query_selector("a[href]")
                         
                    if link:
                        href = await link.get_attribute("href")
                        if href:
                            logger.info(f"✅ Found match link: {href}")
                            return f"https://www.oddsportal.com{href}"
            
            logger.warning(f"❌ Match not found: {home_team} vs {away_team}")
            return None
            
        except Exception as e:
            logger.error(f"Error finding match on {league_url}: {e}")
            return None
        finally:
            await page.close()

    async def scrape_match(self, match_url: str) -> Optional[MatchOddsData]:

        """
        Navigate to a match page and extract odds.
        """
        if not self.context:
            await self.start()
            
        page = await self.context.new_page()
        
        try:
            logger.info(f"🌐 Navigating to match: {match_url}")
            t0 = time.perf_counter()
            
            # Navigate
            response = await page.goto(match_url, wait_until="domcontentloaded", timeout=30000)
            if not response or response.status != 200:
                logger.error(f"❌ Failed to load page. Status: {response.status if response else 'N/A'}")
                return None
            
            # Wait for content
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass  # Proceed even if network doesn't fully idle
                
            # Handle cookie banner
            try:
                accept_btn = await page.query_selector("button:has-text('I Accept'), button:has-text('Accept All')")
                if accept_btn:
                    await accept_btn.click()
                    await asyncio.sleep(0.5)
            except Exception:
                pass
            
            # Scroll to load lazy elements
            await page.evaluate("window.scrollTo(0, 500)")
            await asyncio.sleep(1.0)
            
            # Extract Data
            t_extract_start = time.perf_counter()
            data = await self._extract_data(page, match_url)
            extract_duration = time.perf_counter() - t_extract_start
            
            if data:
                data.extraction_time_ms = extract_duration * 1000
                logger.info(f"✅ Extracted odds for {data.home_team} vs {data.away_team} ({len(data.bookie_odds)} bookies)")
            
            return data
            
        except Exception as e:
            logger.error(f"❌ Error scraping match {match_url}: {e}")
            return None
        finally:
            await page.close()

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
                    
                    result.bookies.push({
                        name: bookieName,
                        odds1: odds[0] || '-',
                        oddsX: odds.length === 3 ? (odds[1] || '-') : '-',
                        odds2: odds.length === 3 ? (odds[2] || '-') : (odds[1] || '-'),
                        payout: payout,
                    });
                }
                
                // --- Extract Betfair Exchange ---
                // Search for the section directly
                const exchangeSection = document.querySelector('div[data-testid="betting-exchanges-section"]');
                if (exchangeSection) {
                    const allOddContainers = exchangeSection.querySelectorAll('div[data-testid="odd-container"]');
                    
                    if (allOddContainers.length >= 6) {
                        const extractOddFromContainer = (container) => {
                            const ps = container.querySelectorAll('p');
                            for (const p of ps) {
                                const txt = p.textContent.trim();
                                if (!txt || txt === '-') continue;
                                if (/^\d+(\.\d+)?$/.test(txt)) return txt;
                            }
                            return null;
                        };

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
                        
                        // Extract payout from section text if available
                        const sectionText = exchangeSection.innerText || '';
                        const payMatch = sectionText.match(/(\d{2,3}\.\d)%/);
                        if (payMatch) result.betfairPayout = payMatch[0];
                    }
                }
                
                return result;
            }
        """)

        # Convert to Python Objects
        match_data = MatchOddsData(match_url=match_url)
        match_data.home_team = raw_data.get('homeTeam', 'Unknown')
        match_data.away_team = raw_data.get('awayTeam', 'Unknown')
        
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
                
        return asyncio.run(_run())
    except Exception as e:
        logger.error(f"Error in scrape_match_sync: {e}")
        return None


