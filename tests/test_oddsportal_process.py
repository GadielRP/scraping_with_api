import asyncio
import os
import sys
import argparse
import time
import json
from loguru import logger
from datetime import datetime
from dotenv import load_dotenv

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# Load environment variables (particularly PROXY_ENABLED)
load_dotenv()

# Setup logging once at startup
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(name)s:%(funcName)s:%(lineno)d - %(message)s'
)

os.environ["DEBUG_TIMING"] = "true"

from database import db_manager
from repository import EventRepository, MarketRepository
from modules.oddsportal.oddsportal_config import (
    ODDSPORTAL_COMPETITION_ROUTES,
    SPORT_SCRAPING_ROUTES,
)
from modules.oddsportal import OddsPortalScraper, MatchOddsData, MarketExtraction

def generate_report(event, debug_dir: str, result: MatchOddsData, duration: float, bandwidth_bytes: int = 0):
    report_path = os.path.join(debug_dir, "report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# OddsPortal Debug Report\n\n")
        f.write(f"- **Event**: {event.home_team} vs {event.away_team}\n")
        f.write(f"- **Event ID**: {event.id}\n")
        f.write(f"- **Timestamp**: {datetime.now().isoformat()}\n")
        f.write(f"- **Duration**: {duration:.2f} seconds\n")
        if bandwidth_bytes > 0:
            bw_mb = bandwidth_bytes / (1024 * 1024)
            f.write(f"- **Bandwidth Used**: {bw_mb:.2f} MB ({bandwidth_bytes} bytes)\n")
        
        f.write(f"## Scraping Result\n\n")
        if not result:
            f.write(f"**FAILED**: No data extracted.\n")
            return
        
        f.write(f"- **Sport**: {result.sport}\n")
        f.write(f"- **Periods extracted**: {len(result.extractions)}\n")
        total_bookies = sum(len(e.bookie_odds) for e in result.extractions)
        f.write(f"- **Total bookie entries**: {total_bookies}\n\n")
        
        for i, ext in enumerate(result.extractions):
            f.write(f"### Period {i+1}: {ext.market_group} / {ext.market_period}\n\n")
            f.write(f"- **Market Name**: {ext.market_name}\n")
            f.write(f"- **Bookies**: {len(ext.bookie_odds)}\n\n")
            
            for b in ext.bookie_odds:
                if b.initial_odds_1 or b.initial_odds_x or b.initial_odds_2:
                    is_ou = ext.market_group == "Over/Under"
                    lbl_1 = "Over" if is_ou else "1"
                    lbl_2 = "Under" if is_ou else "2"
                    lbl_x_open = f", X={b.initial_odds_x}" if b.initial_odds_x and not is_ou else ""
                    lbl_x_final = f", X={b.odds_x}" if b.odds_x and not is_ou else ""
                    hc_lbl = f" [AH: {b.handicap}]" if getattr(b, "handicap", None) else ""
                    
                    f.write(f"**{b.name}**{hc_lbl}\n")
                    f.write(f"- Opening: {lbl_1}={b.initial_odds_1}{lbl_x_open}, {lbl_2}={b.initial_odds_2}\n")
                    f.write(f"- Final: {lbl_1}={b.odds_1}{lbl_x_final}, {lbl_2}={b.odds_2}\n\n")
                    
            if ext.betfair:
                f.write(f"#### Betfair Exchange\n\n")
                f.write(f"**Back**\n")
                f.write(f"- Opening: 1={ext.betfair.initial_back_1}, X={ext.betfair.initial_back_x}, 2={ext.betfair.initial_back_2}\n")
                f.write(f"- Final: 1={ext.betfair.back_1}, X={ext.betfair.back_x}, 2={ext.betfair.back_2}\n\n")
                f.write(f"**Lay**\n")
                f.write(f"- Opening: 1={ext.betfair.initial_lay_1}, X={ext.betfair.initial_lay_x}, 2={ext.betfair.initial_lay_2}\n")
                f.write(f"- Final: 1={ext.betfair.lay_1}, X={ext.betfair.lay_x}, 2={ext.betfair.lay_2}\n\n")
            
        logger.info(f"💾 Report generated at: {report_path}")

async def run_test(event_id: int, headless: bool = False):
    logger.info(f"🚀 Starting OddsPortal isolation test for Event ID: {event_id}")
    
    try:
        event = EventRepository.get_event_by_id(event_id)
        if not event:
            logger.error(f"❌ Event ID {event_id} not found in database.")
            return

        logger.info(f"✅ Found event: {event.home_team} vs {event.away_team} (Season ID: {event.season_id})")
        
        op_info = ODDSPORTAL_COMPETITION_ROUTES.get(event.competition_id)
        if not op_info:
            logger.error(
                f"❌ Competition ID {event.competition_id} is not mapped in "
                "ODDSPORTAL_COMPETITION_ROUTES."
            )
            return

        league_url = f"https://www.oddsportal.com/{op_info['sport']}/{op_info['country']}/{op_info['league']}/"
        logger.info(f"🌐 League URL: {league_url}")

        slug = f"{event.home_team}-vs-{event.away_team}".lower().replace(" ", "-").replace("/", "-")
        
        scraper = OddsPortalScraper(headless=headless, debug_dir=None) # We will set debug dir later
        await scraper.start()

        # Bandwidth tracker
        total_bandwidth_bytes = 0
        def handle_response(response):
            nonlocal total_bandwidth_bytes
            try:
                # Add request sizes roughly
                req = response.request
                req_size = len(req.url) + 50
                for k, v in req.headers.items():
                    req_size += len(k) + len(v) + 4
                
                # Add response sizes
                res_size = 50
                for k, v in response.headers.items():
                    res_size += len(k) + len(v) + 4
                
                body_size = 0
                if "content-length" in response.headers:
                    body_size = int(response.headers["content-length"])
                
                total_bandwidth_bytes += req_size + res_size + body_size
            except Exception:
                pass
                
        if scraper.context:
            scraper.context.on("response", handle_response)

        start_time = time.perf_counter()
        result = None
        
        try:
            logger.info("🔍 Checking match URL in cache...")
            match_url = scraper.find_match_url_from_cache(event.season_id, event.home_team, event.away_team)
            
            cache_suffix = "_cached" if match_url else "_no_cache"
            debug_dir = os.path.join(os.getcwd(), f"debug_{slug}{cache_suffix}")
            os.makedirs(debug_dir, exist_ok=True)
            logger.info(f"📂 Debug directory: {debug_dir}")
            scraper.debug_dir = debug_dir # Set the debug directory now
            
            if match_url:
                logger.info(f"⚡ Cache hit (test script)! Found match URL: {match_url}")
            else:
                logger.info("❌ Cache miss. Finding match URL from league page...")
                t_find_url = time.perf_counter()
                match_url = await scraper.find_match_url(league_url, event.home_team, event.away_team, season_id=event.season_id)
                logger.info(f"⏱️ [Test Timing] find_match_url took {time.perf_counter() - t_find_url:.2f}s")
            
            if not match_url:
                logger.error("❌ Could not find match URL on league page.")
            else:
                logger.info(f"🌐 Found Match URL: {match_url}")
                logger.info("🔍 Extracting odds...")
                
                # Resolve sport for scraping route
                sport = op_info.get('sport')
                route = SPORT_SCRAPING_ROUTES.get(sport)
                if route and 'groups' in route:
                    groups = route['groups']
                    logger.info(f"🗺️ Sport: {sport}, Route: {len(groups)} groups configured")
                    for g in groups:
                        logger.info(f"   Group: {g.get('group_key')}, periods={[p[0] for p in g.get('periods', [])]}")
                else:
                    logger.info(f"⚠️ No scraping route for sport '{sport}', will use legacy mode")
                
                t_scrape_match = time.perf_counter()
                result = await scraper.scrape_match(match_url, sport=sport)
                logger.info(f"⏱️ [Test Timing] scrape_match took {time.perf_counter() - t_scrape_match:.2f}s")
                
                # Session-aware retry: if scrape returned None (likely timeout/bad IP),
                # restart browser with fresh proxy session and retry once.
                if result is None:
                    logger.warning("🔄 Scrape returned no data — restarting browser with new proxy session and retrying...")
                    await scraper.stop()
                    await scraper.start()
                    logger.info(f"🔄 Retrying with new session-{scraper._session_id}")
                    
                    t_retry = time.perf_counter()
                    result = await scraper.scrape_match(match_url, sport=sport)
                    logger.info(f"⏱️ [Test Timing] retry scrape_match took {time.perf_counter() - t_retry:.2f}s")
                    if result:
                        logger.info(f"✅ RETRY SUCCEEDED with new session-{scraper._session_id}")
                    else:
                        logger.warning("⚠️ Retry also returned no data")
                
                if result:
                    # Log per-period results
                    logger.info(f"📊 Total extractions: {len(result.extractions)}")
                    for i, ext in enumerate(result.extractions):
                        logger.info(f"  [{i+1}] {ext.market_group}/{ext.market_period}: {len(ext.bookie_odds)} bookies, Betfair={'Yes' if ext.betfair else 'No'}")
                    
                    # Save raw data as JSON (now includes per-period breakdown)
                    data_dict = {
                        "home_team": result.home_team,
                        "away_team": result.away_team,
                        "sport": result.sport,
                        "extraction_time_ms": result.extraction_time_ms,
                        "extractions": [
                            {
                                "market_group": ext.market_group,
                                "market_period": ext.market_period,
                                "market_name": ext.market_name,
                                "bookies": [
                                    {
                                        "name": b.name,
                                        "final_1": b.odds_1,
                                        "final_x": b.odds_x,
                                        "final_2": b.odds_2,
                                        "initial_1": b.initial_odds_1,
                                        "initial_x": b.initial_odds_x,
                                        "initial_2": b.initial_odds_2,
                                        "handicap": getattr(b, "handicap", None)
                                    } for b in ext.bookie_odds
                                ],
                                "betfair": {
                                    "final_back": {"1": ext.betfair.back_1, "X": ext.betfair.back_x, "2": ext.betfair.back_2},
                                    "final_lay": {"1": ext.betfair.lay_1, "X": ext.betfair.lay_x, "2": ext.betfair.lay_2},
                                    "initial_back": {"1": ext.betfair.initial_back_1, "X": ext.betfair.initial_back_x, "2": ext.betfair.initial_back_2},
                                    "initial_lay": {"1": ext.betfair.initial_lay_1, "X": ext.betfair.initial_lay_x, "2": ext.betfair.initial_lay_2},
                                } if ext.betfair else None
                            } for ext in result.extractions
                        ]
                    }
                        
                    json_path = os.path.join(debug_dir, "scraped_data.json")
                    with open(json_path, "w", encoding="utf-8") as f:
                        json.dump(data_dict, f, indent=4)
                        
        except Exception as e:
            import traceback
            logger.error(f"❌ Scraper exception: {e}\n{traceback.format_exc()}")
        finally:
            await scraper.stop()
            duration = time.perf_counter() - start_time
            bw_mb = total_bandwidth_bytes / (1024 * 1024)
            logger.info(f"⏱️ [Test Timing] Entire process took {duration:.2f}s | Bandwidth used: {bw_mb:.2f} MB")
            try:
                generate_report(event, debug_dir, result, duration, total_bandwidth_bytes)
            except NameError:
                # In case debug_dir is undefined from earlier exception
                pass
            logger.info("✅ Done.")

    except Exception as e:
        logger.error(f"❌ Error during test: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Isolate and test OddsPortal scraping process")
    parser.add_argument("event_id", type=int, help="The SofaScore Event ID to test")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    args = parser.parse_args()
    
    # Required for Windows if event loop is not correctly setup
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    asyncio.run(run_test(args.event_id, headless=args.headless))
