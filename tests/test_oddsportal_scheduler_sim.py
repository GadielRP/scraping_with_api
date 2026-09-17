import asyncio
import os
import sys
import argparse
import time
import json
from datetime import datetime
from dotenv import load_dotenv
import logging

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

load_dotenv()

# Enable internal scraper timing logs
os.environ["DEBUG_TIMING"] = "true"

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | %(levelname)-7s | %(name)s:%(funcName)s:%(lineno)d - %(message)s'
)
logger = logging.getLogger(__name__)

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories import EventRepository, MarketRepository
from modules.oddsportal.oddsportal_config import (
    ODDSPORTAL_COMPETITION_ROUTES,
    SPORT_SCRAPING_ROUTES,
)
from modules.oddsportal import scrape_multiple_matches_parallel_sync
from modules.oddsportal import OddsPortalScraper

def run_test(event_ids: list, headless: bool = False):
    logger.info(f"🚀 Starting OddsPortal Scheduler Simulation test for Event IDs: {event_ids}")
    logger.info(f"⚙️ Headless Mode: {headless}")
    
    # Force the headless mode onto the scrapper orchestrator without changing the original file.
    original_init = OddsPortalScraper.__init__
    def patched_init(self, *args, **kwargs):
        kwargs['headless'] = headless
        kwargs['testing_mode'] = True
        return original_init(self, *args, **kwargs)
    OddsPortalScraper.__init__ = patched_init

    op_tasks = []
    
    t_start_prep = time.perf_counter()
    for event_id in event_ids:
        event = EventRepository.get_event_by_id(event_id)
        if not event:
            logger.error(f"❌ Event ID {event_id} not found in database. Skipping.")
            continue
            
        op_info = ODDSPORTAL_COMPETITION_ROUTES.get(event.competition_id)
        if not op_info:
            logger.error(
                f"❌ Competition ID {event.competition_id} for event "
                f"{event_id} is not mapped in "
                "ODDSPORTAL_COMPETITION_ROUTES. Skipping."
            )
            continue
            
        league_url = f"https://www.cuotasahora.com/{op_info['sport']}/{op_info['country']}/{op_info['league']}/"
        logger.info(f"✅ Found eligible event {event_id}: {event.home_team} vs {event.away_team} -> {league_url}")
        
        # Log scraping route configuration like the old test script
        sport = op_info.get('sport')
        route = SPORT_SCRAPING_ROUTES.get(sport)
        if route and 'groups' in route:
            groups = route['groups']
            logger.info(f"🗺️ Sport: {sport}, Route: {len(groups)} groups configured")
            for g in groups:
                logger.info(f"   Group: {g.get('group_key')}, periods={[p[0] for p in g.get('periods', [])]}")
        else:
            logger.info(f"⚠️ No scraping route for sport '{sport}', will use legacy mode")

        op_tasks.append({
            'event_id': event.id,
            'league_url': league_url,
            'home_team': event.home_team,
            'away_team': event.away_team,
            'season_id': event.season_id,
            'sport': op_info['sport'],
        })
    
    logger.info(f"⏱️ [Test Timing] Initial event preparation took {time.perf_counter() - t_start_prep:.2f}s")
        
    if not op_tasks:
        logger.warning("No eligible events to process.")
        return
        
    debug_dir = os.path.join(CURRENT_DIR, "oddsportal_sim_debug")
    os.makedirs(debug_dir, exist_ok=True)
    
    saved_counts = {}

    def _on_event_scraped(ev_id, op_data):
        if op_data:
            try:
                logger.info(f"📊 Total extractions for event {ev_id}: {len(op_data.extractions)}")
                for i, ext in enumerate(op_data.extractions):
                    logger.info(f"  [{i+1}] {ext.market_group}/{ext.market_period}: {len(ext.bookie_odds)} bookies, Betfair={'Yes' if ext.betfair else 'No'}")
                
                # Save raw data as JSON
                data_dict = {
                    "home_team": op_data.home_team,
                    "away_team": op_data.away_team,
                    "sport": op_data.sport,
                    "extraction_time_ms": op_data.extraction_time_ms,
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
                        } for ext in op_data.extractions
                    ]
                }
                
                json_path = os.path.join(debug_dir, f"scraped_data_{ev_id}.json")
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(data_dict, f, indent=4)
                logger.info(f"💾 Saved JSON payload for event {ev_id} to {json_path}")
                
                # Same saving logic as scheduler.py
                saved = MarketRepository.save_markets_from_oddsportal(ev_id, op_data)
                saved_counts[ev_id] = saved
                logger.info(f"✅ OddsPortal: Saved {saved} markets/bookies for event {ev_id}")
            except Exception as e:
                logger.error(f"❌ OddsPortal: Error saving data for event {ev_id}: {e}")
                saved_counts[ev_id] = None
        else:
            logger.warning(f"⚠️ OddsPortal: No data for event {ev_id}")
            saved_counts[ev_id] = None

    num_browsers = len(op_tasks)
    logger.info(f"🌐 OddsPortal Simulation: Dispatching {len(op_tasks)} tasks to Tiered Orchestrator with {num_browsers} browser(s)...")
    
    start_time = time.perf_counter()
    
    op_results = scrape_multiple_matches_parallel_sync(
        op_tasks, 
        num_browsers=num_browsers, 
        debug_dir=debug_dir,
        on_result=_on_event_scraped
    )
    
    duration = time.perf_counter() - start_time
    logger.info(f"🌐 OddsPortal Simulation completed in {duration:.2f}s returned {len(op_results)} results")
    logger.info(f"💾 Saving results: {saved_counts}")
    logger.info("✅ Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate OddsPortal scheduler parallel task")
    parser.add_argument("event_ids", type=int, nargs="+", help="SofaScore Event IDs to test (space separated)")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    args = parser.parse_args()
    
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    run_test(args.event_ids, headless=args.headless)
