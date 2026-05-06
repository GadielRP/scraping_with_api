"""OddsPortal batch and sync dispatch helpers."""

import asyncio
import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from threading import Condition
from typing import Any, Dict, List, Optional, Tuple

from .scraper_impl import OddsPortalScraper
from .dataclasses import MatchOddsData, ScrapeAttemptResult, GroupSeedResult
from .oddsportal_config import SEASON_ODDSPORTAL_MAP
from .cache_utils import _coerce_current_date, _build_league_group_key, _format_group_key
from .logging_context import _log_prefix

logger = logging.getLogger(__name__)
_scraper = None


def _get_scraper_cls():
    try:
        from oddsportal_scraper import OddsPortalScraper as RootOddsPortalScraper

        return RootOddsPortalScraper
    except Exception:
        return OddsPortalScraper


async def get_scaler():
    global _scraper
    if _scraper is None:
        _scraper = _get_scraper_cls()()
        await _scraper.start()
    return _scraper


async def _scrape_task_with_recovery(
    scraper: OddsPortalScraper,
    task: Dict[str, Any],
    source_label: str,
    on_task_started=None,
):
    """Retry a task with persisted resume state until it succeeds or stops making progress."""
    match_url = task.get("match_url")
    sport = task.get("sport")
    clear_state = task.get("clear_state", False)
    resume_state = task.get("_oddsportal_resume_state")
    partial_match_data = task.get("_oddsportal_partial_match_data")
    task_started = False

    attempts = 0
    last_result = None
    while attempts < 3:
        attempts += 1
        if on_task_started and not task_started:
            try:
                on_task_started(task.get("event_id"), task)
            except Exception:
                logger.exception("on_task_started callback failed for %s", source_label)
            task_started = True
        result = await scraper.scrape_match_attempt(
            match_url,
            sport=sport,
            clear_state=clear_state,
            resume_state=resume_state,
            partial_match_data=partial_match_data,
        )
        last_result = result
        if result.data is not None:
            task["_oddsportal_resume_state"] = None
            task["_oddsportal_partial_match_data"] = None
            return result.data, result

        resume_state = result.resume_state or resume_state
        partial_match_data = result.partial_match_data or partial_match_data
        task["_oddsportal_resume_state"] = resume_state
        task["_oddsportal_partial_match_data"] = partial_match_data

        if attempts >= 3:
            break

        if result.failed_reason == "MATCH_RENDER_TIMEOUT":
            await scraper.stop()
            await scraper.start()

        if not resume_state:
            break

    return None, last_result

async def scrape_match_odds(match_url: str) -> Optional[MatchOddsData]:
    """Helper function to scrape a single match using shared scraper."""
    scraper = await get_scaler()
    return await scraper.scrape_match(match_url)

def scrape_match_sync(match_url: str=None, league_url: str=None, home_team: str=None, away_team: str=None, sport: str=None) -> Optional[MatchOddsData]:
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
            scraper = _get_scraper_cls()()
            await scraper.start()
            try:
                target_url = match_url
                if not target_url and league_url and home_team and away_team:
                    target_url = await scraper.find_match_url(league_url, home_team, away_team)
                data = None
                if target_url:
                    task = {
                        "match_url": target_url,
                        "sport": sport,
                        "clear_state": False,
                        "_oddsportal_resume_state": None,
                        "_oddsportal_partial_match_data": None,
                    }
                    data, _ = await _scrape_task_with_recovery(scraper, task, "scrape_match_sync")
                if data is None:
                    logger.warning(f'🔄 scrape_match_sync: No data (or match discovery failed) — restarting browser with new proxy session and retrying...')
                    await scraper.stop()
                    await scraper.start()
                    retry_url = match_url
                    if not retry_url and league_url and home_team and away_team:
                        retry_url = await scraper.find_match_url(league_url, home_team, away_team)
                    if retry_url:
                        retry_task = {
                            "match_url": retry_url,
                            "sport": sport,
                            "clear_state": False,
                            "_oddsportal_resume_state": None,
                            "_oddsportal_partial_match_data": None,
                        }
                        data, _ = await _scrape_task_with_recovery(scraper, retry_task, "scrape_match_sync_retry")
                        if data:
                            logger.info(f'✅ scrape_match_sync: RETRY SUCCEEDED with new session-{scraper._session_id}')
                        else:
                            logger.warning('⚠️ scrape_match_sync: Retry also returned no data')
                return data
            finally:
                await scraper.stop()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_run())
        finally:
            loop.close()
    except Exception as e:
        import traceback
        logger.error(f'Error in scrape_match_sync: {e}\n{traceback.format_exc()}')
        return None

def scrape_multiple_matches_sync(
    tasks: List[Dict],
    debug_dir: Optional[str]=None,
    on_result=None,
    on_task_started=None,
    current_date=None,
) -> Dict[int, Optional[MatchOddsData]]:
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
    logger.info(f'🔍 OddsPortal batch: scraping {len(tasks)} matches with shared browser')
    try:

        async def _run():
            scraper = _get_scraper_cls()(debug_dir=debug_dir)
            await scraper.start()
            try:
                for i, task in enumerate(tasks):
                    event_id = task['event_id']
                    season_id = task.get('season_id')
                    try:
                        logger.info(f"🔍 OddsPortal [{i + 1}/{len(tasks)}]: {task['home_team']} vs {task['away_team']}")
                        match_url = None
                        if season_id:
                            match_url = scraper.find_match_url_from_cache(season_id, task['home_team'], task['away_team'], current_date=current_date)
                        if not match_url:
                            match_url = await scraper.find_match_url(task['league_url'], task['home_team'], task['away_team'], season_id=season_id, current_date=current_date)
                        task_sport = task.get('sport')
                        if not task_sport and season_id:
                            op_info = SEASON_ODDSPORTAL_MAP.get(season_id)
                            if op_info:
                                task_sport = op_info.get('sport')
                        data = None
                        if match_url:
                            clear_state = task.get('clear_state', False)
                            task_payload = {
                                "event_id": event_id,
                                "match_url": match_url,
                                "sport": task_sport,
                                "clear_state": clear_state,
                                "_oddsportal_resume_state": task.get("_oddsportal_resume_state"),
                                "_oddsportal_partial_match_data": task.get("_oddsportal_partial_match_data"),
                            }
                            data, _ = await _scrape_task_with_recovery(
                                scraper,
                                task_payload,
                                f"batch-{i + 1}",
                                on_task_started=on_task_started,
                            )
                            task["_oddsportal_resume_state"] = task_payload.get("_oddsportal_resume_state")
                            task["_oddsportal_partial_match_data"] = task_payload.get("_oddsportal_partial_match_data")
                        if data is None:
                            logger.warning(f'🔄 OddsPortal [{i + 1}/{len(tasks)}]: No data (or match discovery failed) 🔄 restarting browser with new proxy session and retrying...')
                            await scraper.stop()
                            await scraper.start()
                            retry_url = match_url
                            if not retry_url and season_id:
                                retry_url = scraper.find_match_url_from_cache(season_id, task['home_team'], task['away_team'], current_date=current_date)
                            if not retry_url:
                                retry_url = await scraper.find_match_url(task['league_url'], task['home_team'], task['away_team'], season_id=season_id, current_date=current_date)
                            if retry_url:
                                retry_task = {
                                    "event_id": event_id,
                                    "match_url": retry_url,
                                    "sport": task_sport,
                                    "clear_state": clear_state,
                                    "_oddsportal_resume_state": None,
                                    "_oddsportal_partial_match_data": None,
                                }
                                data, _ = await _scrape_task_with_recovery(
                                    scraper,
                                    retry_task,
                                    f"batch-retry-{i + 1}",
                                    on_task_started=on_task_started,
                                )
                                if data:
                                    logger.info(f'✅ OddsPortal [{i + 1}/{len(tasks)}]: RETRY SUCCEEDED with new session-{scraper._session_id}')
                                else:
                                    logger.warning(f'⚠️ OddsPortal [{i + 1}/{len(tasks)}]: Retry also returned no data')
                        results[event_id] = data
                        if data:
                            logger.info(f'✅ OddsPortal [{i + 1}/{len(tasks)}]: Got {len(data.extractions)} period(s), {len(data.bookie_odds)} bookies')
                        else:
                            logger.warning(f'⚠️ OddsPortal [{i + 1}/{len(tasks)}]: Scrape failed (Match not found or navigation error)')
                        if on_result:
                            try:
                                on_result(event_id, data)
                            except Exception as cb_err:
                                logger.error(f'❌ on_result callback error for event {event_id}: {cb_err}')
                        if i < len(tasks) - 1:
                            await asyncio.sleep(1)
                    except Exception as e:
                        logger.error(f'❌ OddsPortal scrape failed for event {event_id}: {e}')
                        results[event_id] = None
                        if on_result:
                            try:
                                on_result(event_id, None)
                            except Exception as cb_err:
                                logger.error(f'❌ on_result callback error for event {event_id}: {cb_err}')
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
        logger.error(f'❌ Error in scrape_multiple_matches_sync: {e}\n{traceback.format_exc()}')
        return results

def scrape_multiple_matches_parallel_sync(
    tasks: List[Dict],
    num_browsers: int=1,
    debug_dir: Optional[str]=None,
    on_result=None,
    on_task_started=None,
    current_date=None,
) -> Dict[int, Optional[MatchOddsData]]:
    """
    Distribute scrape tasks across multiple concurrent Playwright browsers.
    If num_browsers == 1, delegates directly to scrape_multiple_matches_sync.
    Otherwise, splits tasks and processes them in a ThreadPoolExecutor.
    """
    if not tasks:
        return {}
    if num_browsers <= 1 or len(tasks) == 1:
        return scrape_multiple_matches_sync(
            tasks,
            debug_dir=debug_dir,
            on_result=on_result,
            on_task_started=on_task_started,
            current_date=current_date,
        )
    logger.info(f'🚀 OddsPortal Parallel: Splitting {len(tasks)} tasks across {num_browsers} browsers')
    from collections import defaultdict
    season_groups = defaultdict(list)
    no_season_tasks = []
    for task in tasks:
        sid = task.get('season_id')
        if sid:
            season_groups[sid].append(task)
        else:
            no_season_tasks.append(task)
    seeds = []
    remaining = []
    for sid, group in season_groups.items():
        seeds.append(group[0])
        remaining.extend(group[1:])
    chunks = [[] for _ in range(num_browsers)]
    for task in seeds + no_season_tasks:
        smallest = min(range(len(chunks)), key=lambda i: len(chunks[i]))
        chunks[smallest].append(task)
    for task in remaining:
        smallest = min(range(len(chunks)), key=lambda i: len(chunks[i]))
        chunks[smallest].append(task)
    logger.info(f'📦 Season-aware distribution: {len(seeds)} seed(s) + {len(no_season_tasks)} no-season + {len(remaining)} cache-benefiting across {num_browsers} browsers')
    for idx, chunk in enumerate(chunks):
        season_ids_in_chunk = [t.get('season_id', '?') for t in chunk]
        logger.info(f'  Browser {idx}: {len(chunk)} tasks — seasons: {season_ids_in_chunk}')
    chunks = [c for c in chunks if c]
    all_results = {}
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=len(chunks)) as executor:
        future_to_chunk = {
            executor.submit(
                scrape_multiple_matches_sync,
                chunk,
                debug_dir,
                on_result,
                on_task_started,
                current_date,
            ): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(future_to_chunk):
            chunk_idx = future_to_chunk[future]
            try:
                chunk_result = future.result()
                all_results.update(chunk_result)
                logger.info(f'✅ OddsPortal Parallel: Browser {chunk_idx + 1}/{len(chunks)} completed successfully')
            except Exception as e:
                import traceback
                logger.error(f'❌ OddsPortal Parallel: Browser {chunk_idx + 1} failed: {e}\n{traceback.format_exc()}')
    return all_results
