"""OddsPortal attempt helpers."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from threading import Condition, local
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

try:
    from playwright.async_api import async_playwright, Page, Browser, BrowserContext
except ImportError:
    async_playwright = None
    Page = Browser = BrowserContext = Any
try:
    from infrastructure.network import ProxyIdentityManager
except ImportError:
    class ProxyIdentityManager:
        pass

try:
    from infrastructure.settings import Config
except ImportError:
    class MockConfig:
        PROXY_ENABLED = False
        PROXY_ENDPOINT = None
        PROXY_USERNAME = None
        PROXY_PASSWORD = None
        PROXY_PROVIDER = "legacy"
        PROXY_PROTOCOL = "http"
        PROXY_USERNAME_BASE = None
        PROXY_COUNTRY = "mx"
        PROXY_CITY = ""
        PROXY_SESSION_DURATION_MINUTES = 10
        PROXY_MODE_ODDSPORTAL = "sticky"
        PROXY_MODE_SOFASCORE = "rotating"
        PROXY_ROTATE_ON_ODDSPORTAL_BROWSER_RESTART = True
        PROXY_ROTATE_ON_SOFASCORE_PROXY_ERROR = True
        PROXY_LOG_SAFE = True
        ODDSPORTAL_MATCH_GOTO_TIMEOUT_MS = 30000
        ODDSPORTAL_FAST_FAIL_EMPTY_TIMEOUT_MS = 15000
        ODDSPORTAL_MARKET_RENDER_TIMEOUT_MS = 60000
        ODDSPORTAL_SHELL_GRACE_TIMEOUT_MS = 8000
        ODDSPORTAL_TAB_WAIT_TIMEOUT = 20
        ODDSPORTAL_SAVE_DEBUG_ON_GOTO_TIMEOUT = True
        ODDSPORTAL_ENABLE_SHELL_GRACE = True
        ODDSPORTAL_BLOCK_SERVICE_WORKERS = True
        ODDSPORTAL_PRE_NAVIGATION_CLEAR_STATE = True
        POLL_INTERVAL_MINUTES = 5

        @staticmethod
        def validate_oddsportal_proxy_alignment(logger):
            return None

    Config = MockConfig()

from .oddsportal_config import (
    SEASON_ODDSPORTAL_MAP, BOOKIE_ALIASES, TEAM_ALIASES, PRIORITY_BOOKIES,
    OP_GROUPS, OP_GROUPS_DISPLAY, OP_PERIODS, SPORT_SCRAPING_ROUTES,
    build_op_fragment, build_match_url_with_fragment, flatten_sport_scraping_route,
    INSTITUTIONAL_NOISE, get_current_date,
)
from .team_matcher import TeamMatcher
from .dataclasses import (
    CacheQualityMetrics, BookieOdds, BetfairExchangeOdds, MarketExtraction,
    MatchOddsData, ScrapeAttemptResult, GroupSeedResult,
)
from .cache_utils import (
    DEBUG_TIMING, ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS, ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS,
    ODDSPORTAL_SESSION_RESTART_ATTEMPTS, EN_DASH, TEAM_SEPARATOR_PATTERN,
    LEGACY_CACHE_MATCH_PATTERN, TEAM_PREFIX_CLEAN_PATTERN, ODDSPORTAL_CACHE_DATE_FORMATS,
    ODDSPORTAL_RELATIVE_DATE_OFFSETS, log_timing, _normalize_league_url,
    _build_league_group_key, _normalize_cache_date, _build_structured_league_cache,
    _coerce_current_date, _parse_oddsportal_cache_date, _is_cache_date_current_or_future,
    _calculate_cache_homogeneity, _evaluate_cache_quality, _format_group_key,
)
from .logging_context import _LOG_CONTEXT, _OddsPortalLogPrefixFilter, _log_prefix

logger = logging.getLogger(__name__)


class OddsPortalAttemptMixin:
    async def scrape_match(self, match_url: str, sport: str=None, clear_state: bool=False) -> Optional[MatchOddsData]:
        """Compatibility wrapper for callers expecting only MatchOddsData."""
        attempt = await self.scrape_match_attempt(match_url, sport=sport, clear_state=clear_state)
        return attempt.data

    async def scrape_match_attempt(self, match_url: str, sport: str=None, clear_state: bool=False, resume_state: Optional[Dict[str, Any]]=None, partial_match_data: Optional[MatchOddsData]=None) -> ScrapeAttemptResult:
        """
            Scrape a match while preserving route-step progress so retries can resume
            from the failed `group_key + period_key` fragment.
            """
        route_steps = flatten_sport_scraping_route(sport)
        route_step_count = len(route_steps)
        state_is_consistent = isinstance(resume_state, dict) and resume_state.get('sport') == sport and (resume_state.get('route_step_count') == route_step_count)
        if not state_is_consistent:
            partial_match_data = None
        normalized_resume_state = self._normalize_resume_state(resume_state, sport, route_steps)
        match_data = self._restore_partial_match_data(partial_match_data, match_url, sport)
        normalized_resume_state['partial_extraction_count'] = len(match_data.extractions)
        if route_step_count == 0:
            self._ensure_legacy_match_level_fields(match_data)
            self._log_structured_recap(match_data)
            return ScrapeAttemptResult(data=match_data, resume_state=normalized_resume_state, partial_match_data=match_data)
        start_step_idx = normalized_resume_state.get('next_step_idx', 0)
        if not isinstance(start_step_idx, int):
            start_step_idx = 0
        start_step_idx = min(max(start_step_idx, 0), route_step_count)
        normalized_resume_state['next_step_idx'] = start_step_idx
        if start_step_idx >= route_step_count:
            normalized_resume_state['resume_fragment'] = None
            self._ensure_legacy_match_level_fields(match_data)
            self._log_structured_recap(match_data)
            return ScrapeAttemptResult(data=match_data, resume_state=normalized_resume_state, partial_match_data=match_data)
        start_step = route_steps[start_step_idx]
        normalized_resume_state['resume_fragment'] = start_step.get('fragment')
        initial_url = build_match_url_with_fragment(match_url, start_step.get('group_key'), start_step.get('period_key'))
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
                    logger.error(f'❌ Failed to create fresh context for {match_url}: {ctx_err}')
                    return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason='CONTEXT_CREATE_FAILED', failed_step_idx=start_step_idx)
                self.context = fresh_context
            elif not self.context:
                self.context = await self._create_fresh_context()
            should_clear_state = bool(self.context and (clear_state or getattr(Config, 'ODDSPORTAL_PRE_NAVIGATION_CLEAR_STATE', True)))
            if should_clear_state:
                if clear_state:
                    logger.info('🧹 Running pre-navigation browser-state cleanup before OddsPortal navigation (clear_state=True)')
                else:
                    logger.info('🧹 Running pre-navigation browser-state cleanup before first OddsPortal navigation')
                await self._clear_browser_state()
            page = await self.context.new_page()
        except Exception as setup_err:
            logger.error(f'❌ Failed to set up page for {match_url}: {setup_err}')
            if fresh_context:
                try:
                    await fresh_context.close()
                except Exception:
                    pass
                self.context = previous_context
            return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason='PAGE_SETUP_FAILED', failed_step_idx=start_step_idx)
        try:
            t0 = time.perf_counter()
            logger.info(f"🗺️ Scraping route for '{sport}': {route_step_count} steps (resume step={start_step_idx}, fragment={normalized_resume_state.get('resume_fragment')})")
            logger.info(f'🌐 Navigating to match: {initial_url}')
            self._original_debug_dir = self.debug_dir
            self._event_debug_dir_created = False
            if self.debug_dir and match_url:
                match_slug = self._normalize_base_match_url(match_url).split('/')[-1]
                if match_slug:
                    self.debug_dir = os.path.join(self.debug_dir, f'debug_{match_slug}')
                    self._event_debug_dir_created = True
            response = None
            e_goto = None
            goto_error_code = None
            goto_error_summary = None
            goto_timeout_ms = Config.ODDSPORTAL_MATCH_GOTO_TIMEOUT_MS
            try:
                response = await asyncio.wait_for(self._goto_fresh(page, initial_url, wait_until='domcontentloaded', timeout=goto_timeout_ms), timeout=goto_timeout_ms / 1000.0 + 5.0)
            except Exception as e:
                e_goto = e
                goto_error_code, goto_error_summary = self._classify_goto_exception(e)
                logger.warning(f'Navigation exception before page load: {goto_error_summary} ({goto_error_code}).')
                logger.warning(f'Inspecting page state after navigation exception: {e}')
            t_goto = time.perf_counter()
            log_timing(f'Match page load ({initial_url}) took {t_goto - t0:.2f}s')
            if e_goto is not None:
                state = await self._collect_match_page_state(page)
                classification = self._classify_match_page_state(state)
                state_summary = self._format_page_state_summary(state, classification)
                logger.info(f'Page state after navigation exception: {state_summary}')
                if classification in ('NO_SHELL', 'HTTP_ERROR_404'):
                    reason_prefix = goto_error_code or 'GOTO_FAILED'
                    reason = f'{reason_prefix}_{classification}'
                    logger.error(f'FAST FAIL: {reason}. {state_summary}')
                    if getattr(Config, 'ODDSPORTAL_SAVE_DEBUG_ON_GOTO_TIMEOUT', True):
                        await self._save_debug_artifacts(page, reason, {'error': str(e_goto), **self._resume_state_for_debug(normalized_resume_state)})
                    return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason, failed_step_idx=start_step_idx)
                elif classification == 'DATA_RENDERED':
                    logger.info('Navigation threw, but data is already rendered. Continuing.')
                else:
                    logger.info(f'Navigation exception left page in {classification}. Falling through to smart-wait/render checks.')
            try:
                page_title = await page.title()
                if any((blocked in page_title for blocked in ['Access Denied', 'Just a moment...', 'Attention Required!', 'Security check', 'Cloudflare'])):
                    reason = 'CLOUDFLARE_BLOCK'
                    logger.error(f'FAST FAIL: {reason}')
                    await self._save_debug_artifacts(page, reason, self._resume_state_for_debug(normalized_resume_state))
                    return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason, failed_step_idx=start_step_idx)
            except Exception:
                pass
            if response is not None and response.status >= 400:
                reason = f'HTTP_{response.status}'
                logger.error(f'FAST FAIL: {reason}. Waiting 2.5s for SPA error page to render before taking snapshot.')
                try:
                    await page.wait_for_timeout(2500)
                except Exception:
                    pass
                await self._save_debug_artifacts(page, reason, self._resume_state_for_debug(normalized_resume_state))
                return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason, failed_step_idx=start_step_idx)
            first_extract_fn = start_step.get('extract_fn', 'standard')
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
            render_timeout_ms = getattr(Config, 'ODDSPORTAL_MARKET_RENDER_TIMEOUT_MS', 60000)
            render_task = asyncio.create_task(self._wait_for_market_render(page, first_extract_fn, timeout_ms=render_timeout_ms))
            race_timeout_s = max(js_timeout, render_timeout_ms) / 1000.0 + 5.0
            try:
                done, pending = await asyncio.wait_for(asyncio.wait([fast_fail_task, render_task], return_when=asyncio.FIRST_COMPLETED), timeout=race_timeout_s)
            except asyncio.TimeoutError:
                fast_fail_task.cancel()
                render_task.cancel()
                reason = 'RENDER_RACE_TIMEOUT'
                logger.error(f'FAST FAIL: {reason} after {race_timeout_s:.1f}s (js_timeout_ms={js_timeout}, render_timeout_ms={render_timeout_ms})')
                await self._save_debug_artifacts(page, reason, self._resume_state_for_debug(normalized_resume_state))
                return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason, failed_step_idx=start_step_idx)
            for pending_task in pending:
                pending_task.cancel()
            if fast_fail_task in done:
                ff_reason = fast_fail_task.result()
                if ff_reason is not None:
                    if ff_reason in ['Shell loaded, skeleton persisted, no data rows', 'Shell loaded, no data rows']:
                        logger.info(f"⏳ JS Observer detected '{ff_reason}'. Routing to shell-grace logic.")
                        if getattr(Config, 'ODDSPORTAL_ENABLE_SHELL_GRACE', True):
                            rendered = await self._wait_for_market_render(page, first_extract_fn, timeout_ms=getattr(Config, 'ODDSPORTAL_SHELL_GRACE_TIMEOUT_MS', 8000))
                            if not rendered:
                                reason_code = 'SHELL_WITH_SKELETON_NO_DATA' if 'skeleton persisted' in ff_reason else 'SHELL_WITH_NAV_NO_DATA'
                                logger.error(f'FAST FAIL: {reason_code} (after shell grace).')
                                await self._save_debug_artifacts(page, reason_code, self._resume_state_for_debug(normalized_resume_state))
                                return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason_code, failed_step_idx=start_step_idx)
                            logger.info('✅ Shell-grace successful.')
                        else:
                            reason_code = 'SHELL_WITH_SKELETON_NO_DATA' if 'skeleton persisted' in ff_reason else 'SHELL_WITH_NAV_NO_DATA'
                            logger.error(f'FAST FAIL: {reason_code} (shell-grace disabled).')
                            await self._save_debug_artifacts(page, reason_code, self._resume_state_for_debug(normalized_resume_state))
                            return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason_code, failed_step_idx=start_step_idx)
                    else:
                        reason_code = 'FAST_FAIL_' + ff_reason.replace(' ', '_').upper()
                        state = await self._collect_match_page_state(page)
                        summary = self._format_page_state_summary(state, self._classify_match_page_state(state))
                        logger.error(f'FAST FAIL: {reason_code}. {summary}')
                        await self._save_debug_artifacts(page, reason_code, self._resume_state_for_debug(normalized_resume_state))
                        return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason_code, failed_step_idx=start_step_idx)
            if render_task in done:
                rendered = render_task.result()
                if not rendered:
                    state = await self._collect_match_page_state(page)
                    classification = self._classify_match_page_state(state)
                    state_summary = self._format_page_state_summary(state, classification)
                    reason = f'MATCH_RENDER_TIMEOUT_{classification}'
                    logger.error(f'❌ Match page failure: {reason}. {state_summary}')
                    await self._save_debug_artifacts(page, reason, self._resume_state_for_debug(normalized_resume_state))
                    return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason, failed_step_idx=start_step_idx)
            log_timing(f'Primary rendering + wait race took {time.perf_counter() - t_goto:.2f}s')
            t_cookie = time.perf_counter()
            for btn_sel in ['#onetrust-accept-btn-handler', 'button.onetrust-close-btn-handler', "button:has-text('I Accept')", "button:has-text('Accept All')", "button:has-text('Accept')"]:
                try:
                    btn = await page.query_selector(btn_sel)
                    if btn:
                        await btn.click()
                        logger.debug(f'🍪 Dismissed consent via: {btn_sel}')
                        await asyncio.sleep(0.5)
                        break
                except Exception:
                    continue
            log_timing(f'Dismissing cookie banners took {time.perf_counter() - t_cookie:.2f}s')
            await page.evaluate('window.scrollTo(0, 500)')
            await asyncio.sleep(1.0)
            completed_step_keys = set(normalized_resume_state.get('completed_step_keys', []))
            for step in route_steps[start_step_idx:]:
                step_idx = step.get('step_idx', 0)
                step_key = step.get('step_key')
                group_key = step.get('group_key')
                period_key = step.get('period_key')
                db_market_group = step.get('db_market_group', '1X2')
                db_market_period = step.get('db_market_period', 'Full Time')
                db_market_name = step.get('db_market_name', db_market_period)
                if step_key in completed_step_keys:
                    normalized_resume_state['next_step_idx'] = max(normalized_resume_state.get('next_step_idx', 0), step_idx + 1)
                    continue
                t_period = time.perf_counter()
                logger.info(f'📊 [step {step_idx + 1}/{route_step_count}] Extracting: {db_market_group} / {db_market_period}')
                is_first_step_in_attempt = step_idx == start_step_idx
                prev_step = route_steps[step_idx - 1] if step_idx > 0 else None
                if not is_first_step_in_attempt:
                    if group_key and (not prev_step or prev_step.get('group_key') != group_key):
                        tab_label = step.get('group_display') or OP_GROUPS_DISPLAY.get(group_key, group_key)
                        switched_group = await self._click_market_group_tab(
                            page,
                            tab_label,
                            group_key=group_key,
                        )
                        if not switched_group:
                            reason_code = f'GROUP_SWITCH_FAILED_{group_key}'
                            self._mark_step_failed(normalized_resume_state, step, reason_code)
                            debug_extra = {'step': step, **self._resume_state_for_debug(normalized_resume_state)}
                            await self._save_debug_artifacts(page, reason_code, debug_extra)
                            return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason_code, failed_step_idx=step_idx)
                    should_click_period = False

                    if group_key:
                        if prev_step and prev_step.get('group_key') == group_key:
                            should_click_period = step.get('period_key') != prev_step.get('period_key')
                        elif step.get('period_idx', 0) > 0:
                            should_click_period = True

                    if should_click_period:
                        logger.info(f'🔀 Switching to period: {db_market_period} (tab click)')
                        t_frag = time.perf_counter()
                        tab_clicked = await self._click_period_tab(
                            page,
                            step.get('period_display', db_market_period),
                            period_key=period_key,
                        )
                        if not tab_clicked:
                            reason_code = f'PERIOD_SWITCH_FAILED_{period_key}'
                            self._mark_step_failed(normalized_resume_state, step, reason_code)
                            debug_extra = {'step': step, **self._resume_state_for_debug(normalized_resume_state)}
                            await self._save_debug_artifacts(page, reason_code, debug_extra)
                            return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason_code, failed_step_idx=step_idx)
                        log_timing(f'Period tab-click navigation to {period_key} took {time.perf_counter() - t_frag:.2f}s')
                        
                extract_fn = step.get('extract_fn', 'standard')
                t_extract = time.perf_counter()
                if extract_fn == 'over_under':
                    period_data = await self._extract_data_over_under(page, match_url)
                elif extract_fn == 'asian_handicap':
                    period_data = await self._extract_data_asian_handicap(page, match_url)
                else:
                    period_data = await self._extract_data(page, match_url)
                log_timing(f'JS extraction for {db_market_period} took {time.perf_counter() - t_extract:.2f}s')
                if not period_data:
                    reason_code = f'PERIOD_DATA_EMPTY_{period_key}'
                    self._mark_step_failed(normalized_resume_state, step, reason_code)
                    debug_extra = {'step': step, **self._resume_state_for_debug(normalized_resume_state)}
                    await self._save_debug_artifacts(page, reason_code, debug_extra)
                    return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=reason_code, failed_step_idx=step_idx)
                if match_data.home_team in (None, '', 'Unknown'):
                    match_data.home_team = period_data.home_team
                if match_data.away_team in (None, '', 'Unknown'):
                    match_data.away_team = period_data.away_team
                if self.debug_dir and (not self._event_debug_dir_created) and match_data.home_team and match_data.away_team:
                    slug = f'{match_data.home_team}-vs-{match_data.away_team}'.lower().replace(' ', '-').replace('/', '-')
                    event_debug_dir = os.path.join(self.debug_dir, f'debug_{slug}')
                    try:
                        os.makedirs(event_debug_dir, exist_ok=True)
                        self.debug_dir = event_debug_dir
                        self._event_debug_dir_created = True
                        logger.info(f'📂 Event debug directory: {self.debug_dir}')
                    except Exception as e:
                        logger.warning(f'⚠️ Failed to create event debug directory: {e}')
                logger.info(f'✅ Extracted {len(period_data.bookie_odds)} bookies for {db_market_period}')
                target_bookie_obj = None
                for priority_name in PRIORITY_BOOKIES:
                    for b in period_data.bookie_odds:
                        if priority_name.lower() in b.name.lower() or b.name.lower() in priority_name.lower():
                            target_bookie_obj = b
                            break
                    if target_bookie_obj:
                        break
                if target_bookie_obj:
                    logger.info(f'🎯 Extracting opening odds via hover for: {target_bookie_obj.name} ({db_market_period})')
                    t_hover = time.perf_counter()
                    opening = await self._extract_opening_odds_for_bookie(page, target_bookie_obj.name)
                    log_timing(f'Hover extraction for {target_bookie_obj.name} ({db_market_period}) took {time.perf_counter() - t_hover:.2f}s')
                    is_ou = db_market_group == 'Over/Under'
                    lbl_1 = 'Over' if is_ou else '1'
                    lbl_2 = 'Under' if is_ou else '2'
                    _three_way_market = not is_ou and getattr(target_bookie_obj, 'odds_x', None) not in (None, '', '-')
                    if is_ou or not _three_way_market:
                        expected_keys = ['1', '2']
                    else:
                        expected_keys = ['1', 'X', '2']
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
                        extracted_keys = [k for k in expected_keys if opening.get(k)]
                        missing_keys = [k for k in expected_keys if not opening.get(k)]
                        if not missing_keys:
                            lbl_x_part = f' X={target_bookie_obj.initial_odds_x}' if 'X' in expected_keys else ''
                            logger.info(f'✅ FULL_SUCCESS Opening odds ({db_market_period}): {lbl_1}={target_bookie_obj.initial_odds_1}{lbl_x_part} {lbl_2}={target_bookie_obj.initial_odds_2} (Time: {target_bookie_obj.movement_odds_time})')
                        else:
                            lbl_x_part = f' X={target_bookie_obj.initial_odds_x}' if 'X' in expected_keys else ''
                            logger.warning(f'⚠️ PARTIAL_SUCCESS Opening odds ({db_market_period}): {lbl_1}={target_bookie_obj.initial_odds_1}{lbl_x_part} {lbl_2}={target_bookie_obj.initial_odds_2} (missing: {missing_keys})')
                    else:
                        logger.warning(f'⚠️ TOTAL_FAIL Opening odds ({db_market_period}): could not extract any opening odds for {target_bookie_obj.name}')
                else:
                    logger.info(f'ℹ️ No priority bookie found for {db_market_period}, skipping opening odds hover')
                extraction_betfair = None
                if step.get('betfair_enabled') and period_data.betfair:
                    logger.info(f'🎯 Extracting Betfair Exchange opening odds via hover ({db_market_period})')
                    t_bf = time.perf_counter()
                    bf_opening = await self._extract_opening_odds_betfair(page)
                    log_timing(f'Betfair hover extraction ({db_market_period}) took {time.perf_counter() - t_bf:.2f}s')
                    if bf_opening:
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
                        _bf_three_way = any((k in bf_opening for k in ('back_x', 'lay_x'))) or period_data.betfair.back_x not in (None, '-', '')
                        if _bf_three_way:
                            bf_expected_back = ['back_1', 'back_x', 'back_2']
                            bf_expected_lay = ['lay_1', 'lay_x', 'lay_2']
                        else:
                            bf_expected_back = ['back_1', 'back_2']
                            bf_expected_lay = ['lay_1', 'lay_2']
                        bf_expected = bf_expected_back + bf_expected_lay
                        bf_extracted = [k for k in bf_expected if bf_opening.get(k)]
                        bf_missing = [k for k in bf_expected if not bf_opening.get(k)]
                        if not bf_missing:
                            logger.info(f'✅ FULL_SUCCESS Betfair opening odds ({db_market_period}): Back 1={period_data.betfair.initial_back_1} X={period_data.betfair.initial_back_x} 2={period_data.betfair.initial_back_2} | Lay 1={period_data.betfair.initial_lay_1} X={period_data.betfair.initial_lay_x} 2={period_data.betfair.initial_lay_2}')
                        else:
                            logger.warning(f'⚠️ PARTIAL_SUCCESS Betfair opening odds ({db_market_period}): Back 1={period_data.betfair.initial_back_1} X={period_data.betfair.initial_back_x} 2={period_data.betfair.initial_back_2} | Lay 1={period_data.betfair.initial_lay_1} X={period_data.betfair.initial_lay_x} 2={period_data.betfair.initial_lay_2} (missing: {bf_missing})')
                    else:
                        logger.warning(f'⚠️ TOTAL_FAIL Betfair opening odds ({db_market_period}): could not extract any opening odds from Betfair Exchange')
                    extraction_betfair = period_data.betfair
                extraction = MarketExtraction(market_group=db_market_group, market_period=db_market_period, market_name=db_market_name, bookie_odds=period_data.bookie_odds, betfair=extraction_betfair)
                match_data.extractions.append(extraction)
                self._mark_step_completed(normalized_resume_state, step, match_data)
                completed_step_keys.add(step_key)
                next_idx = normalized_resume_state.get('next_step_idx', 0)
                normalized_resume_state['resume_fragment'] = route_steps[next_idx].get('fragment') if next_idx < route_step_count else None
                normalized_resume_state['partial_extraction_count'] = len(match_data.extractions)
                log_timing(f'Total period extraction for {db_market_period} took {time.perf_counter() - t_period:.2f}s')
            self._ensure_legacy_match_level_fields(match_data)
            total_duration = time.perf_counter() - t0
            match_data.extraction_time_ms = total_duration * 1000
            log_timing(f'Total match scraping process (scrape_match_attempt) took {total_duration:.2f}s')
            total_bookies = sum((len(e.bookie_odds) for e in match_data.extractions))
            logger.info(f'✅ Completed scraping {match_data.home_team} vs {match_data.away_team}: {len(match_data.extractions)} periods, {total_bookies} bookie entries total')
            self._log_structured_recap(match_data)
            return ScrapeAttemptResult(data=match_data, resume_state=normalized_resume_state, partial_match_data=match_data)
        except Exception as e:
            logger.error(f'❌ Error scraping match {match_url}: {e}')
            return ScrapeAttemptResult(data=None, resume_state=normalized_resume_state, partial_match_data=match_data, failed_reason=f'SCRAPE_EXCEPTION_{type(e).__name__}', failed_step_idx=normalized_resume_state.get('next_step_idx'))
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
