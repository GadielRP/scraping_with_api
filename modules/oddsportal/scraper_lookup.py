"""OddsPortal lookup helpers."""

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
    INSTITUTIONAL_NOISE, get_oddsportal_current_date,
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


class OddsPortalLookupMixin:
    def _load_cached_candidates(self, season_id: int, league_url: Optional[str]=None, current_date: Optional[date]=None) -> List[Dict[str, str]]:
        """Load structured league candidates from the DB cache."""
        from infrastructure.persistence.repositories import OddsPortalCacheRepository
        reference_date = _coerce_current_date(current_date)
        cached = OddsPortalCacheRepository.get_league_cache(season_id)
        if not cached:
            return []
        total_cached_entries = len([href for href in cached.keys() if href])
        logger.info(f'Cache found for season {season_id}: total_entries={total_cached_entries}, current_date={reference_date.isoformat()}')
        candidates = []
        skipped_stale_candidates = 0
        for href, data in cached.items():
            if not href:
                continue
            if isinstance(data, dict):
                if not _is_cache_date_current_or_future(data.get('date', ''), reference_date):
                    skipped_stale_candidates += 1
                    continue
                candidates.append({'home': data.get('home', ''), 'away': data.get('away', ''), 'href': href, 'raw_text': data.get('raw_text', ''), 'date': data.get('date', '')})
                continue
            skipped_stale_candidates += 1
            row_text = data or ''
            parts = re.split(TEAM_SEPARATOR_PATTERN, row_text)
            if len(parts) >= 2:
                p1, p2 = (parts[0], parts[1])
            else:
                match = re.search(LEGACY_CACHE_MATCH_PATTERN, row_text)
                if match:
                    p1, p2 = (match.group(1), match.group(2))
                else:
                    p1, p2 = (None, None)
            if p1 and p2:
                home = p1.split('\n')[-1].strip()
                away = p2.split('\n')[0].strip()
                home = re.sub(TEAM_PREFIX_CLEAN_PATTERN, '', home)
                away = re.sub('\\s+\\d+\\.\\d+.*', '', away)
                if home and away:
                    continue
        if skipped_stale_candidates:
            logger.info(f'Cache date filter for season {season_id}: valid_entries={len(candidates)}, stale_or_undated={skipped_stale_candidates}, current_date={reference_date.isoformat()}')
        else:
            logger.info(f'Cache date filter for season {season_id}: valid_entries={len(candidates)}, stale_or_undated=0, current_date={reference_date.isoformat()}')
        return candidates

    async def _extract_league_candidates(self, league_url: str, season_id: Optional[int], skip_cache_save: bool=False, current_date: Optional[date]=None) -> List[Dict[str, str]]:
        """
            Navigate to a league page, extract candidates once, and populate cache if available.
            This shared path is reused by both priming and cache-miss discovery.
            """
        reference_date = _coerce_current_date(current_date)
        if not self.browser:
            await self.start()
        _temp_ctx = None
        ctx = self.context
        if not ctx:
            _temp_ctx = await self._create_fresh_context()
            ctx = _temp_ctx
        page = await ctx.new_page()
        navigation_league_url = league_url
        normalized_league_url = _normalize_league_url(league_url) or league_url
        try:
            logger.info(f'🌐 Navigating to league: {navigation_league_url}')
            t0 = time.perf_counter()
            try:
                response = await page.goto(navigation_league_url, wait_until='domcontentloaded', timeout=ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS)
            except Exception as e:
                error_str = str(e).lower()
                if 'timeout' in error_str or 'err_' in error_str or 'net::' in error_str:
                    logger.error(f'🔄 FAST FAIL (League goto): navigation failed quickly after {ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS}ms: {str(e).split(chr(10))[0]}')
                    return []
                raise
            t_goto = time.perf_counter()
            log_timing(f'League page load ({navigation_league_url}) took {t_goto - t0:.2f}s')
            if not response or response.status != 200:
                logger.error(f"❌ Failed to load league page. Status: {(response.status if response else 'N/A')}")
                return []
            try:
                page_title = await page.title()
                if any((blocked in page_title for blocked in ['Access Denied', 'Just a moment...', 'Attention Required!', 'Security check', 'Cloudflare'])):
                    logger.error(f"🔄 FAST FAIL (League title): Proxy IP blocked. Title: '{page_title}'")
                    return []
            except Exception:
                pass
            try:
                league_container_selector = 'div[class*="empty:min-h-[80vh]"]'
                await page.wait_for_selector(f'{league_container_selector} div.eventRow', timeout=ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS)
                t_wait = time.perf_counter()
                log_timing(f'Waiting for scoped event rows took {t_wait - t_goto:.2f}s')
            except Exception:
                logger.error(f'🔄 FAST FAIL (League rows): no scoped event rows loaded within {ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS}ms on {navigation_league_url}')
                return []
            try:
                accept_btn = await page.query_selector("button:has-text('I Accept'), button:has-text('Accept All')")
                if accept_btn:
                    await accept_btn.click()
                    await asyncio.sleep(0.5)
            except Exception:
                pass
            t_js_league = time.perf_counter()
            rows_data = await page.evaluate('() => {\n                const container = document.querySelector(\'div[class*="empty:min-h-[80vh]"]\');\n                if (!container) return [];\n\n                let currentDate = "";\n                const results = [];\n                const rows = Array.from(container.querySelectorAll(\'div.eventRow\'));\n\n                for (const row of rows) {\n                    const rowId = row.getAttribute(\'id\') || \'\';\n                    const rect = row.getBoundingClientRect();\n                    const style = window.getComputedStyle(row);\n                    const isVisible =\n                        rect.width > 0 &&\n                        rect.height > 0 &&\n                        style.display !== \'none\' &&\n                        style.visibility !== \'hidden\';\n\n                    if (!isVisible) continue;\n\n                    const dateHeader = row.querySelector(\'[data-testid="date-header"]\');\n                    if (dateHeader) {\n                        currentDate = dateHeader.innerText.trim();\n                    }\n\n                    const matchAnchor = row.querySelector(\'div.group.flex[data-testid="game-row"] > a[href]\');\n                    if (!matchAnchor) continue;\n\n                    let originalHref = matchAnchor.getAttribute(\'href\') || \'\';\n                    let href = originalHref;\n                    if (href && !href.includes(\'/#\') && rowId) {\n                        href = href.replace(/\\/+$/, \'\') + \'/#\' + rowId;\n                    }\n                    if (href && href.includes(\'/inplay-odds\')) {\n                        href = href.replace(\'/inplay-odds\', \'\')\n                    }\n\n                    const participantAnchors = row.querySelectorAll(\'div[data-testid="event-participants"] a[title]\');\n                    const titles = Array.from(participantAnchors)\n                        .map(a => (a.getAttribute(\'title\') || \'\').trim())\n                        .filter(Boolean);\n\n                    results.push({\n                        original_href: originalHref,\n                        href,\n                        row_id: rowId,\n                        date: currentDate,\n                        home: titles[0] || \'\',\n                        away: titles[1] || \'\',\n                        game_text: row.innerText.trim(),\n                    });\n                }\n                return results;\n            }')
            log_timing(f'Extracting league rows via JS evaluating took {time.perf_counter() - t_js_league:.2f}s')
            if not rows_data:
                logger.warning(f'⚠️ No event rows found on {navigation_league_url}')
                return []
            candidates = []
            total_rows = len(rows_data)
            already_had_fragment = 0
            repaired_with_row_id = 0
            missing_row_id_for_repair = 0
            accepted_candidates = 0
            skipped_empty_href = 0
            skipped_wrong_sport = 0
            skipped_wrong_structure = 0
            skipped_stale = 0
            skipped_league_self = 0
            skipped_missing_teams = 0
            league_relative_path = navigation_league_url.replace(f'https://www.{Config.ODDSPORTAL_DOMAIN}', '').rstrip('/')
            path_parts = [p for p in league_relative_path.split('/') if p]
            sport_slug = path_parts[0] if len(path_parts) >= 1 else None
            country_slug = path_parts[1] if len(path_parts) >= 2 else None
            sport_prefix = f'/{sport_slug}/' if sport_slug else None
            legacy_prefix = f'/{sport_slug}/{country_slug}/' if sport_slug and country_slug else None
            modern_prefix = f'/{sport_slug}/h2h/' if sport_slug else None
            for item in rows_data:
                original_href = item.get('original_href', '')
                href = item.get('href', '')
                row_id = item.get('row_id', '')
                game_text = item.get('game_text', '')
                date_val = item.get('date', '')
                if not href:
                    skipped_empty_href += 1
                    continue
                if '/#' not in original_href and (not row_id):
                    missing_row_id_for_repair += 1
                    logger.warning(f'Missing row_id for repair, original_href={original_href}')
                elif '/#' in original_href:
                    already_had_fragment += 1
                elif original_href != href:
                    repaired_with_row_id += 1
                    logger.debug(f'Repaired fragment: orig={original_href} row_id={row_id} repaired={href}')
                href_no_fragment = href.split('#', 1)[0].rstrip('/')
                if not href_no_fragment:
                    skipped_empty_href += 1
                    continue
                if sport_prefix and (not href_no_fragment.startswith(sport_prefix)):
                    skipped_wrong_sport += 1
                    continue
                is_modern = bool(modern_prefix and href_no_fragment.startswith(modern_prefix))
                is_legacy = bool(legacy_prefix and href_no_fragment.startswith(legacy_prefix))
                if not (is_modern or is_legacy):
                    skipped_wrong_structure += 1
                    continue
                if not _is_cache_date_current_or_future(date_val, reference_date):
                    skipped_stale += 1
                    continue
                if href_no_fragment == league_relative_path:
                    skipped_league_self += 1
                    continue
                home = (item.get('home') or '').strip()
                away = (item.get('away') or '').strip()
                if not home or not away:
                    parts = re.split(TEAM_SEPARATOR_PATTERN, game_text)
                    if len(parts) >= 2:
                        home = parts[0].strip()
                        away = parts[1].strip()
                home = re.sub('^\\d{2}:\\d{2}\\s+', '', home)
                away = re.sub('\\s+\\d+\\.\\d+.*', '', away)
                if home and away:
                    accepted_candidates += 1
                    candidates.append({'home': home, 'away': away, 'href': href, 'raw_text': game_text, 'date': date_val})
                else:
                    skipped_missing_teams += 1
            logger.info(f'League extraction summary for {navigation_league_url}: total_rows={total_rows} accepted={accepted_candidates} (skipped: stale={skipped_stale}, sport={skipped_wrong_sport}, struct={skipped_wrong_structure}, teams={skipped_missing_teams}, self={skipped_league_self}, empty={skipped_empty_href})')
            if season_id and candidates and (not skip_cache_save):
                try:
                    from infrastructure.persistence.repositories import OddsPortalCacheRepository
                    cache_dict = _build_structured_league_cache(candidates, current_date=reference_date)
                    if cache_dict:
                        new_quality = _evaluate_cache_quality(cache_dict, reference_date)
                        new_count = new_quality.total_count
                        new_homog = new_quality.homogeneity
                        new_score = new_quality.score
                        old_cache = OddsPortalCacheRepository.get_league_cache(season_id)
                        save_cache = True
                        if old_cache:
                            logger.info(f'Starting cache evaluation for season {season_id}: existing_cache_entries={len(old_cache)}, new_cache_entries={len(cache_dict)}, current_date={reference_date.isoformat()}')
                            old_quality = _evaluate_cache_quality(old_cache, reference_date)
                            old_count = old_quality.total_count
                            old_homog = old_quality.homogeneity
                            old_score = old_quality.score
                            logger.info(f'Cache evaluation details for season {season_id}: new(total={new_quality.total_count}, fresh={new_quality.fresh_count}, stale={new_quality.stale_count}, ratio={new_quality.freshness_ratio:.2f}, homog={new_homog:.2f}, score={new_score:.1f}) vs old(total={old_quality.total_count}, fresh={old_quality.fresh_count}, stale={old_quality.stale_count}, ratio={old_quality.freshness_ratio:.2f}, homog={old_homog:.2f}, score={old_score:.1f})')
                            logger.info(f'📊 New cache score {new_score:.1f} (count={new_count}, homog={new_homog:.2f}) vs Old cache score {old_score:.1f} (count={old_count}, homog={old_homog:.2f})')
                            if new_quality.comparison_key < old_quality.comparison_key:
                                save_cache = False
                                logger.warning(f'Cache evaluation result for season {season_id}: new_key={new_quality.comparison_key} < old_key={old_quality.comparison_key}')
                                logger.warning(f'⚠️ Rejecting new cache. New score {new_score:.1f} is lower than Old score {old_score:.1f}')
                            else:
                                logger.info(f'Cache evaluation result for season {season_id}: new_key={new_quality.comparison_key} >= old_key={old_quality.comparison_key}')
                                logger.info(f'✅ Accepting new cache. Score is better or equal to the Old cache.')
                        if save_cache:
                            if OddsPortalCacheRepository.save_league_cache(season_id, cache_dict):
                                logger.info(f'⚡¦ Cached {len(cache_dict)} match URLs for season {season_id}')
                            else:
                                logger.warning(f'⚠️ Cache save returned False for season {season_id}')
                except Exception as cache_err:
                    logger.warning(f'⚠️ Cache save failed: {cache_err}')
            return candidates
        finally:
            await page.close()
            if _temp_ctx:
                try:
                    await _temp_ctx.close()
                except Exception:
                    pass

    async def find_match_url(self, league_url: str, home_team: str, away_team: str, season_id: int=None, force_live: bool=False, skip_cache_save: bool=False, current_date: Optional[date]=None) -> Optional[str]:
        """
            Resolve a match URL by team names.
            Cache hits return immediately; cache misses reuse the shared league extraction path.
            """
        if season_id and (not force_live):
            cached_url = self.find_match_url_from_cache(season_id, home_team, away_team, league_url=league_url, current_date=current_date)
            if cached_url:
                logger.info(f'Cache hit (internal): {cached_url}')
                return cached_url
            logger.info(f'Cache lookup did not resolve an upcoming match for season {season_id}; falling back to league navigation for {home_team} vs {away_team}')
        try:
            candidates = await self._extract_league_candidates(league_url, season_id, skip_cache_save=skip_cache_save, current_date=current_date)
            if not candidates:
                logger.warning(f'OddsPortal discovery returned no candidates for {league_url}')
                return None
            logger.info(f'Scanning {len(candidates)} candidates for {home_team} vs {away_team}...')
            best_match = self.team_matcher.find_best_match(home_team, away_team, candidates)
            if best_match:
                logger.info(f"Match found: {best_match['home']} vs {best_match['away']} (Score: {best_match['max_score']:.1f}, Reversed: {best_match['is_reversed']})")
                return f"https://www.{Config.ODDSPORTAL_DOMAIN}{best_match['href']}"
            logger.warning(f'Match not found: {home_team} vs {away_team}')
            return None
        except Exception as e:
            logger.error(f'Error finding match on {league_url}: {e}')
            return None

    def find_match_url_from_cache(self, season_id: int, home_team: str, away_team: str, league_url: Optional[str]=None, current_date: Optional[date]=None) -> Optional[str]:
        """Try to find a match URL from the DB-backed league cache."""
        try:
            candidates = self._load_cached_candidates(season_id, league_url=league_url, current_date=current_date)
            if not candidates:
                logger.debug(f'No valid candidates parsed from cache for season {season_id}')
                return None
            logger.debug(f'Scanning {len(candidates)} cached candidates for {home_team} vs {away_team}...')
            best_match = self.team_matcher.find_best_match(home_team, away_team, candidates)
            if best_match and best_match['max_score'] >= 80:
                logger.info(f"Cache hit: {best_match['home']} vs {best_match['away']} (Score: {best_match['max_score']:.1f})")
                return f"https://www.{Config.ODDSPORTAL_DOMAIN}{best_match['href']}"
            logger.info(f'Cache found for season {season_id}, but no stored match matched {home_team} vs {away_team} with threshold >= 80 (candidate_count={len(candidates)})')
            return None
        except Exception as e:
            logger.debug(f'Cache lookup failed: {e}')
            return None
