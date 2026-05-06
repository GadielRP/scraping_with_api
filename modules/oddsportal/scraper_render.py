"""OddsPortal render helpers."""

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
from .oddsportal_tab_normalizer import (
    get_group_tab_candidates,
    get_period_tab_candidates,
    tab_label_matches,
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


class OddsPortalRenderMixin:
    GET_REFERENCE_VALUE_JS = """
                () => {
                    const getFirstVisibleText = (selector) => {
                        const el = Array.from(document.querySelectorAll(selector)).find(
                            e => e.getBoundingClientRect().width > 0 &&
                                e.getBoundingClientRect().height > 0
                        );

                        return el ? el.innerText.trim() : null;
                    };

                    const oddsVal = getFirstVisibleText('div.odds-cell');
                    if (oddsVal) return oddsVal;

                    const ouVal = getFirstVisibleText(
                        'div[data-testid="over-under-collapsed-row"] p'
                    );
                    if (ouVal) return ouVal;

                    const ahVal = getFirstVisibleText(
                        'div[data-testid="asian-handicap-collapsed-row"] p'
                    );
                    if (ahVal) return ahVal;

                    return null;
                }
            """
    async def _wait_for_market_render(self, page: Page, extract_fn: str, timeout_ms: int=15000) -> bool:
        """
            Wait for selectors appropriate for the extraction mode.
            standard: bookmaker rows
            over_under/asian_handicap: collapsed/expanded accordion rows (or standard fallback)
            """
        try:
            if extract_fn == 'standard':
                await page.wait_for_selector('div.border-black-borders.flex.h-9', state='visible', timeout=timeout_ms)
            else:
                await page.wait_for_selector("div[data-testid='over-under-collapsed-row'], div[data-testid='asian-handicap-collapsed-row'], div[data-testid='over-under-expanded-row'], div.border-black-borders.flex.h-9", state='visible', timeout=timeout_ms)
            return True
        except Exception as e:
            try:
                state = await self._collect_match_page_state(page)
                classification = self._classify_match_page_state(state)
                summary = self._format_page_state_summary(state, classification)
                logger.warning(f'⚠️ Wait for market render ({extract_fn}) failed. Final state: {summary} - {e}')
            except Exception as log_e:
                logger.debug(f'Failed to log state during _wait_for_market_render timeout: {log_e}')
            return False

    async def _click_period_tab(
        self,
        page: Page,
        period_display_name: str,
        period_key: Optional[str] = None,
    ) -> bool:
        """
            Click a market period sub-tab and wait for the odds table to update.

            Success criteria are strict to prevent stale-data extraction:
            - target period tab must be active, and
            - market content must be present, and
            - either odds changed OR active-tab state transitioned to target.
            """
        try:
            ref_value = await page.evaluate(self.GET_REFERENCE_VALUE_JS)
            logger.info(f'📸 Reference odds value before period switch: {ref_value}')
            before_active_labels = await self._get_active_period_labels(page)
            tab_language = getattr(Config, "ODDSPORTAL_UI_LANGUAGE", "en")
            period_candidates = get_period_tab_candidates(
                period_key=period_key,
                display_name=period_display_name,
                language=tab_language,
            )
            logger.info(f"🔍 Target period candidates ({tab_language}): {period_candidates}")
            before_active_has_target = any(
                tab_label_matches(lbl, period_candidates)
                for lbl in before_active_labels
            )
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
                    text_stripped = text.strip() if text else ''
                    logger.info(f"🔍 Found period tab with text: '{text_stripped}'")
                    if text_stripped:
                        available_tabs.append(text_stripped)
                        if tab_label_matches(text_stripped, period_candidates):
                            target_tab = tab
                            break
                if target_tab:
                    break
            if not target_tab:
                logger.warning(f"⚠️ Period tab '{period_display_name}' not found in any kickoff-events-nav block")
                logger.warning(f'   Available tabs: {available_tabs}')
                return False
            testid = await target_tab.get_attribute('data-testid')
            is_active = testid == 'sub-nav-active-tab'
            if is_active:
                logger.info(f"ℹ️ Period tab '{period_display_name}' is already active. Verifying content...")
                if await self._has_market_content(page):
                    return True
                logger.info('  ⏳ Period tab is active but content is missing. Waiting for render...')
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
                new_value = await page.evaluate(self.GET_REFERENCE_VALUE_JS)
                active_now = await self._is_target_period_active(
                    page,
                    period_display_name,
                    period_key=period_key,
                )
                if active_now and new_value and (ref_value is None or new_value != ref_value):
                    logger.info(f'✅ Odds table updated after {elapsed:.1f}s: {ref_value} → {new_value}')
                    table_changed = True
                    break
            if not table_changed:
                content_present = await self._has_market_content(page)
                active_now = await self._is_target_period_active(
                    page,
                    period_display_name,
                    period_key=period_key,
                )
                if active_now and content_present and (not before_active_has_target or is_active):
                    logger.info(f'ℹ️ No odds-text delta after {max_wait_s}s, but target period is active and content is present — treating as success.')
                    table_changed = True
                else:
                    logger.warning(f"⚠️ Period switch validation failed for '{period_display_name}' (active_now={active_now}, content_present={content_present}, before_active_has_target={before_active_has_target}, ref={ref_value})")
            await asyncio.sleep(0.5)
            return table_changed
        except Exception as e:
            logger.error(f"❌ Error clicking period tab '{period_display_name}': {e}")
            return False

    async def _click_market_group_tab(
        self,
        page: Page,
        group_display_name: str,
        group_key: Optional[str] = None,
    ) -> bool:
        """
            Click a market group tab (e.g. "Over/Under", "1X2") and wait for the table to update.

            Success criteria are strict to avoid false positives that can cause stale extractions.
            """
        try:
            ref_value = await page.evaluate(self.GET_REFERENCE_VALUE_JS)
            logger.info(f'📸 Reference value before group switch: {ref_value}')
            before_active_group = await self._get_active_group_label(page)
            tab_language = getattr(Config, "ODDSPORTAL_UI_LANGUAGE", "en")
            group_candidates = get_group_tab_candidates(
                group_key=group_key,
                display_name=group_display_name,
                language=tab_language,
            )
            logger.info(f"🔍 Target group candidates ({tab_language}): {group_candidates}")
            before_active_is_target = tab_label_matches(
                before_active_group,
                group_candidates,
            )
            tabs = await page.query_selector_all('ul.visible-links.odds-tabs li')
            if not tabs:
                logger.warning('⚠️ Market group tabs not found')
                return False
            target_tab = None
            available_tabs = []
            for tab in tabs:
                text = await tab.text_content()
                text_stripped = text.strip() if text else ''
                if text_stripped:
                    available_tabs.append(text_stripped)
                    if tab_label_matches(text_stripped, group_candidates):
                        target_tab = tab
                        break
            if not target_tab:
                logger.warning(f"⚠️ Market group tab '{group_display_name}' not found")
                logger.warning(f'   Available tabs: {available_tabs}')
                return False
            is_active = await target_tab.evaluate("el => el.classList.contains('active-odds')")
            if is_active:
                logger.info(f"ℹ️ Market group tab '{group_display_name}' is already active. Verifying content...")
                if await self._has_market_content(page):
                    return True
                logger.info('  ⏳ Tab is active but content is missing. Waiting for render...')
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
                new_value = await page.evaluate(self.GET_REFERENCE_VALUE_JS)
                active_now = await self._is_target_group_active(
                    page,
                    group_display_name,
                    group_key=group_key,
                )
                if active_now and new_value and (ref_value is None or new_value != ref_value):
                    logger.info(f'✅ Odds table updated after {elapsed:.1f}s: {ref_value} → {new_value}')
                    table_changed = True
                    break
            if not table_changed:
                content_present = await self._has_market_content(page)
                active_now = await self._is_target_group_active(
                    page,
                    group_display_name,
                    group_key=group_key,
                )
                if active_now and content_present and (not before_active_is_target or is_active):
                    logger.info(f'ℹ️ No odds-text delta after {max_wait_s}s, but target group is active and content is present — treating as success.')
                    table_changed = True
                else:
                    logger.warning(f"⚠️ Group switch validation failed for '{group_display_name}' (active_now={active_now}, content_present={content_present}, before_active_is_target={before_active_is_target}, ref={ref_value})")
            await asyncio.sleep(0.5)
            return table_changed
        except Exception as e:
            logger.error(f"❌ Error clicking market group tab '{group_display_name}': {e}")
            return False
