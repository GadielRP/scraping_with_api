"""OddsPortal hover helpers."""

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


class OddsPortalHoverMixin:
    def _parse_opening_odds_from_modal_html(self, modal_html: str, label: str='') -> Optional[Tuple[str, str]]:
        """
            Parse the opening odds value from the tooltip modal HTML.

            Contract: returns (opening_val, movement_time) only when a real opening odd was found.
            Never returns (None, movement_time). Returns None on any failure.

            The modal HTML contains:
              - movement time in '<div class="text-[10px] font-normal">'
              - an 'Opening odds:' section with a flex row: [date/time div] [font-bold value div]
              Betfair adds a third div with volume like "(0)" which must be ignored.
            """
        try:
            movement_time = None
            if self.testing_mode and self.debug_dir:
                try:
                    debug_filename = f'modal_{label}.html' if label else 'modal_unknown.html'
                    debug_filename = ''.join([c if c.isalnum() or c in '._-' else '_' for c in debug_filename])
                    os.makedirs(self.debug_dir, exist_ok=True)
                    debug_path = os.path.join(self.debug_dir, debug_filename)
                    with open(debug_path, 'w', encoding='utf-8') as f:
                        f.write(modal_html)
                    logger.debug(f'💾 Saved modal HTML: {debug_filename}')
                except Exception as e:
                    logger.warning(f'⚠️ Failed to save modal HTML for {label}: {e}')
            idx_opening_anchor = modal_html.find('Opening odds')
            pre_section = modal_html[:idx_opening_anchor] if idx_opening_anchor != -1 else modal_html
            time_matches = re.findall('<div[^>]*text-\\[10px\\][^>]*font-normal[^>]*>\\s*([^<]+)\\s*</div>', pre_section)
            if time_matches:
                movement_time = time_matches[0].strip()
            else:
                date_matches = re.findall('(?:>|^\\s*)(\\d{1,2}\\s+[A-Za-z]{3},\\s+\\d{2}:\\d{2})(?:<|\\s*$)', pre_section)
                if date_matches:
                    movement_time = date_matches[0].strip()
            if 'Opening odds' not in modal_html:
                return None
            idx = modal_html.find('Opening odds')
            section = modal_html[idx:idx + 600]
            matches = re.findall('<div[^>]*>\\s*([^<]+)\\s*</div>\\s*<div[^>]*font-bold[^>]*>([\\d.]+)</div>', section)
            extracted_val = None
            for _, val in matches:
                val = val.strip()
                try:
                    f = float(val)
                    if 1.0 <= f <= 1001.0:
                        extracted_val = val
                        break
                except ValueError:
                    continue
            if not extracted_val:
                bold_matches = re.findall('<div[^>]*font-bold[^>]*>([\\d.]+)</div>', section)
                for val in bold_matches:
                    val = val.strip()
                    try:
                        f = float(val)
                        if 1.0 <= f <= 1001.0:
                            extracted_val = val
                            break
                    except ValueError:
                        continue
            if extracted_val:
                return (extracted_val, movement_time)
            return None
        except Exception as e:
            logger.warning(f'Error parsing opening odds from modal: {e}')
            return None

    async def _dismiss_odds_movement_tooltip(self, page: Page) -> None:
        """
            Move the mouse off the current hover target so Vue.js dismisses the tooltip,
            then wait briefly for the tooltip to detach from the DOM.
            Errors are swallowed — this helper is never fatal.
            """
        try:
            await page.mouse.move(0, 0)
            await page.wait_for_timeout(300)
            if Config.ODDSPORTAL_UI_LANGUAGE == "es":
                await page.wait_for_selector("h3:has-text('Movimiento de cuotas')", state='detached', timeout=1500)
            else:
                try:
                    await page.wait_for_selector("h3:has-text('Odds movement')", state='detached', timeout=1500)
                except Exception:
                    pass
        except Exception:
            pass

    async def _wait_for_scoped_tooltip_html(self, page: Page, timeout_ms: int=4000) -> Optional[str]:
        """
            Old tooltip lookup behavior:
            after hover, find the visible 'Odds movement' tooltip globally on the page,
            then return the inner_html of its parent element.
            """
        try:
            odds_movement_h3 = await page.wait_for_selector("h3:has-text('Odds movement')", state='visible', timeout=timeout_ms)
            is_visible = await page.is_visible("h3:has-text('Odds movement')")
            if not is_visible:
                return None
            modal_wrapper = await odds_movement_h3.evaluate_handle('node => node.parentElement')
            modal_el = modal_wrapper.as_element()
            if modal_el:
                return await modal_el.inner_html()
        except Exception:
            return None
        return None

    async def _get_hover_target_from_container(self, container):
        """
            Resolve the hover target from an odd-container: prefers the inner
            font-bold flex div, falls back to the container itself.
            """
        try:
            inner = await container.query_selector('div.flex-center.flex-col.font-bold')
            return inner if inner else container
        except Exception:
            return container

    async def _find_bookie_row(self, page: Page, bookie_name: str):
        """
            Locate the bookie row element for *bookie_name*, checking standard rows first
            then any expanded-context rows.  Returns the element or None.
            """
        target_row = await page.query_selector(f"div.border-black-borders.flex.h-9:has(a[title*='{bookie_name}'])")
        if not target_row:
            target_row = await page.query_selector(f"div.border-black-borders.flex.h-9:has(img[alt*='{bookie_name}'])")
        if not target_row:
            rows = await page.query_selector_all('div.border-black-borders.flex.h-9')
            for row in rows:
                name_link = await row.query_selector('a[title]')
                if name_link:
                    title = await name_link.get_attribute('title')
                    if title and bookie_name.lower() in title.lower():
                        target_row = row
                        break
                img = await row.query_selector('img[alt]')
                if img:
                    alt = await img.get_attribute('alt')
                    if alt and bookie_name.lower() in alt.lower():
                        target_row = row
                        break
        if not target_row:
            rows = await page.query_selector_all('div.border-black-borders.flex')
            for row in rows:
                name_link = await row.query_selector('a[title]')
                if name_link:
                    title = await name_link.get_attribute('title')
                    if title and bookie_name.lower() in title.lower():
                        target_row = row
                        break
                img = await row.query_selector('img[alt]')
                if img:
                    alt = await img.get_attribute('alt')
                    if alt and bookie_name.lower() in alt.lower():
                        target_row = row
                        break
        return target_row

    async def _extract_opening_odds_for_bookie(self, page: Page, bookie_name: str) -> Optional[Dict[str, Optional[Tuple[str, str]]]]:
        """
            Hover over each odds cell for a specific bookie to trigger the tooltip,
            then extract the opening odds from the scoped 'Odds movement' tooltip.

            Key changes vs original:
              - row, odd-containers and hover target are re-resolved on every retry
                (no stale handles between attempts).
              - tooltip is captured from within the same odd-container that was hovered
                (scoped, not global page selector).
              - result dict only contains keys with a real opening value; None is never stored.
              - returns None when no key has a real value.
            """
        try:
            await page.wait_for_timeout(500)
            await page.evaluate("\n                () => { document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove()); }\n            ")
            initial_row = await self._find_bookie_row(page, bookie_name)
            if not initial_row:
                logger.warning(f'⚠️ Bookie row not found for: {bookie_name}')
                return None
            initial_containers = await initial_row.query_selector_all("div[data-testid='odd-container']")
            if not initial_containers:
                logger.warning(f'⚠️ No odd containers found in row for: {bookie_name}')
                return None
            is_three_way = len(initial_containers) >= 3
            choice_keys = ['1', 'X', '2'] if is_three_way else ['1', '2']
            logger.info(f'🖱️ Hovering {len(choice_keys)} odds cells for {bookie_name}')
            result: Dict[str, Tuple[str, str]] = {}
            for i, choice in enumerate(choice_keys):
                max_retries = 3
                t_hover_cell = time.perf_counter()
                await self._dismiss_odds_movement_tooltip(page)
                for attempt in range(max_retries):
                    try:
                        target_row = await self._find_bookie_row(page, bookie_name)
                        if not target_row:
                            logger.debug(f'  Cell {choice}: row not found (attempt {attempt + 1})')
                            await asyncio.sleep(0.4)
                            continue
                        containers = await target_row.query_selector_all("div[data-testid='odd-container']")
                        if not containers or i >= len(containers):
                            logger.debug(f'  Cell {choice}: container index {i} out of range (attempt {attempt + 1})')
                            await asyncio.sleep(0.4)
                            continue
                        current_container = containers[i]
                        bbox_check = await current_container.bounding_box()
                        if not bbox_check:
                            logger.debug(f'  Cell {choice}: bounding_box is None (attempt {attempt + 1}), skipping')
                            await asyncio.sleep(0.4)
                            continue
                        hover_target = await self._get_hover_target_from_container(current_container)
                        await hover_target.scroll_into_view_if_needed()
                        await page.evaluate('window.scrollBy(0, -150)')
                        await page.wait_for_timeout(200)
                        await page.evaluate("\n                            () => {\n                                document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());\n                                const onetrust = document.getElementById('onetrust-banner-sdk');\n                                if (onetrust) onetrust.remove();\n                                const shade = document.querySelector('.onetrust-pc-dark-filter');\n                                if (shade) shade.remove();\n                            }\n                        ")
                        bbox = await hover_target.bounding_box()
                        if bbox:
                            cx = bbox['x'] + bbox['width'] / 2
                            cy = bbox['y'] + bbox['height'] / 2
                            await page.mouse.move(cx - 15, cy - 15)
                            await page.wait_for_timeout(50)
                            await page.mouse.move(cx, cy)
                            await page.wait_for_timeout(50)
                        try:
                            await hover_target.hover(force=True, timeout=1500)
                        except Exception:
                            pass
                        await page.evaluate("\n                            (el) => {\n                                el.dispatchEvent(new PointerEvent('pointerover', {bubbles: true, cancelable: true, pointerId: 1}));\n                                el.dispatchEvent(new PointerEvent('pointerenter', {bubbles: true, cancelable: true, pointerId: 1}));\n                                el.dispatchEvent(new MouseEvent('mouseover',  {bubbles: true, cancelable: true}));\n                                el.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true, cancelable: true}));\n                                el.dispatchEvent(new MouseEvent('mousemove',  {bubbles: true, cancelable: true}));\n                            }\n                        ", hover_target)
                        wait_ms = 3000 + attempt * 1000
                        html = await self._wait_for_scoped_tooltip_html(page, timeout_ms=wait_ms)
                        if not html:
                            logger.debug(f'  Cell {choice}: global tooltip not found (attempt {attempt + 1}/{max_retries})')
                            await self._dismiss_odds_movement_tooltip(page)
                            continue
                        label = f'{bookie_name}_{choice}'
                        parsed = self._parse_opening_odds_from_modal_html(html, label=label)
                        if parsed:
                            opening_val, opening_time = parsed
                            result[choice] = (opening_val, opening_time)
                            logger.debug(f'  Cell {choice}: opening={opening_val} at {opening_time} (attempt {attempt + 1})')
                        else:
                            logger.debug(f'  Cell {choice}: tooltip found but no opening odd parsed (attempt {attempt + 1})')
                        await self._dismiss_odds_movement_tooltip(page)
                        break
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.debug(f'  Cell {choice}: hover error (attempt {attempt + 1}), retrying: {e}')
                            await self._dismiss_odds_movement_tooltip(page)
                            continue
                        logger.warning(f'  Error hovering cell {choice} for {bookie_name}: {e}')
                await self._dismiss_odds_movement_tooltip(page)
                if choice in result:
                    log_timing(f"Hovering and extracting '{choice}' opening odd for {bookie_name} took {time.perf_counter() - t_hover_cell:.2f}s")
                else:
                    log_timing(f"Failed to extract '{choice}' opening odd for {bookie_name} after {time.perf_counter() - t_hover_cell:.2f}s")
            return result if result else None
        except Exception as e:
            logger.error(f'Error in _extract_opening_odds_for_bookie({bookie_name}): {e}')
            return None

    async def _extract_opening_odds_betfair(self, page: Page) -> Optional[Dict[str, Optional[Tuple[str, str]]]]:
        """
            Extract opening/initial odds for Betfair Exchange by hovering over its odds cells.

            Key changes vs original:
              - exchange section, containers and hover target are re-resolved on every retry.
              - tooltip is captured scoped to the hovered odd-container, not via global h3 selector.
              - visibility is not inferred from CSS classes (Betfair keeps 'hidden' on tooltip root).
              - result dict only contains keys with a real opening value; None is never stored.
              - returns None when no key has a real value.
            """
        try:
            exchange_section = await page.query_selector("div[data-testid='betting-exchanges-section']")
            if not exchange_section:
                logger.warning('⚠️ Betfair Exchange section not found for hover extraction')
                return None
            await page.wait_for_timeout(500)
            odd_containers_init = await exchange_section.query_selector_all("div[data-testid='odd-container']")
            if not odd_containers_init:
                logger.warning('⚠️ No odd containers found in Betfair Exchange section')
                return None

            def _build_betfair_choice_to_index(container_count: int) -> Dict[str, int]:
                if container_count >= 6:
                    return {'back_1': 0, 'back_x': 1, 'back_2': 2, 'lay_1': 3, 'lay_x': 4, 'lay_2': 5}
                if container_count >= 4:
                    return {'back_1': 0, 'back_2': 1, 'lay_1': 2, 'lay_2': 3}
                return {}

            initial_mapping = _build_betfair_choice_to_index(len(odd_containers_init))
            if not initial_mapping:
                logger.warning(f'⚠️ Unexpected Betfair container count: {len(odd_containers_init)}')
                return None

            logger.debug(f"  Betfair: {len(odd_containers_init)} containers detected -> {('3-way' if len(odd_containers_init) >= 6 else '2-way')}")
            logger.info('🖱️ Hovering Betfair cells (Back & Lay) with live layout remap')

            processed_choices = set()
            await page.evaluate("\n                () => {\n                    document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());\n                    const onetrust = document.getElementById('onetrust-banner-sdk');\n                    if (onetrust) onetrust.remove();\n                    const shade = document.querySelector('.onetrust-pc-dark-filter');\n                    if (shade) shade.remove();\n                }\n            ")
            result: Dict[str, Tuple[str, str]] = {}
            while True:
                ex_sec_now = await page.query_selector("div[data-testid='betting-exchanges-section']")
                if not ex_sec_now:
                    logger.warning('⚠️ Betfair Exchange section disappeared before hover extraction')
                    break
                containers_now = await ex_sec_now.query_selector_all("div[data-testid='odd-container']")
                live_mapping = _build_betfair_choice_to_index(len(containers_now))
                if not live_mapping:
                    logger.warning(f'⚠️ Unexpected Betfair container count during hover extraction: {len(containers_now)}')
                    break
                pending_choices = [k for k in live_mapping.keys() if k not in processed_choices]
                if not pending_choices:
                    break
                choice = pending_choices[0]
                t_hover_bf = time.perf_counter()
                max_retries = 3
                await self._dismiss_odds_movement_tooltip(page)
                for attempt in range(max_retries):
                    try:
                        ex_sec = await page.query_selector("div[data-testid='betting-exchanges-section']")
                        if not ex_sec:
                            logger.debug(f'  Betfair {choice}: exchange section not found (attempt {attempt + 1})')
                            await asyncio.sleep(0.4)
                            continue
                        containers = await ex_sec.query_selector_all("div[data-testid='odd-container']")
                        current_mapping = _build_betfair_choice_to_index(len(containers))
                        if not current_mapping:
                            logger.debug(f'  Betfair {choice}: unexpected container count {len(containers)} (attempt {attempt + 1})')
                            await asyncio.sleep(0.4)
                            continue
                        if choice not in current_mapping:
                            logger.debug(f"  Betfair {choice}: choice not present in current {('3-way' if len(containers) >= 6 else '2-way')} layout (attempt {attempt + 1})")
                            break
                        current_container = containers[current_mapping[choice]]
                        bbox_check = await current_container.bounding_box()
                        if not bbox_check:
                            logger.debug(f'  Betfair {choice}: bounding_box is None (attempt {attempt + 1}), skipping')
                            await asyncio.sleep(0.4)
                            continue
                        hover_target = await self._get_hover_target_from_container(current_container)
                        await hover_target.scroll_into_view_if_needed()
                        await page.evaluate('window.scrollBy(0, -150)')
                        await page.wait_for_timeout(200)
                        await page.evaluate("\n                            () => {\n                                document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());\n                                const onetrust = document.getElementById('onetrust-banner-sdk');\n                                if (onetrust) onetrust.remove();\n                                const shade = document.querySelector('.onetrust-pc-dark-filter');\n                                if (shade) shade.remove();\n                            }\n                        ")
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
                        await page.evaluate("\n                            (el) => {\n                                el.dispatchEvent(new PointerEvent('pointerover', {bubbles: true, cancelable: true, pointerId: 1}));\n                                el.dispatchEvent(new PointerEvent('pointerenter', {bubbles: true, cancelable: true, pointerId: 1}));\n                                el.dispatchEvent(new MouseEvent('mouseover',  {bubbles: true, cancelable: true}));\n                                el.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true, cancelable: true}));\n                                el.dispatchEvent(new MouseEvent('mousemove',  {bubbles: true, cancelable: true}));\n                            }\n                        ", hover_target)
                        wait_ms = 3000 + attempt * 1000
                        html = await self._wait_for_scoped_tooltip_html(page, timeout_ms=wait_ms)
                        if not html:
                            logger.debug(f'  Betfair {choice}: global tooltip not found (attempt {attempt + 1}/{max_retries})')
                            await self._dismiss_odds_movement_tooltip(page)
                            continue
                        label = f'Betfair_{choice}'
                        parsed = self._parse_opening_odds_from_modal_html(html, label=label)
                        if parsed:
                            opening_val, opening_time = parsed
                            result[choice] = (opening_val, opening_time)
                            logger.debug(f'  Betfair {choice}: opening={opening_val} at {opening_time} (attempt {attempt + 1})')
                        else:
                            logger.debug(f'  Betfair {choice}: tooltip found but no opening odd parsed (attempt {attempt + 1})')
                        await self._dismiss_odds_movement_tooltip(page)
                        break
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.debug(f'  Betfair {choice}: modal not found (attempt {attempt + 1}/{max_retries}), retrying...')
                            await self._dismiss_odds_movement_tooltip(page)
                            continue
                        logger.warning(f'  Betfair {choice}: all retries failed: {e}')
                await self._dismiss_odds_movement_tooltip(page)
                if choice in result:
                    log_timing(f"Hovering and extracting Betfair '{choice}' opening odd took {time.perf_counter() - t_hover_bf:.2f}s")
                else:
                    log_timing(f"Failed to extract Betfair '{choice}' opening odd after {time.perf_counter() - t_hover_bf:.2f}s")
                processed_choices.add(choice)
            return result if result else None
        except Exception as e:
            logger.error(f'Error in _extract_opening_odds_betfair: {e}')
            return None
