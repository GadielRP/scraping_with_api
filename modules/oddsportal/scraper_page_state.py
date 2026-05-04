"""OddsPortal page state helpers."""

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


class OddsPortalPageStateMixin:
    async def _remove_interaction_overlays(self, page: Page):
        """Best-effort cleanup of UI elements that can block clicks/hovers."""
        try:
            await page.evaluate("\n                () => {\n                    document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());\n                    const onetrust = document.getElementById('onetrust-banner-sdk');\n                    if (onetrust) onetrust.remove();\n                    const shade = document.querySelector('.onetrust-pc-dark-filter');\n                    if (shade) shade.remove();\n                }\n            ")
        except Exception:
            pass

    async def _has_market_content(self, page: Page) -> bool:
        """Return True when any known market data container is present."""
        try:
            return await page.evaluate('\n                () => {\n                    const hasRows = document.querySelectorAll(\'div.border-black-borders.flex.h-9\').length > 0;\n                    const hasOUCollapsed = document.querySelectorAll(\'div[data-testid="over-under-collapsed-row"]\').length > 0;\n                    const hasAHCollapsed = document.querySelectorAll(\'div[data-testid="asian-handicap-collapsed-row"]\').length > 0;\n                    const hasOUExpanded = document.querySelectorAll(\'div[data-testid="over-under-expanded-row"]\').length > 0;\n                    return hasRows || hasOUCollapsed || hasAHCollapsed || hasOUExpanded;\n                }\n            ')
        except Exception:
            return False

    async def _get_active_period_labels(self, page: Page) -> List[str]:
        """Collect active period labels from all kickoff-events-nav blocks."""
        try:
            labels = await page.evaluate("""
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

    async def _is_target_period_active(
        self,
        page: Page,
        period_display_name: str,
        period_key: Optional[str] = None,
    ) -> bool:
        tab_language = getattr(Config, "ODDSPORTAL_UI_LANGUAGE", "en")

        candidates = get_period_tab_candidates(
            period_key=period_key,
            display_name=period_display_name,
            language=tab_language,
        )

        if not candidates:
            return False

        labels = await self._get_active_period_labels(page)
        return any(
            tab_label_matches(lbl, candidates)
            for lbl in labels
        )

    async def _get_active_group_label(self, page: Page) -> str:
        """Return active market group tab label (e.g. '1X2', 'Over/Under')."""
        try:
            return await page.evaluate('\n                () => {\n                    const active = document.querySelector(\'ul.visible-links.odds-tabs li.active-odds\')\n                        || document.querySelector(\'li[data-testid="navigation-active-tab"]\');\n                    return active ? active.textContent.trim() : \'\';\n                }\n            ') or ''
        except Exception:
            return ''

    async def _is_target_group_active(
        self,
        page: Page,
        group_display_name: str,
        group_key: Optional[str] = None,
    ) -> bool:
        tab_language = getattr(Config, "ODDSPORTAL_UI_LANGUAGE", "en")

        candidates = get_group_tab_candidates(
            group_key=group_key,
            display_name=group_display_name,
            language=tab_language,
        )

        if not candidates:
            return False

        active_label = await self._get_active_group_label(page)

        return tab_label_matches(active_label, candidates)

    async def _collect_match_page_state(self, page: Page) -> Dict[str, Any]:
        """Gather detailed page state metrics to diagnose partial or empty loads.

            Uses selectors validated against actual OddsPortal match-page DOM.
            """
        try:
            return await page.evaluate('\n                () => {\n                    const getCount = (sel) => document.querySelectorAll(sel).length;\n\n                    // --- Shell markers ---\n                    const event_container_count = getCount(\'div.event-container\');\n                    const group_tab_count = getCount(\'ul.visible-links.odds-tabs li\');\n                    const period_nav_count = getCount(\'div[data-testid="kickoff-events-nav"]\');\n\n                    // --- Data markers ---\n                    const bookmaker_row_count = getCount(\'div.border-black-borders.flex.h-9\');\n                    const odds_cell_count = getCount(\'div.odds-cell\');\n                    const ou_collapsed_count = getCount(\'div[data-testid="over-under-collapsed-row"]\');\n                    const ah_collapsed_count = getCount(\'div[data-testid="asian-handicap-collapsed-row"]\');\n                    const ou_expanded_count = getCount(\'div[data-testid="over-under-expanded-row"]\');\n\n                    // --- Skeleton / placeholder markers ---\n                    const skeleton_row_count = getCount(\'div.animate-pulse.bg-gray-light\')\n                        + getCount(\'[class*="skeleton"]\');\n\n                    // --- Event container child count ---\n                    const firstContainer = document.querySelector(\'div.event-container\');\n                    const event_container_child_count = firstContainer ? firstContainer.children.length : 0;\n\n                    // --- Active group label ---\n                    const activeGroupEl = document.querySelector(\'ul.visible-links.odds-tabs li.active-odds\')\n                        || document.querySelector(\'li[data-testid="navigation-active-tab"]\');\n                    const active_group_label = activeGroupEl ? activeGroupEl.textContent.trim() : \'\';\n\n                    // --- Active period labels ---\n                    const active_period_labels = [];\n                    const navs = document.querySelectorAll(\'div[data-testid="kickoff-events-nav"]\');\n                    navs.forEach(nav => {\n                        const active = nav.querySelector(\'div[data-testid="sub-nav-active-tab"]\');\n                        if (active) {\n                            const txt = active.textContent.trim();\n                            if (txt) active_period_labels.push(txt);\n                        }\n                    });\n\n                    // --- Error Code (e.g. 404) ---\n                    const errEl = document.querySelector(\'div.error-code\');\n                    const error_code_text = errEl ? errEl.textContent.trim() : \'\';\n\n                    // --- Derived booleans ---\n                    const has_shell_markers = event_container_count > 0\n                        || group_tab_count > 0\n                        || period_nav_count > 0;\n\n                    const has_data_markers = bookmaker_row_count > 0\n                        || odds_cell_count > 0\n                        || ou_collapsed_count > 0\n                        || ah_collapsed_count > 0\n                        || ou_expanded_count > 0;\n\n                    return {\n                        error_code_text,\n                        url: window.location.href,\n                        title: document.title,\n                        ready_state: document.readyState,\n                        event_container_count,\n                        event_container_child_count,\n                        group_tab_count,\n                        period_nav_count,\n                        active_group_label,\n                        active_period_labels,\n                        bookmaker_row_count,\n                        odds_cell_count,\n                        ou_collapsed_count,\n                        ah_collapsed_count,\n                        ou_expanded_count,\n                        skeleton_row_count,\n                        has_shell_markers,\n                        has_data_markers,\n                    };\n                }\n            ')
        except Exception:
            return {'error_code_text': '', 'url': '', 'title': '', 'ready_state': '', 'event_container_count': 0, 'event_container_child_count': 0, 'group_tab_count': 0, 'period_nav_count': 0, 'active_group_label': '', 'active_period_labels': [], 'bookmaker_row_count': 0, 'odds_cell_count': 0, 'ou_collapsed_count': 0, 'ah_collapsed_count': 0, 'ou_expanded_count': 0, 'skeleton_row_count': 0, 'has_shell_markers': False, 'has_data_markers': False}

    def _classify_match_page_state(self, state: Dict[str, Any]) -> str:
        """Classify the dictionary returned by _collect_match_page_state.

            Rules are ordered so that data-present pages never false-fail,
            and MISSING_EVENT_CONTAINER only fires when shell markers exist
            but the event container itself is absent.
            """
        if '404' in str(state.get('error_code_text', '')):
            return 'HTTP_ERROR_404'
        if state.get('has_data_markers', False):
            return 'DATA_RENDERED'
        if not state.get('has_shell_markers', False):
            return 'NO_SHELL'
        if state.get('event_container_count', 0) == 0:
            return 'MISSING_EVENT_CONTAINER'
        if state.get('event_container_count', 0) > 0 and state.get('event_container_child_count', 0) == 0:
            return 'EMPTY_EVENT_CONTAINER'
        if state.get('skeleton_row_count', 0) > 0 and (not state.get('has_data_markers', False)):
            return 'SHELL_WITH_SKELETON_NO_DATA'
        if (state.get('group_tab_count', 0) > 0 or state.get('period_nav_count', 0) > 0) and state.get('skeleton_row_count', 0) == 0 and (not state.get('has_data_markers', False)):
            return 'SHELL_WITH_NAV_NO_DATA'
        return 'UNKNOWN_PARTIAL_STATE'

    def _format_page_state_summary(self, state: Dict[str, Any], classification: str) -> str:
        """Create a compact log line for the state."""
        return f"[{classification}] errCode='{state.get('error_code_text', '')}' title='{state.get('title', '')}' url={state.get('url', '')} evtC={state.get('event_container_count', 0)} grpTab={state.get('group_tab_count', 0)} periodNav={state.get('period_nav_count', 0)} skel={state.get('skeleton_row_count', 0)} rows={state.get('bookmaker_row_count', 0)} odds={state.get('odds_cell_count', 0)} ou={state.get('ou_collapsed_count', 0)}/{state.get('ou_expanded_count', 0)} ah={state.get('ah_collapsed_count', 0)}"

    def _classify_goto_exception(self, error: Exception) -> Tuple[str, str]:
        """Map Playwright goto exceptions into clearer log/debug reasons."""
        logger.info(f'for debugging purposes: {error}')
        error_text = str(error or '')
        upper_text = error_text.upper()
        if 'ERR_TUNNEL_CONNECTION_FAILED' in upper_text:
            return ('GOTO_PROXY_TUNNEL_FAILED', 'proxy tunnel connection failed before page load')
        if 'ERR_CONNECTION_RESET' in upper_text:
            return ('GOTO_CONNECTION_RESET', 'connection reset before page load')
        if 'ERR_CONNECTION_CLOSED' in upper_text:
            return ('GOTO_CONNECTION_CLOSED', 'connection closed before page load')
        if 'ERR_CONNECTION_REFUSED' in upper_text:
            return ('GOTO_CONNECTION_REFUSED', 'connection refused before page load')
        if 'ERR_NAME_NOT_RESOLVED' in upper_text:
            return ('GOTO_DNS_RESOLUTION_FAILED', 'DNS resolution failed before page load')
        if 'ERR_CERT_AUTHORITY_INVALID' in upper_text:
            return ('GOTO_CERT_AUTHORITY_INVALID', 'certificate authority not trusted')
        if 'ERR_CERT_COMMON_NAME_INVALID' in upper_text:
            return ('GOTO_CERT_COMMON_NAME_INVALID', 'TLS certificate common name mismatch')
        if 'ERR_CERT_DATE_INVALID' in upper_text:
            return ('GOTO_CERT_DATE_INVALID', 'TLS certificate date invalid')
        if 'ERR_CERT_INVALID' in upper_text:
            return ('GOTO_CERT_INVALID', 'TLS certificate invalid')
        if 'ERR_TIMED_OUT' in upper_text or 'TIMEOUT' in upper_text:
            return ('GOTO_TIMEOUT', 'navigation timed out before usable content loaded')
        return ('GOTO_FAILED', 'navigation failed before usable content loaded')

    async def _save_debug_artifacts(self, page: Page, reason: str, extra: Dict[str, Any]=None):
        """Save screenshot, HTML, and JSON manifest on failure if debug_dir is set."""
        if not self.debug_dir:
            return
        try:
            import datetime
            import json
            import os
            import re
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            safe_reason = reason.replace('/', '_').replace(' ', '_').lower()
            base_name = f'op_fail_{timestamp}_{safe_reason}'
            os.makedirs(self.debug_dir, exist_ok=True)
            png_path = os.path.join(self.debug_dir, f'{base_name}.png')
            await page.screenshot(path=png_path, full_page=True)
            html_content = await page.content()
            styles = '\n'.join(re.findall('<style[^>]*>(.*?)</style>', html_content, flags=re.IGNORECASE | re.DOTALL))
            if styles.strip():
                with open(os.path.join(self.debug_dir, f'{base_name}.css'), 'w', encoding='utf-8') as f:
                    f.write(styles)
            scripts = '\n'.join(re.findall('<script[^>]*>(.*?)</script>', html_content, flags=re.IGNORECASE | re.DOTALL))
            if scripts.strip():
                with open(os.path.join(self.debug_dir, f'{base_name}.js'), 'w', encoding='utf-8') as f:
                    f.write(scripts)
            raw_html = re.sub('<(style|script)[^>]*>.*?</\\1>', '', html_content, flags=re.IGNORECASE | re.DOTALL)
            html_path = os.path.join(self.debug_dir, f'{base_name}.html')
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(raw_html)
            state = await self._collect_match_page_state(page)
            classification = self._classify_match_page_state(state)
            extras_payload = extra or {}
            resume_manifest = {}
            for key in ['completed_step_keys', 'next_step_idx', 'failed_step_key', 'failed_reason', 'resume_fragment', 'partial_extraction_count']:
                if key in extras_payload:
                    resume_manifest[key] = extras_payload.get(key)
            if not resume_manifest and isinstance(extras_payload.get('resume_state'), dict):
                nested_resume = extras_payload['resume_state']
                for key in ['completed_step_keys', 'next_step_idx', 'failed_step_key', 'failed_reason', 'resume_fragment', 'partial_extraction_count']:
                    if key in nested_resume:
                        resume_manifest[key] = nested_resume.get(key)
            manifest = {'timestamp': timestamp, 'reason': reason, 'url': page.url, 'title': await page.title(), 'session_id': getattr(self, '_session_id', 'unknown'), 'state': state, 'classification': classification, 'config': {'goto_timeout': getattr(Config, 'ODDSPORTAL_MATCH_GOTO_TIMEOUT_MS', 30000), 'empty_timeout': getattr(Config, 'ODDSPORTAL_FAST_FAIL_EMPTY_TIMEOUT_MS', 15000), 'render_timeout': getattr(Config, 'ODDSPORTAL_MARKET_RENDER_TIMEOUT_MS', 60000), 'shell_grace': getattr(Config, 'ODDSPORTAL_SHELL_GRACE_TIMEOUT_MS', 8000)}, 'extras': extras_payload, **resume_manifest}
            json_path = os.path.join(self.debug_dir, f'{base_name}.json')
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=4)
            logger.info(f'💾 Saved debug artifacts for {reason} at {self.debug_dir}/{base_name}.*')
        except Exception as e:
            logger.error(f'Failed to save debug artifacts: {e}')
