"""OddsPortal browser helpers."""

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


class OddsPortalBrowserMixin:
    def __init__(self, headless: bool=True, debug_dir: Optional[str]=None, testing_mode: bool=False):
        self.headless = headless
        self.debug_dir = debug_dir
        self.testing_mode = testing_mode
        self.browser: Optional[Browser] = None
        self.playwright = None
        self.context: Optional[BrowserContext] = None
        self._session_id = 'no-proxy'
        self.proxy_manager = ProxyIdentityManager(Config, client_name='oddsportal')
        self._proxy_identity = None
        self._ignore_https_errors = getattr(Config, 'ODDSPORTAL_IGNORE_HTTPS_ERRORS', True)
        self._fresh_context_per_event = getattr(Config, 'ODDSPORTAL_FRESH_CONTEXT_PER_EVENT', True)
        self.team_matcher = TeamMatcher(team_aliases=TEAM_ALIASES, noise_list=INSTITUTIONAL_NOISE)
        if self.debug_dir:
            import os
            os.makedirs(self.debug_dir, exist_ok=True)

    def _should_rotate_proxy_on_browser_restart(self) -> bool:
        return self.proxy_manager.should_rotate_on_browser_restart()

    async def start(self, rotate_proxy_session: bool=False):
        """Start the browser session."""
        if self.browser:
            return
        Config.validate_oddsportal_proxy_alignment(logger)
        self.playwright = await async_playwright().start()
        launch_args = {'headless': self.headless, 'args': ['--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-dev-shm-usage', '--disable-infobars', '--window-size=1920,1080']}
        if self.proxy_manager.proxy_enabled:
            self._proxy_identity = self.proxy_manager.get_identity(rotate_session=rotate_proxy_session, reason='browser_restart' if rotate_proxy_session else 'browser_start')
            launch_proxy = self.proxy_manager.build_playwright_proxy(self._proxy_identity)
            if launch_proxy:
                launch_args['proxy'] = launch_proxy
                self._session_id = self.proxy_manager.session_label(self._proxy_identity)
                state_label = 'rotated' if rotate_proxy_session else 'active'
                logger.info(f'OddsPortalScraper: Launching Playwright with proxy ({state_label}, session-{self._session_id}, {self.proxy_manager.describe_identity(self._proxy_identity)})')
            else:
                self._proxy_identity = None
                self._session_id = 'no-proxy'
                logger.warning('OddsPortalScraper: Proxy is enabled but proxy identity is incomplete')
        else:
            self._proxy_identity = None
            self._session_id = 'no-proxy'
        self.browser = await self.playwright.chromium.launch(**launch_args)
        self.context = None
        logger.info(f'✅ OddsPortalScraper: Browser started (no context yet — fresh_context_per_event={self._fresh_context_per_event}, ignore_https_errors={self._ignore_https_errors})')

    async def _intercept_route(self, route):
        """Intercept network requests to block heavy assets and known trackers to save proxy bandwidth."""
        request = route.request
        url = request.url.lower()
        if request.resource_type in ['image', 'media']:
            await route.abort()
            return
        blocked_domains = ['cookielaw.org', 'googletagmanager.com', 'google-analytics.com', 'clarity.ms', 'surveygizmo.eu', 'onetrust.com', 'googlesyndication.com']
        if any((d in url for d in blocked_domains)):
            await route.abort()
            return
        if self._should_force_fresh_request(request):
            original_url = request.url
            rewritten_url = self._build_cache_busted_request_url(original_url)
            try:
                original_headers = await request.all_headers()
            except Exception:
                original_headers = getattr(request, 'headers', {}) or {}
            rewritten_headers = self._build_no_cache_request_headers(original_headers)
            logger.debug('🔄 Cache-busting OddsPortal inner request (%s): %s -> %s', request.resource_type, original_url, rewritten_url)
            await route.continue_(url=rewritten_url, headers=rewritten_headers)
            return
        await route.continue_()

    def _build_context_options(self) -> Dict[str, Any]:
        """Centralize all options for browser.new_context()."""
        user_agents = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36']
        context_options = {'viewport': {'width': 1920, 'height': 1080}, 'user_agent': random.choice(user_agents), 'locale': 'en-US', 'timezone_id': 'America/Mexico_City', 'java_script_enabled': True, 'ignore_https_errors': self._ignore_https_errors}
        if getattr(Config, 'ODDSPORTAL_BLOCK_SERVICE_WORKERS', True):
            context_options['service_workers'] = 'block'
        return context_options

    def _get_evasion_init_script(self) -> str:
        """Return the anti-detection init script as a reusable string."""
        return "\n            // Overwrite webdriver property\n            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });\n            \n            // Mock permissions\n            const originalQuery = window.navigator.permissions.query;\n            window.navigator.permissions.query = (parameters) => (\n                parameters.name === 'notifications' ?\n                Promise.resolve({ state: Notification.permission }) :\n                originalQuery(parameters)\n            );\n            \n            // Mock plugins and languages\n            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });\n            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });\n            \n            // Mock platform\n            Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });\n            \n            // Mock chrome object\n            window.chrome = { runtime: {} };\n        "

    async def _install_context_features(self, context: BrowserContext) -> None:
        """Apply init script and resource-blocking route to a context."""
        await context.add_init_script(self._get_evasion_init_script())
        if getattr(Config, 'ODDSPORTAL_BLOCK_RESOURCES', True):
            await context.route('**/*', self._intercept_route)

    async def _create_fresh_context(self) -> BrowserContext:
        """Create a brand-new BrowserContext from the already-open browser."""
        if not self.browser:
            raise RuntimeError('Cannot create fresh context: browser is not started. Call start() first.')
        context_options = self._build_context_options()
        ctx = await self.browser.new_context(**context_options)
        await self._install_context_features(ctx)
        logger.info(f"🔒 Fresh context created (session-{self._session_id}, ignore_https_errors={self._ignore_https_errors}, service_workers={context_options.get('service_workers', 'allow')})")
        return ctx

    async def stop(self):
        """Stop the browser session."""
        try:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        except Exception:
            pass
        finally:
            if self.playwright:
                await self.playwright.stop()
            self.browser = None
            self.context = None
            self.playwright = None
            self._proxy_identity = None
            logger.info('🛑 OddsPortalScraper: Browser stopped')

    async def _goto_fresh(self, page: Page, url: str, **kwargs) -> object:
        """Cache-defeating wrapper around page.goto().

            1. Sets no-cache/no-store HTTP headers for this navigation only.
            2. Appends a _t=<timestamp> cache-buster to the URL (before the fragment).
            3. Used by match-page navigation to emulate a fresh/incognito-style load
               as closely as server behavior allows.
            """
        if '#' in url:
            base_part, fragment = url.split('#', 1)
        else:
            base_part, fragment = (url, None)
        cache_buster = f'_t={int(time.time())}'
        if '?' in base_part:
            base_part = f'{base_part}&{cache_buster}'
        else:
            base_part = f'{base_part}?{cache_buster}'
        fresh_url = f'{base_part}#{fragment}' if fragment else base_part
        logger.debug(f'🔄 _goto_fresh: {fresh_url}')
        try:
            await page.set_extra_http_headers({'Cache-Control': 'no-cache, no-store', 'Pragma': 'no-cache'})
            response = await page.goto(fresh_url, **kwargs)
            return response
        finally:
            await page.set_extra_http_headers({})

    def _should_force_fresh_request(self, request) -> bool:
        """Return True for dynamic OddsPortal payload requests that should bypass cache reuse."""
        if request.resource_type not in {'xhr', 'fetch'}:
            return False
        parsed = urlsplit(request.url)
        hostname = (parsed.hostname or '').lower()
        if 'oddsportal' not in hostname:
            return False
        request_url = request.url.lower()
        markers = ('/feed/match/', '.dat', 'ajax-nextgames')
        return any((marker in request_url for marker in markers))

    def _build_cache_busted_request_url(self, url: str) -> str:
        """Add or refresh a cache-busting query parameter while preserving the original URL shape."""
        parsed = urlsplit(url)
        query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
        timestamp_ms = str(int(time.time() * 1000))
        has_underscore_param = any((key == '_' for key, _ in query_pairs))
        rewritten_pairs: List[Tuple[str, str]] = []
        replaced_key = None
        for key, value in query_pairs:
            if key == '_' and replaced_key is None:
                rewritten_pairs.append((key, timestamp_ms))
                replaced_key = '_'
            elif key == '_t' and replaced_key is None and (not has_underscore_param):
                rewritten_pairs.append((key, timestamp_ms))
                replaced_key = '_t'
            else:
                rewritten_pairs.append((key, value))
        if replaced_key is None:
            rewritten_pairs.append(('_t', timestamp_ms))
        return urlunsplit(parsed._replace(query=urlencode(rewritten_pairs, doseq=True)))

    def _build_no_cache_request_headers(self, headers: Dict[str, str]) -> Dict[str, Optional[str]]:
        """Override cache headers and drop validators while keeping all other request headers intact."""
        rewritten_headers: Dict[str, Optional[str]] = dict(headers or {})
        for key in list(rewritten_headers.keys()):
            lowered = key.lower()
            if lowered in {'cache-control', 'pragma', 'expires', 'if-none-match', 'if-modified-since'}:
                rewritten_headers[key] = None
        rewritten_headers['Cache-Control'] = 'no-cache, no-store, max-age=0'
        rewritten_headers['Pragma'] = 'no-cache'
        rewritten_headers['Expires'] = '0'
        rewritten_headers['If-None-Match'] = None
        rewritten_headers['If-Modified-Since'] = None
        return rewritten_headers

    async def _clear_browser_state(self) -> None:
        """Clear cookies, web storage, cache storage, and service workers from the active context.

            This method is a best-effort stale-state mitigation used before
            navigation when a fresh network/browser state is desired.
            """
        if not self.context:
            logger.debug('⚠️ _clear_browser_state: no active context, skipping')
            return
        try:
            await self.context.clear_cookies()
            try:
                tmp_page = await self.context.new_page()
                await tmp_page.goto('about:blank')
                await tmp_page.evaluate('() => { localStorage.clear(); sessionStorage.clear(); }')
                try:
                    await tmp_page.evaluate("\n                        async () => {\n                            if (typeof caches !== 'undefined') {\n                                const names = await caches.keys();\n                                await Promise.all(names.map(n => caches.delete(n)));\n                            }\n                        }\n                    ")
                except Exception:
                    pass
                try:
                    await tmp_page.evaluate('\n                        async () => {\n                            if (navigator.serviceWorker) {\n                                const regs = await navigator.serviceWorker.getRegistrations();\n                                await Promise.all(regs.map(r => r.unregister()));\n                            }\n                        }\n                    ')
                except Exception:
                    pass
                await tmp_page.close()
            except Exception as storage_err:
                logger.debug(f'⚠️ Storage/cache clear on about:blank failed (non-fatal): {storage_err}')
            logger.info('🧹 OddsPortalScraper: Browser state cleared (cookies + storage + cache storage + service workers)')
        except Exception as e:
            logger.warning(f'⚠️ _clear_browser_state failed (non-fatal): {e}')

    def _normalize_base_match_url(self, match_url: str) -> str:
        """Remove fragment/trailing slash and return canonical match base URL."""
        return (match_url or '').split('#')[0].rstrip('/')
