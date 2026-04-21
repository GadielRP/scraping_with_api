"""OddsPortal resume helpers."""

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


class OddsPortalResumeMixin:
    def _make_initial_resume_state(self, match_url: str, sport: Optional[str], route_steps: List[Dict[str, Any]]) -> Dict[str, Any]:
        next_fragment = route_steps[0].get('fragment') if route_steps else None
        return {'sport': sport, 'route_step_count': len(route_steps), 'next_step_idx': 0, 'completed_step_keys': [], 'failed_step_key': None, 'failed_group_key': None, 'failed_period_key': None, 'failed_reason': None, 'resume_fragment': next_fragment, 'last_completed_fragment': None, 'partial_extraction_count': 0}

    def _normalize_resume_state(self, resume_state: Optional[Dict[str, Any]], sport: Optional[str], route_steps: List[Dict[str, Any]]) -> Dict[str, Any]:
        initial_state = self._make_initial_resume_state('', sport, route_steps)
        if not isinstance(resume_state, dict):
            return initial_state
        expected_step_count = len(route_steps)
        if resume_state.get('sport') != sport or resume_state.get('route_step_count') != expected_step_count:
            return initial_state
        step_key_to_idx = {step.get('step_key'): step.get('step_idx', idx) for idx, step in enumerate(route_steps) if step.get('step_key')}
        ordered_completed: List[str] = []
        for key in resume_state.get('completed_step_keys', []) or []:
            if key in step_key_to_idx and key not in ordered_completed:
                ordered_completed.append(key)
        max_completed_idx = -1
        if ordered_completed:
            max_completed_idx = max((step_key_to_idx[key] for key in ordered_completed))
        raw_next_idx = resume_state.get('next_step_idx', 0)
        next_step_idx = raw_next_idx if isinstance(raw_next_idx, int) else 0
        next_step_idx = max(next_step_idx, max_completed_idx + 1)
        next_step_idx = min(max(next_step_idx, 0), expected_step_count)
        failed_step_key = resume_state.get('failed_step_key')
        if failed_step_key not in step_key_to_idx:
            failed_step_key = None
        return {'sport': sport, 'route_step_count': expected_step_count, 'next_step_idx': next_step_idx, 'completed_step_keys': ordered_completed, 'failed_step_key': failed_step_key, 'failed_group_key': resume_state.get('failed_group_key') if failed_step_key else None, 'failed_period_key': resume_state.get('failed_period_key') if failed_step_key else None, 'failed_reason': resume_state.get('failed_reason') if failed_step_key else None, 'resume_fragment': route_steps[next_step_idx].get('fragment') if next_step_idx < expected_step_count else None, 'last_completed_fragment': resume_state.get('last_completed_fragment'), 'partial_extraction_count': int(resume_state.get('partial_extraction_count', 0) or 0)}

    def _mark_step_completed(self, resume_state: Dict[str, Any], step: Dict[str, Any], match_data: MatchOddsData) -> None:
        completed = resume_state.setdefault('completed_step_keys', [])
        step_key = step.get('step_key')
        if step_key and step_key not in completed:
            completed.append(step_key)
        route_count = int(resume_state.get('route_step_count', 0) or 0)
        next_step_idx = step.get('step_idx', 0) + 1
        if route_count > 0:
            next_step_idx = min(next_step_idx, route_count)
        resume_state['next_step_idx'] = next_step_idx
        resume_state['failed_step_key'] = None
        resume_state['failed_group_key'] = None
        resume_state['failed_period_key'] = None
        resume_state['failed_reason'] = None
        resume_state['resume_fragment'] = None
        resume_state['last_completed_fragment'] = step.get('fragment')
        resume_state['partial_extraction_count'] = len(match_data.extractions)

    def _mark_step_failed(self, resume_state: Dict[str, Any], step: Dict[str, Any], reason: str) -> None:
        resume_state['failed_step_key'] = step.get('step_key')
        resume_state['failed_group_key'] = step.get('group_key')
        resume_state['failed_period_key'] = step.get('period_key')
        resume_state['failed_reason'] = reason
        resume_state['resume_fragment'] = step.get('fragment')
        resume_state['next_step_idx'] = step.get('step_idx', 0)
        resume_state['partial_extraction_count'] = int(resume_state.get('partial_extraction_count', 0) or 0)

    def _restore_partial_match_data(self, partial_match_data: Optional[MatchOddsData], match_url: str, sport: Optional[str]) -> MatchOddsData:
        if isinstance(partial_match_data, MatchOddsData):
            match_data = partial_match_data
            match_data.match_url = match_url
            if sport is not None:
                match_data.sport = sport
            if match_data.extractions is None:
                match_data.extractions = []
        else:
            match_data = MatchOddsData(match_url=match_url, sport=sport or '')
        self._ensure_legacy_match_level_fields(match_data)
        return match_data

    def _ensure_legacy_match_level_fields(self, match_data: MatchOddsData) -> None:
        if match_data.extractions:
            first = match_data.extractions[0]
            match_data.bookie_odds = first.bookie_odds
            match_data.betfair = first.betfair
        else:
            match_data.bookie_odds = []
            match_data.betfair = None

    def _log_structured_recap(self, match_data: MatchOddsData) -> None:
        """Emit a readable recap of all extracted markets for debugging."""
        if not match_data.extractions:
            return
        match_label = f'{match_data.home_team} vs {match_data.away_team}'
        logger.info(f'📝 -- ODDS RECAP: {match_label} --')
        for ext in match_data.extractions:
            handicap_str = ''
            if ext.bookie_odds and getattr(ext.bookie_odds[0], 'handicap', None):
                handicap_str = f' [{ext.bookie_odds[0].handicap}]'
            logger.info(f'   🛒 {ext.market_group} | {ext.market_period} | {ext.market_name}{handicap_str}')
            is_ou = ext.market_group == 'Over/Under'
            is_ah = ext.market_group == 'Asian Handicap'
            if is_ou:
                col_labels = ('Over', 'Under')
            elif is_ah:
                col_labels = ('1', '2')
            else:
                col_labels = ('1', 'X', '2')
            for b in ext.bookie_odds:
                if is_ou or is_ah:
                    current = f"{col_labels[0]}={b.odds_1 or '-'} {col_labels[-1]}={b.odds_2 or '-'}"
                    opening = f"{col_labels[0]}={b.initial_odds_1 or '-'} {col_labels[-1]}={b.initial_odds_2 or '-'}"
                else:
                    current = f"1={b.odds_1 or '-'} X={b.odds_x or '-'} 2={b.odds_2 or '-'}"
                    opening = f"1={b.initial_odds_1 or '-'} X={b.initial_odds_x or '-'} 2={b.initial_odds_2 or '-'}"
                logger.info(f'      {b.name}: {current} (open: {opening})')
            if ext.betfair:
                bf = ext.betfair
                if is_ou or is_ah:
                    back_str = f"{col_labels[0]}={bf.back_1 or '-'} {col_labels[-1]}={bf.back_2 or '-'}"
                    lay_str = f"{col_labels[0]}={bf.lay_1 or '-'} {col_labels[-1]}={bf.lay_2 or '-'}"
                else:
                    back_str = f"1={bf.back_1 or '-'} X={bf.back_x or '-'} 2={bf.back_2 or '-'}"
                    lay_str = f"1={bf.lay_1 or '-'} X={bf.lay_x or '-'} 2={bf.lay_2 or '-'}"
                logger.info(f'      Betfair Back: {back_str} | Lay: {lay_str}')
        logger.info(f'📝 -- END RECAP: {match_label} --')

    def _resume_state_for_debug(self, resume_state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(resume_state, dict):
            return {}
        return {'completed_step_keys': list(resume_state.get('completed_step_keys', []) or []), 'next_step_idx': resume_state.get('next_step_idx'), 'failed_step_key': resume_state.get('failed_step_key'), 'failed_reason': resume_state.get('failed_reason'), 'resume_fragment': resume_state.get('resume_fragment'), 'partial_extraction_count': resume_state.get('partial_extraction_count')}
