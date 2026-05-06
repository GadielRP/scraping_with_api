"""OddsPortal data helpers."""

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


class OddsPortalDataMixin:
    async def _extract_data(self, page: Page, match_url: str) -> MatchOddsData:
        """Execute JS to extract structured data."""
        raw_data = await page.evaluate("""
        () => {
            const result = {
                homeTeam: '',
                awayTeam: '',
                bookies: [],
                betfairBack: null,
                betfairLay: null,
                betfairPayout: null
            };

            // --- Team Names ---
            const h1 = document.querySelector('h1');

            if (h1) {
                let h1Text = h1.textContent.trim();
                const dashIdx = h1Text.indexOf(' - ');

                if (dashIdx > 0) h1Text = h1Text.substring(0, dashIdx);

                const vsSplit = h1Text.split(' vs ');

                if (vsSplit.length >= 2) {
                    result.homeTeam = vsSplit[0].trim();
                    result.awayTeam = vsSplit[1].trim();
                }
            }

            // --- Bookmakers ---
            const allDivs = document.querySelectorAll('div.flex.h-9');

            for (const row of allDivs) {
                if (!row.className.includes('border-black-borders')) continue;

                const oddsCells = row.querySelectorAll('div.odds-cell');
                if (oddsCells.length < 2) continue;

                let bookieName = null;

                const nameLink = row.querySelector('a[title]');
                if (nameLink) bookieName = nameLink.getAttribute('title');

                if (!bookieName) {
                    const img = row.querySelector('img[alt]');
                    if (img) bookieName = img.getAttribute('alt');
                }

                if (!bookieName || ['Oddsportal', 'Search'].includes(bookieName)) continue;

                const odds = Array.from(oddsCells).map(c => c.textContent.trim());

                // Payout
                const lastChild = row.children[row.children.length - 1];
                let payout = '-';

                if (lastChild && lastChild.textContent.includes('%')) {
                    payout = lastChild.textContent.trim();
                }

                const isThreeWay = odds.length >= 3;

                result.bookies.push({
                    name: bookieName,
                    odds1: odds[0] || '-',
                    oddsX: isThreeWay ? (odds[1] || '-') : null,
                    odds2: isThreeWay ? (odds[2] || '-') : (odds[1] || '-'),
                    payout: payout,
                });
            }

            // --- Extract Betfair Exchange ---
            // Search for the section directly
            const exchangeSection = document.querySelector('div[data-testid="betting-exchanges-section"]');

            if (exchangeSection) {
                const allOddContainers = exchangeSection.querySelectorAll('div[data-testid="odd-container"]');

                const extractOddFromContainer = (container) => {
                    const ps = container.querySelectorAll('p');

                    for (const p of ps) {
                        const txt = p.textContent.trim();

                        if (!txt || txt === '-') continue;
                        if (/^\\d+(\\.\\d+)?$/.test(txt)) return txt;
                    }

                    return null;
                };

                if (allOddContainers.length >= 6) {
                    // 3-Way Market (1X2)
                    // Back Odds (Indices 0, 1, 2)
                    const back1 = extractOddFromContainer(allOddContainers[0]);
                    const backX = extractOddFromContainer(allOddContainers[1]);
                    const back2 = extractOddFromContainer(allOddContainers[2]);

                    // Lay Odds (Indices 3, 4, 5)
                    const lay1 = extractOddFromContainer(allOddContainers[3]);
                    const layX = extractOddFromContainer(allOddContainers[4]);
                    const lay2 = extractOddFromContainer(allOddContainers[5]);

                    if (back1 || backX || back2) {
                        result.betfairBack = {
                            odds1: back1 || '-',
                            oddsX: backX || '-',
                            odds2: back2 || '-'
                        };
                    }

                    if (lay1 || layX || lay2) {
                        result.betfairLay = {
                            odds1: lay1 || '-',
                            oddsX: layX || '-',
                            odds2: lay2 || '-'
                        };
                    }
                } else if (allOddContainers.length >= 4) {
                    // 2-Way Market (Home/Away)
                    // Back Odds (Indices 0, 1)
                    const back1 = extractOddFromContainer(allOddContainers[0]);
                    const back2 = extractOddFromContainer(allOddContainers[1]);

                    // Lay Odds (Indices 2, 3)
                    const lay1 = extractOddFromContainer(allOddContainers[2]);
                    const lay2 = extractOddFromContainer(allOddContainers[3]);

                    if (back1 || back2) {
                        result.betfairBack = {
                            odds1: back1 || '-',
                            oddsX: '-',
                            odds2: back2 || '-'
                        };
                    }

                    if (lay1 || lay2) {
                        result.betfairLay = {
                            odds1: lay1 || '-',
                            oddsX: '-',
                            odds2: lay2 || '-'
                        };
                    }
                }

                // Extract payout from section text if available
                const sectionText = exchangeSection.innerText || '';
                const payMatch = sectionText.match(/(\\d{2,3}\\.\\d)%/);

                if (payMatch) result.betfairPayout = payMatch[0];
            }

            return result;
        }
        """)
        match_data = MatchOddsData(match_url=match_url)
        match_data.home_team = raw_data.get('homeTeam', 'Unknown')
        match_data.away_team = raw_data.get('awayTeam', 'Unknown')
        for b in raw_data.get('bookies', []):
            match_data.bookie_odds.append(BookieOdds(name=b['name'], odds_1=b['odds1'], odds_x=b['oddsX'], odds_2=b['odds2'], payout=b['payout']))
        bf_back = raw_data.get('betfairBack')
        if bf_back:
            bf_lay = raw_data.get('betfairLay') or {}
            match_data.betfair = BetfairExchangeOdds(back_1=bf_back.get('odds1'), back_1_vol=bf_back.get('vol1'), back_x=bf_back.get('oddsX'), back_x_vol=bf_back.get('volX'), back_2=bf_back.get('odds2'), back_2_vol=bf_back.get('vol2'), lay_1=bf_lay.get('odds1'), lay_1_vol=bf_lay.get('vol1'), lay_x=bf_lay.get('oddsX'), lay_x_vol=bf_lay.get('volX'), lay_2=bf_lay.get('odds2'), lay_2_vol=bf_lay.get('vol2'), payout=raw_data.get('betfairPayout', '-'))
        return match_data

    async def _extract_data_over_under(self, page: Page, match_url: str) -> Optional[MatchOddsData]:
        """
            Extracts Over/Under market data from the current page.
            Identifies the handicap value, Home (Over), and Away (Under) odds for each bookmaker.
            """
        logger.info('  🔄 Finding main Over/Under line (closest odds)...')
        rows_data = await page.evaluate('\n            () => {\n                const rows = Array.from(document.querySelectorAll(\'div[data-testid="over-under-collapsed-row"]\'));\n                return rows.map((row, index) => {\n                    let over = null;\n                    let under = null;\n                    let handicapText = "";\n                    \n                    const optionBox = row.querySelector(\'div[data-testid="over-under-collapsed-option-box"] p\');\n                    if (optionBox) {\n                        handicapText = optionBox.innerText.trim();\n                    }\n\n                    const containers = row.querySelectorAll(\'.flex-center.border-black-main\');\n                    if (containers.length >= 2) {\n                        over = parseFloat(containers[0].innerText.trim());\n                        under = parseFloat(containers[1].innerText.trim());\n                    }\n                    return { index, handicapText, over, under };\n                });\n            }\n        ')
        min_diff = float('inf')
        target_index = -1
        target_handicap = None
        logger.info(f'  📊 Evaluating {len(rows_data)} Over/Under rows for the closest odds...')
        for row in rows_data:
            idx = row.get('index')
            hc = row.get('handicapText', 'Unknown')
            over = row.get('over')
            under = row.get('under')
            if isinstance(over, (int, float)) and isinstance(under, (int, float)):
                diff = abs(over - under)
                if diff < min_diff:
                    min_diff = diff
                    target_index = idx
                    target_handicap = hc
            else:
                pass
        if target_index != -1:
            logger.info(f'  👉 Selecting row {target_index} ({target_handicap}) with min difference {min_diff:.2f}')
            await page.locator('div[data-testid="over-under-collapsed-row"]').nth(target_index).click()
        else:
            logger.warning('⚠️ Could not determine main line Over/Under row')
            return None
        await page.wait_for_timeout(1500)
        raw_data = await page.evaluate('\n            (hc) => {\n                const result = {\n                    homeTeam: \'Unknown\',\n                    awayTeam: \'Unknown\',\n                    bookies: [],\n                    handicap: hc\n                };\n                \n                // --- Team Names ---\n                const h1 = document.querySelector(\'h1\');\n                if (h1) {\n                    let h1Text = h1.textContent.trim();\n                    const dashIdx = h1Text.indexOf(\' - \');\n                    if (dashIdx > 0) h1Text = h1Text.substring(0, dashIdx);\n                    const vsSplit = h1Text.split(\' vs \');\n                    if (vsSplit.length >= 2) {\n                        result.homeTeam = vsSplit[0].trim();\n                        result.awayTeam = vsSplit[1].trim();\n                    }\n                }\n                \n                // --- Bookmakers ---\n                const allRows = document.querySelectorAll(\'div[data-testid="over-under-expanded-row"]\');\n                for (const row of allRows) {\n                    let bookieName = null;\n                    const namePara = row.querySelector(\'[data-testid="outrights-expanded-bookmaker-name"]\');\n                    if (namePara) bookieName = namePara.textContent.trim();\n                    if (!bookieName) {\n                        const img = row.querySelector(\'[data-testid="outrights-expanded-bookmaker-logo"] img\');\n                        if (img) bookieName = img.getAttribute(\'alt\') || img.getAttribute(\'title\');\n                    }\n                    if (!bookieName || [\'Oddsportal\', \'Search\'].includes(bookieName)) continue;\n                    \n                    // Handicap (from arguments since it\'s not visible in the expanded row)\n                    let handicap = hc;\n                    \n                    // Odds extractor\n                    const cleanOdd = (container) => {\n                        const p = container.querySelector(\'p.odds-text\') || container.querySelector(\'p\');\n                        if (p) return p.textContent.trim();\n                        return container.textContent.trim(); // fallback\n                    };\n                    \n                    // Odds\n                    const oddContainers = row.querySelectorAll(\'[data-testid="odd-container"]\');\n                    let odds1 = \'-\', odds2 = \'-\';\n                    if (oddContainers.length >= 2) {\n                        // Over is first, Under is second\n                        odds1 = cleanOdd(oddContainers[0]);\n                        odds2 = cleanOdd(oddContainers[1]);\n                    }\n                    \n                    // Payout\n                    let payout = \'-\';\n                    const payoutContainer = row.querySelector(\'[data-testid="payout-container"]\');\n                    if (payoutContainer) payout = payoutContainer.textContent.trim();\n                    else {\n                         // Backup payout finder\n                         const lastChild = row.children[row.children.length - 1];\n                         if (lastChild && lastChild.textContent.includes(\'%\')) {\n                             payout = lastChild.textContent.trim();\n                         }\n                    }\n                    \n                    result.bookies.push({\n                        name: bookieName,\n                        handicap: handicap,\n                        odds1: odds1 || \'-\',\n                        oddsX: \'-\',\n                        odds2: odds2 || \'-\',\n                        payout: payout,\n                    });\n                }\n                \n                return result;\n            }\n        ', target_handicap)
        match_data = MatchOddsData(match_url=match_url)
        match_data.home_team = raw_data.get('homeTeam', 'Unknown')
        match_data.away_team = raw_data.get('awayTeam', 'Unknown')
        clean_hc = raw_data.get('handicap')
        if clean_hc:
            clean_hc = clean_hc.replace('Over/Under', '').replace('O/U', '').replace('+', '').strip()
        for b in raw_data.get('bookies', []):
            match_data.bookie_odds.append(BookieOdds(name=b['name'], odds_1=b['odds1'], odds_x=b['oddsX'], odds_2=b['odds2'], payout=b['payout'], handicap=clean_hc))
        return match_data

    async def _extract_data_asian_handicap(self, page: Page, match_url: str) -> Optional[MatchOddsData]:
        """
            Extracts Asian Handicap market data from the current page.
            Identifies the handicap value, Home (1) and Away (2) odds for each bookmaker.
            """
        logger.info('  🔄 Finding main Asian Handicap line (closest odds)...')
        rows_data = await page.evaluate('\n            () => {\n                const rows = Array.from(document.querySelectorAll(\'div[data-testid="over-under-collapsed-row"]\'));\n                return rows.map((row, index) => {\n                    let odd1 = null;\n                    let odd2 = null;\n                    let handicapText = "";\n                    \n                    const optionBox = row.querySelector(\'div[data-testid="over-under-collapsed-option-box"] p\');\n                    if (optionBox) {\n                        handicapText = optionBox.innerText.trim();\n                    }\n\n                    const containers = row.querySelectorAll(\'.flex-center.border-black-main\');\n                    if (containers.length >= 2) {\n                        odd1 = parseFloat(containers[0].innerText.trim());\n                        odd2 = parseFloat(containers[1].innerText.trim());\n                    }\n                    return { index, handicapText, odd1, odd2 };\n                });\n            }\n        ')
        min_diff = float('inf')
        target_index = -1
        target_handicap = None
        logger.info(f'  📊 Evaluating {len(rows_data)} Asian Handicap rows for the closest odds...')
        for row in rows_data:
            idx = row.get('index')
            hc = row.get('handicapText', 'Unknown')
            odd1 = row.get('odd1')
            odd2 = row.get('odd2')
            if isinstance(odd1, (int, float)) and isinstance(odd2, (int, float)):
                diff = abs(odd1 - odd2)
                if diff < min_diff:
                    min_diff = diff
                    target_index = idx
                    target_handicap = hc
            else:
                pass
        if target_index != -1:
            logger.info(f'  👉 Selecting row {target_index} ({target_handicap}) with min difference {min_diff:.2f}')
            await page.locator('div[data-testid="over-under-collapsed-row"]').nth(target_index).click()
        else:
            logger.warning('⚠️ Could not determine main line Asian Handicap row')
            return None
        await page.wait_for_timeout(1500)
        raw_data = await page.evaluate('\n            (hc) => {\n                const result = {\n                    homeTeam: \'Unknown\',\n                    awayTeam: \'Unknown\',\n                    bookies: [],\n                    handicap: hc\n                };\n                \n                // --- Team Names ---\n                const h1 = document.querySelector(\'h1\');\n                if (h1) {\n                    let h1Text = h1.textContent.trim();\n                    const dashIdx = h1Text.indexOf(\' - \');\n                    if (dashIdx > 0) h1Text = h1Text.substring(0, dashIdx);\n                    const vsSplit = h1Text.split(\' vs \');\n                    if (vsSplit.length >= 2) {\n                        result.homeTeam = vsSplit[0].trim();\n                        result.awayTeam = vsSplit[1].trim();\n                    }\n                }\n                \n                // --- Bookmakers ---\n                const allRows = document.querySelectorAll(\'div[data-testid="over-under-expanded-row"]\');\n                for (const row of allRows) {\n                    let bookieName = null;\n                    const namePara = row.querySelector(\'[data-testid="outrights-expanded-bookmaker-name"]\');\n                    if (namePara) bookieName = namePara.textContent.trim();\n                    if (!bookieName) {\n                        const img = row.querySelector(\'[data-testid="outrights-expanded-bookmaker-logo"] img\');\n                        if (img) bookieName = img.getAttribute(\'alt\') || img.getAttribute(\'title\');\n                    }\n                    if (!bookieName || [\'Oddsportal\', \'Search\'].includes(bookieName)) continue;\n                    \n                    // Handicap (from arguments since it\'s not visible in the expanded row)\n                    let handicap = hc;\n                    \n                    // Odds extractor\n                    const cleanOdd = (container) => {\n                        const p = container.querySelector(\'p.odds-text\') || container.querySelector(\'p\');\n                        if (p) return p.textContent.trim();\n                        return container.textContent.trim(); // fallback\n                    };\n                    \n                    // Odds\n                    const oddContainers = row.querySelectorAll(\'[data-testid="odd-container"]\');\n                    let odds1 = \'-\', odds2 = \'-\';\n                    if (oddContainers.length >= 2) {\n                        // 1 is first, 2 is second\n                        odds1 = cleanOdd(oddContainers[0]);\n                        odds2 = cleanOdd(oddContainers[1]);\n                    }\n                    \n                    // Payout\n                    let payout = \'-\';\n                    const payoutContainer = row.querySelector(\'[data-testid="payout-container"]\');\n                    if (payoutContainer) payout = payoutContainer.textContent.trim();\n                    else {\n                         // Backup payout finder\n                         const lastChild = row.children[row.children.length - 1];\n                         if (lastChild && lastChild.textContent.includes(\'%\')) {\n                             payout = lastChild.textContent.trim();\n                         }\n                    }\n                    \n                    result.bookies.push({\n                        name: bookieName,\n                        handicap: handicap,\n                        odds1: odds1 || \'-\',\n                        oddsX: \'-\',\n                        odds2: odds2 || \'-\',\n                        payout: payout,\n                    });\n                }\n                \n                return result;\n            }\n        ', target_handicap)
        match_data = MatchOddsData(match_url=match_url)
        match_data.home_team = raw_data.get('homeTeam', 'Unknown')
        match_data.away_team = raw_data.get('awayTeam', 'Unknown')
        clean_hc = raw_data.get('handicap')
        if clean_hc:
            clean_hc = clean_hc.replace('Asian Handicap', '').replace('AH', '').strip()
        for b in raw_data.get('bookies', []):
            match_data.bookie_odds.append(BookieOdds(name=b['name'], odds_1=b['odds1'], odds_x=b['oddsX'], odds_2=b['odds2'], payout=b['payout'], handicap=clean_hc))
        return match_data
