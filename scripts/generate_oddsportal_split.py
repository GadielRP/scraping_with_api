from __future__ import annotations

import ast
from pathlib import Path
import textwrap


ROOT = Path(r"C:/Users/gadie/Documents/projects/sofascore")
SRC_PATH = ROOT / "oddsportal_scraper.py"
OUT = ROOT / "modules" / "oddsportal"


def read_source() -> str:
    return SRC_PATH.read_text(encoding="utf-8", errors="replace")


def get_class(source: str, name: str) -> ast.ClassDef:
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise RuntimeError(f"class {name} not found")


def get_functions(source: str) -> dict[str, ast.AST]:
    tree = ast.parse(source)
    result: dict[str, ast.AST] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            result[node.name] = node
    return result


def segment(source: str, node: ast.AST) -> str:
    text = ast.get_source_segment(source, node)
    if text is None:
        raise RuntimeError(f"missing source for {getattr(node, 'name', node)}")
    return text


def write(path: Path, content: str) -> None:
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def render_module(
    *,
    class_name: str,
    module_doc: str,
    imports: list[str],
    method_sources: list[str],
) -> str:
    body = "\n\n".join(textwrap.dedent(src).rstrip() for src in method_sources if src.strip())
    imports_text = "\n".join(imports)
    return f'{module_doc}\n\n{imports_text}\n\n\nclass {class_name}:\n' + textwrap.indent(body, "    ") + "\n"


def main() -> None:
    source = read_source()
    cls = get_class(source, "OddsPortalScraper")
    funcs = get_functions(source)

    # Shared heavy import block for scraper mixins.
    heavy_imports = [
        "import asyncio",
        "import logging",
        "import os",
        "import random",
        "import re",
        "import time",
        "from collections import deque",
        "from contextlib import contextmanager",
        "from dataclasses import dataclass, field",
        "from datetime import date, datetime, timedelta",
        "from threading import Condition, local",
        "from typing import Any, Dict, List, Optional, Tuple",
        "from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit",
        "",
        "from playwright.async_api import async_playwright, Page, Browser, BrowserContext",
        "from infrastructure.network import ProxyIdentityManager",
        "from infrastructure.settings import Config",
        "from .oddsportal_config import (",
        "    SEASON_ODDSPORTAL_MAP, BOOKIE_ALIASES, TEAM_ALIASES, PRIORITY_BOOKIES,",
        "    OP_GROUPS, OP_GROUPS_DISPLAY, OP_PERIODS, SPORT_SCRAPING_ROUTES,",
        "    build_op_fragment, build_match_url_with_fragment, flatten_sport_scraping_route,",
        "    INSTITUTIONAL_NOISE, get_oddsportal_current_date,",
        ")",
        "from .team_matcher import TeamMatcher",
        "from .dataclasses import (",
        "    CacheQualityMetrics, BookieOdds, BetfairExchangeOdds, MarketExtraction,",
        "    MatchOddsData, ScrapeAttemptResult, GroupSeedResult,",
        ")",
        "from .cache_utils import (",
        "    DEBUG_TIMING, ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS, ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS,",
        "    ODDSPORTAL_SESSION_RESTART_ATTEMPTS, EN_DASH, TEAM_SEPARATOR_PATTERN,",
        "    LEGACY_CACHE_MATCH_PATTERN, TEAM_PREFIX_CLEAN_PATTERN, ODDSPORTAL_CACHE_DATE_FORMATS,",
        "    ODDSPORTAL_RELATIVE_DATE_OFFSETS, log_timing, _normalize_league_url,",
        "    _build_league_group_key, _normalize_cache_date, _build_structured_league_cache,",
        "    _coerce_current_date, _parse_oddsportal_cache_date, _is_cache_date_current_or_future,",
        "    _calculate_cache_homogeneity, _evaluate_cache_quality, _format_group_key,",
        ")",
        "from .logging_context import _LOG_CONTEXT, _OddsPortalLogPrefixFilter, _log_prefix",
        "",
        "logger = logging.getLogger(__name__)",
    ]

    def class_methods(names: list[str]) -> list[str]:
        return [segment(source, item) for item in cls.body if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name in names]

    browser_methods = [
        "__init__",
        "_should_rotate_proxy_on_browser_restart",
        "start",
        "_intercept_route",
        "_build_context_options",
        "_get_evasion_init_script",
        "_install_context_features",
        "_create_fresh_context",
        "stop",
        "_goto_fresh",
        "_should_force_fresh_request",
        "_build_cache_busted_request_url",
        "_build_no_cache_request_headers",
        "_clear_browser_state",
        "_normalize_base_match_url",
    ]
    state_methods = [
        "_remove_interaction_overlays",
        "_has_market_content",
        "_get_active_period_labels",
        "_is_target_period_active",
        "_get_active_group_label",
        "_is_target_group_active",
        "_collect_match_page_state",
        "_classify_match_page_state",
        "_format_page_state_summary",
        "_classify_goto_exception",
        "_save_debug_artifacts",
    ]
    resume_methods = [
        "_make_initial_resume_state",
        "_normalize_resume_state",
        "_mark_step_completed",
        "_mark_step_failed",
        "_restore_partial_match_data",
        "_ensure_legacy_match_level_fields",
        "_log_structured_recap",
        "_resume_state_for_debug",
    ]
    lookup_methods = [
        "_load_cached_candidates",
        "_extract_league_candidates",
        "find_match_url",
        "find_match_url_from_cache",
    ]
    render_methods = [
        "_wait_for_market_render",
        "_click_period_tab",
        "_click_market_group_tab",
    ]
    hover_basic_methods = [
        "_parse_opening_odds_from_modal_html",
        "_dismiss_odds_movement_tooltip",
        "_wait_for_scoped_tooltip_html",
        "_get_hover_target_from_container",
        "_find_bookie_row",
    ]
    hover_odds_methods = [
        "_extract_opening_odds_for_bookie",
        "_extract_opening_odds_betfair",
    ]
    data_methods = [
        "_extract_data",
        "_extract_data_over_under",
        "_extract_data_asian_handicap",
    ]

    write(
        OUT / "scraper_browser.py",
        render_module(
            class_name="OddsPortalBrowserMixin",
            module_doc='"""Browser lifecycle and context management for OddsPortal."""',
            imports=heavy_imports,
            method_sources=class_methods(browser_methods),
        ),
    )
    write(
        OUT / "scraper_page_state.py",
        render_module(
            class_name="OddsPortalPageStateMixin",
            module_doc='"""Page state, diagnostics, and debug artifact helpers."""',
            imports=heavy_imports,
            method_sources=class_methods(state_methods),
        ),
    )
    write(
        OUT / "scraper_resume.py",
        render_module(
            class_name="OddsPortalResumeMixin",
            module_doc='"""Resume-state helpers for OddsPortal scrapes."""',
            imports=heavy_imports,
            method_sources=class_methods(resume_methods),
        ),
    )
    write(
        OUT / "scraper_lookup.py",
        render_module(
            class_name="OddsPortalLookupMixin",
            module_doc='"""League cache resolution and match lookup helpers."""',
            imports=heavy_imports,
            method_sources=class_methods(lookup_methods),
        ),
    )
    write(
        OUT / "scraper_render.py",
        render_module(
            class_name="OddsPortalRenderMixin",
            module_doc='"""Market render waits and tab switching helpers."""',
            imports=heavy_imports,
            method_sources=class_methods(render_methods),
        ),
    )
    write(
        OUT / "scraper_hover_basic.py",
        render_module(
            class_name="OddsPortalHoverBasicMixin",
            module_doc='"""Tooltip discovery and hover-target lookup helpers."""',
            imports=heavy_imports,
            method_sources=class_methods(hover_basic_methods),
        ),
    )
    write(
        OUT / "scraper_hover_odds.py",
        render_module(
            class_name="OddsPortalHoverOddsMixin",
            module_doc='"""Opening odds hover extraction helpers."""',
            imports=heavy_imports,
            method_sources=class_methods(hover_odds_methods),
        ),
    )
    write(
        OUT / "scraper_data.py",
        render_module(
            class_name="OddsPortalDataMixin",
            module_doc='"""DOM extraction helpers for OddsPortal market tables."""',
            imports=heavy_imports,
            method_sources=class_methods(data_methods),
        ),
    )

    scrape_attempt = funcs["scrape_match_attempt"]
    scrape_attempt_src = segment(source, scrape_attempt)
    marker = "completed_step_keys = set(normalized_resume_state.get(\"completed_step_keys\", []))"
    if marker not in scrape_attempt_src:
        raise RuntimeError("split marker not found in scrape_match_attempt")
    pre_src, post_src = scrape_attempt_src.split(marker, 1)

    attempt_imports = heavy_imports + [
        "from .scraper_browser import OddsPortalBrowserMixin",
        "from .scraper_page_state import OddsPortalPageStateMixin",
        "from .scraper_resume import OddsPortalResumeMixin",
        "from .scraper_lookup import OddsPortalLookupMixin",
        "from .scraper_render import OddsPortalRenderMixin",
        "from .scraper_hover_basic import OddsPortalHoverBasicMixin",
        "from .scraper_hover_odds import OddsPortalHoverOddsMixin",
        "from .scraper_data import OddsPortalDataMixin",
    ]

    pre_body = textwrap.dedent(pre_src).rstrip() + "\n\n" + textwrap.indent(
        textwrap.dedent(
            """
            return {
                "page": page,
                "fresh_context": fresh_context,
                "previous_context": previous_context,
                "match_data": match_data,
                "normalized_resume_state": normalized_resume_state,
                "route_steps": route_steps,
                "route_step_count": route_step_count,
                "start_step_idx": start_step_idx,
                "match_url": match_url,
                "initial_url": initial_url,
                "t0": t0,
                "route_step_count": route_step_count,
            }
            """
        ).strip(),
        "    ",
    )

    post_header = textwrap.dedent(
        """
        async def _run_scrape_match_attempt_steps(self, ctx: Dict[str, Any]) -> ScrapeAttemptResult:
            page = ctx["page"]
            fresh_context = ctx["fresh_context"]
            previous_context = ctx["previous_context"]
            match_data = ctx["match_data"]
            normalized_resume_state = ctx["normalized_resume_state"]
            route_steps = ctx["route_steps"]
            route_step_count = ctx["route_step_count"]
            start_step_idx = ctx["start_step_idx"]
            match_url = ctx["match_url"]
            initial_url = ctx["initial_url"]
            t0 = ctx["t0"]
        """
    ).rstrip()
    post_body = post_header + "\n" + textwrap.indent(textwrap.dedent(marker + post_src).rstrip(), "    ")

    attempt_mixin = f'''"""Attempt orchestration for OddsPortal scrapes."""\n\n{"\n".join(attempt_imports)}\n\n\nclass OddsPortalAttemptMixin:\n    async def scrape_match(self, match_url: str, sport: str = None, clear_state: bool = False) -> Optional[MatchOddsData]:\n        attempt = await self.scrape_match_attempt(match_url, sport=sport, clear_state=clear_state)\n        return attempt.data\n\n    async def scrape_match_attempt(\n        self,\n        match_url: str,\n        sport: str = None,\n        clear_state: bool = False,\n        resume_state: Optional[Dict[str, Any]] = None,\n        partial_match_data: Optional[MatchOddsData] = None,\n    ) -> ScrapeAttemptResult:\n        ctx = await self._prepare_scrape_match_attempt(\n            match_url,\n            sport=sport,\n            clear_state=clear_state,\n            resume_state=resume_state,\n            partial_match_data=partial_match_data,\n        )\n        if isinstance(ctx, ScrapeAttemptResult):\n            return ctx\n        return await self._run_scrape_match_attempt_steps(ctx)\n\n    async def _prepare_scrape_match_attempt(\n        self,\n        match_url: str,\n        sport: str = None,\n        clear_state: bool = False,\n        resume_state: Optional[Dict[str, Any]] = None,\n        partial_match_data: Optional[MatchOddsData] = None,\n    ) -> ScrapeAttemptResult | Dict[str, Any]:\n{ textwrap.indent(textwrap.dedent(pre_src).rstrip() + "\n\n" + textwrap.indent(textwrap.dedent("return {"), "        ") + "\n" + textwrap.indent(textwrap.dedent('''\n            "page": page,\n            "fresh_context": fresh_context,\n            "previous_context": previous_context,\n            "match_data": match_data,\n            "normalized_resume_state": normalized_resume_state,\n            "route_steps": route_steps,\n            "route_step_count": route_step_count,\n            "start_step_idx": start_step_idx,\n            "match_url": match_url,\n            "initial_url": initial_url,\n            "t0": t0,\n        }''').strip(), "        "), "        ") }\n\n{ textwrap.indent(post_body, "    ") }\n'''\n+\n+    # The f-string above is intentionally large; it is assembled from the existing source.\n+    write(OUT / "scraper_attempt.py", attempt_mixin)\n+\n+    impl = textwrap.dedent(\n+        '''\n+        """Concrete OddsPortal scraper implementation composed from mixins."""\n+\n+        from .scraper_browser import OddsPortalBrowserMixin\n+        from .scraper_page_state import OddsPortalPageStateMixin\n+        from .scraper_resume import OddsPortalResumeMixin\n+        from .scraper_lookup import OddsPortalLookupMixin\n+        from .scraper_render import OddsPortalRenderMixin\n+        from .scraper_hover_basic import OddsPortalHoverBasicMixin\n+        from .scraper_hover_odds import OddsPortalHoverOddsMixin\n+        from .scraper_data import OddsPortalDataMixin\n+        from .scraper_attempt import OddsPortalAttemptMixin\n+\n+\n+        class OddsPortalScraper(\n+            OddsPortalAttemptMixin,\n+            OddsPortalBrowserMixin,\n+            OddsPortalPageStateMixin,\n+            OddsPortalResumeMixin,\n+            OddsPortalLookupMixin,\n+            OddsPortalRenderMixin,\n+            OddsPortalHoverBasicMixin,\n+            OddsPortalHoverOddsMixin,\n+            OddsPortalDataMixin,\n+        ):\n+            pass\n+        '''\n+    )\n+    write(OUT / "scraper_impl.py", impl)\n+\n+    core_proxy = textwrap.dedent(\n+        '''\n+        """Lazy compatibility wrapper that instantiates the real OddsPortal scraper on demand."""\n+\n+\n+        class OddsPortalScraper:\n+            def __new__(cls, *args, **kwargs):\n+                from .scraper_impl import OddsPortalScraper as Impl\n+\n+                return Impl(*args, **kwargs)\n+        '''\n+    )\n+    write(OUT / "oddsportal_scraper_core.py", core_proxy)\n+\n+    dispatcher_src = []\n+    for name in [\n+        "get_scaler",\n+        "scrape_match_odds",\n+        "scrape_match_sync",\n+        "_resolve_task_sport",\n+        "_resolve_task_match_url",\n+        "_scrape_task_with_recovery",\n+        "_seed_group_cache_only",\n+        "_build_dispatch_groups",\n+        "_attach_cached_match_urls",\n+        "scrape_multiple_matches_sync",\n+        "scrape_multiple_matches_parallel_sync",\n+    ]:\n+        dispatcher_src.append(segment(source, funcs[name]))\n+\n+    dispatcher_imports = [\n+        '\"\"\"OddsPortal batch/sync dispatch helpers.\"\"\"',\n+        \"\",\n+        \"from collections import deque\",\n+        \"from concurrent.futures import ThreadPoolExecutor\",\n+        \"from threading import Condition\",\n+        \"from typing import Any, Dict, List, Optional, Tuple\",\n+        \"\",\n+        \"from .oddsportal_scraper_core import OddsPortalScraper\",\n+        \"from .dataclasses import MatchOddsData, ScrapeAttemptResult, GroupSeedResult\",\n+        \"from .oddsportal_config import SEASON_ODDSPORTAL_MAP\",\n+        \"from .cache_utils import _coerce_current_date, _build_league_group_key, _format_group_key\",\n+        \"from .logging_context import _log_prefix\",\n+        \"\",\n+    ]\n+    write(\n+        OUT / \"oddsportal_dispatcher.py\",\n+        \"\\n\".join(dispatcher_imports) + \"\\n\\n\" + \"\\n\\n\".join(textwrap.dedent(s).rstrip() for s in dispatcher_src) + \"\\n\",\n+    )\n+\n+    root_shim = textwrap.dedent(\n+        '''\n+        \"\"\"Compatibility shim for the refactored OddsPortal package.\"\"\"\n+\n+        from modules.oddsportal import *  # noqa: F401,F403\n+        '''\n+    )\n+    write(SRC_PATH, root_shim)\n+\n+\n+if __name__ == \"__main__\":\n+    main()\n*** End Patch
