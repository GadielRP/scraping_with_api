"""OddsPortal movement-tooltip interaction and parsing."""

from __future__ import annotations

import asyncio
import html
import logging
import os
import re
import time
from typing import Dict, Optional, Tuple

from shared.odds_utils import normalize_odds_value

logger = logging.getLogger(__name__)


class OddsPortalHoverMixin:
    def _parse_opening_odds_tooltip_html(
        self,
        modal_html: str,
    ) -> Optional[Tuple[str, Optional[str]]]:
        """Parse the dedicated opening-odds block from tooltip HTML.

        Movement-history rows are deliberately ignored. The localized
        ``Opening odds`` block is authoritative for ``initialOdds``. Its date
        is returned when the provider includes one, regardless of CSS class.
        Decimal and fractional prices are normalized to decimal.
        """
        if not modal_html:
            return None

        heading_match = re.search(
            r"<h3\b[^>]*>\s*(?:Odds movement|Movimiento de cuotas)\s*</h3>",
            modal_html,
            flags=re.IGNORECASE,
        )
        if not heading_match:
            return None

        tooltip_body = modal_html[heading_match.end():]
        opening_label = re.search(
            r"Opening odds|Cuotas de apertura|Cuotas iniciales",
            tooltip_body,
            flags=re.IGNORECASE,
        )
        if not opening_label:
            return None
        opening_html = tooltip_body[opening_label.end():]

        opening_text = html.unescape(re.sub(r"<[^>]+>", " ", opening_html))
        opening_time_match = re.search(
            r"\b\d{1,2}\s+[^\s,<]{3,12},\s*\d{2}:\d{2}\b",
            opening_text,
        )
        opening_time = (
            opening_time_match.group(0).strip()
            if opening_time_match
            else None
        )

        bold_values = re.findall(
            r"<div\b[^>]*class=[\"'][^\"']*\bfont-bold\b[^\"']*[\"'][^>]*>(.*?)</div>",
            opening_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        for raw_value in bold_values:
            value_text = html.unescape(
                re.sub(r"<[^>]+>", "", raw_value)
            ).strip()
            normalized_value = normalize_odds_value(value_text)
            if normalized_value is not None:
                return normalized_value, opening_time

        return None

    async def _extract_opening_odds_by_hover(
        self,
        page,
        *,
        source: str,
        bookie_name: Optional[str] = None,
    ) -> Optional[Dict[str, Tuple[str, Optional[str]]]]:
        """Hover regular or Betfair cells and return their opening odds.

        ``source`` is either ``bookmaker`` or ``betfair``. Target selection is
        data-driven inside this single interaction boundary so both layouts use
        identical cleanup, retry, scoped-tooltip, localization, and parsing
        behavior.
        """
        if source not in {"bookmaker", "betfair"}:
            raise ValueError(f"Unsupported OddsPortal hover source: {source}")
        if source == "bookmaker" and not bookie_name:
            raise ValueError("bookie_name is required for bookmaker hover")

        await page.wait_for_timeout(500)
        await page.evaluate(
            """() => {
                document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());
            }"""
        )

        if source == "betfair":
            initial_scope = await page.query_selector(
                "div[data-testid='betting-exchanges-section']"
            )
            if not initial_scope:
                logger.warning("Betfair Exchange section not found for hover extraction")
                return None
            initial_containers = await initial_scope.query_selector_all(
                "div[data-testid='odd-container']"
            )
            if len(initial_containers) >= 6:
                choice_indexes = {
                    "back_1": 0,
                    "back_x": 1,
                    "back_2": 2,
                    "lay_1": 3,
                    "lay_x": 4,
                    "lay_2": 5,
                }
            elif len(initial_containers) >= 4:
                choice_indexes = {
                    "back_1": 0,
                    "back_2": 1,
                    "lay_1": 2,
                    "lay_2": 3,
                }
            else:
                logger.warning(
                    "Unexpected Betfair container count: %s",
                    len(initial_containers),
                )
                return None
        else:
            initial_scope = None
            rows = await page.query_selector_all("div.border-black-borders.flex.h-9")
            if not rows:
                rows = await page.query_selector_all("div.border-black-borders.flex")
            for row in rows:
                link = await row.query_selector("a[title]")
                image = await row.query_selector("img[alt]")
                link_title = await link.get_attribute("title") if link else ""
                image_alt = await image.get_attribute("alt") if image else ""
                if (
                    bookie_name.lower() in (link_title or "").lower()
                    or bookie_name.lower() in (image_alt or "").lower()
                ):
                    initial_scope = row
                    break
            if not initial_scope:
                logger.warning("Bookie row not found for: %s", bookie_name)
                return None
            initial_containers = await initial_scope.query_selector_all(
                "div[data-testid='odd-container']"
            )
            if not initial_containers:
                logger.warning("No odd containers found for: %s", bookie_name)
                return None
            choice_keys = ["1", "X", "2"] if len(initial_containers) >= 3 else ["1", "2"]
            choice_indexes = {choice: idx for idx, choice in enumerate(choice_keys)}

        logger.info(
            "Hovering %s OddsPortal cells for %s",
            len(choice_indexes),
            "Betfair Exchange" if source == "betfair" else bookie_name,
        )
        results: Dict[str, Tuple[str, Optional[str]]] = {}

        for choice, configured_index in choice_indexes.items():
            started_at = time.perf_counter()
            await page.mouse.move(0, 0)
            await page.wait_for_timeout(300)

            for attempt in range(3):
                try:
                    if source == "betfair":
                        scope = await page.query_selector(
                            "div[data-testid='betting-exchanges-section']"
                        )
                        if not scope:
                            await asyncio.sleep(0.4)
                            continue
                        containers = await scope.query_selector_all(
                            "div[data-testid='odd-container']"
                        )
                        if len(containers) >= 6:
                            live_indexes = {
                                "back_1": 0,
                                "back_x": 1,
                                "back_2": 2,
                                "lay_1": 3,
                                "lay_x": 4,
                                "lay_2": 5,
                            }
                        elif len(containers) >= 4:
                            live_indexes = {
                                "back_1": 0,
                                "back_2": 1,
                                "lay_1": 2,
                                "lay_2": 3,
                            }
                        else:
                            await asyncio.sleep(0.4)
                            continue
                        container_index = live_indexes.get(choice)
                        if container_index is None:
                            break
                    else:
                        scope = None
                        rows = await page.query_selector_all(
                            "div.border-black-borders.flex.h-9"
                        )
                        if not rows:
                            rows = await page.query_selector_all(
                                "div.border-black-borders.flex"
                            )
                        for row in rows:
                            link = await row.query_selector("a[title]")
                            image = await row.query_selector("img[alt]")
                            link_title = await link.get_attribute("title") if link else ""
                            image_alt = await image.get_attribute("alt") if image else ""
                            if (
                                bookie_name.lower() in (link_title or "").lower()
                                or bookie_name.lower() in (image_alt or "").lower()
                            ):
                                scope = row
                                break
                        if not scope:
                            await asyncio.sleep(0.4)
                            continue
                        containers = await scope.query_selector_all(
                            "div[data-testid='odd-container']"
                        )
                        container_index = configured_index

                    if container_index >= len(containers):
                        await asyncio.sleep(0.4)
                        continue
                    current_container = containers[container_index]
                    if not await current_container.bounding_box():
                        await asyncio.sleep(0.4)
                        continue

                    hover_target = await current_container.query_selector(
                        "div.flex-center.flex-col.font-bold"
                    )
                    hover_target = hover_target or current_container
                    await hover_target.scroll_into_view_if_needed()
                    await page.evaluate("window.scrollBy(0, -150)")
                    await page.wait_for_timeout(200)
                    await page.evaluate(
                        """() => {
                            document.querySelectorAll('.overlay-bookie-modal').forEach(el => el.remove());
                            const consent = document.getElementById('onetrust-banner-sdk');
                            if (consent) consent.remove();
                            const shade = document.querySelector('.onetrust-pc-dark-filter');
                            if (shade) shade.remove();
                        }"""
                    )

                    box = await hover_target.bounding_box()
                    if box:
                        center_x = box["x"] + box["width"] / 2
                        center_y = box["y"] + box["height"] / 2
                        await page.mouse.move(center_x - 15, center_y - 15)
                        await page.wait_for_timeout(50)
                        await page.mouse.move(center_x, center_y)
                        await page.wait_for_timeout(50)
                    try:
                        await hover_target.hover(force=True, timeout=2000)
                    except Exception:
                        pass
                    await page.evaluate(
                        """el => {
                            el.dispatchEvent(new PointerEvent('pointerover', {bubbles: true, cancelable: true, pointerId: 1}));
                            el.dispatchEvent(new PointerEvent('pointerenter', {bubbles: true, cancelable: true, pointerId: 1}));
                            el.dispatchEvent(new MouseEvent('mouseover', {bubbles: true, cancelable: true}));
                            el.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true, cancelable: true}));
                            el.dispatchEvent(new MouseEvent('mousemove', {bubbles: true, cancelable: true}));
                        }""",
                        hover_target,
                    )

                    timeout_ms = 3000 + attempt * 1000
                    deadline = time.monotonic() + timeout_ms / 1000
                    tooltip_html = None
                    while time.monotonic() < deadline:
                        headings = await current_container.query_selector_all("h3")
                        for heading in headings:
                            heading_text = (await heading.text_content() or "").strip().lower()
                            if heading_text not in {
                                "odds movement",
                                "movimiento de cuotas",
                            }:
                                continue
                            if not await heading.is_visible():
                                continue
                            wrapper_handle = await heading.evaluate_handle(
                                "node => node.parentElement"
                            )
                            wrapper = wrapper_handle.as_element()
                            if wrapper:
                                tooltip_html = await wrapper.inner_html()
                            break
                        if tooltip_html:
                            break
                        await page.wait_for_timeout(100)

                    if not tooltip_html:
                        await page.mouse.move(0, 0)
                        await page.wait_for_timeout(300)
                        continue

                    if getattr(self, "testing_mode", False) and getattr(self, "debug_dir", None):
                        label = (
                            f"Betfair_{choice}"
                            if source == "betfair"
                            else f"{bookie_name}_{choice}"
                        )
                        safe_label = "".join(
                            char if char.isalnum() or char in "._-" else "_"
                            for char in label
                        )
                        os.makedirs(self.debug_dir, exist_ok=True)
                        with open(
                            os.path.join(self.debug_dir, f"modal_{safe_label}.html"),
                            "w",
                            encoding="utf-8",
                        ) as debug_file:
                            debug_file.write(tooltip_html)

                    parsed = self._parse_opening_odds_tooltip_html(tooltip_html)
                    if parsed:
                        results[choice] = parsed
                    await page.mouse.move(0, 0)
                    await page.wait_for_timeout(300)
                    break
                except Exception as exc:
                    logger.debug(
                        "Hover failed source=%s choice=%s attempt=%s: %s",
                        source,
                        choice,
                        attempt + 1,
                        exc,
                    )
                    await page.mouse.move(0, 0)
                    await page.wait_for_timeout(300)

            logger.debug(
                "OddsPortal hover source=%s choice=%s success=%s duration_s=%.2f",
                source,
                choice,
                choice in results,
                time.perf_counter() - started_at,
            )

        return results or None
