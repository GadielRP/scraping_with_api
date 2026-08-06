"""OddsPortal movement-tooltip interaction and parsing."""

from __future__ import annotations

import asyncio
import html
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from shared.odds_utils import normalize_odds_value
from .timestamps import parse_oddsportal_tooltip_time

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TooltipOddsSnapshot:
    """Opening and latest timestamped prices parsed from one tooltip."""

    opening_odds: Optional[str] = None
    opening_time: Optional[str] = None
    current_odds: Optional[str] = None
    current_time: Optional[str] = None
    movement_odds: Tuple[str, ...] = field(
        default=(),
        compare=False,
        repr=False,
    )


class OddsPortalHoverMixin:
    _TOOLTIP_TIME_PATTERN = re.compile(
        r"\b\d{1,2}\s+[^\s,<]{3,12},\s*\d{2}:\d{2}\b",
        flags=re.IGNORECASE,
    )

    @staticmethod
    def _classed_div_values(fragment: str, class_name: str) -> list[str]:
        """Return text from divs containing one exact CSS class token."""

        pattern = re.compile(
            rf"<div\b[^>]*class=[\"'][^\"']*\b{re.escape(class_name)}\b[^\"']*[\"'][^>]*>(.*?)</div>",
            flags=re.IGNORECASE | re.DOTALL,
        )
        return [
            html.unescape(re.sub(r"<[^>]+>", "", value)).strip()
            for value in pattern.findall(fragment)
        ]

    @staticmethod
    def _history_div_tokens(fragment: str) -> list[Tuple[int, set[str], str]]:
        """Extract ordered leaf-like div tokens without building a DOM tree."""

        token_pattern = re.compile(
            r"<div\b[^>]*class=[\"']([^\"']*)[\"'][^>]*>\s*([^<]*)",
            flags=re.IGNORECASE,
        )
        return [
            (
                match.start(),
                set(match.group(1).split()),
                html.unescape(match.group(2)).strip(),
            )
            for match in token_pattern.finditer(fragment)
        ]

    def _movement_entries(self, history_html: str) -> list[Tuple[str, str]]:
        """Pair movement times and prices for row- or column-based tooltips."""

        tokens = self._history_div_tokens(history_html)
        dated_tokens = [
            token
            for token in tokens
            if self._TOOLTIP_TIME_PATTERN.fullmatch(token[2])
        ]
        if not dated_tokens:
            return []

        # Exchange tooltips may render one flex row per movement. In that
        # layout, the quote occurs after its date and before the next date.
        row_entries: list[Tuple[str, str]] = []
        complete_row_layout = True
        for index, (date_position, _, source_time) in enumerate(dated_tokens):
            next_date_position = (
                dated_tokens[index + 1][0]
                if index + 1 < len(dated_tokens)
                else len(history_html)
            )
            odds_value = next(
                (
                    normalized
                    for position, classes, value in tokens
                    if date_position < position < next_date_position
                    and "font-bold" in classes
                    and (normalized := normalize_odds_value(value)) is not None
                ),
                None,
            )
            if odds_value is None:
                complete_row_layout = False
                break
            row_entries.append((source_time, odds_value))
        if complete_row_layout:
            return row_entries

        # Regular bookmaker tooltips render parallel columns: every timestamp,
        # then every quote, then movement deltas. Preserve placeholders by
        # position so one unavailable price cannot shift subsequent pairings.
        last_date_position = dated_tokens[-1][0]
        raw_odds = [
            value
            for position, classes, value in tokens
            if position > last_date_position and "font-bold" in classes
        ][:len(dated_tokens)]
        return [
            (source_time, odds_value)
            for (_, _, source_time), raw_odds_value in zip(dated_tokens, raw_odds)
            if (odds_value := normalize_odds_value(raw_odds_value)) is not None
        ]

    def _parse_odds_tooltip_html(
        self,
        modal_html: str,
    ) -> Optional[TooltipOddsSnapshot]:
        """Parse opening and latest timestamped odds from one hover tooltip.

        OddsPortal can render a stale visible cell before its movement tooltip
        is refreshed. The current value is therefore selected from the
        timestamped movement entries, not from the first displayed entry.
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
        history_html = (
            tooltip_body[:opening_label.start()]
            if opening_label
            else tooltip_body
        )
        opening_html = (
            tooltip_body[opening_label.end():]
            if opening_label
            else ""
        )

        timestamped_prices = []
        for source_time, odds_value in self._movement_entries(history_html):
            sort_key = parse_oddsportal_tooltip_time(source_time)
            if sort_key is not None:
                timestamped_prices.append((sort_key, source_time, odds_value))

        current_odds = None
        current_time = None
        if timestamped_prices:
            _, current_time, current_odds = max(
                timestamped_prices,
                key=lambda entry: entry[0],
            )

        opening_time_match = self._TOOLTIP_TIME_PATTERN.search(
            html.unescape(re.sub(r"<[^>]+>", " ", opening_html))
        )
        opening_time = (
            opening_time_match.group(0).strip()
            if opening_time_match
            else None
        )
        opening_odds = next(
            (
                normalized
                for value in self._classed_div_values(opening_html, "font-bold")
                if (normalized := normalize_odds_value(value)) is not None
            ),
            None,
        )

        if opening_odds is None and current_odds is None:
            return None
        return TooltipOddsSnapshot(
            opening_odds=opening_odds,
            opening_time=opening_time,
            current_odds=current_odds,
            current_time=current_time,
            movement_odds=tuple(entry[2] for entry in timestamped_prices),
        )

    @staticmethod
    async def _visible_container_odds(container) -> Optional[str]:
        """Read the legacy visible quote without entering tooltip descendants."""

        raw_value = await container.evaluate(
            r"""element => {
                const candidates = element.querySelectorAll('a.odds-link, p');
                const oddsPattern = /^(?:\d+(?:[.,]\d+)?|\d+\s*\/\s*\d+)$/;
                for (const candidate of candidates) {
                    if (candidate.closest('.tooltip, .odds-tooltip')) continue;
                    const value = (candidate.textContent || '').trim();
                    if (oddsPattern.test(value)) return value;
                }
                return null;
            }"""
        )
        return normalize_odds_value(raw_value)

    @staticmethod
    def _tooltip_matches_visible_cell(
        parsed: TooltipOddsSnapshot,
        visible_odds: Optional[str],
    ) -> bool:
        """Reject a stale tooltip rendered for a different odds cell."""

        if visible_odds is None or not parsed.movement_odds:
            return True
        return visible_odds in parsed.movement_odds

    @staticmethod
    def _debug_filename_component(value: str) -> str:
        normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or ""))
        return normalized.strip("_").lower() or "unknown"

    def _save_parsed_tooltip_html(
        self,
        *,
        tooltip_html: str,
        source: str,
        bookie_name: Optional[str],
        choice: str,
    ) -> Optional[str]:
        """Save one accepted tooltip under its current event debug context."""

        if not getattr(self, "debug_mode", False):
            return None
        debug_root = getattr(self, "_debug_root_dir", None)
        event_id = getattr(self, "_debug_event_id", None)
        if not debug_root:
            return None

        if event_id is not None:
            target_dir = os.path.join(
                debug_root,
                f"oddsportal_{event_id}_tooltips",
            )
        else:
            # Compatibility for manual scraper callers without an event ID.
            target_dir = getattr(self, "debug_dir", None) or debug_root

        bookmaker_label = (
            "Betfair Exchange" if source == "betfair" else (bookie_name or source)
        )
        filename = (
            f"{self._debug_filename_component(bookmaker_label)}_"
            f"{self._debug_filename_component(choice)}_tooltip.html"
        )
        filepath = os.path.join(target_dir, filename)
        try:
            os.makedirs(target_dir, exist_ok=True)
            with open(filepath, "w", encoding="utf-8") as debug_file:
                debug_file.write(tooltip_html)
        except OSError as exc:
            logger.warning(
                "Failed to save parsed OddsPortal tooltip %s: %s",
                filepath,
                exc,
            )
            return None
        logger.info(
            "Saved parsed OddsPortal tooltip event=%s source=%s choice=%s path=%s",
            event_id,
            bookmaker_label,
            choice,
            filepath,
        )
        return filepath

    def _parse_opening_odds_tooltip_html(
        self,
        modal_html: str,
    ) -> Optional[Tuple[str, Optional[str]]]:
        """Compatibility view of the dedicated opening-odds block.

        New ingestion code uses :meth:`_parse_odds_tooltip_html` so the same
        parsed tooltip also supplies the real timestamped current price.
        """
        parsed = self._parse_odds_tooltip_html(modal_html)
        if parsed is None or parsed.opening_odds is None:
            return None
        return parsed.opening_odds, parsed.opening_time

    async def _extract_tooltip_odds_by_hover(
        self,
        page,
        *,
        source: str,
        bookie_name: Optional[str] = None,
    ) -> Optional[Dict[str, TooltipOddsSnapshot]]:
        """Hover each regular or Betfair cell once and parse both price states.

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
        results: Dict[str, TooltipOddsSnapshot] = {}

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
                    visible_odds = await self._visible_container_odds(
                        current_container
                    )

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

                    parsed = self._parse_odds_tooltip_html(tooltip_html)
                    if parsed and not self._tooltip_matches_visible_cell(
                        parsed,
                        visible_odds,
                    ):
                        logger.warning(
                            "Discarding mismatched OddsPortal tooltip "
                            "source=%s choice=%s visible=%s history=%s attempt=%s",
                            source,
                            choice,
                            visible_odds,
                            parsed.movement_odds,
                            attempt + 1,
                        )
                        await page.mouse.move(0, 0)
                        await page.wait_for_timeout(300)
                        continue
                    if parsed:
                        results[choice] = parsed
                        self._save_parsed_tooltip_html(
                            tooltip_html=tooltip_html,
                            source=source,
                            bookie_name=bookie_name,
                            choice=choice,
                        )
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

    async def _extract_opening_odds_by_hover(
        self,
        page,
        *,
        source: str,
        bookie_name: Optional[str] = None,
    ) -> Optional[Dict[str, Tuple[str, Optional[str]]]]:
        """Legacy opening-only API retained for compatibility.

        Production ingestion calls ``_extract_tooltip_odds_by_hover`` directly;
        this wrapper does not participate in that flow and therefore does not
        add a second hover action.
        """

        parsed = await self._extract_tooltip_odds_by_hover(
            page,
            source=source,
            bookie_name=bookie_name,
        )
        if not parsed:
            return None
        opening = {
            choice: (snapshot.opening_odds, snapshot.opening_time)
            for choice, snapshot in parsed.items()
            if snapshot.opening_odds is not None
        }
        return opening or None
