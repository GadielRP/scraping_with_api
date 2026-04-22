"""Parse SofaScore odds responses for daily discovery."""

from __future__ import annotations

import logging
from typing import Dict

from shared.odds_utils import fractional_to_decimal

logger = logging.getLogger(__name__)


def parse_today_odds_response(odds_response: Dict) -> Dict[int, Dict]:
    try:
        if not odds_response or "odds" not in odds_response:
            logger.warning("No odds data found in response")
            return {}

        odds_data = odds_response["odds"]
        if isinstance(odds_data, list):
            if not odds_data:
                logger.debug("Odds data is an empty list, no odds found")
                return {}
            logger.warning(
                "Odds data is a list with %s items, expected dict. First item: %s",
                len(odds_data),
                odds_data[0],
            )
            return {}

        odds_map: Dict[int, Dict] = {}
        for event_id_str, event_odds in odds_data.items():
            try:
                event_id = int(event_id_str)
                choices = event_odds.get("choices", [])
                if not choices:
                    logger.debug("No choices found for event %s", event_id)
                    continue

                processed_odds = {
                    "one_initial": None,
                    "x_initial": None,
                    "two_initial": None,
                    "one_final": None,
                    "x_final": None,
                    "two_final": None,
                }

                for choice in choices:
                    choice_name = choice.get("name", "")
                    initial_fractional = choice.get("initialFractionalValue", "")
                    current_fractional = choice.get("fractionalValue", "")

                    initial_decimal = fractional_to_decimal(initial_fractional)
                    current_decimal = fractional_to_decimal(current_fractional)

                    if choice_name == "1":
                        processed_odds["one_initial"] = initial_decimal
                        processed_odds["one_final"] = current_decimal
                    elif choice_name == "X":
                        processed_odds["x_initial"] = initial_decimal
                        processed_odds["x_final"] = current_decimal
                    elif choice_name == "2":
                        processed_odds["two_initial"] = initial_decimal
                        processed_odds["two_final"] = current_decimal

                if processed_odds["one_initial"] and processed_odds["two_initial"]:
                    odds_map[event_id] = processed_odds
                    logger.debug(
                        "Processed odds for event %s: 1:%s, X:%s, 2:%s",
                        event_id,
                        processed_odds["one_final"],
                        processed_odds["x_final"],
                        processed_odds["two_final"],
                    )
                else:
                    logger.debug("Incomplete odds for event %s, skipping", event_id)
            except (ValueError, TypeError) as exc:
                logger.warning("Error processing odds for event %s: %s", event_id_str, exc)
                continue

        logger.info("Processed odds for %s events", len(odds_map))
        return odds_map
    except Exception as exc:
        logger.error("Error processing odds response: %s", exc)
        return {}
