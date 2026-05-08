"""Parse SofaScore odds responses for daily discovery."""

from __future__ import annotations

import logging
from typing import Dict

from modules.odds_ingestion.adapters.sofascore_market_adapter import SofaScoreMarketAdapter

logger = logging.getLogger(__name__)


def parse_today_market_odds_response(odds_response: Dict) -> Dict[int, Dict]:
    try:
        if not odds_response or "odds" not in odds_response:
            logger.warning("No odds data found in response")
            return {}

        odds_data = odds_response["odds"]
        if not isinstance(odds_data, dict):
            logger.warning("Odds data is not a dict; cannot parse market odds")
            return {}

        odds_map: Dict[int, Dict] = {}
        for event_id_str, event_odds in odds_data.items():
            try:
                event_id = int(event_id_str)
                normalized = SofaScoreMarketAdapter.from_daily_odds_entry(event_odds)
                if normalized.get("markets"):
                    odds_map[event_id] = normalized
            except (ValueError, TypeError) as exc:
                logger.warning("Error processing market odds for event %s: %s", event_id_str, exc)

        logger.info("Processed market odds for %s events", len(odds_map))
        return odds_map
    except Exception as exc:
        logger.error("Error processing market odds response: %s", exc)
        return {}
