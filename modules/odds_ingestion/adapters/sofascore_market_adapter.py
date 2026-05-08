"""Adapters that normalize SofaScore odds feed variants into market payloads."""

from __future__ import annotations

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class SofaScoreMarketAdapter:
    @staticmethod
    def from_event_odds_response(odds_response: Dict) -> Dict:
        if not odds_response:
            return {"markets": []}

        markets = odds_response.get("markets")
        if isinstance(markets, list):
            normalized_markets = [
                normalized
                for market in markets
                if (normalized := SofaScoreMarketAdapter._normalize_market(market)) is not None
            ]
            return {"markets": normalized_markets}

        return SofaScoreMarketAdapter._adapt_single_odds_entry_to_market_response(odds_response)

    @staticmethod
    def from_dropping_odds_map_entry(odds_map_entry: Dict) -> Dict:
        if not odds_map_entry:
            return {"markets": []}

        odds_entry = odds_map_entry.get("odds") if isinstance(odds_map_entry.get("odds"), dict) else odds_map_entry
        return SofaScoreMarketAdapter._adapt_single_odds_entry_to_market_response(odds_entry)

    @staticmethod
    def from_daily_odds_entry(daily_odds_entry: Dict) -> Dict:
        return SofaScoreMarketAdapter._adapt_single_odds_entry_to_market_response(daily_odds_entry or {})

    @staticmethod
    def _adapt_single_odds_entry_to_market_response(entry: Dict) -> Dict:
        choices = entry.get("choices", []) if isinstance(entry, dict) else []
        choices = SofaScoreMarketAdapter._normalize_choices(choices)
        if not choices:
            return {"markets": []}

        market_name = SofaScoreMarketAdapter._normalize_text(entry.get("marketName"))
        if not market_name:
            logger.info("Skipping SofaScore market odds entry because marketName is missing")
            return {"markets": []}

        market = {
            "marketName": market_name,
            "choiceGroup": SofaScoreMarketAdapter._normalize_text(entry.get("choiceGroup")),
            "isLive": bool(entry.get("isLive", False)),
            "choices": choices,
        }
        market_group = SofaScoreMarketAdapter._normalize_text(entry.get("marketGroup"))
        if market_group:
            market["marketGroup"] = market_group
        market_period = SofaScoreMarketAdapter._normalize_period(entry.get("marketPeriod"))
        if market_period:
            market["marketPeriod"] = market_period

        return {
            "markets": [market]
        }

    @staticmethod
    def _normalize_market(market: Dict) -> Dict | None:
        normalized = dict(market or {})
        market_name = SofaScoreMarketAdapter._normalize_text(normalized.get("marketName"))
        if not market_name:
            logger.info("Skipping SofaScore market because marketName is missing")
            return None

        choices = SofaScoreMarketAdapter._normalize_choices(normalized.get("choices", []))
        if not choices:
            return None

        market = {
            "marketName": market_name,
            "choiceGroup": SofaScoreMarketAdapter._normalize_text(normalized.get("choiceGroup")),
            "isLive": bool(normalized.get("isLive", False)),
            "choices": choices,
        }
        market_group = SofaScoreMarketAdapter._normalize_text(normalized.get("marketGroup"))
        if market_group:
            market["marketGroup"] = market_group
        market_period = SofaScoreMarketAdapter._normalize_period(normalized.get("marketPeriod"))
        if market_period:
            market["marketPeriod"] = market_period

        return market

    @staticmethod
    def _normalize_choices(choices: List[Dict]) -> List[Dict]:
        normalized = []
        for choice in choices or []:
            name = choice.get("name")
            if name not in {"1", "X", "2"}:
                continue

            normalized.append(
                {
                    "name": name,
                    "initialFractionalValue": choice.get("initialFractionalValue"),
                    "fractionalValue": choice.get("fractionalValue"),
                    "initialDecimalValue": choice.get("initialDecimalValue"),
                    "decimalValue": choice.get("decimalValue"),
                    "initialOdds": choice.get("initialOdds"),
                    "currentOdds": choice.get("currentOdds"),
                    "odds": choice.get("odds"),
                    "change": choice.get("change", 0),
                }
            )

        choice_names = {choice["name"] for choice in normalized}
        if "1" not in choice_names or "2" not in choice_names:
            return []

        return normalized

    @staticmethod
    def _normalize_period(period: str) -> str:
        normalized = SofaScoreMarketAdapter._normalize_text(period)
        if not normalized:
            return None
        if normalized in {"Full Time", "Full time", "Full-time", "Fulltime", "FT"}:
            return "Full-time"
        return normalized

    @staticmethod
    def _normalize_text(value: str) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None
