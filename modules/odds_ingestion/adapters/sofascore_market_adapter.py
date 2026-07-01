"""Adapters that normalize SofaScore odds feed variants into market payloads."""

from __future__ import annotations

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)


class SofaScoreMarketAdapter:
    @staticmethod
    def from_event_odds_response(
        odds_response: Dict,
        home_team: str | None = None,
        away_team: str | None = None,
    ) -> Dict:
        if not odds_response:
            return {"markets": []}

        markets = odds_response.get("markets")
        if isinstance(markets, list):
            normalized_markets = [
                normalized
                for market in markets
                if (
                    normalized := SofaScoreMarketAdapter._normalize_market(
                        market,
                        home_team=home_team,
                        away_team=away_team,
                    )
                )
                is not None
            ]
            return {"markets": normalized_markets}
        if isinstance(markets, dict):
            normalized_markets = [
                normalized
                for market in markets.values()
                if (
                    normalized := SofaScoreMarketAdapter._normalize_market(
                        market,
                        home_team=home_team,
                        away_team=away_team,
                    )
                )
                is not None
            ]
            return {"markets": normalized_markets}

        return SofaScoreMarketAdapter._adapt_single_odds_entry_to_market_response(
            odds_response,
            home_team=home_team,
            away_team=away_team,
        )

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
    def _adapt_single_odds_entry_to_market_response(
        entry: Dict,
        home_team: str | None = None,
        away_team: str | None = None,
    ) -> Dict:
        raw_choices = entry.get("choices", []) if isinstance(entry, dict) else []
        choices = SofaScoreMarketAdapter._normalize_choices(
            raw_choices,
            home_team=home_team,
            away_team=away_team,
        )
        if not choices:
            return {"markets": []}

        market_name = SofaScoreMarketAdapter._normalize_text(entry.get("marketName"))
        if not market_name:
            logger.info("Skipping SofaScore market odds entry because marketName is missing")
            return {"markets": []}

        market_group = SofaScoreMarketAdapter._normalize_text(entry.get("marketGroup"))
        market_period = SofaScoreMarketAdapter._normalize_text(entry.get("marketPeriod"))
        choice_group = SofaScoreMarketAdapter._normalize_text(entry.get("choiceGroup"))

        source_market_id = entry.get("sourceId")
        if source_market_id is None:
            source_market_id = entry.get("id")
        if source_market_id is None:
            source_market_id = entry.get("marketId")

        market = {
            "marketName": market_name,
            "choiceGroup": choice_group,
            "isLive": bool(entry.get("isLive", False)),
            "choices": choices,
        }
        if market_group:
            market["marketGroup"] = market_group
        if market_period:
            market["marketPeriod"] = market_period
        if source_market_id is not None:
            market["sourceMarketId"] = str(source_market_id)

        return {
            "markets": [market]
        }

    @staticmethod
    def _normalize_market(
        market: Dict,
        home_team: str | None = None,
        away_team: str | None = None,
    ) -> Dict | None:
        normalized = dict(market or {})
        adapted = SofaScoreMarketAdapter._adapt_single_odds_entry_to_market_response(
            normalized,
            home_team=home_team,
            away_team=away_team,
        )
        markets = adapted.get("markets", [])
        return markets[0] if markets else None

    @staticmethod
    def _normalize_choices(
        choices: List[Dict],
        home_team: str | None = None,
        away_team: str | None = None,
    ) -> List[Dict]:
        del home_team, away_team  # Team semantics belong to CanonicalMarketNormalizer.
        normalized = []
        iterable = choices.values() if isinstance(choices, dict) else choices or []
        for choice in iterable:
            if not isinstance(choice, dict):
                continue
            name = SofaScoreMarketAdapter._normalize_text(choice.get("name"))
            if not name:
                continue
            source_outcome_id = choice.get("sourceId")
            if source_outcome_id is None:
                source_outcome_id = choice.get("id")
            if source_outcome_id is None:
                source_outcome_id = choice.get("outcomeId")
            normalized.append({
                "name": name,
                "choiceGroup": SofaScoreMarketAdapter._normalize_text(choice.get("choiceGroup")),
                "initialFractionalValue": choice.get("initialFractionalValue"),
                "fractionalValue": choice.get("fractionalValue"),
                "initialDecimalValue": choice.get("initialDecimalValue"),
                "decimalValue": choice.get("decimalValue"),
                "initialOdds": choice.get("initialOdds"),
                "currentOdds": choice.get("currentOdds"),
                "odds": choice.get("odds"),
                "change": choice.get("change", 0),
                "sourceOutcomeId": str(source_outcome_id) if source_outcome_id is not None else None,
            })
        return normalized

    @staticmethod
    def _normalize_text(value: str) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None
