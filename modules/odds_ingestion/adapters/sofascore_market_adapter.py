"""Adapters that normalize SofaScore odds feed variants into market payloads."""

from __future__ import annotations

import logging
import re
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
        market_period = SofaScoreMarketAdapter._normalize_period(entry.get("marketPeriod"))
        choice_group = SofaScoreMarketAdapter._normalize_text(entry.get("choiceGroup"))
        if not choice_group:
            raw_choice_iterable = raw_choices.values() if isinstance(raw_choices, dict) else raw_choices or []
            for raw_choice in raw_choice_iterable:
                choice_name = SofaScoreMarketAdapter._normalize_text(
                    raw_choice.get("name") if isinstance(raw_choice, dict) else None
                )
                if not choice_name:
                    continue
                match = re.match(r"^\(\s*([^)]+?)\s*\)\s*.+$", choice_name)
                if match:
                    choice_group = SofaScoreMarketAdapter._normalize_text(match.group(1))
                    if choice_group:
                        break

        source_market_id = entry.get("sourceId")
        if source_market_id is None:
            source_market_id = entry.get("id")
        if source_market_id is None:
            source_market_id = entry.get("marketId")

        for choice in choices:
            if source_market_id is not None and choice.get("sourceMarketId") is None:
                choice["sourceMarketId"] = str(source_market_id)

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
        normalized = []
        iterable = choices.values() if isinstance(choices, dict) else choices or []
        home_team_normalized = SofaScoreMarketAdapter._normalize_text(home_team)
        away_team_normalized = SofaScoreMarketAdapter._normalize_text(away_team)
        home_team_key = home_team_normalized.lower() if home_team_normalized else None
        away_team_key = away_team_normalized.lower() if away_team_normalized else None
        for choice in iterable:
            if not isinstance(choice, dict):
                continue
            raw_name = SofaScoreMarketAdapter._normalize_text(choice.get("name"))
            name = raw_name
            if not name:
                continue

            if raw_name not in {"1", "X", "2", "1X", "X2", "12"}:
                match = re.match(r"^\(\s*([^)]+?)\s*\)\s*(.+)$", raw_name)
                if match:
                    parsed_choice_group = SofaScoreMarketAdapter._normalize_text(match.group(1))
                    parsed_team_name = SofaScoreMarketAdapter._normalize_text(match.group(2))
                    if parsed_choice_group:
                        choice["choiceGroup"] = parsed_choice_group
                    parsed_team_key = parsed_team_name.lower() if parsed_team_name else None
                    if parsed_team_key and (
                        parsed_team_key == home_team_key
                        or parsed_team_key == away_team_key
                    ):
                        name = "1" if parsed_team_key == home_team_key else "2"

            source_outcome_id = choice.get("sourceId")
            if source_outcome_id is None:
                source_outcome_id = choice.get("id")
            if source_outcome_id is None:
                source_outcome_id = choice.get("outcomeId")

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
                    "sourceOutcomeId": str(source_outcome_id) if source_outcome_id is not None else None,
                }
            )

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
