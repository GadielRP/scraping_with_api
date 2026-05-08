"""Adapters that normalize SofaScore odds feed variants into market payloads."""

from __future__ import annotations

from typing import Dict, List


class SofaScoreMarketAdapter:
    @staticmethod
    def from_event_odds_response(odds_response: Dict) -> Dict:
        if not odds_response:
            return {"markets": []}

        markets = odds_response.get("markets")
        if isinstance(markets, list):
            return {"markets": [SofaScoreMarketAdapter._normalize_market(market) for market in markets]}

        return SofaScoreMarketAdapter._entry_to_response(odds_response)

    @staticmethod
    def from_dropping_odds_map_entry(odds_map_entry: Dict) -> Dict:
        if not odds_map_entry:
            return {"markets": []}

        odds_entry = odds_map_entry.get("odds") if isinstance(odds_map_entry.get("odds"), dict) else odds_map_entry
        return SofaScoreMarketAdapter._entry_to_response(odds_entry)

    @staticmethod
    def from_daily_odds_entry(daily_odds_entry: Dict) -> Dict:
        return SofaScoreMarketAdapter._entry_to_response(daily_odds_entry or {})

    @staticmethod
    def _entry_to_response(entry: Dict) -> Dict:
        choices = entry.get("choices", []) if isinstance(entry, dict) else []
        choices = SofaScoreMarketAdapter._normalize_choices(choices)
        if not choices:
            return {"markets": []}

        choice_names = {choice.get("name") for choice in choices}
        has_draw = "X" in choice_names

        return {
            "markets": [
                {
                    "marketName": entry.get("marketName") or "Full time",
                    "marketGroup": entry.get("marketGroup") or ("Full time" if has_draw else "Home/Away"),
                    "marketPeriod": SofaScoreMarketAdapter._normalize_period(entry.get("marketPeriod") or "Full-time"),
                    "choiceGroup": entry.get("choiceGroup"),
                    "isLive": bool(entry.get("isLive", False)),
                    "choices": choices,
                }
            ]
        }

    @staticmethod
    def _normalize_market(market: Dict) -> Dict:
        normalized = dict(market or {})
        normalized["marketName"] = normalized.get("marketName") or "Full time"
        normalized["marketPeriod"] = SofaScoreMarketAdapter._normalize_period(
            normalized.get("marketPeriod") or "Full-time"
        )
        normalized["isLive"] = bool(normalized.get("isLive", False))
        normalized["choices"] = SofaScoreMarketAdapter._normalize_choices(normalized.get("choices", []))

        if not normalized.get("marketGroup"):
            choice_names = {choice.get("name") for choice in normalized["choices"]}
            normalized["marketGroup"] = "Full time" if "X" in choice_names else "Home/Away"

        return normalized

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
        if period is None:
            return "Full-time"

        normalized = str(period).strip()
        if normalized in {"Full Time", "Full time", "Full-time", "Fulltime", "FT"}:
            return "Full-time"
        return normalized
