"""Normalize OddsPapi bookmaker odds into the repository market contract."""

from __future__ import annotations

from typing import Any, Iterable


class OddspapiMarketAdapter:
    @staticmethod
    def _entries(value: Any) -> Iterable[tuple[str, dict]]:
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, dict):
                    yield str(key), item
        elif isinstance(value, list):
            for index, item in enumerate(value):
                if not isinstance(item, dict):
                    continue
                key = (
                    item.get("slug")
                    or item.get("marketId")
                    or item.get("outcomeId")
                    or index
                )
                yield str(key), item

    @staticmethod
    def _catalog_items(catalog: Any, wrapper_key: str) -> list[dict]:
        if isinstance(catalog, dict) and isinstance(catalog.get(wrapper_key), list):
            return [item for item in catalog[wrapper_key] if isinstance(item, dict)]
        if isinstance(catalog, list):
            return [item for item in catalog if isinstance(item, dict)]
        if isinstance(catalog, dict):
            result = []
            for key, item in catalog.items():
                if not isinstance(item, dict):
                    continue
                normalized = dict(item)
                if wrapper_key == "markets":
                    normalized.setdefault("marketId", key)
                else:
                    normalized.setdefault("slug", key)
                result.append(normalized)
            return result
        return []

    @staticmethod
    def _format_line(value: Any) -> str | None:
        if value is None or value == "":
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return str(value).strip() or None
        if number.is_integer():
            return str(int(number))
        return str(number).rstrip("0").rstrip(".")

    @staticmethod
    def _period(value: Any) -> str:
        normalized = str(value or "").strip()
        compact = normalized.lower().replace("-", "").replace("_", "").replace(" ", "")
        if not compact or compact in {"fulltime", "match", "ft"}:
            return "Full-time"
        return normalized.replace("_", " ").replace("-", " ").title()

    @staticmethod
    def _bookmaker_name(slug: str, bookmaker_index: dict[str, dict]) -> str:
        catalog_entry = bookmaker_index.get(slug)
        if catalog_entry:
            name = str(catalog_entry.get("bookmakerName") or "").strip()
            if name:
                return name
        return slug.replace("-", " ").replace("_", " ").title()

    @staticmethod
    def _market_kind(market_type: Any, outcome_name: Any, bookmaker_outcome_id: Any) -> str | None:
        market_type_text = str(market_type or "").strip().lower().replace("-", "").replace("_", "")
        outcome_text = str(outcome_name or "").strip().lower()
        bookmaker_text = str(bookmaker_outcome_id or "").strip().lower()
        parts = bookmaker_text.rsplit("/", 1)
        suffix = parts[-1] if parts else ""

        if "total" in market_type_text or suffix in {"over", "under"}:
            return "totals"
        if "spread" in market_type_text or "handicap" in market_type_text:
            return "spreads"
        if len(parts) == 2 and suffix in {"home", "away"}:
            try:
                float(parts[0])
                return "spreads"
            except ValueError:
                pass
        if market_type_text in {"moneyline", "1x2", "homeaway", "matchwinner", "winner"}:
            return "moneyline"
        if outcome_text in {"home", "draw", "away", "1", "x", "2"}:
            return "moneyline"
        if bookmaker_text in {"home", "draw", "away", "1", "x", "2"}:
            return "moneyline"
        return None

    @staticmethod
    def _normalized_choice(
        kind: str,
        outcome_name: Any,
        bookmaker_outcome_id: Any,
    ) -> str | None:
        outcome_text = str(outcome_name or "").strip().lower()
        bookmaker_text = str(bookmaker_outcome_id or "").strip().lower()
        token = outcome_text or bookmaker_text.rsplit("/", 1)[-1]
        if kind == "moneyline":
            return {
                "home": "1",
                "1": "1",
                "draw": "X",
                "x": "X",
                "away": "2",
                "2": "2",
            }.get(token)
        if kind == "totals":
            return {"over": "Over", "under": "Under"}.get(token)
        if kind == "spreads":
            return {"home": "1", "1": "1", "away": "2", "2": "2"}.get(token)
        return None

    @staticmethod
    def _choice_group(kind: str, bookmaker_outcome_id: Any, handicap: Any) -> str | None:
        if kind == "moneyline":
            return None
        bookmaker_text = str(bookmaker_outcome_id or "").strip()
        if "/" in bookmaker_text:
            line = bookmaker_text.rsplit("/", 1)[0].strip()
            if line:
                return line
        return OddspapiMarketAdapter._format_line(handicap)

    @staticmethod
    def from_odds_response(
        odds_response: dict,
        market_catalog: dict | list | None = None,
        bookmaker_catalog: dict | list | None = None,
    ) -> dict:
        payload = odds_response if isinstance(odds_response, dict) else {}
        fixture_id = payload.get("fixtureId")

        market_items = OddspapiMarketAdapter._catalog_items(market_catalog, "markets")
        market_index = {
            str(item.get("marketId")): item
            for item in market_items
            if item.get("marketId") is not None
        }
        bookmaker_items = OddspapiMarketAdapter._catalog_items(bookmaker_catalog, "bookmakers")
        bookmaker_index = {
            str(item.get("slug") or "").strip().lower(): item
            for item in bookmaker_items
            if str(item.get("slug") or "").strip()
        }

        normalized_bookmakers = []
        for bookmaker_key, bookmaker_data in OddspapiMarketAdapter._entries(
            payload.get("bookmakerOdds", {})
        ):
            slug = str(bookmaker_data.get("slug") or bookmaker_key).strip().lower()
            markets_data = bookmaker_data.get("markets")
            if not slug or not markets_data:
                continue

            grouped_markets: dict[tuple, dict] = {}
            for source_market_id, market_data in OddspapiMarketAdapter._entries(markets_data):
                if market_data.get("marketActive") is False:
                    continue
                catalog_market = market_index.get(str(source_market_id), {})
                market_type = catalog_market.get("marketType")
                period = OddspapiMarketAdapter._period(catalog_market.get("period"))
                handicap = catalog_market.get("handicap")
                outcome_index = {
                    str(outcome.get("outcomeId")): outcome.get("outcomeName")
                    for outcome in catalog_market.get("outcomes", [])
                    if isinstance(outcome, dict) and outcome.get("outcomeId") is not None
                }

                for source_outcome_id, outcome_data in OddspapiMarketAdapter._entries(
                    market_data.get("outcomes", {})
                ):
                    outcome_name = outcome_index.get(str(source_outcome_id))
                    players = outcome_data.get("players")
                    if not players and "price" in outcome_data:
                        players = [outcome_data]

                    for _, player in OddspapiMarketAdapter._entries(players or {}):
                        if player.get("active") is False:
                            continue
                        price = player.get("price")
                        if price is None or price == "":
                            continue
                        try:
                            decimal_value = round(float(price), 3)
                        except (TypeError, ValueError):
                            continue

                        bookmaker_outcome_id = player.get("bookmakerOutcomeId")
                        kind = OddspapiMarketAdapter._market_kind(
                            market_type,
                            outcome_name,
                            bookmaker_outcome_id,
                        )
                        if kind is None:
                            continue
                        choice_name = OddspapiMarketAdapter._normalized_choice(
                            kind,
                            outcome_name,
                            bookmaker_outcome_id,
                        )
                        if choice_name is None:
                            continue

                        choice_group = OddspapiMarketAdapter._choice_group(
                            kind,
                            bookmaker_outcome_id,
                            handicap,
                        )
                        if kind == "moneyline":
                            market_name, market_group = "Full-time", "1X2"
                        elif kind == "totals":
                            market_name, market_group = "Total", "Over/Under"
                        else:
                            market_name = market_group = "Asian handicap"

                        market_key = (
                            market_name,
                            market_group,
                            period,
                            choice_group,
                            bool(market_data.get("isLive", payload.get("isLive", False))),
                        )
                        normalized_market = grouped_markets.setdefault(
                            market_key,
                            {
                                "marketName": market_name,
                                "marketGroup": market_group,
                                "marketPeriod": period,
                                "choiceGroup": choice_group,
                                "isLive": market_key[-1],
                                "choices": [],
                            },
                        )
                        if any(choice["name"] == choice_name for choice in normalized_market["choices"]):
                            continue

                        choice = {
                            "name": choice_name,
                            "decimalValue": decimal_value,
                            "sourceMarketId": str(source_market_id),
                            "sourceOutcomeId": str(source_outcome_id),
                            "bookmakerOutcomeId": bookmaker_outcome_id,
                            "changedAt": player.get("changedAt"),
                            "mainLine": player.get("mainLine"),
                            "limit": player.get("limit"),
                        }
                        normalized_market["choices"].append(choice)

            markets = [market for market in grouped_markets.values() if market["choices"]]
            if markets:
                normalized_bookmakers.append(
                    {
                        "slug": slug,
                        "name": OddspapiMarketAdapter._bookmaker_name(slug, bookmaker_index),
                        "markets": markets,
                    }
                )

        return {"fixtureId": fixture_id, "bookmakers": normalized_bookmakers}
