"""Provider payloads to canonical, persistence-ready market payloads."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from infrastructure.persistence.repositories.canonical_market_type_repository import (
    CanonicalMarketTypeRepository,
)

from .choice_normalization import ChoiceNormalizationContext, ChoiceNormalizer

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketNormalizationContext:
    source: str
    home_team: str | None = None
    away_team: str | None = None


class CanonicalMarketNormalizer:
    _SPACE = re.compile(r"\s+")
    _FULL_TIME_PERIODS = {"match", "full time"}

    @staticmethod
    def _text(value) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @staticmethod
    def _semantic_token(value) -> str:
        text = CanonicalMarketNormalizer._text(value) or ""
        return CanonicalMarketNormalizer._SPACE.sub(" ", text.lower().replace("-", " ")).strip()

    @staticmethod
    def _resolve_sofascore_key(market: dict) -> str | None:
        name = CanonicalMarketNormalizer._semantic_token(market.get("marketName"))
        group = CanonicalMarketNormalizer._semantic_token(market.get("marketGroup"))
        period = CanonicalMarketNormalizer._semantic_token(market.get("marketPeriod"))
        full_time = period in CanonicalMarketNormalizer._FULL_TIME_PERIODS

        if group == "1x2" and name == "full time" and full_time:
            return "1x2_full_time"
        if group == "1x2" and name == "1st half" and period == "1st half":
            return "1x2_1st_half"
        if group == "home/away" and name == "full time" and full_time:
            return "home_away_full_time"
        if group == "home/away" and name == "1st half" and period == "1st half":
            return "home_away_1st_half"
        if name in {"game total", "total points"} and group == "over/under" and full_time:
            return "over_under_full_time"
        if name == "match goals" and group in {"match goals", "over/under"} and full_time:
            return "over_under_full_time"
        if name == "asian handicap" and group == "asian handicap" and full_time:
            return "asian_handicap_full_time"
        if name == "point spread" and group == "point spread" and full_time:
            return "asian_handicap_full_time"
        if name == "both teams to score" and group == "both teams to score" and full_time:
            return "both_teams_to_score_full_time"
        if name == "first team to score" and group == "first team to score" and full_time:
            return "first_team_to_score_full_time"
        return None

    @staticmethod
    def normalize_response(normalized_response: dict, context: MarketNormalizationContext) -> dict:
        if not context.source.casefold().startswith("sofascore"):
            return normalized_response
        return CanonicalMarketNormalizer.normalize_sofascore_response(
            normalized_response,
            home_team=context.home_team,
            away_team=context.away_team,
        )

    @staticmethod
    def normalize_sofascore_response(
        normalized_response: dict,
        home_team: str | None = None,
        away_team: str | None = None,
    ) -> dict:
        diagnostics = {
            "unmapped_markets": [],
            "unmapped_choices": [],
            "skipped_missing_choice_group": [],
        }
        canonical_types = CanonicalMarketTypeRepository.build_index(enabled_only=True)
        normalized_markets = []

        for raw_market in (normalized_response or {}).get("markets", []):
            if not isinstance(raw_market, dict):
                continue
            canonical_key = CanonicalMarketNormalizer._resolve_sofascore_key(raw_market)
            if canonical_key is None:
                detail = {
                    "source": "sofascore",
                    "marketName": raw_market.get("marketName"),
                    "marketGroup": raw_market.get("marketGroup"),
                    "marketPeriod": raw_market.get("marketPeriod"),
                    "choiceGroup": raw_market.get("choiceGroup"),
                    "reason": "unsupported_sofascore_market_shape",
                }
                diagnostics["unmapped_markets"].append(detail)
                logger.warning(
                    "Unsupported SofaScore market skipped: marketName=%s marketGroup=%s marketPeriod=%s choiceGroup=%s reason=%s",
                    detail["marketName"], detail["marketGroup"], detail["marketPeriod"],
                    detail["choiceGroup"], detail["reason"],
                )
                continue

            canonical_type = canonical_types.get(canonical_key)
            if canonical_type is None:
                detail = {
                    "source": "sofascore",
                    "marketName": raw_market.get("marketName"),
                    "marketGroup": raw_market.get("marketGroup"),
                    "marketPeriod": raw_market.get("marketPeriod"),
                    "choiceGroup": raw_market.get("choiceGroup"),
                    "canonicalMarketKey": canonical_key,
                    "reason": "canonical_market_type_unavailable",
                }
                diagnostics["unmapped_markets"].append(detail)
                logger.warning("Enabled canonical market type unavailable; SofaScore market skipped: %s", detail)
                continue

            market_choice_group = CanonicalMarketNormalizer._text(raw_market.get("choiceGroup"))
            choices = []
            for raw_choice in raw_market.get("choices", []):
                if not isinstance(raw_choice, dict):
                    continue
                raw_choice_name = raw_choice.get("name")
                choice_context = ChoiceNormalizationContext(
                    market_family=canonical_type.market_family,
                    canonical_market_key=canonical_key,
                    home_team=home_team,
                    away_team=away_team,
                    choice_group=(
                        market_choice_group
                        or CanonicalMarketNormalizer._text(raw_choice.get("choiceGroup"))
                    ),
                )
                choice_result = ChoiceNormalizer.normalize_choice_name(raw_choice_name, choice_context)
                if not choice_result.resolved:
                    detail = {
                        "source": "sofascore",
                        "canonicalMarketKey": canonical_key,
                        "marketFamily": canonical_type.market_family,
                        "rawChoiceName": raw_choice_name,
                        "reason": choice_result.reason,
                    }
                    diagnostics["unmapped_choices"].append(detail)
                    logger.warning("Unsupported SofaScore choice skipped: %s", detail)
                    continue
                market_choice_group = market_choice_group or choice_result.choice_group
                choice = {
                    key: value
                    for key, value in raw_choice.items()
                    if key not in {"name", "choiceGroup", "sourceMarketId"}
                }
                choice["name"] = choice_result.canonical_choice_name
                choices.append(choice)

            if not choices:
                continue
            if canonical_type.requires_choice_group and not market_choice_group:
                detail = {
                    "source": "sofascore",
                    "canonicalMarketKey": canonical_key,
                    "marketFamily": canonical_type.market_family,
                    "marketName": raw_market.get("marketName"),
                    "marketGroup": raw_market.get("marketGroup"),
                    "marketPeriod": raw_market.get("marketPeriod"),
                    "reason": "missing_required_choice_group",
                }
                diagnostics["skipped_missing_choice_group"].append(detail)
                logger.warning("SofaScore market missing required choiceGroup; skipped: %s", detail)
                continue

            normalized_markets.append({
                "canonicalMarketKey": canonical_key,
                "marketName": canonical_type.canonical_market_name,
                "marketGroup": canonical_type.canonical_market_group,
                "marketPeriod": canonical_type.canonical_market_period,
                "choiceGroup": market_choice_group,
                "isLive": bool(raw_market.get("isLive", False)),
                "choices": choices,
            })

        return {"markets": normalized_markets, "diagnostics": diagnostics}
