"""Provider payloads to canonical, persistence-ready market payloads."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from infrastructure.persistence.repositories.canonical_market_type_repository import (
    CanonicalMarketTypeRepository,
)
from modules.odds_ingestion.canonical_market_resolver import resolve_sofascore_key

from .choice_normalization import ChoiceNormalizationContext, ChoiceNormalizer

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketNormalizationContext:
    source: str
    home_team: str | None = None
    away_team: str | None = None


class CanonicalMarketNormalizer:
    @staticmethod
    def _resolve_sofascore_key(market: dict) -> str | None:
        return resolve_sofascore_key(market)

    @staticmethod
    def _text(value) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

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
            "skipped_missing_line_value": [],
        }
        canonical_types = CanonicalMarketTypeRepository.build_index(enabled_only=True)
        normalized_markets = []

        for raw_market in (normalized_response or {}).get("markets", []):
            if not isinstance(raw_market, dict):
                continue
            canonical_key = resolve_sofascore_key(raw_market)
            if canonical_key is None:
                detail = {
                    "source": "sofascore",
                    "marketName": raw_market.get("marketName"),
                    "marketGroup": raw_market.get("marketGroup"),
                    "marketPeriod": raw_market.get("marketPeriod"),
                    "lineValue": raw_market.get("lineValue"),
                    "reason": "unsupported_sofascore_market_shape",
                }
                diagnostics["unmapped_markets"].append(detail)
                logger.warning(
                    "Unsupported SofaScore market skipped: marketName=%s marketGroup=%s marketPeriod=%s lineValue=%s reason=%s",
                    detail["marketName"], detail["marketGroup"], detail["marketPeriod"],
                    detail["lineValue"], detail["reason"],
                )
                continue

            canonical_type = canonical_types.get(canonical_key)
            if canonical_type is None:
                detail = {
                    "source": "sofascore",
                    "marketName": raw_market.get("marketName"),
                    "marketGroup": raw_market.get("marketGroup"),
                    "marketPeriod": raw_market.get("marketPeriod"),
                    "lineValue": raw_market.get("lineValue"),
                    "canonicalMarketKey": canonical_key,
                    "reason": "canonical_market_type_unavailable",
                }
                diagnostics["unmapped_markets"].append(detail)
                logger.warning("Enabled canonical market type unavailable; SofaScore market skipped: %s", detail)
                continue

            # The provider adapter owns the external SofaScore field name
            # (choiceGroup) and exposes the canonical internal contract as
            # lineValue.  The normalizer must only consume that internal key.
            market_line_value = CanonicalMarketNormalizer._text(raw_market.get("lineValue"))
            # Catalog marketId is the stable SofaScore market key we persist as
            # quote.source_market_id (e.g. "1"). Prefer it over instance ids.
            market_source_market_id = None
            if raw_market.get("marketId") is not None:
                market_source_market_id = str(raw_market.get("marketId")).strip() or None
            if market_source_market_id is None:
                market_source_market_id = CanonicalMarketNormalizer._text(
                    raw_market.get("sourceMarketId")
                )
            raw_choices = [c for c in raw_market.get("choices", []) if isinstance(c, dict)]
            total_choices_count = len(raw_choices)
            choices = []
            for idx, raw_choice in enumerate(raw_choices):
                raw_choice_name = raw_choice.get("name")
                choice_context = ChoiceNormalizationContext(
                    market_family=canonical_type.market_family,
                    canonical_market_key=canonical_key,
                    home_team=home_team,
                    away_team=away_team,
                    line_value=(
                        market_line_value
                        or CanonicalMarketNormalizer._text(raw_choice.get("lineValue"))
                    ),
                    choice_index=idx,
                    total_choices=total_choices_count,
                )
                choice_result = ChoiceNormalizer.normalize_choice_name(raw_choice_name, choice_context)
                if not choice_result.resolved:
                    detail = {
                        "source": "sofascore",
                        "source_event_id": (normalized_response or {}).get("eventId"),
                        "canonicalMarketKey": canonical_key,
                        "marketFamily": canonical_type.market_family,
                        "raw_market_name": raw_market.get("marketName"),
                        "raw_market_group": raw_market.get("marketGroup"),
                        "raw_market_period": raw_market.get("marketPeriod"),
                        "rawChoiceName": raw_choice_name,
                        "reason": choice_result.reason,
                    }
                    diagnostics["unmapped_choices"].append(detail)
                    logger.warning("Unsupported SofaScore choice skipped: %s", detail)
                    continue
                market_line_value = market_line_value or choice_result.line_value
                choice = {
                    key: value
                    for key, value in raw_choice.items()
                    if key not in {"name", "lineValue"}
                }
                choice["name"] = choice_result.canonical_choice_name
                # Always stamp catalog sourceMarketId onto the persisted choice
                # object (MarketRepository reads choice_data["sourceMarketId"]).
                if market_source_market_id:
                    choice["sourceMarketId"] = market_source_market_id
                choices.append(choice)

            if not choices:
                continue
            if canonical_type.requires_line_value and not market_line_value:
                detail = {
                    "source": "sofascore",
                    "canonicalMarketKey": canonical_key,
                    "marketFamily": canonical_type.market_family,
                    "marketName": raw_market.get("marketName"),
                    "marketGroup": raw_market.get("marketGroup"),
                    "marketPeriod": raw_market.get("marketPeriod"),
                    "lineValue": raw_market.get("lineValue"),
                    "reason": "missing_required_line_value",
                }
                diagnostics["skipped_missing_line_value"].append(detail)
                logger.warning("SofaScore market missing required lineValue; skipped: %s", detail)
                continue

            normalized_markets.append({
                "canonicalMarketKey": canonical_key,
                "marketName": canonical_type.canonical_market_name,
                "marketGroup": canonical_type.canonical_market_group,
                "marketPeriod": canonical_type.canonical_market_period,
                "lineValue": market_line_value,
                "isLive": bool(raw_market.get("isLive", False)),
                "choices": choices,
            })

        return {"markets": normalized_markets, "diagnostics": diagnostics}
