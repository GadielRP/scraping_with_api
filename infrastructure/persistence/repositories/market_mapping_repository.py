"""Repository helpers for runtime market source mapping lookups."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import joinedload

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import MarketSourceMapping
from modules.oddspapi.format_utils import normalize_source, normalize_source_id


@dataclass
class CanonicalMarketResolution:
    resolved: bool
    mapping_id: int | None = None
    canonical_market_key: str | None = None
    canonical_market_name: str | None = None
    canonical_market_group: str | None = None
    canonical_market_period: str | None = None
    market_family: str | None = None
    requires_choice_group: bool = False
    source_handicap: str | None = None
    reason: str | None = None


@dataclass
class CanonicalOutcomeResolution:
    resolved: bool
    canonical_choice_name: str | None = None
    display_order: int | None = None
    reason: str | None = None


@dataclass
class MarketMappingIndex:
    market_mappings: dict
    outcome_mappings: dict


class MarketMappingRepository:
    @staticmethod
    def _normalize_source(source) -> str:
        return normalize_source(source)

    @staticmethod
    def _normalize_source_id(value) -> str | None:
        return normalize_source_id(value)

    @staticmethod
    def build_index(
        source: str = "oddspapi",
        enabled_only: bool = True,
    ) -> MarketMappingIndex:
        normalized_source = MarketMappingRepository._normalize_source(source)
        if not normalized_source:
            return MarketMappingIndex(market_mappings={}, outcome_mappings={})

        with db_manager.get_session() as session:
            query = (
                session.query(MarketSourceMapping)
                .options(
                    joinedload(MarketSourceMapping.canonical_market_type),
                    joinedload(MarketSourceMapping.outcome_mappings),
                )
                .filter(MarketSourceMapping.source == normalized_source)
            )
            market_mappings = {}
            outcome_mappings = {}
            for mapping in query.all():
                canonical_market_type = mapping.canonical_market_type
                if canonical_market_type is None:
                    continue
                if enabled_only and not canonical_market_type.enabled_for_ingestion:
                    continue

                market_key = (
                    normalized_source,
                    MarketMappingRepository._normalize_source_id(mapping.source_sport_id),
                    MarketMappingRepository._normalize_source_id(mapping.source_market_id),
                )
                market_mappings[market_key] = CanonicalMarketResolution(
                    resolved=True,
                    mapping_id=mapping.mapping_id,
                    canonical_market_key=mapping.canonical_market_key,
                    canonical_market_name=mapping.canonical_market_name,
                    canonical_market_group=mapping.canonical_market_group,
                    canonical_market_period=mapping.canonical_market_period,
                    market_family=canonical_market_type.market_family,
                    requires_choice_group=bool(canonical_market_type.requires_choice_group),
                    source_handicap=mapping.source_handicap,
                    reason="resolved_from_db_mapping",
                )

                for outcome_mapping in mapping.outcome_mappings:
                    outcome_key = (
                        mapping.mapping_id,
                        MarketMappingRepository._normalize_source_id(
                            outcome_mapping.source_outcome_id
                        ),
                    )
                    outcome_mappings[outcome_key] = CanonicalOutcomeResolution(
                        resolved=True,
                        canonical_choice_name=outcome_mapping.canonical_choice_name,
                        display_order=outcome_mapping.display_order,
                        reason="resolved_from_db_mapping",
                    )

            return MarketMappingIndex(
                market_mappings=market_mappings,
                outcome_mappings=outcome_mappings,
            )

    @staticmethod
    def resolve_market(
        index: MarketMappingIndex,
        source: str,
        source_sport_id,
        source_market_id,
    ) -> CanonicalMarketResolution:
        if index is None:
            return CanonicalMarketResolution(
                resolved=False,
                reason="market_mapping_index_unavailable",
            )

        normalized_source = MarketMappingRepository._normalize_source(source)
        normalized_sport_id = MarketMappingRepository._normalize_source_id(source_sport_id)
        normalized_market_id = MarketMappingRepository._normalize_source_id(source_market_id)
        if not normalized_source or not normalized_market_id:
            return CanonicalMarketResolution(
                resolved=False,
                reason="invalid_market_lookup_key",
            )

        exact_key = (normalized_source, normalized_sport_id, normalized_market_id)
        resolved = index.market_mappings.get(exact_key)
        if resolved is not None:
            return resolved

        fallback_key = (normalized_source, None, normalized_market_id)
        resolved = index.market_mappings.get(fallback_key)
        if resolved is not None:
            return resolved

        return CanonicalMarketResolution(resolved=False, reason="market_mapping_not_found")

    @staticmethod
    def resolve_outcome(
        index: MarketMappingIndex,
        market_source_mapping_id: int | None,
        source_outcome_id,
    ) -> CanonicalOutcomeResolution:
        if index is None:
            return CanonicalOutcomeResolution(
                resolved=False,
                reason="market_mapping_index_unavailable",
            )
        normalized_outcome_id = MarketMappingRepository._normalize_source_id(source_outcome_id)
        if market_source_mapping_id is None or not normalized_outcome_id:
            return CanonicalOutcomeResolution(
                resolved=False,
                reason="invalid_outcome_lookup_key",
            )

        resolved = index.outcome_mappings.get((market_source_mapping_id, normalized_outcome_id))
        if resolved is not None:
            return resolved
        return CanonicalOutcomeResolution(resolved=False, reason="outcome_mapping_not_found")
