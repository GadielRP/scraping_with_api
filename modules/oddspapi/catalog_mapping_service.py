"""OddsPapi catalog import helpers for market source mappings."""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.orm import Session

from infrastructure.persistence.catalogs.canonical_market_types import (
    get_canonical_market_type_seed,
)
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    CanonicalMarketType,
    MarketOutcomeSourceMapping,
    MarketSourceMapping,
)
from infrastructure.persistence.repositories.canonical_market_type_repository import (
    CanonicalMarketTypeRepository,
)
from modules.odds_ingestion.canonical_market_resolver import (
    FAMILY_CHOICE_ROLES,
    outcome_role,
    resolve_oddspapi_key,
)
from modules.oddspapi.format_utils import (
    format_line,
    normalize_name,
    normalize_source,
    normalize_source_id,
)

logger = logging.getLogger(__name__)


def canonical_choice_from_outcome(
    canonical_market_key: str,
    source_outcome_name,
) -> str | None:
    role = outcome_role(source_outcome_name)
    if role is None:
        return None

    seed = get_canonical_market_type_seed(canonical_market_key) or {}
    family = seed.get("market_family")
    allowed = FAMILY_CHOICE_ROLES.get(family or "")
    if not allowed:
        return None
    return role if role in allowed else None


def upsert_market_source_mapping_from_catalog_item(
    item,
    source: str = "oddspapi",
    session: Optional[Session] = None,
    include_unsupported: bool = False,
) -> MarketSourceMapping | None:
    normalized_source = normalize_source(source)
    if not normalized_source:
        raise ValueError("source is required")
    if not isinstance(item, dict):
        raise ValueError("catalog item must be a dict")

    source_sport_id = normalize_source_id(item.get("sportId"))
    source_market_id = normalize_source_id(item.get("marketId"))
    source_market_name = normalize_name(item.get("marketName"))
    source_market_group = normalize_name(item.get("marketType")) or None
    source_period = normalize_name(item.get("period")) or None
    source_handicap = format_line(item.get("handicap"))
    player_prop = bool(item.get("playerProp"))

    if not source_market_id:
        raise ValueError("source_market_id is required")
    if not source_market_name:
        raise ValueError("source_market_name is required")

    canonical_market_key, _ = resolve_oddspapi_key(item)
    if canonical_market_key is None:
        if include_unsupported:
            logger.info(
                "Skipping unsupported market mapping import for source=%s market_id=%s; unsupported rows are not persisted in phase 2.0",
                normalized_source,
                source_market_id,
            )
        return None

    def _upsert(active_session: Session) -> MarketSourceMapping | None:
        canonical_market_type = active_session.get(CanonicalMarketType, canonical_market_key)
        if canonical_market_type is None:
            CanonicalMarketTypeRepository.seed_canonical_market_types(active_session)
            canonical_market_type = active_session.get(CanonicalMarketType, canonical_market_key)
        if canonical_market_type is None:
            raise ValueError(f"Missing canonical market seed: {canonical_market_key}")

        query = active_session.query(MarketSourceMapping).filter(
            MarketSourceMapping.source == normalized_source,
            MarketSourceMapping.source_market_id == source_market_id,
        )
        if source_sport_id is None:
            query = query.filter(MarketSourceMapping.source_sport_id.is_(None))
        else:
            query = query.filter(MarketSourceMapping.source_sport_id == source_sport_id)

        mapping = query.first()
        if mapping is None:
            mapping = MarketSourceMapping(
                source=normalized_source,
                source_sport_id=source_sport_id,
                source_market_id=source_market_id,
            )
            active_session.add(mapping)

        mapping.canonical_market_key = canonical_market_key
        mapping.source_market_name = source_market_name
        mapping.source_market_group = source_market_group
        mapping.source_period = source_period
        mapping.source_handicap = source_handicap
        mapping.player_prop = player_prop
        mapping.canonical_market_name = canonical_market_type.canonical_market_name
        mapping.canonical_market_group = canonical_market_type.canonical_market_group
        mapping.canonical_market_period = canonical_market_type.canonical_market_period
        mapping.match_method = "catalog_rule"
        mapping.confidence = 1.000
        active_session.flush()

        existing_outcomes = {
            outcome.source_outcome_id: outcome
            for outcome in (
                active_session.query(MarketOutcomeSourceMapping)
                .filter(
                    MarketOutcomeSourceMapping.market_source_mapping_id == mapping.mapping_id
                )
                .all()
            )
        }
        seen_outcome_ids = set()
        for display_order, outcome in enumerate(item.get("outcomes", []), start=1):
            if not isinstance(outcome, dict):
                continue
            source_outcome_id = normalize_source_id(outcome.get("outcomeId"))
            source_outcome_name = normalize_name(outcome.get("outcomeName"))
            if not source_outcome_id or not source_outcome_name:
                continue
            canonical_choice_name = canonical_choice_from_outcome(
                canonical_market_key,
                source_outcome_name,
            )
            if canonical_choice_name is None:
                continue

            seen_outcome_ids.add(source_outcome_id)
            outcome_mapping = existing_outcomes.get(source_outcome_id)
            if outcome_mapping is None:
                outcome_mapping = MarketOutcomeSourceMapping(
                    market_source_mapping_id=mapping.mapping_id,
                    source_outcome_id=source_outcome_id,
                )
                active_session.add(outcome_mapping)
            outcome_mapping.source_outcome_name = source_outcome_name
            outcome_mapping.canonical_choice_name = canonical_choice_name
            outcome_mapping.display_order = display_order

        for source_outcome_id, stale_mapping in existing_outcomes.items():
            if source_outcome_id not in seen_outcome_ids:
                active_session.delete(stale_mapping)

        active_session.flush()
        return mapping

    if session is not None:
        return _upsert(session)
    with db_manager.get_session() as db_session:
        return _upsert(db_session)
