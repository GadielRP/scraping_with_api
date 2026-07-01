"""Read access to canonical market type definitions."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import CanonicalMarketType

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CanonicalMarketTypeResolution:
    canonical_market_key: str
    canonical_market_name: str
    canonical_market_group: str
    canonical_market_period: str
    market_family: str
    requires_choice_group: bool
    enabled_for_ingestion: bool


class CanonicalMarketTypeRepository:
    @staticmethod
    def build_index(enabled_only: bool = True) -> dict[str, CanonicalMarketTypeResolution]:
        try:
            with db_manager.get_session() as session:
                query = session.query(CanonicalMarketType)
                if enabled_only:
                    query = query.filter(CanonicalMarketType.enabled_for_ingestion.is_(True))
                return {
                    row.canonical_market_key: CanonicalMarketTypeResolution(
                        canonical_market_key=row.canonical_market_key,
                        canonical_market_name=row.canonical_market_name,
                        canonical_market_group=row.canonical_market_group,
                        canonical_market_period=row.canonical_market_period,
                        market_family=row.market_family,
                        requires_choice_group=bool(row.requires_choice_group),
                        enabled_for_ingestion=bool(row.enabled_for_ingestion),
                    )
                    for row in query.all()
                }
        except Exception as exc:
            logger.error("Unable to load canonical market types: %s", exc)
            return {}
