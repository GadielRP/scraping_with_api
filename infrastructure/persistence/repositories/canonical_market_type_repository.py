"""Read/write access to canonical market type definitions."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import inspect
from sqlalchemy.orm import Session

from infrastructure.persistence.catalogs.canonical_market_types import (
    CANONICAL_MARKET_KEY_RENAMES,
    CANONICAL_MARKET_TYPE_IDS,
    CANONICAL_MARKET_TYPE_SEEDS,
    persisted_seed_values,
)
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import CanonicalMarketType, MarketSourceMapping

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CanonicalMarketTypeResolution:
    canonical_market_key: str
    canonical_market_name: str
    canonical_market_group: str
    canonical_market_period: str
    market_family: str
    requires_line_value: bool
    enabled_for_ingestion: bool
    # Optional only for backwards-compatible adapter fixtures. Resolutions
    # loaded from persistence always carry the stable numeric identifier.
    market_type_id: Optional[int] = None


class CanonicalMarketTypeRepository:
    @staticmethod
    def _apply_canonical_key_renames(active_session: Session) -> int:
        """Rename deprecated canonical keys and remap dependent source mappings."""
        renamed = 0
        mappings_available = "market_source_mappings" in inspect(
            active_session.get_bind()
        ).get_table_names()
        for old_key, new_key in CANONICAL_MARKET_KEY_RENAMES.items():
            if old_key == new_key:
                continue

            old_row = active_session.get(CanonicalMarketType, old_key)
            new_row = active_session.get(CanonicalMarketType, new_key)
            seed = CANONICAL_MARKET_TYPE_SEEDS.get(new_key)
            if seed is None:
                logger.warning(
                    "Canonical key rename %s -> %s skipped; target missing from seed catalog",
                    old_key,
                    new_key,
                )
                continue

            persisted = persisted_seed_values(seed)
            if old_row is None:
                continue

            if new_row is None:
                new_row = CanonicalMarketType(
                    canonical_market_key=new_key,
                    market_type_id=CANONICAL_MARKET_TYPE_IDS[new_key],
                )
                active_session.add(new_row)
            new_row.market_type_id = CANONICAL_MARKET_TYPE_IDS[new_key]
            for field_name, field_value in persisted.items():
                setattr(new_row, field_name, field_value)
            active_session.flush()

            mapping_count = 0
            if mappings_available:
                mapping_count = (
                    active_session.query(MarketSourceMapping)
                    .filter(MarketSourceMapping.canonical_market_key == old_key)
                    .update(
                        {
                            MarketSourceMapping.canonical_market_key: new_key,
                            MarketSourceMapping.market_type_id: new_row.market_type_id,
                        },
                        synchronize_session=False,
                    )
                )
            if old_row is not None:
                active_session.delete(old_row)
            active_session.flush()
            renamed += 1
            logger.info(
                "Renamed canonical market key %s -> %s (remapped %s source mapping rows)",
                old_key,
                new_key,
                mapping_count,
            )
        return renamed

    @staticmethod
    def _sync_mapping_numeric_identity(active_session: Session) -> int:
        """Keep numeric source-mapping identity aligned with the seed catalog."""
        if "market_source_mappings" not in inspect(
            active_session.get_bind()
        ).get_table_names():
            return 0
        updated = 0
        for canonical_market_key, values in CANONICAL_MARKET_TYPE_SEEDS.items():
            persisted = persisted_seed_values(values)
            count = (
                active_session.query(MarketSourceMapping)
                .filter(MarketSourceMapping.canonical_market_key == canonical_market_key)
                .update(
                    {
                        MarketSourceMapping.market_type_id: CANONICAL_MARKET_TYPE_IDS[
                            canonical_market_key
                        ],
                    },
                    synchronize_session=False,
                )
            )
            updated += count or 0
        return updated

    @staticmethod
    def seed_canonical_market_types(session: Optional[Session] = None) -> list[CanonicalMarketType]:
        def _seed(active_session: Session) -> list[CanonicalMarketType]:
            CanonicalMarketTypeRepository._apply_canonical_key_renames(active_session)

            seeded = []
            for canonical_market_key, values in CANONICAL_MARKET_TYPE_SEEDS.items():
                row = active_session.get(CanonicalMarketType, canonical_market_key)
                if row is None:
                    row = CanonicalMarketType(
                        canonical_market_key=canonical_market_key,
                        market_type_id=CANONICAL_MARKET_TYPE_IDS[canonical_market_key],
                    )
                    active_session.add(row)
                row.market_type_id = CANONICAL_MARKET_TYPE_IDS[canonical_market_key]
                for field_name, field_value in persisted_seed_values(values).items():
                    setattr(row, field_name, field_value)
                seeded.append(row)
            active_session.flush()

            sync_count = CanonicalMarketTypeRepository._sync_mapping_numeric_identity(
                active_session
            )
            if sync_count:
                logger.info(
                    "Synced denormalized canonical fields on %s market_source_mappings rows",
                    sync_count,
                )
            active_session.flush()
            return seeded

        if session is not None:
            return _seed(session)
        with db_manager.get_session() as db_session:
            return _seed(db_session)

    @staticmethod
    def build_index(
        enabled_only: bool = True,
        session: Optional[Session] = None,
    ) -> dict[str, CanonicalMarketTypeResolution]:
        def _build(active_session: Session) -> dict[str, CanonicalMarketTypeResolution]:
            query = active_session.query(CanonicalMarketType).filter(
                CanonicalMarketType.market_type_id.isnot(None)
            )
            if enabled_only:
                query = query.filter(CanonicalMarketType.enabled_for_ingestion.is_(True))
            return {
                row.canonical_market_key: CanonicalMarketTypeResolution(
                    market_type_id=int(row.market_type_id),
                    canonical_market_key=row.canonical_market_key,
                    canonical_market_name=row.canonical_market_name,
                    canonical_market_group=row.canonical_market_group,
                    canonical_market_period=row.canonical_market_period,
                    market_family=row.market_family,
                    requires_line_value=bool(row.requires_line_value),
                    enabled_for_ingestion=bool(row.enabled_for_ingestion),
                )
                for row in query.all()
            }

        try:
            if session is not None:
                return _build(session)
            with db_manager.get_session() as db_session:
                return _build(db_session)
        except Exception as exc:
            logger.error("Unable to load canonical market types: %s", exc)
            return {}
