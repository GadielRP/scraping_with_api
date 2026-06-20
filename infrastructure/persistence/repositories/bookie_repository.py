"""Repository helpers for canonical bookies and source lineage mappings."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Bookie, BookieSourceMapping

logger = logging.getLogger(__name__)


@dataclass
class BookieResolution:
    bookie: Bookie | None
    resolved: bool
    created: bool = False
    reused: bool = False
    mapping_created: bool = False
    mapping_updated: bool = False
    match_method: str | None = None
    reason: str | None = None


class BookieRepository:
    BOOKIE_SOURCE_ALIASES = {
        ("oddsportal", "betfair-exchange"): {
            "canonical_slug": "betfair-ex",
            "canonical_name": "Betfair Exchange",
            "match_method": "manual_alias",
            "confidence": 1.000,
        },
        ("oddspapi", "betfair-ex"): {
            "canonical_slug": "betfair-ex",
            "canonical_name": "BetFair Exchange",
            "match_method": "canonical_slug_seed",
            "confidence": 1.000,
        },
    }

    @staticmethod
    def _normalize_source(source: str) -> str:
        return str(source or "").strip().lower()

    @staticmethod
    def _normalize_slug(slug: str) -> str:
        return str(slug or "").strip().lower()

    @staticmethod
    def _normalize_name(name: str) -> str:
        return str(name or "").strip()

    @staticmethod
    def _build_mapping_resolution(
        *,
        bookie: Bookie,
        existing_mapping: Optional[BookieSourceMapping],
        mapping: BookieSourceMapping,
        match_method: str,
        created: bool = False,
    ) -> BookieResolution:
        mapping_updated = False
        if existing_mapping is not None:
            mapping_updated = any(
                (
                    existing_mapping.source_bookie_name != mapping.source_bookie_name,
                    existing_mapping.match_method != mapping.match_method,
                    existing_mapping.confidence != mapping.confidence,
                )
            )

        return BookieResolution(
            bookie=bookie,
            resolved=True,
            created=created,
            reused=not created,
            mapping_created=existing_mapping is None,
            mapping_updated=mapping_updated,
            match_method=match_method,
        )

    @staticmethod
    def get_bookie_by_slug(slug: str, session: Optional[Session] = None) -> Optional[Bookie]:
        normalized_slug = BookieRepository._normalize_slug(slug)
        if not normalized_slug:
            return None

        def _lookup(active_session: Session) -> Optional[Bookie]:
            return (
                active_session.query(Bookie)
                .filter(Bookie.slug == normalized_slug)
                .first()
            )

        if session is not None:
            return _lookup(session)

        with db_manager.get_session() as db_session:
            return _lookup(db_session)

    @staticmethod
    def get_bookie_source_mapping(
        source: str,
        source_bookie_slug: str,
        session: Optional[Session] = None,
    ) -> Optional[BookieSourceMapping]:
        normalized_source = BookieRepository._normalize_source(source)
        normalized_slug = BookieRepository._normalize_slug(source_bookie_slug)
        if not normalized_source or not normalized_slug:
            return None

        def _lookup(active_session: Session) -> Optional[BookieSourceMapping]:
            return (
                active_session.query(BookieSourceMapping)
                .options(joinedload(BookieSourceMapping.bookie))
                .filter(
                    BookieSourceMapping.source == normalized_source,
                    BookieSourceMapping.source_bookie_slug == normalized_slug,
                )
                .first()
            )

        if session is not None:
            return _lookup(session)

        with db_manager.get_session() as db_session:
            return _lookup(db_session)

    @staticmethod
    def upsert_source_mapping(
        bookie_id: int,
        source: str,
        source_bookie_name: str,
        source_bookie_slug: str,
        match_method: str = "direct",
        confidence: Optional[float] = None,
        session: Optional[Session] = None,
    ) -> BookieSourceMapping:
        normalized_source = BookieRepository._normalize_source(source)
        normalized_name = BookieRepository._normalize_name(source_bookie_name)
        normalized_slug = BookieRepository._normalize_slug(source_bookie_slug)

        if not normalized_source:
            raise ValueError("source is required")
        if not normalized_name:
            raise ValueError("source_bookie_name is required")
        if not normalized_slug:
            raise ValueError("source_bookie_slug is required")

        def _upsert(active_session: Session) -> BookieSourceMapping:
            existing = (
                active_session.query(BookieSourceMapping)
                .filter(
                    BookieSourceMapping.source == normalized_source,
                    BookieSourceMapping.source_bookie_slug == normalized_slug,
                )
                .first()
            )

            if existing:
                if existing.bookie_id != bookie_id:
                    logger.warning(
                        "Bookie source mapping conflict for %s/%s: existing bookie_id=%s, requested=%s",
                        normalized_source,
                        normalized_slug,
                        existing.bookie_id,
                        bookie_id,
                    )
                    raise ValueError("bookie source mapping already points to another bookie")

                changed = False
                if existing.source_bookie_name != normalized_name:
                    existing.source_bookie_name = normalized_name
                    changed = True
                if existing.match_method != match_method:
                    existing.match_method = match_method
                    changed = True
                if existing.confidence != confidence:
                    existing.confidence = confidence
                    changed = True

                if changed:
                    active_session.flush()
                return existing

            mapping = BookieSourceMapping(
                bookie_id=bookie_id,
                source=normalized_source,
                source_bookie_name=normalized_name,
                source_bookie_slug=normalized_slug,
                match_method=match_method,
                confidence=confidence,
            )
            active_session.add(mapping)
            active_session.flush()
            return mapping

        if session is not None:
            return _upsert(session)

        with db_manager.get_session() as db_session:
            return _upsert(db_session)

    @staticmethod
    def resolve_bookie_from_source(
        source: str,
        source_bookie_name: str,
        source_bookie_slug: str,
        allow_create: bool = False,
        session: Optional[Session] = None,
    ) -> BookieResolution:
        normalized_source = BookieRepository._normalize_source(source)
        normalized_name = BookieRepository._normalize_name(source_bookie_name)
        normalized_slug = BookieRepository._normalize_slug(source_bookie_slug)

        if not normalized_source:
            return BookieResolution(bookie=None, resolved=False, reason="canonical_bookie_not_found")
        if not normalized_name and not normalized_slug:
            return BookieResolution(bookie=None, resolved=False, reason="canonical_bookie_not_found")

        def _resolve(active_session: Session) -> BookieResolution:
            existing_mapping = BookieRepository.get_bookie_source_mapping(
                normalized_source,
                normalized_slug,
                session=active_session,
            )
            if existing_mapping and existing_mapping.bookie:
                return BookieResolution(
                    bookie=existing_mapping.bookie,
                    resolved=True,
                    reused=True,
                    match_method="existing_source_mapping",
                )
            if existing_mapping and existing_mapping.bookie is None:
                logger.warning(
                    "Bookie source mapping exists without canonical bookie for %s/%s",
                    normalized_source,
                    normalized_slug,
                )

            alias = BookieRepository.BOOKIE_SOURCE_ALIASES.get((normalized_source, normalized_slug))
            if alias:
                canonical_bookie = BookieRepository.get_bookie_by_slug(alias["canonical_slug"], session=active_session)
                if canonical_bookie is not None:
                    mapping = BookieRepository.upsert_source_mapping(
                        bookie_id=canonical_bookie.bookie_id,
                        source=normalized_source,
                        source_bookie_name=alias["canonical_name"],
                        source_bookie_slug=normalized_slug,
                        match_method=alias["match_method"],
                        confidence=alias["confidence"],
                        session=active_session,
                    )
                    return BookieRepository._build_mapping_resolution(
                        bookie=canonical_bookie,
                        existing_mapping=existing_mapping,
                        mapping=mapping,
                        match_method=alias["match_method"],
                    )
                logger.warning(
                    "Canonical bookie %s not found for source alias %s/%s",
                    alias["canonical_slug"],
                    normalized_source,
                    normalized_slug,
                )

            canonical_bookie = BookieRepository.get_bookie_by_slug(normalized_slug, session=active_session)
            if canonical_bookie is not None:
                mapping = BookieRepository.upsert_source_mapping(
                    bookie_id=canonical_bookie.bookie_id,
                    source=normalized_source,
                    source_bookie_name=normalized_name or canonical_bookie.name,
                    source_bookie_slug=normalized_slug,
                    match_method="canonical_slug_match",
                    confidence=1.000,
                    session=active_session,
                )
                return BookieRepository._build_mapping_resolution(
                    bookie=canonical_bookie,
                    existing_mapping=existing_mapping,
                    mapping=mapping,
                    match_method="canonical_slug_match",
                )

            if normalized_name:
                candidates = (
                    active_session.query(Bookie)
                    .filter(func.lower(Bookie.name) == normalized_name.lower())
                    .all()
                )
                if len(candidates) == 1:
                    canonical_bookie = candidates[0]
                    mapping = BookieRepository.upsert_source_mapping(
                        bookie_id=canonical_bookie.bookie_id,
                        source=normalized_source,
                        source_bookie_name=normalized_name,
                        source_bookie_slug=normalized_slug,
                        match_method="exact_name_match",
                        confidence=0.950,
                        session=active_session,
                    )
                    return BookieRepository._build_mapping_resolution(
                        bookie=canonical_bookie,
                        existing_mapping=existing_mapping,
                        mapping=mapping,
                        match_method="exact_name_match",
                    )
                if len(candidates) > 1:
                    logger.warning(
                        "Ambiguous canonical bookie name match for source %s slug %s: %s candidates",
                        normalized_source,
                        normalized_slug,
                        len(candidates),
                    )

            if not allow_create:
                return BookieResolution(
                    bookie=None,
                    resolved=False,
                    reason="canonical_bookie_not_found",
                )

            canonical_name = normalized_name or normalized_slug.replace("-", " ").title()
            canonical_slug = normalized_slug or canonical_name.lower().replace(" ", "-")
            bookie = Bookie(name=canonical_name, slug=canonical_slug)
            active_session.add(bookie)
            active_session.flush()

            BookieRepository.upsert_source_mapping(
                bookie_id=bookie.bookie_id,
                source=normalized_source,
                source_bookie_name=normalized_name or canonical_name,
                source_bookie_slug=normalized_slug or canonical_slug,
                match_method="created_source_bookie",
                confidence=1.000,
                session=active_session,
            )
            return BookieResolution(
                bookie=bookie,
                resolved=True,
                created=True,
                mapping_created=True,
                match_method="created_source_bookie",
            )

        if session is not None:
            return _resolve(session)

        with db_manager.get_session() as db_session:
            return _resolve(db_session)
