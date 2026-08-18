"""Persistence helpers for OddsPapi mainLine outcome cache."""

from __future__ import annotations

import logging
from datetime import timedelta

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import OddspapiMainlineOutcomeCache
from modules.oddspapi.format_utils import normalize_source_id
from shared.timezone_utils import get_local_now

logger = logging.getLogger(__name__)


class OddspapiMainlineCacheRepository:
    """Upsert and query mainLine outcomes captured from OddsPapi /odds."""

    _UNIQUE_INDEX_ELEMENTS = (
        "event_id",
        "bookmaker_slug",
        "source_market_id",
        "source_outcome_id",
    )

    @staticmethod
    def _normalize_outcome_row(
        event_id: int,
        fixture_id: str,
        source_sport_id: str | None,
        outcome: dict,
        *,
        captured_at,
    ) -> dict | None:
        bookmaker_slug = str(outcome.get("bookmaker_slug") or "").strip().lower()
        source_market_id = normalize_source_id(outcome.get("source_market_id"))
        source_outcome_id = normalize_source_id(outcome.get("source_outcome_id"))
        if not bookmaker_slug or source_market_id is None or source_outcome_id is None:
            return None

        canonical_market_key = outcome.get("canonical_market_key")
        if canonical_market_key is not None:
            canonical_market_key = str(canonical_market_key).strip() or None

        return {
            "event_id": int(event_id),
            "fixture_id": str(fixture_id),
            "source_sport_id": (
                normalize_source_id(source_sport_id)
                if source_sport_id is not None
                else None
            ),
            "bookmaker_slug": bookmaker_slug,
            "source_market_id": source_market_id,
            "source_outcome_id": source_outcome_id,
            "canonical_market_key": canonical_market_key,
            "is_exchange": bool(outcome.get("is_exchange")),
            "captured_at": captured_at,
        }

    @staticmethod
    def save_mainline_outcomes(
        event_id: int,
        fixture_id: str,
        source_sport_id: str | None,
        mainline_outcomes: list[dict],
    ) -> int:
        """Upsert mainline outcomes into oddspapi_mainline_outcome_cache."""
        captured_at = get_local_now()
        rows: list[dict] = []
        seen: set[tuple[str, str, str]] = set()
        for outcome in mainline_outcomes or []:
            if not isinstance(outcome, dict):
                continue
            row = OddspapiMainlineCacheRepository._normalize_outcome_row(
                event_id,
                fixture_id,
                source_sport_id,
                outcome,
                captured_at=captured_at,
            )
            if row is None:
                continue
            key = (
                row["bookmaker_slug"],
                row["source_market_id"],
                row["source_outcome_id"],
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)

        if not rows:
            return 0

        try:
            with db_manager.get_session() as session:
                dialect_name = session.get_bind().dialect.name
                if dialect_name in {"postgresql", "sqlite"}:
                    if dialect_name == "postgresql":
                        from sqlalchemy.dialects.postgresql import insert
                    else:
                        from sqlalchemy.dialects.sqlite import insert

                    statement = insert(OddspapiMainlineOutcomeCache).values(rows)
                    statement = statement.on_conflict_do_update(
                        index_elements=list(
                            OddspapiMainlineCacheRepository._UNIQUE_INDEX_ELEMENTS
                        ),
                        set_={
                            "fixture_id": statement.excluded.fixture_id,
                            "source_sport_id": statement.excluded.source_sport_id,
                            "canonical_market_key": (
                                statement.excluded.canonical_market_key
                            ),
                            "is_exchange": statement.excluded.is_exchange,
                            "captured_at": statement.excluded.captured_at,
                        },
                    )
                    session.execute(statement)
                else:
                    for row in rows:
                        existing = (
                            session.query(OddspapiMainlineOutcomeCache)
                            .filter_by(
                                event_id=row["event_id"],
                                bookmaker_slug=row["bookmaker_slug"],
                                source_market_id=row["source_market_id"],
                                source_outcome_id=row["source_outcome_id"],
                            )
                            .one_or_none()
                        )
                        if existing is None:
                            session.add(OddspapiMainlineOutcomeCache(**row))
                            continue
                        existing.fixture_id = row["fixture_id"]
                        existing.source_sport_id = row["source_sport_id"]
                        existing.canonical_market_key = row["canonical_market_key"]
                        existing.is_exchange = row["is_exchange"]
                        existing.captured_at = row["captured_at"]
            return len(rows)
        except Exception as exc:
            logger.error(
                "Error saving Oddspapi mainline cache event_id=%s fixture_id=%s: %s",
                event_id,
                fixture_id,
                exc,
            )
            return 0

    @staticmethod
    def event_ids_with_cache(event_ids: list[int] | set[int] | tuple[int, ...]) -> set[int]:
        """Return event ids that already have at least one cached mainline outcome."""
        ids = {
            int(event_id)
            for event_id in (event_ids or [])
            if event_id is not None
        }
        if not ids:
            return set()
        try:
            with db_manager.get_session() as session:
                rows = (
                    session.query(OddspapiMainlineOutcomeCache.event_id)
                    .filter(OddspapiMainlineOutcomeCache.event_id.in_(sorted(ids)))
                    .distinct()
                    .all()
                )
            return {
                int(row[0])
                for row in rows
                if row and row[0] is not None
            }
        except Exception as exc:
            logger.error(
                "Error checking Oddspapi mainline cache event_ids=%s: %s",
                sorted(ids),
                exc,
            )
            return set()

    @staticmethod
    def get_mainline_outcome_ids_by_bookmaker(
        event_id: int,
    ) -> dict[str, set[str]]:
        """Return mainline source_outcome_id sets keyed by bookmaker_slug."""
        try:
            with db_manager.get_session() as session:
                rows = (
                    session.query(
                        OddspapiMainlineOutcomeCache.bookmaker_slug,
                        OddspapiMainlineOutcomeCache.source_outcome_id,
                    )
                    .filter(OddspapiMainlineOutcomeCache.event_id == int(event_id))
                    .all()
                )
            grouped: dict[str, set[str]] = {}
            for slug, outcome_id in rows:
                bookmaker = str(slug or "").strip().lower()
                normalized_outcome_id = str(outcome_id).strip() if outcome_id is not None else ""
                if not bookmaker or not normalized_outcome_id:
                    continue
                grouped.setdefault(bookmaker, set()).add(normalized_outcome_id)
            return grouped
        except Exception as exc:
            logger.error(
                "Error loading Oddspapi mainline outcome ids by bookmaker event_id=%s: %s",
                event_id,
                exc,
            )
            return {}

    @staticmethod
    def get_mainline_outcome_ids(event_id: int) -> set[str]:
        """Return set of source_outcome_id strings stored as mainline for an event."""
        grouped = OddspapiMainlineCacheRepository.get_mainline_outcome_ids_by_bookmaker(
            event_id
        )
        return {
            outcome_id
            for outcome_ids in grouped.values()
            for outcome_id in outcome_ids
        }

    @staticmethod
    def get_exchange_mainline_selections(
        event_id: int,
        exchange_bookmakers: list[str] | None,
    ) -> list[dict]:
        """Return cached exchange mainline rows for historical outcome requests."""
        requested = {
            str(slug).strip().lower()
            for slug in exchange_bookmakers or []
            if str(slug).strip()
        }
        if not requested:
            return []

        try:
            with db_manager.get_session() as session:
                rows = (
                    session.query(OddspapiMainlineOutcomeCache)
                    .filter(
                        OddspapiMainlineOutcomeCache.event_id == int(event_id),
                        OddspapiMainlineOutcomeCache.is_exchange.is_(True),
                        OddspapiMainlineOutcomeCache.bookmaker_slug.in_(
                            sorted(requested)
                        ),
                    )
                    .order_by(
                        OddspapiMainlineOutcomeCache.bookmaker_slug,
                        OddspapiMainlineOutcomeCache.source_market_id,
                        OddspapiMainlineOutcomeCache.source_outcome_id,
                    )
                    .all()
                )
                selections: list[dict] = []
                seen: set[tuple[str, str]] = set()
                for row in rows:
                    key = (row.bookmaker_slug, row.source_outcome_id)
                    if key in seen:
                        continue
                    seen.add(key)
                    selections.append(
                        {
                            "bookmaker_slug": row.bookmaker_slug,
                            "source_market_id": row.source_market_id,
                            "source_outcome_id": row.source_outcome_id,
                            "canonical_market_key": row.canonical_market_key or "",
                        }
                    )
                return selections
        except Exception as exc:
            logger.error(
                "Error loading Oddspapi exchange mainline selections event_id=%s: %s",
                event_id,
                exc,
            )
            return []

    @staticmethod
    def purge_stale_cache(days: int = 2) -> int:
        """Purge cache entries older than N days."""
        retention_days = max(0, int(days))
        cutoff = get_local_now() - timedelta(days=retention_days)
        try:
            with db_manager.get_session() as session:
                deleted = (
                    session.query(OddspapiMainlineOutcomeCache)
                    .filter(OddspapiMainlineOutcomeCache.captured_at < cutoff)
                    .delete(synchronize_session=False)
                )
            if deleted:
                logger.info(
                    "Purged %s Oddspapi mainline cache row(s) older than %s day(s)",
                    deleted,
                    retention_days,
                )
            return int(deleted or 0)
        except Exception as exc:
            logger.error("Error purging Oddspapi mainline cache: %s", exc)
            return 0
