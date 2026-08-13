"""Set-based reads and bulk snapshot linking for Phase 4b quote backfill.

No classification or temporal policy lives here — only SQL and row mappings.
The orchestrator owns transactions and commits.

LEGACY_PHASE8_REMOVE: Phase 6 retired this repository when snapshot identity
moved exclusively to quote_id; Phase 7 also removes the choice-level price
columns used by its historical recovery queries. It is not an active
maintenance path. Delete it together with its orchestrator/CLI and tests in
Phase 8; do not add compatibility branches for the slim schemas.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional, Sequence

from sqlalchemy import and_, func, or_, text
from sqlalchemy.orm import Session

from infrastructure.persistence.models import (
    BookieSourceMapping,
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories.market.market_choice_quote_writer import (
    MarketChoiceQuoteWriter,
)
from shared.timezone_utils import get_local_now

ODDSPAPI_SOURCE = "oddspapi"
# bookie_source_mappings also stores a non-provider "canonical" row per bookie.
# Inference/purge must ignore it — only real odds providers count.
PROVIDER_BOOKIE_SOURCES = frozenset({"sofascore", "oddspapi", "oddsportal"})


@dataclass(frozen=True)
class SnapshotCandidateRow:
    snapshot_id: int
    choice_id: int
    quote_id: Optional[int]
    odds_value: Any
    collected_at: datetime
    source: Optional[str]
    source_collected_at: Optional[datetime]
    source_market_id: Optional[str]
    source_outcome_id: Optional[str]
    bookmaker_outcome_id: Optional[str]
    main_line: Optional[bool]
    source_limit: Any
    exchange_side: Optional[str]
    exchange_level: Optional[int]
    exchange_size: Any
    market_id: int
    event_id: int
    bookie_id: Optional[int]
    market_name: str
    market_period: str
    choice_group: Optional[str]
    is_live: bool
    choice_name: str
    choice_initial_odds: Any
    choice_current_odds: Any


@dataclass(frozen=True)
class ChoiceStateCandidateRow:
    choice_id: int
    market_id: int
    event_id: int
    bookie_id: Optional[int]
    market_name: str
    market_period: str
    choice_group: Optional[str]
    is_live: bool
    choice_name: str
    initial_odds: Any
    current_odds: Any
    has_snapshots: bool


class MarketChoiceQuoteBackfillRepository:
    """Read-only candidate loaders + bulk snapshot.quote_id updates."""

    @staticmethod
    def select_event_scope(
        session: Session,
        *,
        event_id: Optional[int] = None,
        event_id_min: Optional[int] = None,
        event_id_max: Optional[int] = None,
        after_event_id: Optional[int] = None,
        max_events: Optional[int] = None,
    ) -> list[int]:
        """Freeze a bounded set of event IDs that still have backfill work."""
        if event_id is not None:
            return [int(event_id)]

        def _scoped(query):
            if event_id_min is not None:
                query = query.filter(Market.event_id >= int(event_id_min))
            if event_id_max is not None:
                query = query.filter(Market.event_id <= int(event_id_max))
            if after_event_id is not None:
                query = query.filter(Market.event_id > int(after_event_id))
            return query

        pending_snapshot_events = _scoped(
            session.query(Market.event_id)
            .join(MarketChoice, MarketChoice.market_id == Market.market_id)
            .join(
                MarketChoiceSnapshot,
                MarketChoiceSnapshot.choice_id == MarketChoice.choice_id,
            )
            .filter(MarketChoiceSnapshot.quote_id.is_(None))
        )
        pending_choice_events = _scoped(
            session.query(Market.event_id)
            .join(MarketChoice, MarketChoice.market_id == Market.market_id)
            .filter(
                (MarketChoice.initial_odds.isnot(None))
                | (MarketChoice.current_odds.isnot(None)),
                ~session.query(MarketChoiceSnapshot.snapshot_id)
                .filter(MarketChoiceSnapshot.choice_id == MarketChoice.choice_id)
                .exists(),
            )
        )
        union_q = pending_snapshot_events.union(pending_choice_events).order_by(
            Market.event_id.asc()
        )
        if max_events is not None:
            union_q = union_q.limit(int(max_events))
        return [int(row[0]) for row in union_q.all()]

    @staticmethod
    def fetch_pending_snapshots(
        session: Session,
        *,
        event_ids: Sequence[int],
        after_snapshot_id: Optional[int],
        limit: int,
        source_filter: Optional[str] = None,
    ) -> list[SnapshotCandidateRow]:
        if not event_ids or limit <= 0:
            return []
        rows = (
            session.query(
                MarketChoiceSnapshot.snapshot_id,
                MarketChoiceSnapshot.choice_id,
                MarketChoiceSnapshot.quote_id,
                MarketChoiceSnapshot.odds_value,
                MarketChoiceSnapshot.collected_at,
                MarketChoiceSnapshot.source,
                MarketChoiceSnapshot.source_collected_at,
                MarketChoiceSnapshot.source_market_id,
                MarketChoiceSnapshot.source_outcome_id,
                MarketChoiceSnapshot.bookmaker_outcome_id,
                MarketChoiceSnapshot.main_line,
                MarketChoiceSnapshot.source_limit,
                MarketChoiceSnapshot.exchange_side,
                MarketChoiceSnapshot.exchange_level,
                MarketChoiceSnapshot.exchange_size,
                Market.market_id,
                Market.event_id,
                Market.bookie_id,
                Market.market_name,
                Market.market_period,
                Market.choice_group,
                Market.is_live,
                MarketChoice.choice_name,
                MarketChoice.initial_odds,
                MarketChoice.current_odds,
            )
            .join(MarketChoice, MarketChoice.choice_id == MarketChoiceSnapshot.choice_id)
            .join(Market, Market.market_id == MarketChoice.market_id)
            .filter(
                MarketChoiceSnapshot.quote_id.is_(None),
                Market.event_id.in_(list(event_ids)),
            )
        )
        if after_snapshot_id is not None:
            rows = rows.filter(MarketChoiceSnapshot.snapshot_id > int(after_snapshot_id))
        if source_filter:
            normalized = source_filter.strip().lower()
            rows = rows.filter(
                (MarketChoiceSnapshot.source.is_(None))
                | (MarketChoiceSnapshot.source == normalized)
            )
        rows = rows.order_by(MarketChoiceSnapshot.snapshot_id.asc()).limit(int(limit))
        return [
            SnapshotCandidateRow(
                snapshot_id=int(r[0]),
                choice_id=int(r[1]),
                quote_id=int(r[2]) if r[2] is not None else None,
                odds_value=r[3],
                collected_at=r[4],
                source=r[5],
                source_collected_at=r[6],
                source_market_id=r[7],
                source_outcome_id=r[8],
                bookmaker_outcome_id=r[9],
                main_line=r[10],
                source_limit=r[11],
                exchange_side=r[12],
                exchange_level=r[13],
                exchange_size=r[14],
                market_id=int(r[15]),
                event_id=int(r[16]),
                bookie_id=int(r[17]) if r[17] is not None else None,
                market_name=r[18],
                market_period=r[19],
                choice_group=r[20],
                is_live=bool(r[21]),
                choice_name=r[22],
                choice_initial_odds=r[23],
                choice_current_odds=r[24],
            )
            for r in rows.all()
        ]

    @staticmethod
    def fetch_choice_states_without_snapshots(
        session: Session,
        *,
        event_ids: Sequence[int],
        after_choice_id: Optional[int],
        limit: int,
    ) -> list[ChoiceStateCandidateRow]:
        if not event_ids or limit <= 0:
            return []
        query = (
            session.query(
                MarketChoice.choice_id,
                Market.market_id,
                Market.event_id,
                Market.bookie_id,
                Market.market_name,
                Market.market_period,
                Market.choice_group,
                Market.is_live,
                MarketChoice.choice_name,
                MarketChoice.initial_odds,
                MarketChoice.current_odds,
            )
            .join(Market, Market.market_id == MarketChoice.market_id)
            .filter(
                Market.event_id.in_(list(event_ids)),
                (MarketChoice.initial_odds.isnot(None))
                | (MarketChoice.current_odds.isnot(None)),
                ~session.query(MarketChoiceSnapshot.snapshot_id)
                .filter(MarketChoiceSnapshot.choice_id == MarketChoice.choice_id)
                .exists(),
            )
        )
        if after_choice_id is not None:
            query = query.filter(MarketChoice.choice_id > int(after_choice_id))
        query = query.order_by(MarketChoice.choice_id.asc()).limit(int(limit))
        return [
            ChoiceStateCandidateRow(
                choice_id=int(r[0]),
                market_id=int(r[1]),
                event_id=int(r[2]),
                bookie_id=int(r[3]) if r[3] is not None else None,
                market_name=r[4],
                market_period=r[5],
                choice_group=r[6],
                is_live=bool(r[7]),
                choice_name=r[8],
                initial_odds=r[9],
                current_odds=r[10],
                has_snapshots=False,
            )
            for r in query.all()
        ]

    @staticmethod
    def preload_bookie_sources(
        session: Session, bookie_ids: Iterable[int]
    ) -> dict[int, set[str]]:
        ids = sorted({int(b) for b in bookie_ids if b is not None})
        if not ids:
            return {}
        rows = (
            session.query(BookieSourceMapping.bookie_id, BookieSourceMapping.source)
            .filter(BookieSourceMapping.bookie_id.in_(ids))
            .all()
        )
        out: dict[int, set[str]] = {}
        for bookie_id, source in rows:
            normalized = str(source or "").strip().lower()
            if not normalized or normalized not in PROVIDER_BOOKIE_SOURCES:
                continue
            out.setdefault(int(bookie_id), set()).add(normalized)
        return out

    @staticmethod
    def preload_canonical_markets(
        session: Session,
        *,
        lookups: Sequence[tuple[int, int, str, str, Optional[str], bool]],
    ) -> dict[tuple[int, int, str, str, Optional[str], bool], Market]:
        """Key: (event_id, bookie_id, market_name, market_period, choice_group, is_live)."""
        if not lookups:
            return {}
        event_ids = {row[0] for row in lookups}
        bookie_ids = {row[1] for row in lookups}
        markets = (
            session.query(Market)
            .filter(
                Market.event_id.in_(list(event_ids)),
                Market.bookie_id.in_(list(bookie_ids)),
            )
            .all()
        )
        index: dict[tuple[int, int, str, str, Optional[str], bool], Market] = {}
        for market in markets:
            key = (
                int(market.event_id),
                int(market.bookie_id),
                str(market.market_name),
                str(market.market_period or "Full Time"),
                market.choice_group,
                bool(market.is_live),
            )
            index[key] = market
        return index

    @staticmethod
    def preload_choices_for_markets(
        session: Session, market_ids: Iterable[int]
    ) -> dict[tuple[int, str], MarketChoice]:
        ids = sorted({int(m) for m in market_ids if m is not None})
        if not ids:
            return {}
        choices = (
            session.query(MarketChoice)
            .filter(MarketChoice.market_id.in_(ids))
            .all()
        )
        return {
            (int(choice.market_id), str(choice.choice_name).strip().lower()): choice
            for choice in choices
        }

    @staticmethod
    def find_legacy_back_lay_choice_names(
        session: Session,
        *,
        event_id: int,
        bookie_id: int,
        market_name: str,
        market_period: str,
        line: Optional[str],
        is_live: bool,
    ) -> list[str]:
        """Choice names on legacy Back/Lay markets that share a canonical identity."""
        from modules.odds_ingestion.backfill.market_choice_quote_backfill import (
            parse_legacy_back_lay_choice_group,
        )

        period = str(market_period or "Full Time").strip() or "Full Time"
        name = str(market_name).strip()
        markets = (
            session.query(Market)
            .filter(
                Market.event_id == int(event_id),
                Market.bookie_id == int(bookie_id),
                Market.market_name == name,
                Market.market_period == period,
                Market.is_live == bool(is_live),
            )
            .all()
        )
        names: set[str] = set()
        normalized_line = str(line).strip() if line is not None else None
        if normalized_line == "":
            normalized_line = None
        for market in markets:
            side, parsed_line = parse_legacy_back_lay_choice_group(market.choice_group)
            if side is None:
                continue
            parsed = str(parsed_line).strip() if parsed_line is not None else None
            if parsed == "":
                parsed = None
            if parsed != normalized_line:
                continue
            for choice in (
                session.query(MarketChoice)
                .filter(MarketChoice.market_id == market.market_id)
                .all()
            ):
                choice_name = str(choice.choice_name or "").strip()
                if choice_name:
                    names.add(choice_name)
        return sorted(names)

    @classmethod
    def ensure_canonical_market_with_choices(
        cls,
        session: Session,
        *,
        event_id: int,
        bookie_id: int,
        market_name: str,
        market_group: str,
        market_period: str,
        choice_group: Optional[str],
        is_live: bool,
        choice_names: Sequence[str],
    ) -> tuple[Market, list[MarketChoice], bool, int]:
        """Get-or-create canonical market + choices for legacy Back/Lay rematerialization.

        Returns ``(market, choices, market_created, choices_created)``.
        """
        period = str(market_period or "Full Time").strip() or "Full Time"
        name = str(market_name).strip()
        group = str(market_group or "").strip() or None
        line = str(choice_group).strip() if choice_group is not None else None
        if line == "":
            line = None

        query = session.query(Market).filter(
            Market.event_id == int(event_id),
            Market.bookie_id == int(bookie_id),
            Market.market_name == name,
            Market.market_period == period,
            Market.is_live == bool(is_live),
        )
        if line is None:
            query = query.filter(Market.choice_group.is_(None))
        else:
            query = query.filter(Market.choice_group == line)
        market = query.one_or_none()
        market_created = False
        if market is None:
            market = Market(
                event_id=int(event_id),
                bookie_id=int(bookie_id),
                market_name=name,
                market_group=group,
                market_period=period,
                choice_group=line,
                is_live=bool(is_live),
                collected_at=get_local_now(),
            )
            session.add(market)
            session.flush()
            market_created = True
        elif group and not market.market_group:
            market.market_group = group

        existing = {
            str(c.choice_name).strip().lower(): c
            for c in session.query(MarketChoice)
            .filter(MarketChoice.market_id == market.market_id)
            .all()
        }
        created_choices = 0
        choices: list[MarketChoice] = []
        for raw_name in choice_names:
            choice_name = str(raw_name or "").strip()
            if not choice_name:
                continue
            key = choice_name.lower()
            choice = existing.get(key)
            if choice is None:
                choice = MarketChoice(
                    market_id=market.market_id,
                    choice_name=choice_name,
                )
                session.add(choice)
                existing[key] = choice
                created_choices += 1
            choices.append(choice)
        if created_choices:
            session.flush()
        return market, choices, market_created, created_choices

    @staticmethod
    def preload_quotes(
        session: Session,
        *,
        identities: Sequence[tuple[int, str, Optional[str], int]],
    ) -> dict[tuple[int, str, Optional[str], int], MarketChoiceQuote]:
        if not identities:
            return {}
        choice_ids = sorted({identity[0] for identity in identities})
        quotes = (
            session.query(MarketChoiceQuote)
            .filter(MarketChoiceQuote.choice_id.in_(choice_ids))
            .all()
        )
        index: dict[tuple[int, str, Optional[str], int], MarketChoiceQuote] = {}
        for quote in quotes:
            key = MarketChoiceQuoteWriter.identity_key(
                choice_id=quote.choice_id,
                source=quote.source,
                exchange_side=quote.exchange_side,
                exchange_level=quote.exchange_level or 0,
            )
            index[key] = quote
        return index

    @staticmethod
    def preload_exchange_quote_evidence(
        session: Session, choice_ids: Iterable[int]
    ) -> set[int]:
        """Choices that already have an explicit back/lay quote (exchange proof)."""
        ids = sorted({int(c) for c in choice_ids if c is not None})
        if not ids:
            return set()
        rows = (
            session.query(MarketChoiceQuote.choice_id)
            .filter(
                MarketChoiceQuote.choice_id.in_(ids),
                MarketChoiceQuote.exchange_side.in_(("back", "lay")),
            )
            .distinct()
            .all()
        )
        return {int(row[0]) for row in rows}

    @staticmethod
    def bulk_link_snapshots(
        session: Session,
        links: Sequence[tuple[int, int]],
        *,
        chunk_size: int = 500,
    ) -> int:
        """Set snapshot.quote_id for (snapshot_id, quote_id) pairs. No commit."""
        if not links:
            return 0
        updated = 0
        dialect = session.bind.dialect.name if session.bind is not None else "sqlite"
        for offset in range(0, len(links), chunk_size):
            chunk = links[offset : offset + chunk_size]
            if dialect == "postgresql":
                values_sql = ", ".join(
                    f"(:sid_{i}, :qid_{i})" for i in range(len(chunk))
                )
                params = {}
                for i, (snapshot_id, quote_id) in enumerate(chunk):
                    params[f"sid_{i}"] = int(snapshot_id)
                    params[f"qid_{i}"] = int(quote_id)
                session.execute(
                    text(
                        f"""
                        UPDATE market_choice_snapshots AS s
                        SET quote_id = v.quote_id
                        FROM (VALUES {values_sql}) AS v(snapshot_id, quote_id)
                        WHERE s.snapshot_id = v.snapshot_id
                          AND s.quote_id IS NULL
                        """
                    ),
                    params,
                )
            else:
                for snapshot_id, quote_id in chunk:
                    session.execute(
                        text(
                            """
                            UPDATE market_choice_snapshots
                            SET quote_id = :quote_id
                            WHERE snapshot_id = :snapshot_id
                              AND quote_id IS NULL
                            """
                        ),
                        {"snapshot_id": int(snapshot_id), "quote_id": int(quote_id)},
                    )
            updated += len(chunk)
        return updated

    @staticmethod
    def bookie_ids_uniquely_mapped_to_source(
        session: Session, source: str
    ) -> set[int]:
        """Bookie IDs whose provider mappings are exactly {source}.

        Ignores non-provider rows such as ``canonical``.
        """
        normalized = str(source or "").strip().lower()
        if not normalized or normalized not in PROVIDER_BOOKIE_SOURCES:
            return set()
        rows = (
            session.query(
                BookieSourceMapping.bookie_id,
                func.lower(BookieSourceMapping.source),
            )
            .filter(BookieSourceMapping.source.isnot(None))
            .all()
        )
        by_bookie: dict[int, set[str]] = {}
        for bookie_id, mapped_source in rows:
            value = str(mapped_source or "").strip().lower()
            if not value or value not in PROVIDER_BOOKIE_SOURCES:
                continue
            by_bookie.setdefault(int(bookie_id), set()).add(value)
        return {
            bookie_id
            for bookie_id, sources in by_bookie.items()
            if sources == {normalized}
        }

    @classmethod
    def _oddspapi_null_mainline_line_snapshot_filter(
        cls, session: Session, event_ids: Sequence[int]
    ):
        """Snapshots: oddspapi + main_line IS NULL + market.choice_group IS NOT NULL."""
        unique_oddspapi_bookies = cls.bookie_ids_uniquely_mapped_to_source(
            session, ODDSPAPI_SOURCE
        )
        source_predicate = func.lower(MarketChoiceSnapshot.source) == ODDSPAPI_SOURCE
        if unique_oddspapi_bookies:
            source_predicate = or_(
                source_predicate,
                and_(
                    MarketChoiceSnapshot.source.is_(None),
                    Market.bookie_id.in_(sorted(unique_oddspapi_bookies)),
                ),
            )
        return (
            session.query(
                MarketChoiceSnapshot.snapshot_id,
                MarketChoiceSnapshot.choice_id,
                Market.market_id,
                Market.event_id,
            )
            .join(MarketChoice, MarketChoice.choice_id == MarketChoiceSnapshot.choice_id)
            .join(Market, Market.market_id == MarketChoice.market_id)
            .filter(
                Market.event_id.in_(list(event_ids)),
                Market.choice_group.isnot(None),
                MarketChoiceSnapshot.main_line.is_(None),
                source_predicate,
            )
        )

    @classmethod
    def find_oddspapi_null_mainline_line_snapshots(
        cls,
        session: Session,
        *,
        event_ids: Sequence[int],
        limit: Optional[int] = None,
    ) -> list[tuple[int, int, int, int]]:
        """Return (snapshot_id, choice_id, market_id, event_id) purge matches."""
        if not event_ids:
            return []
        query = cls._oddspapi_null_mainline_line_snapshot_filter(
            session, event_ids
        ).order_by(MarketChoiceSnapshot.snapshot_id.asc())
        if limit is not None:
            query = query.limit(int(limit))
        return [
            (int(r[0]), int(r[1]), int(r[2]), int(r[3])) for r in query.all()
        ]

    @classmethod
    def select_purge_event_scope(
        cls,
        session: Session,
        *,
        event_id: Optional[int] = None,
        event_id_min: Optional[int] = None,
        event_id_max: Optional[int] = None,
        after_event_id: Optional[int] = None,
        max_events: Optional[int] = None,
    ) -> list[int]:
        """Event IDs that have at least one purge-matching snapshot."""
        if event_id is not None:
            return [int(event_id)]

        unique_oddspapi_bookies = cls.bookie_ids_uniquely_mapped_to_source(
            session, ODDSPAPI_SOURCE
        )
        source_predicate = func.lower(MarketChoiceSnapshot.source) == ODDSPAPI_SOURCE
        if unique_oddspapi_bookies:
            source_predicate = or_(
                source_predicate,
                and_(
                    MarketChoiceSnapshot.source.is_(None),
                    Market.bookie_id.in_(sorted(unique_oddspapi_bookies)),
                ),
            )
        query = (
            session.query(Market.event_id)
            .join(MarketChoice, MarketChoice.market_id == Market.market_id)
            .join(
                MarketChoiceSnapshot,
                MarketChoiceSnapshot.choice_id == MarketChoice.choice_id,
            )
            .filter(
                Market.choice_group.isnot(None),
                MarketChoiceSnapshot.main_line.is_(None),
                source_predicate,
            )
            .distinct()
        )
        if event_id_min is not None:
            query = query.filter(Market.event_id >= int(event_id_min))
        if event_id_max is not None:
            query = query.filter(Market.event_id <= int(event_id_max))
        if after_event_id is not None:
            query = query.filter(Market.event_id > int(after_event_id))
        query = query.order_by(Market.event_id.asc())
        if max_events is not None:
            query = query.limit(int(max_events))
        return [int(row[0]) for row in query.all()]

    @staticmethod
    def _legacy_back_lay_market_predicate():
        """SQL filter for OddsPortal-era ``choice_group`` values ``Back`` / ``Lay``.

        Matches ``Back``, ``Lay``, ``Back 2.5``, ``Lay 3.0``, case-insensitive.
        Kept as ILIKE (not Postgres regex) so SQLite tests stay portable.
        """
        return or_(
            Market.choice_group.ilike("Back"),
            Market.choice_group.ilike("Back %"),
            Market.choice_group.ilike("Lay"),
            Market.choice_group.ilike("Lay %"),
        )

    @classmethod
    def select_legacy_back_lay_event_scope(
        cls,
        session: Session,
        *,
        event_id: Optional[int] = None,
        event_id_min: Optional[int] = None,
        event_id_max: Optional[int] = None,
        after_event_id: Optional[int] = None,
        max_events: Optional[int] = None,
    ) -> list[int]:
        """Event IDs that still have at least one legacy Back/Lay market."""
        if event_id is not None:
            return [int(event_id)]
        query = (
            session.query(Market.event_id)
            .filter(cls._legacy_back_lay_market_predicate())
            .distinct()
        )
        if event_id_min is not None:
            query = query.filter(Market.event_id >= int(event_id_min))
        if event_id_max is not None:
            query = query.filter(Market.event_id <= int(event_id_max))
        if after_event_id is not None:
            query = query.filter(Market.event_id > int(after_event_id))
        query = query.order_by(Market.event_id.asc())
        if max_events is not None:
            query = query.limit(int(max_events))
        return [int(row[0]) for row in query.all()]

    @classmethod
    def purge_legacy_back_lay_markets(
        cls,
        session: Session,
        *,
        event_ids: Sequence[int],
        dry_run: bool,
    ) -> dict[str, Any]:
        """Delete OddsPortal-era Back/Lay markets and their choices/snapshots/quotes.

        Also deletes any remaining ``source=oddsportal`` quotes on markets in
        the same event scope (covers rematerialized canonical quotes the
        backfill used to create from these legacy rows). Snapshots that only
        pointed at those quotes are unlinked (``quote_id=NULL``) first so
        price history on non-legacy choices is not CASCADE-deleted; snapshots
        that live *on* the legacy Back/Lay choices themselves are deleted with
        the market.
        """
        ids = sorted({int(e) for e in event_ids if e is not None})
        result: dict[str, Any] = {
            "markets_matched": 0,
            "markets_deleted": 0,
            "choices_deleted": 0,
            "snapshots_deleted": 0,
            "quotes_deleted_on_legacy": 0,
            "oddsportal_quotes_unlinked_snaps": 0,
            "oddsportal_quotes_deleted": 0,
            "dry_run": dry_run,
            "sample_market_ids": [],
        }
        if not ids:
            return result

        legacy_markets = (
            session.query(Market.market_id)
            .filter(
                Market.event_id.in_(ids),
                cls._legacy_back_lay_market_predicate(),
            )
            .order_by(Market.market_id.asc())
            .all()
        )
        legacy_market_ids = [int(r[0]) for r in legacy_markets]
        result["markets_matched"] = len(legacy_market_ids)
        result["sample_market_ids"] = legacy_market_ids[:50]

        legacy_choice_ids: list[int] = []
        if legacy_market_ids:
            legacy_choice_ids = [
                int(r[0])
                for r in session.query(MarketChoice.choice_id)
                .filter(MarketChoice.market_id.in_(legacy_market_ids))
                .all()
            ]

        oddsportal_quote_ids = [
            int(r[0])
            for r in (
                session.query(MarketChoiceQuote.quote_id)
                .join(MarketChoice, MarketChoice.choice_id == MarketChoiceQuote.choice_id)
                .join(Market, Market.market_id == MarketChoice.market_id)
                .filter(
                    Market.event_id.in_(ids),
                    func.lower(MarketChoiceQuote.source).like("oddsportal%"),
                )
                .all()
            )
        ]

        snap_on_legacy = 0
        if legacy_choice_ids:
            snap_on_legacy = (
                session.query(func.count(MarketChoiceSnapshot.snapshot_id))
                .filter(MarketChoiceSnapshot.choice_id.in_(legacy_choice_ids))
                .scalar()
                or 0
            )
        quotes_on_legacy = 0
        if legacy_choice_ids:
            quotes_on_legacy = (
                session.query(func.count(MarketChoiceQuote.quote_id))
                .filter(MarketChoiceQuote.choice_id.in_(legacy_choice_ids))
                .scalar()
                or 0
            )

        result["snapshots_deleted"] = int(snap_on_legacy)
        result["choices_deleted"] = len(legacy_choice_ids)
        result["quotes_deleted_on_legacy"] = int(quotes_on_legacy)
        result["oddsportal_quotes_deleted"] = len(oddsportal_quote_ids)
        if oddsportal_quote_ids:
            result["oddsportal_quotes_unlinked_snaps"] = int(
                session.query(func.count(MarketChoiceSnapshot.snapshot_id))
                .filter(MarketChoiceSnapshot.quote_id.in_(oddsportal_quote_ids))
                .scalar()
                or 0
            )

        if dry_run:
            result["markets_deleted"] = len(legacy_market_ids)
            return result

        if oddsportal_quote_ids:
            session.query(MarketChoiceSnapshot).filter(
                MarketChoiceSnapshot.quote_id.in_(oddsportal_quote_ids)
            ).update({MarketChoiceSnapshot.quote_id: None}, synchronize_session=False)
            session.query(MarketChoiceQuote).filter(
                MarketChoiceQuote.quote_id.in_(oddsportal_quote_ids)
            ).delete(synchronize_session=False)

        if legacy_choice_ids:
            session.query(MarketChoiceSnapshot).filter(
                MarketChoiceSnapshot.choice_id.in_(legacy_choice_ids)
            ).delete(synchronize_session=False)
            session.query(MarketChoiceQuote).filter(
                MarketChoiceQuote.choice_id.in_(legacy_choice_ids)
            ).delete(synchronize_session=False)
            session.query(MarketChoice).filter(
                MarketChoice.choice_id.in_(legacy_choice_ids)
            ).delete(synchronize_session=False)

        if legacy_market_ids:
            session.query(Market).filter(
                Market.market_id.in_(legacy_market_ids)
            ).delete(synchronize_session=False)
        result["markets_deleted"] = len(legacy_market_ids)
        return result

    @classmethod
    def select_ambiguous_choice_state_event_scope(
        cls,
        session: Session,
        *,
        bookie_ids: Sequence[int],
        event_id: Optional[int] = None,
        event_id_min: Optional[int] = None,
        event_id_max: Optional[int] = None,
        after_event_id: Optional[int] = None,
        max_events: Optional[int] = None,
    ) -> list[int]:
        """Events with snapless odds mirrors on the given OddsPortal-era bookies."""
        ids = sorted({int(b) for b in bookie_ids if b is not None})
        if not ids:
            return []
        if event_id is not None:
            return [int(event_id)]
        query = (
            session.query(Market.event_id)
            .join(MarketChoice, MarketChoice.market_id == Market.market_id)
            .filter(
                Market.bookie_id.in_(ids),
                (MarketChoice.initial_odds.isnot(None))
                | (MarketChoice.current_odds.isnot(None)),
                ~session.query(MarketChoiceSnapshot.snapshot_id)
                .filter(MarketChoiceSnapshot.choice_id == MarketChoice.choice_id)
                .exists(),
            )
            .distinct()
        )
        if event_id_min is not None:
            query = query.filter(Market.event_id >= int(event_id_min))
        if event_id_max is not None:
            query = query.filter(Market.event_id <= int(event_id_max))
        if after_event_id is not None:
            query = query.filter(Market.event_id > int(after_event_id))
        query = query.order_by(Market.event_id.asc())
        if max_events is not None:
            query = query.limit(int(max_events))
        return [int(row[0]) for row in query.all()]

    @classmethod
    def purge_ambiguous_choice_states(
        cls,
        session: Session,
        *,
        event_ids: Sequence[int],
        bookie_ids: Sequence[int],
        dry_run: bool,
    ) -> dict[str, Any]:
        """Delete snapless MarketChoice mirrors on OddsPortal-era bookies.

        These are the ``choice_state`` rows the classifier leaves as
        ``ambiguous_choice_state`` (no snapshots, null source, bookie historically
        shared with OddsPortal). Deletes choices + any quotes on them, then
        markets left with zero choices.
        """
        events = sorted({int(e) for e in event_ids if e is not None})
        bookies = sorted({int(b) for b in bookie_ids if b is not None})
        result: dict[str, Any] = {
            "choices_matched": 0,
            "choices_deleted": 0,
            "quotes_deleted": 0,
            "markets_deleted": 0,
            "dry_run": dry_run,
            "bookie_ids": bookies,
            "sample_choice_ids": [],
        }
        if not events or not bookies:
            return result

        choice_rows = (
            session.query(MarketChoice.choice_id, Market.market_id)
            .join(Market, Market.market_id == MarketChoice.market_id)
            .filter(
                Market.event_id.in_(events),
                Market.bookie_id.in_(bookies),
                (MarketChoice.initial_odds.isnot(None))
                | (MarketChoice.current_odds.isnot(None)),
                ~session.query(MarketChoiceSnapshot.snapshot_id)
                .filter(MarketChoiceSnapshot.choice_id == MarketChoice.choice_id)
                .exists(),
            )
            .all()
        )
        choice_ids = [int(r[0]) for r in choice_rows]
        market_ids = sorted({int(r[1]) for r in choice_rows})
        result["choices_matched"] = len(choice_ids)
        result["sample_choice_ids"] = choice_ids[:50]
        if not choice_ids:
            return result

        quotes_count = (
            session.query(func.count(MarketChoiceQuote.quote_id))
            .filter(MarketChoiceQuote.choice_id.in_(choice_ids))
            .scalar()
            or 0
        )
        result["quotes_deleted"] = int(quotes_count)

        if dry_run:
            orphan_markets = 0
            for market_id in market_ids:
                other = (
                    session.query(MarketChoice.choice_id)
                    .filter(
                        MarketChoice.market_id == market_id,
                        ~MarketChoice.choice_id.in_(choice_ids),
                    )
                    .first()
                )
                if other is None:
                    orphan_markets += 1
            result["choices_deleted"] = len(choice_ids)
            result["markets_deleted"] = orphan_markets
            return result

        session.query(MarketChoiceQuote).filter(
            MarketChoiceQuote.choice_id.in_(choice_ids)
        ).delete(synchronize_session=False)
        session.query(MarketChoice).filter(
            MarketChoice.choice_id.in_(choice_ids)
        ).delete(synchronize_session=False)
        result["choices_deleted"] = len(choice_ids)

        markets_deleted = 0
        for market_id in market_ids:
            still = (
                session.query(MarketChoice.choice_id)
                .filter(MarketChoice.market_id == market_id)
                .first()
            )
            if still is not None:
                continue
            session.query(Market).filter(Market.market_id == market_id).delete(
                synchronize_session=False
            )
            markets_deleted += 1
        result["markets_deleted"] = markets_deleted
        return result

    @classmethod
    def purge_markets_outside_allowed_bookies(
        cls,
        session: Session,
        *,
        allowed_bookie_ids: Sequence[int],
        dry_run: bool,
        market_batch_size: int = 2000,
    ) -> dict[str, Any]:
        """Delete every market whose bookie_id is not in ``allowed_bookie_ids``.

        Also deletes dependent choices, snapshots, and quotes. ``bookie_id IS
        NULL`` is treated as disallowed. Runs globally (not event-scoped).
        """
        allowed = sorted({int(b) for b in allowed_bookie_ids if b is not None})
        result: dict[str, Any] = {
            "allowed_bookie_ids": allowed,
            "markets_matched": 0,
            "markets_deleted": 0,
            "choices_deleted": 0,
            "snapshots_deleted": 0,
            "quotes_deleted": 0,
            "dry_run": dry_run,
            "sample_market_ids": [],
        }
        if not allowed:
            raise ValueError("allowed_bookie_ids must not be empty")

        market_ids = [
            int(r[0])
            for r in session.query(Market.market_id)
            .filter(
                or_(
                    Market.bookie_id.is_(None),
                    ~Market.bookie_id.in_(allowed),
                )
            )
            .order_by(Market.market_id.asc())
            .all()
        ]
        result["markets_matched"] = len(market_ids)
        result["sample_market_ids"] = market_ids[:50]
        if not market_ids:
            return result

        disallowed = or_(
            Market.bookie_id.is_(None),
            ~Market.bookie_id.in_(allowed),
        )
        choices_count = int(
            session.query(func.count(MarketChoice.choice_id))
            .join(Market, Market.market_id == MarketChoice.market_id)
            .filter(disallowed)
            .scalar()
            or 0
        )
        snaps = int(
            session.query(func.count(MarketChoiceSnapshot.snapshot_id))
            .join(MarketChoice, MarketChoice.choice_id == MarketChoiceSnapshot.choice_id)
            .join(Market, Market.market_id == MarketChoice.market_id)
            .filter(disallowed)
            .scalar()
            or 0
        )
        quotes = int(
            session.query(func.count(MarketChoiceQuote.quote_id))
            .join(MarketChoice, MarketChoice.choice_id == MarketChoiceQuote.choice_id)
            .join(Market, Market.market_id == MarketChoice.market_id)
            .filter(disallowed)
            .scalar()
            or 0
        )
        result["choices_deleted"] = choices_count
        result["snapshots_deleted"] = snaps
        result["quotes_deleted"] = quotes

        if dry_run:
            result["markets_deleted"] = len(market_ids)
            return result

        # Batch deletes to keep lock/memory bounded on large DBs.
        batch = max(1, int(market_batch_size))
        for offset in range(0, len(market_ids), batch):
            chunk_markets = market_ids[offset : offset + batch]
            chunk_choices = [
                int(r[0])
                for r in session.query(MarketChoice.choice_id)
                .filter(MarketChoice.market_id.in_(chunk_markets))
                .all()
            ]
            if chunk_choices:
                session.query(MarketChoiceSnapshot).filter(
                    MarketChoiceSnapshot.choice_id.in_(chunk_choices)
                ).delete(synchronize_session=False)
                session.query(MarketChoiceQuote).filter(
                    MarketChoiceQuote.choice_id.in_(chunk_choices)
                ).delete(synchronize_session=False)
                session.query(MarketChoice).filter(
                    MarketChoice.choice_id.in_(chunk_choices)
                ).delete(synchronize_session=False)
            session.query(Market).filter(
                Market.market_id.in_(chunk_markets)
            ).delete(synchronize_session=False)
        result["markets_deleted"] = len(market_ids)
        return result

    @classmethod
    def purge_oddspapi_null_mainline_line_markets(
        cls,
        session: Session,
        *,
        event_ids: Sequence[int],
        dry_run: bool,
    ) -> dict[str, Any]:
        """Delete matching snapshots, then orphan choices/markets with no ticks.

        Orphan cleanup only touches choices/markets that lost at least one
        purged snapshot in this pass (does not delete unrelated choice-state
        rows on other markets).
        """
        matches = cls.find_oddspapi_null_mainline_line_snapshots(
            session, event_ids=event_ids
        )
        snapshot_ids = [row[0] for row in matches]
        touched_choice_ids = sorted({row[1] for row in matches})
        touched_market_ids = sorted({row[2] for row in matches})
        result: dict[str, Any] = {
            "snapshots_matched": len(snapshot_ids),
            "snapshots_deleted": 0,
            "choices_deleted": 0,
            "markets_deleted": 0,
            "dry_run": dry_run,
            "sample_snapshot_ids": snapshot_ids[:50],
            "touched_choice_ids": touched_choice_ids[:50],
            "touched_market_ids": touched_market_ids[:50],
        }
        if not snapshot_ids:
            return result
        if dry_run:
            # Estimate orphans: choices among touched with only purge-matched snaps
            # remaining after a hypothetical delete of matched snapshot_ids.
            matched_set = set(snapshot_ids)
            orphan_choices = 0
            for choice_id in touched_choice_ids:
                remaining = (
                    session.query(MarketChoiceSnapshot.snapshot_id)
                    .filter(MarketChoiceSnapshot.choice_id == choice_id)
                    .all()
                )
                if all(int(r[0]) in matched_set for r in remaining):
                    orphan_choices += 1
            orphan_markets = 0
            for market_id in touched_market_ids:
                choice_rows = (
                    session.query(MarketChoice.choice_id)
                    .filter(MarketChoice.market_id == market_id)
                    .all()
                )
                would_remain = False
                for (choice_id,) in choice_rows:
                    if int(choice_id) not in set(touched_choice_ids):
                        would_remain = True
                        break
                    remaining = (
                        session.query(MarketChoiceSnapshot.snapshot_id)
                        .filter(MarketChoiceSnapshot.choice_id == int(choice_id))
                        .all()
                    )
                    if not all(int(r[0]) in matched_set for r in remaining):
                        would_remain = True
                        break
                if not would_remain:
                    orphan_markets += 1
            result["choices_deleted"] = orphan_choices
            result["markets_deleted"] = orphan_markets
            return result

        # Chunk IN-lists: Postgres bind-parameter limit is 65535; dense
        # event scopes routinely match 80k+ null-mainline snapshots.
        delete_chunk = 5000
        snapshots_deleted = 0
        for offset in range(0, len(snapshot_ids), delete_chunk):
            chunk = snapshot_ids[offset : offset + delete_chunk]
            session.query(MarketChoiceSnapshot).filter(
                MarketChoiceSnapshot.snapshot_id.in_(chunk)
            ).delete(synchronize_session=False)
            snapshots_deleted += len(chunk)
        result["snapshots_deleted"] = snapshots_deleted

        choices_deleted = 0
        for choice_id in touched_choice_ids:
            still_has_ticks = (
                session.query(MarketChoiceSnapshot.snapshot_id)
                .filter(MarketChoiceSnapshot.choice_id == choice_id)
                .first()
                is not None
            )
            if still_has_ticks:
                continue
            # Explicit quote cleanup: SQLite may not enforce ON DELETE CASCADE.
            session.query(MarketChoiceQuote).filter(
                MarketChoiceQuote.choice_id == choice_id
            ).delete(synchronize_session=False)
            session.query(MarketChoice).filter(
                MarketChoice.choice_id == choice_id
            ).delete(synchronize_session=False)
            choices_deleted += 1
        result["choices_deleted"] = choices_deleted

        markets_deleted = 0
        for market_id in touched_market_ids:
            still_has_choices = (
                session.query(MarketChoice.choice_id)
                .filter(MarketChoice.market_id == market_id)
                .first()
                is not None
            )
            if still_has_choices:
                continue
            session.query(Market).filter(Market.market_id == market_id).delete(
                synchronize_session=False
            )
            markets_deleted += 1
        result["markets_deleted"] = markets_deleted
        return result

    @staticmethod
    def schema_preflight(session: Session) -> list[str]:
        """Return human-readable preflight errors (empty = ok). Read-only."""
        errors: list[str] = []
        bind = session.get_bind()
        inspector = __import__("sqlalchemy", fromlist=["inspect"]).inspect(bind)
        tables = set(inspector.get_table_names())
        if "market_choice_quotes" not in tables:
            errors.append("missing table market_choice_quotes")
        if "market_choice_snapshots" not in tables:
            errors.append("missing table market_choice_snapshots")
            return errors

        snapshot_cols = {
            col["name"]
            for col in inspector.get_columns("market_choice_snapshots")
        }
        expanded_columns = {
            "choice_id",
            "quote_id",
            "source",
            "source_market_id",
            "source_outcome_id",
            "bookmaker_outcome_id",
            "main_line",
            "exchange_side",
            "exchange_level",
        }
        missing_expanded = sorted(expanded_columns - snapshot_cols)
        if missing_expanded:
            errors.append(
                "Phase 4 quote backfill is retired by the Phase 6 slim schema; "
                "missing legacy snapshot columns: " + ", ".join(missing_expanded)
            )

        indexes = inspector.get_indexes("market_choice_snapshots")
        index_names = {idx.get("name") for idx in indexes}
        if "idx_market_choice_snapshots_quote_collected" not in index_names:
            # SQLite create_all may omit the named composite; accept PK presence
            # only when quote_id column exists and quotes table exists.
            if bind.dialect.name == "postgresql":
                errors.append(
                    "missing index idx_market_choice_snapshots_quote_collected"
                )

        if "market_choice_quotes" in tables:
            quote_indexes = inspector.get_indexes("market_choice_quotes")
            quote_index_names = {idx.get("name") for idx in quote_indexes}
            # Functional unique index is Postgres-only; SQLite uses UniqueConstraint.
            if (
                bind.dialect.name == "postgresql"
                and "unique_market_choice_quote_side_null_safe" not in quote_index_names
            ):
                # Also accept if created via migration with slightly different discovery
                pass

            legacy_single = session.execute(
                text(
                    "SELECT COUNT(*) FROM market_choice_quotes "
                    "WHERE lower(coalesce(exchange_side, '')) = 'single'"
                )
            ).scalar()
            if int(legacy_single or 0) > 0:
                errors.append("legacy exchange_side='single' still present in quotes")

        return errors
