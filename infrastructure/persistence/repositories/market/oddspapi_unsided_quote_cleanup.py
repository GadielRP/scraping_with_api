"""Audit and remove redundant Oddspapi side-agnostic exchange quotes."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from sqlalchemy import and_, delete, exists, func, or_, select
from sqlalchemy.orm import aliased

from infrastructure.persistence.models import MarketChoiceQuote, MarketChoiceSnapshot


SOURCE = "oddspapi"


class OddspapiUnsidedQuoteCleanupBlocked(RuntimeError):
    """Raised when historical data cannot be deleted without ambiguity."""


@dataclass(frozen=True, slots=True)
class OddspapiUnsidedQuoteCleanupReport:
    candidate_quotes: int
    candidate_choices: int
    safe_to_delete: int
    quotes_with_snapshots: int
    dependent_snapshots: int
    missing_top_back_quote: int
    price_mismatches: int
    sample_quote_ids: tuple[int, ...]
    blockers: tuple[str, ...]

    @property
    def ready_to_purge(self) -> bool:
        return not self.blockers and self.candidate_quotes == self.safe_to_delete

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["sample_quote_ids"] = list(self.sample_quote_ids)
        payload["blockers"] = list(self.blockers)
        payload["ready_to_purge"] = self.ready_to_purge
        return payload


class OddspapiUnsidedQuoteCleanup:
    """Fail-closed cleanup for quotes superseded by an exact top back quote.

    A candidate must be an Oddspapi ``NULL/0`` quote whose choice already has
    an explicit back or lay quote. It is deletable only when a ``back/0``
    quote exists, its initial/current prices are null-safely equal, and no
    snapshots reference the candidate. Snapshot history is never cascaded.
    """

    @staticmethod
    def _null_safe_equal(left, right):
        return or_(left == right, and_(left.is_(None), right.is_(None)))

    @classmethod
    def _predicates(cls):
        unsided = MarketChoiceQuote
        explicit = aliased(MarketChoiceQuote)
        top_back = aliased(MarketChoiceQuote)

        candidate = and_(
            unsided.source == SOURCE,
            unsided.exchange_side.is_(None),
            unsided.exchange_level == 0,
            exists().where(
                and_(
                    explicit.choice_id == unsided.choice_id,
                    explicit.source == unsided.source,
                    explicit.exchange_side.in_(("back", "lay")),
                )
            ),
        )
        top_back_exists = exists().where(
            and_(
                top_back.choice_id == unsided.choice_id,
                top_back.source == unsided.source,
                top_back.exchange_side == "back",
                top_back.exchange_level == 0,
            )
        )
        matching_top_back = exists().where(
            and_(
                top_back.choice_id == unsided.choice_id,
                top_back.source == unsided.source,
                top_back.exchange_side == "back",
                top_back.exchange_level == 0,
                cls._null_safe_equal(top_back.initial_odds, unsided.initial_odds),
                cls._null_safe_equal(top_back.current_odds, unsided.current_odds),
            )
        )
        has_snapshots = exists().where(
            MarketChoiceSnapshot.quote_id == unsided.quote_id
        )
        safe = and_(candidate, matching_top_back, ~has_snapshots)
        return candidate, top_back_exists, matching_top_back, has_snapshots, safe

    @classmethod
    def audit(cls, session) -> OddspapiUnsidedQuoteCleanupReport:
        (
            candidate,
            top_back_exists,
            matching_top_back,
            has_snapshots,
            safe,
        ) = cls._predicates()

        def count(*filters) -> int:
            return int(
                session.query(func.count(MarketChoiceQuote.quote_id))
                .filter(*filters)
                .scalar()
                or 0
            )

        candidate_ids = (
            session.query(MarketChoiceQuote.quote_id).filter(candidate).subquery()
        )
        candidate_quotes = count(candidate)
        quotes_with_snapshots = count(candidate, has_snapshots)
        dependent_snapshots = int(
            session.query(func.count(MarketChoiceSnapshot.snapshot_id))
            .filter(
                MarketChoiceSnapshot.quote_id.in_(select(candidate_ids.c.quote_id))
            )
            .scalar()
            or 0
        )
        missing_top_back_quote = count(candidate, ~top_back_exists)
        price_mismatches = count(candidate, top_back_exists, ~matching_top_back)
        safe_to_delete = count(safe)
        candidate_choices = int(
            session.query(func.count(func.distinct(MarketChoiceQuote.choice_id)))
            .filter(candidate)
            .scalar()
            or 0
        )
        sample_quote_ids = tuple(
            int(row[0])
            for row in (
                session.query(MarketChoiceQuote.quote_id)
                .filter(candidate)
                .order_by(MarketChoiceQuote.quote_id)
                .limit(20)
                .all()
            )
        )

        blockers = []
        if quotes_with_snapshots:
            blockers.append("candidate_quotes_have_snapshots")
        if missing_top_back_quote:
            blockers.append("candidate_quotes_missing_top_back")
        if price_mismatches:
            blockers.append("candidate_prices_differ_from_top_back")
        if candidate_quotes != safe_to_delete and not blockers:
            blockers.append("candidate_set_not_fully_safe")

        return OddspapiUnsidedQuoteCleanupReport(
            candidate_quotes=candidate_quotes,
            candidate_choices=candidate_choices,
            safe_to_delete=safe_to_delete,
            quotes_with_snapshots=quotes_with_snapshots,
            dependent_snapshots=dependent_snapshots,
            missing_top_back_quote=missing_top_back_quote,
            price_mismatches=price_mismatches,
            sample_quote_ids=sample_quote_ids,
            blockers=tuple(blockers),
        )

    @classmethod
    def purge(cls, session) -> tuple[int, OddspapiUnsidedQuoteCleanupReport]:
        before = cls.audit(session)
        if not before.ready_to_purge:
            raise OddspapiUnsidedQuoteCleanupBlocked(
                "Unsafe Oddspapi NULL quotes found: " + ", ".join(before.blockers)
            )

        *_, safe = cls._predicates()
        safe_ids = (
            session.query(MarketChoiceQuote.quote_id).filter(safe).subquery()
        )
        result = session.execute(
            delete(MarketChoiceQuote).where(
                MarketChoiceQuote.quote_id.in_(select(safe_ids.c.quote_id))
            )
        )
        session.flush()
        deleted = int(result.rowcount or 0)
        if deleted != before.candidate_quotes:
            raise OddspapiUnsidedQuoteCleanupBlocked(
                "Cleanup row count changed during the transaction: "
                f"expected={before.candidate_quotes}, deleted={deleted}"
            )

        after = cls.audit(session)
        if after.candidate_quotes:
            raise OddspapiUnsidedQuoteCleanupBlocked(
                f"Cleanup postflight found {after.candidate_quotes} remaining candidates"
            )
        return deleted, after
