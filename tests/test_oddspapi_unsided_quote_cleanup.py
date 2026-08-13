from datetime import datetime

import pytest

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories.market.oddspapi_unsided_quote_cleanup import (
    OddspapiUnsidedQuoteCleanup,
    OddspapiUnsidedQuoteCleanupBlocked,
)


def _make_manager(tmp_path, name):
    manager = DatabaseManager(f"sqlite:///{tmp_path / name}")
    manager.create_tables()
    return manager


def _seed_choice(session, *, suffix):
    event = Event(
        slug=f"cleanup-{suffix}",
        start_time_utc=datetime(2026, 8, 13, 12, 0, 0),
        sport="Football",
        competition="Cleanup League",
        home_team="Home",
        away_team="Away",
    )
    bookie = Bookie(name=f"Betfair {suffix}", slug=f"betfair-{suffix}")
    session.add_all([event, bookie])
    session.flush()
    market = Market(
        event_id=event.id,
        bookie_id=bookie.bookie_id,
        market_name="1X2 Full Time",
        market_period="Full Time",
        is_live=False,
    )
    session.add(market)
    session.flush()
    choice = MarketChoice(market_id=market.market_id, choice_name="1")
    session.add(choice)
    session.flush()
    return choice


def _quote(
    session,
    choice,
    *,
    side,
    initial=1.9,
    current=2.1,
    source="oddspapi",
):
    quote = MarketChoiceQuote(
        choice_id=choice.choice_id,
        source=source,
        exchange_side=side,
        exchange_level=0,
        initial_odds=initial,
        current_odds=current,
    )
    session.add(quote)
    session.flush()
    return quote


def test_purge_removes_only_safe_oddspapi_null_quote(tmp_path):
    manager = _make_manager(tmp_path, "safe.db")
    with manager.get_session() as session:
        choice = _seed_choice(session, suffix="safe")
        unsided = _quote(session, choice, side=None)
        back = _quote(session, choice, side="back")
        _quote(session, choice, side=None, source="another-source")

        report = OddspapiUnsidedQuoteCleanup.audit(session)
        assert report.ready_to_purge
        assert report.candidate_quotes == 1
        assert report.safe_to_delete == 1
        assert report.sample_quote_ids == (unsided.quote_id,)

        deleted, after = OddspapiUnsidedQuoteCleanup.purge(session)
        assert deleted == 1
        assert after.candidate_quotes == 0
        assert session.get(MarketChoiceQuote, unsided.quote_id) is None
        assert session.get(MarketChoiceQuote, back.quote_id) is not None
        assert session.query(MarketChoiceQuote).count() == 2


def test_purge_aborts_atomically_for_ambiguous_or_historical_quotes(tmp_path):
    manager = _make_manager(tmp_path, "blocked.db")
    with manager.get_session() as session:
        mismatch = _seed_choice(session, suffix="mismatch")
        _quote(session, mismatch, side=None, current=2.0)
        _quote(session, mismatch, side="back", current=2.1)

        historical = _seed_choice(session, suffix="historical")
        historical_null = _quote(session, historical, side=None)
        _quote(session, historical, side="back")
        session.add(
            MarketChoiceSnapshot(
                quote_id=historical_null.quote_id,
                odds_value=2.1,
                collected_at=datetime(2026, 8, 13, 11, 0, 0),
            )
        )

        lay_only = _seed_choice(session, suffix="lay-only")
        _quote(session, lay_only, side=None)
        _quote(session, lay_only, side="lay", current=2.2)
        session.flush()

        report = OddspapiUnsidedQuoteCleanup.audit(session)
        assert not report.ready_to_purge
        assert report.candidate_quotes == 3
        assert report.safe_to_delete == 0
        assert report.quotes_with_snapshots == 1
        assert report.dependent_snapshots == 1
        assert report.missing_top_back_quote == 1
        assert report.price_mismatches == 1

        before_count = session.query(MarketChoiceQuote).count()
        with pytest.raises(OddspapiUnsidedQuoteCleanupBlocked):
            OddspapiUnsidedQuoteCleanup.purge(session)
        assert session.query(MarketChoiceQuote).count() == before_count
