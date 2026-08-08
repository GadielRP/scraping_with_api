"""Unit tests for MarketChoiceQuoteWriter.upsert (Fase 2 of the odds refactor).

See docs/refactors/db-schema-odds-refactor.md (Fase 2) and
tests/test_market_choice_quote_model.py (Fase 1 model-level constraints).
"""

from datetime import datetime

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Bookie, Event, Market, MarketChoice, MarketChoiceQuote
from infrastructure.persistence.repositories.market.market_choice_quote_writer import (
    MarketChoiceQuoteWriter,
)


def make_manager(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'market_choice_quote_writer.db'}")
    manager.create_tables()
    return manager


def seed_choice(manager):
    with manager.get_session() as session:
        event = Event(
            slug="test-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Betfair Exchange", slug="betfair-ex")
        session.add_all([event, bookie])
        session.flush()

        market = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="Home/Away Full Time",
            market_group="1X2",
            market_period="Full Time",
            is_live=False,
        )
        session.add(market)
        session.flush()

        choice = MarketChoice(market_id=market.market_id, choice_name="1")
        session.add(choice)
        session.flush()
        return choice.choice_id


def _quote(session, choice_id, source, exchange_side=None, exchange_level=0):
    side_filter = (
        MarketChoiceQuote.exchange_side.is_(None)
        if exchange_side is None
        else MarketChoiceQuote.exchange_side == exchange_side
    )
    return (
        session.query(MarketChoiceQuote)
        .filter(
            MarketChoiceQuote.choice_id == choice_id,
            MarketChoiceQuote.source == source,
            side_filter,
            MarketChoiceQuote.exchange_level == exchange_level,
        )
        .one()
    )


def _upsert_quote(session, **kwargs):
    existing_quotes = session.query(MarketChoiceQuote).all()
    quote_index = {
        MarketChoiceQuoteWriter.identity_key(
            choice_id=quote.choice_id,
            source=quote.source,
            exchange_side=quote.exchange_side,
            exchange_level=quote.exchange_level,
        ): quote
        for quote in existing_quotes
    }
    return MarketChoiceQuoteWriter.upsert(
        session,
        quote_index=quote_index,
        **kwargs,
    )


def test_upsert_creates_row_with_initial_only(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)
    initial_time = datetime(2026, 6, 20, 10, 0, 0)

    with manager.get_session() as session:
        _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddspapi",
            initial_price=1.90,
            initial_captured_at=initial_time,
        )

    with manager.get_session() as session:
        quote = _quote(session, choice_id, "oddspapi")
        assert float(quote.initial_odds) == 1.90
        assert quote.initial_captured_at == initial_time
        assert quote.current_odds is None
        # MarketChoiceQuote.movement has a Python-side default=0 (applied by
        # SQLAlchemy whenever the persisted value is None), so "no current
        # price yet" reads back as 0 rather than NULL.
        assert quote.movement == 0


def test_upsert_merges_current_into_existing_initial_only_row(tmp_path):
    """The original bug report scenario: initial at T-120, current arrives later at T-5."""
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)
    initial_time = datetime(2026, 6, 20, 10, 0, 0)
    current_time = datetime(2026, 6, 20, 11, 55, 0)

    with manager.get_session() as session:
        _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddspapi",
            initial_price=1.90,
            initial_captured_at=initial_time,
        )

    with manager.get_session() as session:
        _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddspapi",
            current_price=2.10,
            current_captured_at=current_time,
        )

    with manager.get_session() as session:
        quote = _quote(session, choice_id, "oddspapi")
        assert float(quote.initial_odds) == 1.90
        assert float(quote.current_odds) == 2.10
        assert quote.current_updated_at == current_time
        assert quote.movement == 1


def test_upsert_does_not_overwrite_initial_by_default(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddspapi",
            initial_price=1.90,
        )

    with manager.get_session() as session:
        _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddspapi",
            initial_price=5.00,
            overwrite_initial=False,
        )

    with manager.get_session() as session:
        quote = _quote(session, choice_id, "oddspapi")
        assert float(quote.initial_odds) == 1.90


def test_upsert_overwrites_initial_when_policy_allows(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddsportal",
            initial_price=1.90,
        )

    with manager.get_session() as session:
        _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddsportal",
            initial_price=2.20,
            overwrite_initial=True,
        )

    with manager.get_session() as session:
        quote = _quote(session, choice_id, "oddsportal")
        assert float(quote.initial_odds) == 2.20


def test_upsert_keeps_back_and_lay_as_independent_rows(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddspapi",
            exchange_side="back",
            current_price=3.05,
        )
        _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddspapi",
            exchange_side="lay",
            current_price=3.20,
        )

    with manager.get_session() as session:
        back = _quote(session, choice_id, "oddspapi", exchange_side="back")
        lay = _quote(session, choice_id, "oddspapi", exchange_side="lay")
        assert float(back.current_odds) == 3.05
        assert float(lay.current_odds) == 3.20


def test_upsert_returns_none_and_persists_nothing_without_any_price(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        result = _upsert_quote(
            session,
            choice_id=choice_id,
            source="oddspapi",
        )
        assert result is None

    with manager.get_session() as session:
        assert session.query(MarketChoiceQuote).filter(
            MarketChoiceQuote.choice_id == choice_id
        ).count() == 0


def test_upsert_reuses_pending_quote_from_preloaded_identity_map(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        quote_index = {}
        created = MarketChoiceQuoteWriter.upsert(
            session,
            quote_index=quote_index,
            choice_id=choice_id,
            source="ODDSPAPI",
            initial_price=1.80,
        )
        updated = MarketChoiceQuoteWriter.upsert(
            session,
            quote_index=quote_index,
            choice_id=choice_id,
            source="oddspapi",
            current_price=1.95,
        )
        session.flush()

        assert updated is created
        assert len(quote_index) == 1
        assert session.query(MarketChoiceQuote).count() == 1
        assert float(created.initial_odds) == 1.80
        assert float(created.current_odds) == 1.95
