"""Model-level tests for MarketChoiceQuote (Fase 1 of the odds schema refactor).

See docs/refactors/db-schema-odds-refactor.md §5 for the design rationale:
exchange_side must default to 'single' (never NULL) so the unique constraint
(choice_id, source, exchange_side, exchange_level) actually prevents
duplicate rows for non-exchange bookies.
"""

from datetime import datetime

import pytest
from sqlalchemy.exc import IntegrityError

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Bookie, Event, Market, MarketChoice, MarketChoiceQuote


def make_manager(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'market_choice_quotes.db'}")
    manager.create_tables()
    return manager


def seed_choice(manager, *, choice_group=None):
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
            choice_group=choice_group,
            is_live=False,
        )
        session.add(market)
        session.flush()

        choice = MarketChoice(market_id=market.market_id, choice_name="1")
        session.add(choice)
        session.flush()
        return choice.choice_id


def test_exchange_side_defaults_to_single_sentinel(tmp_path):
    """A quote created without exchange_side must NOT persist as NULL.

    NULL != NULL in a UNIQUE constraint, so leaving this nullable would let
    duplicate 'no side' rows for the same (choice_id, source) slip through.
    """
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        quote = MarketChoiceQuote(
            choice_id=choice_id,
            source="oddspapi",
            current_odds=1.90,
        )
        session.add(quote)
        session.flush()
        session.refresh(quote)
        assert quote.exchange_side == "single"
        assert quote.exchange_level == 0


def test_duplicate_single_quote_for_same_source_is_rejected(tmp_path):
    """Two 'single' quotes for the same (choice, source) must violate the unique constraint."""
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        session.add(MarketChoiceQuote(choice_id=choice_id, source="oddspapi", current_odds=1.90))

    with pytest.raises(IntegrityError):
        with manager.get_session() as session:
            session.add(MarketChoiceQuote(choice_id=choice_id, source="oddspapi", current_odds=1.95))


def test_back_and_lay_quotes_coexist_for_same_choice_and_source(tmp_path):
    """Betfair back/lay from the same source must be two independent rows, not a conflict.

    This is the core fix for the Back/Lay duplication bug: one MarketChoice,
    two MarketChoiceQuote rows differentiated only by exchange_side.
    """
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        session.add_all(
            [
                MarketChoiceQuote(
                    choice_id=choice_id,
                    source="oddspapi",
                    exchange_side="back",
                    current_odds=3.05,
                ),
                MarketChoiceQuote(
                    choice_id=choice_id,
                    source="oddspapi",
                    exchange_side="lay",
                    current_odds=3.20,
                ),
            ]
        )

    with manager.get_session() as session:
        quotes = (
            session.query(MarketChoiceQuote)
            .filter(MarketChoiceQuote.choice_id == choice_id)
            .order_by(MarketChoiceQuote.exchange_side)
            .all()
        )
        assert [(q.exchange_side, float(q.current_odds)) for q in quotes] == [
            ("back", 3.05),
            ("lay", 3.20),
        ]


def test_same_choice_different_source_does_not_conflict(tmp_path):
    """OddsPortal opening-only and Oddspapi current must be independent quotes.

    This is the key scenario from the original bug report: OddsPortal writes
    an opening-only 'back' quote and Oddspapi writes a live 'back' quote for
    the same outcome, without either provider overwriting the other's row.
    """
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        session.add_all(
            [
                MarketChoiceQuote(
                    choice_id=choice_id,
                    source="oddsportal",
                    exchange_side="back",
                    initial_odds=1.01,
                    current_odds=None,
                ),
                MarketChoiceQuote(
                    choice_id=choice_id,
                    source="oddspapi",
                    exchange_side="back",
                    initial_odds=3.30,
                    current_odds=3.05,
                ),
            ]
        )

    with manager.get_session() as session:
        quotes = {
            q.source: (
                float(q.initial_odds) if q.initial_odds is not None else None,
                float(q.current_odds) if q.current_odds is not None else None,
            )
            for q in session.query(MarketChoiceQuote).filter(
                MarketChoiceQuote.choice_id == choice_id
            )
        }
        assert quotes == {
            "oddsportal": (1.01, None),
            "oddspapi": (3.30, 3.05),
        }


def test_initial_then_current_arriving_later_updates_same_row(tmp_path):
    """Initial-only, then current arriving in a later write, must upsert in place.

    Mirrors the tolerance MarketChoice already has today via two independent
    nullable columns — MarketChoiceQuote must preserve the same property.
    """
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        session.add(
            MarketChoiceQuote(
                choice_id=choice_id,
                source="oddspapi",
                exchange_side="back",
                initial_odds=3.30,
                current_odds=None,
            )
        )

    with manager.get_session() as session:
        quote = (
            session.query(MarketChoiceQuote)
            .filter(
                MarketChoiceQuote.choice_id == choice_id,
                MarketChoiceQuote.source == "oddspapi",
                MarketChoiceQuote.exchange_side == "back",
            )
            .one()
        )
        assert quote.current_odds is None
        quote.current_odds = 3.05

    with manager.get_session() as session:
        quote = (
            session.query(MarketChoiceQuote)
            .filter(
                MarketChoiceQuote.choice_id == choice_id,
                MarketChoiceQuote.source == "oddspapi",
                MarketChoiceQuote.exchange_side == "back",
            )
            .one()
        )
        assert float(quote.initial_odds) == 3.30
        assert float(quote.current_odds) == 3.05
