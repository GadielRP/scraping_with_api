"""Model-level tests for MarketChoiceQuote (Fase 1 of the odds schema refactor).

See docs/refactors/db-schema-odds-refactor.md §5 for the design rationale:
exchange_side is NULL for non-exchange bookies (same NULL-for-"not
applicable" convention as Market.choice_group). NULL != NULL in a UNIQUE
constraint, so real duplicate-row protection is the functional index
``unique_market_choice_quote_side_null_safe`` (COALESCE(exchange_side, '')),
applied via ``check_and_migrate_schema`` - the plain ORM UniqueConstraint
alone would let two NULL-side rows through.
"""

from datetime import datetime

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)


def make_manager(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'market_choice_quotes.db'}")
    manager.create_tables()
    # create_tables() only runs Base.metadata.create_all(), which applies the
    # plain (NULL-unsafe) UniqueConstraint from __table_args__. The real
    # NULL-safe functional index is only created by the manual migration.
    manager.check_and_migrate_schema()
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


def test_exchange_side_defaults_to_null_for_non_exchange_bookies(tmp_path):
    """A quote created without exchange_side must persist as NULL, not a sentinel string."""
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
        snapshot = MarketChoiceSnapshot(
            quote=quote,
            odds_value=1.90,
        )
        session.add(snapshot)
        session.flush()
        session.refresh(quote)
        assert quote.exchange_side is None
        assert quote.exchange_level == 0
        assert snapshot.quote_id == quote.quote_id

    with manager.get_session() as session:
        snapshot = session.query(MarketChoiceSnapshot).one()
        assert snapshot.quote.choice_id == choice_id
        assert snapshot.quote.snapshots[0].snapshot_id == snapshot.snapshot_id

    inspector = inspect(manager.engine)
    assert "idx_market_choice_snapshots_quote_collected" in {
        index["name"]
        for index in inspector.get_indexes("market_choice_snapshots")
    }
    assert any(
        foreign_key.get("referred_table") == "market_choice_quotes"
        and foreign_key.get("constrained_columns") == ["quote_id"]
        for foreign_key in inspector.get_foreign_keys("market_choice_snapshots")
    )


def test_duplicate_null_side_quote_for_same_source_is_rejected(tmp_path):
    """Two NULL-side quotes for the same (choice, source) must violate the
    functional COALESCE(exchange_side, '') unique index, even though NULL
    is otherwise never equal to NULL."""
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
    nullable columns - MarketChoiceQuote must preserve the same property.
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


def test_startup_does_not_upgrade_legacy_snapshot_schema(tmp_path):
    """Phase 6 structural changes belong only to the explicit script.

    Production tables created under the earlier design may still contain
    exchange_side='single'. Re-running the quotes migration must rewrite
    it, but startup must not alter the snapshot schema.
    """
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        session.add(
            MarketChoiceQuote(
                choice_id=choice_id,
                source="oddspapi",
                exchange_side="single",
                current_odds=1.90,
            )
        )

    # Call the quotes migration directly: a full check_and_migrate_schema()
    # would abort earlier on sqlite test DBs (event-identity validation) and
    # never reach this step - the production path still runs it after that.
    manager._migrate_market_choice_quotes()

    with manager.get_session() as session:
        quote = (
            session.query(MarketChoiceQuote)
            .filter(MarketChoiceQuote.choice_id == choice_id)
            .one()
        )
        assert quote.exchange_side is None

    with manager.get_session() as session:
        session.execute(text("DROP TABLE market_choice_snapshots"))
        session.execute(text("""
            CREATE TABLE market_choice_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                choice_id INTEGER NOT NULL
                    REFERENCES market_choices(choice_id) ON DELETE CASCADE,
                odds_value NUMERIC(8, 3) NOT NULL,
                collected_at TIMESTAMP NOT NULL,
                exchange_side TEXT,
                exchange_level INTEGER,
                exchange_size NUMERIC(18, 3)
            )
        """))

    with pytest.raises(RuntimeError, match="missing columns"):
        manager._migrate_market_choice_snapshot_lineage()

    assert "quote_id" not in {
        column["name"]
        for column in inspect(manager.engine).get_columns(
            "market_choice_snapshots"
        )
    }
