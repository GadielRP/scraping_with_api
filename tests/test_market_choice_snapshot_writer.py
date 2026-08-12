"""Unit tests for the single-responsibility snapshot writer."""

from datetime import datetime
from pathlib import Path

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories.market.market_choice_snapshot_writer import (
    MarketChoiceSnapshotWriter,
)


def make_manager(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'snapshot-writer.db'}")
    manager.create_tables()
    return manager


def seed_choice(manager):
    with manager.get_session() as session:
        event = Event(
            slug="snapshot-writer-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Football",
            competition="Test League",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Betfair Exchange", slug="betfair-ex")
        session.add_all([event, bookie])
        session.flush()
        market = Market(
            event_id=event.id,
            bookie_id=bookie.bookie_id,
            market_name="1X2 Full Time",
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


def test_append_derives_identity_and_stable_lineage_from_quote(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)
    collected_at = datetime(2026, 6, 20, 11, 55, 0)
    source_collected_at = datetime(2026, 6, 20, 11, 54, 30)

    with manager.get_session() as session:
        quote = MarketChoiceQuote(
            choice_id=choice_id,
            source="oddspapi",
            exchange_side="back",
            exchange_level=1,
            main_line=True,
            source_market_id="market-1",
            source_outcome_id="outcome-1",
            bookmaker_outcome_id="bookmaker-outcome-1",
            current_odds=2.10,
        )
        session.add(quote)
        session.flush()
        snapshot = MarketChoiceSnapshotWriter.append(
            session,
            quote=quote,
            odds_value=2.10,
            collected_at=collected_at,
            source_collected_at=source_collected_at,
            source_limit=100,
            exchange_size=75,
        )
        session.flush()
        assert snapshot.quote_id == quote.quote_id

    with manager.get_session() as session:
        snapshot = session.query(MarketChoiceSnapshot).one()
        assert snapshot.quote.choice_id == choice_id
        assert snapshot.quote.source == "oddspapi"
        assert snapshot.quote.exchange_side == "back"
        assert snapshot.quote.exchange_level == 1
        assert snapshot.quote.source_market_id == "market-1"
        assert snapshot.quote.source_outcome_id == "outcome-1"
        assert snapshot.quote.bookmaker_outcome_id == "bookmaker-outcome-1"
        assert snapshot.quote.main_line is True
        assert float(snapshot.source_limit) == 100
        assert float(snapshot.exchange_size) == 75
        assert not hasattr(snapshot, "choice_id")
        assert not hasattr(snapshot, "source")


def test_append_accepts_pending_quote_in_same_unit_of_work(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        quote = MarketChoiceQuote(
            choice_id=choice_id,
            source="oddspapi",
            current_odds=1.90,
        )
        session.add(quote)
        snapshot = MarketChoiceSnapshotWriter.append(
            session,
            quote=quote,
            odds_value=1.90,
            collected_at=datetime(2026, 6, 20, 11, 55, 0),
        )

        # One flush is enough for the complete graph: SQLAlchemy inserts the
        # pending quote first and propagates its PK to the snapshot FK.
        session.flush()
        assert quote.quote_id is not None
        assert snapshot.quote_id == quote.quote_id


def test_market_repository_delegates_every_snapshot_write():
    repository_path = (
        Path(__file__).resolve().parents[1]
        / "infrastructure"
        / "persistence"
        / "repositories"
        / "market_repository.py"
    )
    repository_source = repository_path.read_text(encoding="utf-8")

    assert "MarketChoiceSnapshot(" not in repository_source
    assert "MarketChoiceSnapshotWriter.append(" in repository_source
