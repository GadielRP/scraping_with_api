"""The OddsPapi history recovery must persist the opening (initial) odds."""

from datetime import datetime, timezone

from infrastructure.persistence import database
from infrastructure.persistence.catalogs.canonical_market_types import CANONICAL_MARKET_TYPE_IDS
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Bookie, Event, Market, MarketChoice, MarketChoiceQuote
from infrastructure.persistence.repositories import market_repository
from infrastructure.persistence.repositories.market_repository import MarketRepository

KICKOFF = datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc)
CUTOFF = datetime(2026, 6, 20, 11, 55, tzinfo=timezone.utc)


def test_recovery_persists_initial_odds(tmp_path, monkeypatch):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'recovery.db'}")
    manager.create_tables()
    monkeypatch.setattr(market_repository, "db_manager", manager)
    monkeypatch.setattr(database, "db_manager", manager)

    with manager.get_session() as session:
        event = Event(slug="recovery", starts_at=KICKOFF, sport="Football", competition="League",
                      home_team="Home", away_team="Away")
        bookie = Bookie(name="Pinnacle", slug="pinnacle")
        session.add_all([event, bookie])
        session.flush()
        market = Market(event_id=event.id, bookie_id=bookie.bookie_id, is_live=False,
                        market_type_id=CANONICAL_MARKET_TYPE_IDS["1x2_full_time"])
        session.add(market)
        session.flush()
        choice = MarketChoice(market_id=market.market_id, choice_name="1")
        session.add(choice)
        session.flush()
        session.add(MarketChoiceQuote(choice_id=choice.choice_id, source="oddspapi", exchange_level=0,
                                      source_market_id="101", source_outcome_id="1"))
        event_id, bookie_id = event.id, bookie.bookie_id

    batches = [{"bookie_id": bookie_id, "markets": [{"choices": [{
        "sourceMarketId": "101", "sourceOutcomeId": "1",
        "decimalValue": 2.10, "changedAt": "2026-06-20T11:50:00Z",
        "initialDecimalValue": 2.40, "initialChangedAt": "2026-06-19T09:00:00Z",
    }]}]}]

    MarketRepository._recover_oddspapi_quotes(event_id, batches, CUTOFF)

    with manager.get_session() as session:
        quote = session.query(MarketChoiceQuote).one()
        assert float(quote.initial_odds) == 2.40
        assert float(quote.current_odds) == 2.10
