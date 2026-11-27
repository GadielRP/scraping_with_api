"""The OddsPapi history recovery replaces returned quotes up to kickoff and keeps the rest."""

from datetime import datetime, timezone

import pytest

from infrastructure.persistence import database
from infrastructure.persistence.catalogs.canonical_market_types import CANONICAL_MARKET_TYPE_IDS
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie, Event, Market, MarketChoice, MarketChoiceQuote, MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories import market_repository
from infrastructure.persistence.repositories.market_repository import MarketRepository

KICKOFF = datetime(2026, 6, 20, 12, 0, tzinfo=timezone.utc)


def at(hour, minute):
    return datetime(2026, 6, 20, hour, minute, tzinfo=timezone.utc)


@pytest.mark.parametrize("opening_is_fixed", [False, True])
def test_recovery_replaces_returned_quotes_and_keeps_the_rest(tmp_path, monkeypatch, opening_is_fixed):
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
        returned, kept = MarketChoice(market_id=market.market_id, choice_name="1"), \
            MarketChoice(market_id=market.market_id, choice_name="X")
        session.add_all([returned, kept])
        session.flush()
        quotes = [MarketChoiceQuote(choice_id=choice.choice_id, source="oddspapi", exchange_level=0,
                                    source_market_id="101", source_outcome_id=outcome, initial_odds=9.0)
                  for choice, outcome in ((returned, "1"), (kept, "2"))]
        session.add_all(quotes)
        session.flush()
        session.add_all([MarketChoiceSnapshot(quote_id=quote.quote_id, odds_value=9.0,
                                              collected_at=at(11, 59), source_collected_at=at(11, 59))
                         for quote in quotes])
        event_id, bookie_id = event.id, bookie.bookie_id
        returned_id, kept_id = (quote.quote_id for quote in quotes)

    opening_at = datetime(2026, 6, 19, 9, tzinfo=timezone.utc)
    moments = [
        {"price": 2.10, "createdAt": "2026-06-20T11:50:00Z"},
        {"price": 2.00, "createdAt": "2026-06-20T11:59:30Z"},
        {"price": 1.90, "createdAt": "2026-06-20T12:03:00Z"},
    ]
    if opening_is_fixed:
        moments.insert(0, {"price": 2.40, "createdAt": opening_at.isoformat(), "limit": 125})
    batches = [{"bookie_id": bookie_id, "markets": [{"choices": [{
        "sourceMarketId": "101", "sourceOutcomeId": "1",
        "decimalValue": 2.00, "changedAt": "2026-06-20T11:59:30Z",
        "initialDecimalValue": 2.40, "initialChangedAt": opening_at.isoformat(),
        "initialLimit": 125,
        "momentQuotes": moments,
    }]}]}]

    MarketRepository._recover_oddspapi_quotes(event_id, batches, KICKOFF)

    with manager.get_session() as session:
        quote = session.get(MarketChoiceQuote, returned_id)
        assert float(quote.initial_odds) == 2.40
        assert quote.initial_captured_at == opening_at
        assert float(quote.current_odds) == 2.00
        snapshots = session.query(MarketChoiceSnapshot).order_by(MarketChoiceSnapshot.source_collected_at).all()
        assert [(row.quote_id, float(row.odds_value)) for row in snapshots] == [
            (returned_id, 2.40),
            (returned_id, 2.10), (kept_id, 9.0), (returned_id, 2.00),
        ]
        assert snapshots[0].source_collected_at == opening_at
        assert snapshots[0].source_limit == 125
