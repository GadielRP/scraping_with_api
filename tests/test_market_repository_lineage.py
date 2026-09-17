from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.repositories.market_repository import MarketRepository


def make_manager(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'markets.db'}")
    manager.create_tables()
    return manager


def seed_event_and_bookie(manager):
    with manager.get_session() as session:
        event = Event(
            slug="test-event",
            starts_at=datetime(2026, 6, 20, 12, 0, 0, tzinfo=timezone.utc),
            sport="Football",
            competition="Premier League",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Pinnacle Sports", slug="pinnacle")
        session.add_all([event, bookie])
        session.flush()
        return event.id, bookie.bookie_id


def odds_response(market_period):
    return {
        "markets": [
            {
                "marketName": "Full-time",
                "marketGroup": "1X2",
                "marketPeriod": market_period,
                "choiceGroup": None,
                "isLive": False,
                "choices": [
                    {
                        "name": "1",
                        "decimalValue": "1.900",
                        "changedAt": "2026-06-19T12:34:56Z",
                        "sourceMarketId": "m-1",
                        "sourceOutcomeId": "o-1",
                        "bookmakerOutcomeId": "b-1",
                        "mainLine": True,
                        "limit": "10.5",
                    }
                ],
            }
        ]
    }


def test_save_markets_with_stats_persists_snapshot_lineage(tmp_path):
    manager = make_manager(tmp_path)
    event_id, bookie_id = seed_event_and_bookie(manager)

    with patch("infrastructure.persistence.repositories.market_repository.db_manager", manager):
        result = MarketRepository.save_markets_from_response_with_stats(
            event_id=event_id,
            odds_response=odds_response(None),
            bookie_id=bookie_id,
            source="oddspapi",
        )

    assert result.markets_saved == 1
    assert result.choices_saved == 1
    assert result.snapshots_saved == 1

    with manager.get_session() as session:
        market = session.query(Market).one()
        quote = session.query(MarketChoiceQuote).one()
        snapshot = session.query(MarketChoiceSnapshot).one()

    assert market.market_period == "Full-time"
    assert snapshot.source == "oddspapi"
    assert snapshot.source_market_id == "m-1"
    assert snapshot.source_outcome_id == "o-1"
    assert snapshot.bookmaker_outcome_id == "b-1"
    assert snapshot.main_line is True
    assert snapshot.source_collected_at is not None
    assert snapshot.source_limit is not None
    assert float(snapshot.source_limit) == 10.5
    assert snapshot.quote_id == quote.quote_id
    assert snapshot.choice_id == quote.choice_id


def test_save_markets_from_response_returns_compatibility_int(tmp_path):
    manager = make_manager(tmp_path)
    event_id, bookie_id = seed_event_and_bookie(manager)

    with patch("infrastructure.persistence.repositories.market_repository.db_manager", manager):
        saved = MarketRepository.save_markets_from_response(
            event_id=event_id,
            odds_response=odds_response("Full-time"),
            bookie_id=bookie_id,
        )

    assert saved == 1


@pytest.mark.parametrize("market_period_value", [None, ""])
def test_market_period_null_or_empty_becomes_full_time(tmp_path, market_period_value):
    manager = make_manager(tmp_path)
    event_id, bookie_id = seed_event_and_bookie(manager)

    with patch("infrastructure.persistence.repositories.market_repository.db_manager", manager):
        MarketRepository.save_markets_from_response(
            event_id=event_id,
            odds_response=odds_response(market_period_value),
            bookie_id=bookie_id,
        )

    with manager.get_session() as session:
        market = session.query(Market).one()

    assert market.market_period == "Full-time"
