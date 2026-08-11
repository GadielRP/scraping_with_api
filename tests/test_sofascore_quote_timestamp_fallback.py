"""SofaScore live quotes must advance current_updated_at via extraction time."""

from datetime import datetime, timedelta

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoice,
    MarketChoiceQuote,
)
from infrastructure.persistence.repositories import market_repository as market_repo_module
from infrastructure.persistence.repositories.market_repository import MarketRepository


def test_sofascore_live_updates_quote_without_source_collected_at(tmp_path, monkeypatch):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'sofascore_ts.db'}")
    manager.create_tables()
    monkeypatch.setattr(market_repo_module, "db_manager", manager)

    old_ts = datetime(2026, 8, 5, 17, 0, 0)
    with manager.get_session() as session:
        event = Event(
            slug="ss-ts",
            start_time_utc=datetime(2026, 8, 10, 12, 0, 0),
            sport="Football",
            competition="EPL",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="SofaScore", slug="sofascore")
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
        session.add(
            MarketChoiceQuote(
                choice_id=choice.choice_id,
                source="sofascore",
                exchange_side=None,
                exchange_level=0,
                current_odds=1.60,
                current_updated_at=old_ts,
                main_line=True,
            )
        )
        event_id = event.id
        bookie_id = bookie.bookie_id

    MarketRepository.save_canonical_bookmaker_batches(
        event_id,
        [
            {
                "bookie_id": bookie_id,
                "markets": [
                    {
                        "marketName": "1X2 Full Time",
                        "marketGroup": "1X2",
                        "marketPeriod": "Full Time",
                        "isLive": False,
                        "choices": [
                            {
                                "name": "1",
                                "decimalValue": 1.67,
                                "mainLine": True,
                            }
                        ],
                    }
                ],
            }
        ],
        source="sofascore",
    )

    with manager.get_session() as session:
        quote = session.query(MarketChoiceQuote).one()
        assert float(quote.current_odds) == 1.67
        assert quote.current_updated_at is not None
        assert quote.current_updated_at > old_ts
        assert quote.current_updated_at >= datetime.now() - timedelta(minutes=5)
