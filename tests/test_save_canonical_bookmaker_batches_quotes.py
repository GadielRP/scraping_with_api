"""Integration tests: save_canonical_bookmaker_batches populates MarketChoiceQuote.

This is the concrete fix for the original bug report: Betfair back/lay odds
persisted through OddsPapi's ``exchangeQuotes`` list used to only reach
market_choice_snapshots (a historical log with no "current state" query
path), so an alert built at T-5 minutes showed ``current -> N/A`` even though
a live back/lay price had just been ingested. See
docs/refactors/db-schema-odds-refactor.md (Fase 2).
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Bookie, Event, Market, MarketChoice, MarketChoiceQuote
from infrastructure.persistence.repositories.market_repository import MarketRepository


def _make_manager(tmp_path, name):
    manager = DatabaseManager(f"sqlite:///{tmp_path / name}")
    manager.create_tables()
    return manager


def _seed_event_and_bookie(manager):
    with manager.get_session() as session:
        event = Event(
            slug="betfair-quote-fix",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Football",
            competition="Test League",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Betfair Exchange", slug="betfair-ex")
        session.add_all([event, bookie])
        session.flush()
        return event.id, bookie.bookie_id


def _quote(session, choice_id, source, exchange_side, exchange_level=0):
    return (
        session.query(MarketChoiceQuote)
        .filter(
            MarketChoiceQuote.choice_id == choice_id,
            MarketChoiceQuote.source == source,
            MarketChoiceQuote.exchange_side == exchange_side,
            MarketChoiceQuote.exchange_level == exchange_level,
        )
        .one()
    )


def _batch(*, initial_odds=None, current_odds=None, exchange_quotes=None):
    choice = {"name": "1"}
    if initial_odds is not None:
        choice["initialOdds"] = initial_odds
        choice["initialChangedAt"] = "2026-06-20T10:00:00Z"
    if current_odds is not None:
        choice["currentOdds"] = current_odds
        choice["sourceCollectedAt"] = "2026-06-20T11:55:00Z"
    if exchange_quotes is not None:
        choice["exchangeQuotes"] = exchange_quotes
    return [
        {
            "bookie_id": None,  # filled in by caller
            "markets": [
                {
                    "marketName": "1X2 Full Time",
                    "marketGroup": "1X2",
                    "marketPeriod": "Full Time",
                    "choiceGroup": None,
                    "isLive": False,
                    "choices": [choice],
                }
            ],
        }
    ]


def test_single_side_quote_is_seeded_from_initial_at_t120(tmp_path):
    manager = _make_manager(tmp_path, "t120.db")
    event_id, bookie_id = _seed_event_and_bookie(manager)
    batches = _batch(initial_odds=1.90)
    batches[0]["bookie_id"] = bookie_id

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, batches, source="oddspapi"
        )

    with manager.get_session() as session:
        choice = session.query(MarketChoice).one()
        quote = _quote(session, choice.choice_id, "oddspapi", "single")
        assert float(quote.initial_odds) == 1.90
        assert quote.current_odds is None


def test_back_and_lay_quotes_become_current_state_at_t5(tmp_path):
    """Reproduces the bug: T-120 opening (with back/lay already visible via
    /odds) followed by a T-5 refresh that only carries current back/lay.
    Before this fix, the T-5 back/lay update only ever reached
    market_choice_snapshots, so a reader looking for "current state" saw
    nothing (-> N/A) despite the price having just been ingested.
    """
    manager = _make_manager(tmp_path, "t5.db")
    event_id, bookie_id = _seed_event_and_bookie(manager)

    opening = _batch(
        initial_odds=1.90,
        exchange_quotes=[{"side": "back", "level": 0, "price": 1.90, "size": 60.0}],
    )
    opening[0]["bookie_id"] = bookie_id
    live = _batch(
        current_odds=2.10,
        exchange_quotes=[
            {"side": "back", "level": 0, "price": 2.10, "size": 50.0},
            {"side": "lay", "level": 0, "price": 2.16, "size": 40.0},
        ],
    )
    live[0]["bookie_id"] = bookie_id

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, opening, source="oddspapi"
        )
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, live, source="oddspapi"
        )

    with manager.get_session() as session:
        choice = session.query(MarketChoice).one()
        back = _quote(session, choice.choice_id, "oddspapi", "back")
        lay = _quote(session, choice.choice_id, "oddspapi", "lay")

        # The historical bug: this used to be unreadable as "current state".
        assert float(back.current_odds) == 2.10
        assert float(lay.current_odds) == 2.16
        # Back inherits the choice-level opening value (mirrors the legacy
        # snapshot convention of labelling the opening price "back").
        assert float(back.initial_odds) == 1.90
        assert lay.initial_odds is None
        assert back.movement == 1


def test_partial_arrival_current_only_then_initial_backfilled_later(tmp_path):
    """Current arrives first (no opening captured yet); a later write adds initial."""
    manager = _make_manager(tmp_path, "partial.db")
    event_id, bookie_id = _seed_event_and_bookie(manager)

    current_first = _batch(current_odds=1.95)
    current_first[0]["bookie_id"] = bookie_id
    initial_later = _batch(initial_odds=1.80)
    initial_later[0]["bookie_id"] = bookie_id

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, current_first, source="oddspapi"
        )
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, initial_later, source="oddspapi"
        )

    with manager.get_session() as session:
        choice = session.query(MarketChoice).one()
        quote = _quote(session, choice.choice_id, "oddspapi", "single")
        assert float(quote.current_odds) == 1.95
        assert float(quote.initial_odds) == 1.80
