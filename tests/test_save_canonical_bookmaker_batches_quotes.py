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

from sqlalchemy import event as sqlalchemy_event

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie,
    Event,
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
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
        quote = _quote(session, choice.choice_id, "oddspapi", None)
        snapshot = session.query(MarketChoiceSnapshot).one()
        assert float(quote.initial_odds) == 1.90
        assert quote.current_odds is None
        assert snapshot.quote_id == quote.quote_id
        assert snapshot.quote.choice_id == quote.choice_id


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
        assert (
            session.query(MarketChoiceQuote)
            .filter(MarketChoiceQuote.exchange_side.is_(None))
            .count()
            == 0
        )
        snapshots = (
            session.query(MarketChoiceSnapshot)
            .order_by(MarketChoiceSnapshot.snapshot_id)
            .all()
        )

        # The historical bug: this used to be unreadable as "current state".
        assert float(back.current_odds) == 2.10
        assert float(lay.current_odds) == 2.16
        # Back inherits the choice-level opening value (mirrors the legacy
        # snapshot convention of labelling the opening price "back").
        assert float(back.initial_odds) == 1.90
        assert lay.initial_odds is None
        assert back.movement == 1
        assert len(snapshots) == 4
        assert all(snapshot.quote_id is not None for snapshot in snapshots)
        assert all(
            snapshot.quote.choice_id == choice.choice_id
            for snapshot in snapshots
        )
        assert [
            (snapshot.quote.exchange_side, float(snapshot.odds_value))
            for snapshot in snapshots
        ] == [
            ("back", 1.90),
            ("back", 1.90),
            ("back", 2.10),
            ("lay", 2.16),
        ]
        assert {snapshot.quote_id for snapshot in snapshots[:3]} == {back.quote_id}
        assert snapshots[3].quote_id == lay.quote_id
        assert not any(
            snapshot.quote.exchange_side is None for snapshot in snapshots
        )


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
        quote = _quote(session, choice.choice_id, "oddspapi", None)
        assert float(quote.current_odds) == 1.95
        assert float(quote.initial_odds) == 1.80


def test_existing_batch_uses_constant_select_budget_independent_of_choice_count(tmp_path):
    manager = _make_manager(tmp_path, "query-budget.db")
    event_id, bookie_id = _seed_event_and_bookie(manager)
    choices = [
        {
            "name": f"choice-{index}",
            "initialOdds": 1.50 + index / 100,
            "currentOdds": 1.60 + index / 100,
            "sourceCollectedAt": "2026-06-20T11:55:00Z",
        }
        for index in range(25)
    ]
    batches = [
        {
            "bookie_id": bookie_id,
            "markets": [
                {
                    "marketName": "Correct Score Full Time",
                    "marketGroup": "Correct Score",
                    "marketPeriod": "Full Time",
                    "choiceGroup": None,
                    "isLive": False,
                    "choices": choices,
                }
            ],
        }
    ]

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, batches, source="oddspapi"
        )

        select_statements = []

        def record_selects(_connection, _cursor, statement, *_args):
            if statement.lstrip().upper().startswith("SELECT"):
                select_statements.append(statement)

        sqlalchemy_event.listen(
            manager.engine,
            "before_cursor_execute",
            record_selects,
        )
        try:
            MarketRepository.save_canonical_bookmaker_batches(
                event_id, batches, source="oddspapi"
            )
        finally:
            sqlalchemy_event.remove(
                manager.engine,
                "before_cursor_execute",
                record_selects,
            )

    # Existing market + selectin-loaded choices + all source quotes. There is
    # no SELECT per choice/quote, so 25 choices still cost three reads.
    assert len(select_statements) == 3
    assert sum("market_choice_quotes" in statement for statement in select_statements) == 1


def test_opening_gate_side_and_level_prefers_explicit_exchange_side():
    assert MarketRepository._opening_gate_side_and_level(
        {
            "exchangeSide": "lay",
            "exchangeQuotes": [{"side": "back", "level": 0, "price": 1.9}],
        }
    ) == ("lay", 0)
    assert MarketRepository._opening_gate_side_and_level(
        {
            "exchangeQuotes": [
                {"side": "lay", "level": 0, "price": 2.0},
                {"side": "back", "level": 0, "price": 1.9},
            ]
        }
    ) == ("back", 0)
    assert MarketRepository._opening_gate_side_and_level(
        {"exchangeQuotes": [{"side": "lay", "level": 0, "price": 2.0}]}
    ) == (None, 0)
    assert MarketRepository._opening_gate_side_and_level({"name": "1"}) == (None, 0)


def test_opening_snapshot_gate_uses_exchange_side_quote_not_null_row(tmp_path):
    """Re-ingest of a back-only quote must not invent a second opening snapshot.

    Before the fix, the gate only inspected exchange_side IS NULL. OddsPortal-
    shaped payloads (exchangeSide set, no NULL-side quote) always looked like
    "first opening", which would duplicate opening snapshots under any policy
    with persist_opening_snapshots=True. Uses a non-oddsportal source so the
    default policy keeps opening snapshots enabled.
    """
    manager = _make_manager(tmp_path, "opening-gate-side.db")
    manager.check_and_migrate_schema()
    event_id, bookie_id = _seed_event_and_bookie(manager)

    batch = [
        {
            "bookie_id": bookie_id,
            "markets": [
                {
                    "marketName": "Home/Away Full Time",
                    "marketGroup": "1X2",
                    "marketPeriod": "Full Time",
                    "choiceGroup": None,
                    "isLive": False,
                    "choices": [
                        {
                            "name": "1",
                            "exchangeSide": "back",
                            "initialOdds": 1.01,
                            "initialChangedAt": "2026-06-20T10:00:00Z",
                            "currentOdds": 1.05,
                            "sourceCollectedAt": "2026-06-20T11:00:00Z",
                        }
                    ],
                }
            ],
        }
    ]

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, batch, source="oddspapi"
        )
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, batch, source="oddspapi"
        )

    with manager.get_session() as session:
        choice = session.query(MarketChoice).one()
        assert (
            session.query(MarketChoiceQuote)
            .filter(
                MarketChoiceQuote.choice_id == choice.choice_id,
                MarketChoiceQuote.exchange_side.is_(None),
            )
            .count()
            == 0
        )
        back = _quote(session, choice.choice_id, "oddspapi", "back")
        assert float(back.initial_odds) == 1.01
        opening_ticks = (
            session.query(MarketChoiceSnapshot)
            .filter(
                MarketChoiceSnapshot.quote_id == back.quote_id,
                MarketChoiceSnapshot.odds_value == 1.01,
            )
            .count()
        )
        # Without the side-aware gate this would be 2 (every re-ingest looks
        # like a first opening because no NULL-side quote exists).
        assert opening_ticks == 1


def test_moment_quotes_write_snapshots_with_their_collected_at_and_dedup(tmp_path):
    manager = _make_manager(tmp_path, "moments.db")
    event_id, bookie_id = _seed_event_and_bookie(manager)
    moment_at = datetime(2026, 6, 20, 10, 0, 0)
    batches = _batch(current_odds=2.10)
    batches[0]["markets"][0]["choices"][0]["momentQuotes"] = [
        {
            "minutesUntilStart": 120,
            "price": 1.95,
            "createdAt": "2026-06-20T16:00:00Z",
            "collectedAt": moment_at,
            "limit": None,
        }
    ]
    batches[0]["bookie_id"] = bookie_id

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        first = MarketRepository.save_canonical_bookmaker_batches(
            event_id, batches, source="oddspapi"
        )
        second = MarketRepository.save_canonical_bookmaker_batches(
            event_id, batches, source="oddspapi"
        )

    with manager.get_session() as session:
        choice = session.query(MarketChoice).one()
        quote = _quote(session, choice.choice_id, "oddspapi", None)
        moment_ticks = (
            session.query(MarketChoiceSnapshot)
            .filter(
                MarketChoiceSnapshot.quote_id == quote.quote_id,
                MarketChoiceSnapshot.collected_at == moment_at,
            )
            .all()
        )
        assert len(moment_ticks) == 1
        assert float(moment_ticks[0].odds_value) == 1.95
    assert first.snapshots_saved >= 2
    assert second.snapshots_saved == 1


def test_oddspapi_missing_changed_at_still_writes_live_current(tmp_path):
    manager = _make_manager(tmp_path, "no-changed-at.db")
    event_id, bookie_id = _seed_event_and_bookie(manager)
    unclocked = [
        {
            "bookie_id": bookie_id,
            "markets": [
                {
                    "marketName": "1X2 Full Time",
                    "marketGroup": "1X2",
                    "marketPeriod": "Full Time",
                    "choiceGroup": None,
                    "isLive": False,
                    "choices": [{"name": "1", "decimalValue": 1.483}],
                }
            ],
        }
    ]
    clocked = _batch(current_odds=1.628)
    clocked[0]["bookie_id"] = bookie_id

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, unclocked, source="oddspapi"
        )
        with manager.get_session() as session:
            quote = session.query(MarketChoiceQuote).one()
            assert float(quote.current_odds) == 1.483
            assert quote.current_updated_at is not None

        MarketRepository.save_canonical_bookmaker_batches(
            event_id, clocked, source="oddspapi"
        )

    with manager.get_session() as session:
        quote = session.query(MarketChoiceQuote).one()
        assert float(quote.current_odds) == 1.628
        assert quote.current_updated_at is not None


def test_oddspapi_live_current_uses_this_ingest_not_source_clock_order(tmp_path):
    manager = _make_manager(tmp_path, "keep-newer-source.db")
    event_id, bookie_id = _seed_event_and_bookie(manager)
    newer = _batch(current_odds=1.645)
    newer[0]["bookie_id"] = bookie_id
    newer[0]["markets"][0]["choices"][0]["sourceCollectedAt"] = (
        "2026-08-17T23:14:00.000Z"
    )
    older = _batch(current_odds=1.628)
    older[0]["bookie_id"] = bookie_id
    older[0]["markets"][0]["choices"][0]["sourceCollectedAt"] = (
        "2026-08-17T23:03:08.245Z"
    )

    with patch(
        "infrastructure.persistence.repositories.market_repository.db_manager",
        manager,
    ):
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, newer, source="oddspapi"
        )
        MarketRepository.save_canonical_bookmaker_batches(
            event_id, older, source="oddspapi"
        )

    with manager.get_session() as session:
        quote = session.query(MarketChoiceQuote).one()
        assert float(quote.current_odds) == 1.628

