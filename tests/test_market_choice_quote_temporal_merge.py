"""Pure and writer-level tests for QuoteMergeMode temporal rules (Phase 4b.1)."""

from datetime import datetime, timedelta

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Bookie, Event, Market, MarketChoice, MarketChoiceQuote
from infrastructure.persistence.repositories.market.market_choice_quote_merge_policy import (
    QuoteCandidateState,
    QuoteExistingState,
    QuoteMergeMode,
    decide_quote_merge,
)
from infrastructure.persistence.repositories.market.market_choice_quote_writer import (
    MarketChoiceQuoteWriter,
)


T0 = datetime(2026, 6, 20, 10, 0, 0)
T1 = T0 + timedelta(minutes=5)
T2 = T0 + timedelta(minutes=10)


def test_live_applies_current_when_existing_is_null():
    decision = decide_quote_merge(
        existing=QuoteExistingState(exists=True),
        candidate=QuoteCandidateState(current_price=2.0, current_captured_at=T1),
        mode=QuoteMergeMode.LIVE,
    )
    assert decision.apply_current is True
    assert decision.current_odds == 2.0


def test_newer_timestamp_wins_in_both_modes():
    existing = QuoteExistingState(
        exists=True,
        current_odds=1.5,
        current_updated_at=T0,
    )
    candidate = QuoteCandidateState(current_price=1.8, current_captured_at=T2)
    for mode in (QuoteMergeMode.LIVE, QuoteMergeMode.BACKFILL_FILL_ONLY):
        decision = decide_quote_merge(
            existing=existing, candidate=candidate, mode=mode
        )
        assert decision.apply_current is True
        assert decision.current_odds == 1.8


def test_live_applies_current_without_timestamp_order():
    existing = QuoteExistingState(
        exists=True,
        current_odds=1.483,
        current_updated_at=T2,
    )
    candidate = QuoteCandidateState(current_price=1.628, current_captured_at=T0)
    decision = decide_quote_merge(
        existing=existing, candidate=candidate, mode=QuoteMergeMode.LIVE
    )
    assert decision.apply_current is True
    assert decision.current_odds == 1.628


def test_older_timestamp_is_stale_in_backfill():
    existing = QuoteExistingState(
        exists=True,
        current_odds=2.0,
        current_updated_at=T2,
    )
    candidate = QuoteCandidateState(current_price=1.1, current_captured_at=T0)
    decision = decide_quote_merge(
        existing=existing,
        candidate=candidate,
        mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
    )
    assert decision.apply_current is False
    assert "current" in decision.stale_fields


def test_equal_timestamp_same_value_is_noop():
    existing = QuoteExistingState(
        exists=True,
        current_odds=2.0,
        current_updated_at=T1,
    )
    candidate = QuoteCandidateState(current_price=2.0, current_captured_at=T1)
    decision = decide_quote_merge(
        existing=existing, candidate=candidate, mode=QuoteMergeMode.LIVE
    )
    assert decision.apply_current is False
    assert decision.conflicts == ()
    assert decision.stale_fields == ()


def test_equal_timestamp_different_value_is_applied_in_live():
    existing = QuoteExistingState(
        exists=True,
        current_odds=2.0,
        current_updated_at=T1,
    )
    candidate = QuoteCandidateState(current_price=2.5, current_captured_at=T1)
    decision = decide_quote_merge(
        existing=existing, candidate=candidate, mode=QuoteMergeMode.LIVE
    )
    assert decision.apply_current is True
    assert decision.current_odds == 2.5


def test_live_applies_current_without_candidate_timestamp():
    existing = QuoteExistingState(
        exists=True,
        current_odds=2.0,
        current_updated_at=T1,
    )
    candidate = QuoteCandidateState(current_price=9.0, current_captured_at=None)
    decision = decide_quote_merge(
        existing=existing, candidate=candidate, mode=QuoteMergeMode.LIVE
    )
    assert decision.apply_current is True
    assert decision.current_odds == 9.0


def test_live_applies_timestamped_current_over_untimestamped_existing():
    existing = QuoteExistingState(exists=True, current_odds=2.0, current_updated_at=None)
    candidate = QuoteCandidateState(current_price=2.2, current_captured_at=T1)
    decision = decide_quote_merge(
        existing=existing, candidate=candidate, mode=QuoteMergeMode.LIVE
    )
    assert decision.apply_current is True


def test_backfill_does_not_overwrite_untimestamped_existing_with_timestamped():
    existing = QuoteExistingState(exists=True, current_odds=2.0, current_updated_at=None)
    candidate = QuoteCandidateState(current_price=2.2, current_captured_at=T1)
    decision = decide_quote_merge(
        existing=existing,
        candidate=candidate,
        mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
    )
    assert decision.apply_current is False
    assert "current" in decision.stale_fields


def test_backfill_fills_initial_only_when_null():
    existing = QuoteExistingState(exists=True, initial_odds=1.9, initial_captured_at=T0)
    candidate = QuoteCandidateState(initial_price=3.0, initial_captured_at=T1)
    decision = decide_quote_merge(
        existing=existing,
        candidate=candidate,
        mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
    )
    assert decision.apply_initial is False
    assert "initial" in decision.conflicts


def test_backfill_can_fill_matching_initial_timestamp_only():
    existing = QuoteExistingState(exists=True, initial_odds=1.9, initial_captured_at=None)
    candidate = QuoteCandidateState(initial_price=1.9, initial_captured_at=T0)
    decision = decide_quote_merge(
        existing=existing,
        candidate=candidate,
        mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
    )
    assert decision.apply_initial is False
    assert decision.apply_initial_timestamp_only is True
    assert decision.initial_captured_at == T0


def test_metadata_conflict_does_not_overwrite():
    existing = QuoteExistingState(
        exists=True,
        source_market_id="m-1",
        current_odds=None,
    )
    candidate = QuoteCandidateState(
        current_price=2.0,
        current_captured_at=T1,
        source_market_id="m-2",
    )
    decision = decide_quote_merge(
        existing=existing,
        candidate=candidate,
        mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
    )
    assert decision.apply_current is True
    assert "metadata_source_market_id" in decision.conflicts
    assert "source_market_id" not in decision.metadata_updates


def test_source_limit_follows_winning_current():
    existing = QuoteExistingState(
        exists=True,
        current_odds=1.5,
        current_updated_at=T0,
        source_limit=10,
    )
    candidate = QuoteCandidateState(
        current_price=1.8,
        current_captured_at=T2,
        source_limit=25,
    )
    decision = decide_quote_merge(
        existing=existing, candidate=candidate, mode=QuoteMergeMode.LIVE
    )
    assert decision.apply_current is True
    assert decision.apply_source_limit is True
    assert decision.source_limit == 25


def test_source_limit_fills_null_without_current_change():
    existing = QuoteExistingState(
        exists=True,
        current_odds=2.0,
        current_updated_at=T2,
        source_limit=None,
    )
    candidate = QuoteCandidateState(
        current_price=1.0,
        current_captured_at=T0,
        source_limit=40,
    )
    decision = decide_quote_merge(
        existing=existing,
        candidate=candidate,
        mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
    )
    assert decision.apply_current is False
    assert decision.apply_source_limit is True
    assert decision.source_limit == 40


def make_manager(tmp_path):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'temporal_merge.db'}")
    manager.create_tables()
    return manager


def seed_choice(manager):
    with manager.get_session() as session:
        event = Event(
            slug="temporal-event",
            start_time_utc=datetime(2026, 6, 20, 12, 0, 0),
            sport="Basketball",
            competition="WNBA",
            home_team="Home",
            away_team="Away",
        )
        bookie = Bookie(name="Betfair Exchange", slug="betfair-ex-temporal")
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


def test_writer_live_applies_current_from_this_ingest(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        quote_index = {}
        MarketChoiceQuoteWriter.upsert(
            session,
            quote_index=quote_index,
            choice_id=choice_id,
            source="oddspapi",
            current_price=3.0,
            current_captured_at=T2,
        )
        refreshed = MarketChoiceQuoteWriter.upsert(
            session,
            quote_index=quote_index,
            choice_id=choice_id,
            source="oddspapi",
            current_price=1.0,
            current_captured_at=T0,
        )
        session.flush()
        assert refreshed is not None
        assert refreshed.decision.apply_current is True
        quote = session.query(MarketChoiceQuote).one()
        assert float(quote.current_odds) == 1.0


def test_writer_backfill_fill_only_does_not_overwrite_stronger_current(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        quote_index = {}
        MarketChoiceQuoteWriter.upsert(
            session,
            quote_index=quote_index,
            choice_id=choice_id,
            source="oddspapi",
            current_price=3.0,
            current_captured_at=T2,
            mode=QuoteMergeMode.LIVE,
        )
        backfill = MarketChoiceQuoteWriter.upsert(
            session,
            quote_index=quote_index,
            choice_id=choice_id,
            source="oddspapi",
            current_price=1.5,
            current_captured_at=T0,
            mode=QuoteMergeMode.BACKFILL_FILL_ONLY,
        )
        session.flush()
        assert backfill.decision.apply_current is False
        quote = session.query(MarketChoiceQuote).one()
        assert float(quote.current_odds) == 3.0


def test_writer_preserves_overwrite_initial_in_live_mode(tmp_path):
    manager = make_manager(tmp_path)
    choice_id = seed_choice(manager)

    with manager.get_session() as session:
        quote_index = {}
        MarketChoiceQuoteWriter.upsert(
            session,
            quote_index=quote_index,
            choice_id=choice_id,
            source="oddsportal",
            initial_price=1.9,
            initial_captured_at=T0,
        )
        result = MarketChoiceQuoteWriter.upsert(
            session,
            quote_index=quote_index,
            choice_id=choice_id,
            source="oddsportal",
            initial_price=2.2,
            initial_captured_at=T1,
            overwrite_initial=True,
            mode=QuoteMergeMode.LIVE,
        )
        session.flush()
        assert result.decision.apply_initial is True
        assert float(result.quote.initial_odds) == 2.2
