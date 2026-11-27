"""The cached-odds backfill selects past events with pre-kickoff cache and reuses the job flow."""

import json
from datetime import timedelta

from infrastructure.persistence import database
from infrastructure.persistence.catalogs.canonical_market_types import CANONICAL_MARKET_TYPE_IDS
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Bookie, Event, EventSourceMapping, Market, MarketChoice, MarketChoiceQuote, MarketChoiceSnapshot,
    OddspapiMainlineOutcomeCache,
)
from scripts.maintenance.oddspapi import recover_cached_prematch_odds as script
from shared.temporal import utc_now


def seed(session, slug, kickoff, cache_at):
    event = Event(slug=slug, starts_at=kickoff, sport="Football", competition="League",
                  home_team="Home", away_team="Away")
    session.add(event)
    session.flush()
    session.add(EventSourceMapping(event_id=event.id, source="oddspapi", source_event_id=f"fx-{slug}"))
    if cache_at is not None:
        session.add(OddspapiMainlineOutcomeCache(
            event_id=event.id, fixture_id=f"fx-{slug}", bookmaker_slug="pinnacle",
            source_market_id="101", source_outcome_id="1", captured_at=cache_at))
    return event.id


def seed_quotes(session, event_id, collected_at):
    bookie = Bookie(name="Pinnacle", slug="pinnacle")
    session.add(bookie)
    session.flush()
    market = Market(event_id=event_id, bookie_id=bookie.bookie_id, is_live=False,
                    market_type_id=CANONICAL_MARKET_TYPE_IDS["1x2_full_time"])
    session.add(market)
    session.flush()
    quotes = []
    for name, outcome in (("1", "1"), ("X", "2")):
        choice = MarketChoice(market_id=market.market_id, choice_name=name)
        session.add(choice)
        session.flush()
        quote = MarketChoiceQuote(choice_id=choice.choice_id, source="oddspapi", exchange_level=0,
                                  source_market_id="101", source_outcome_id=outcome)
        session.add(quote)
        session.flush()
        session.add(MarketChoiceSnapshot(quote_id=quote.quote_id, odds_value=2.0, collected_at=collected_at))
        quotes.append(quote.quote_id)
    return quotes


def test_selects_past_cached_events_and_reports_untouched_quotes(tmp_path, monkeypatch):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'backfill.db'}")
    manager.create_tables()
    monkeypatch.setattr(database, "db_manager", manager)
    monkeypatch.setattr(script, "REPORT_DIRECTORY", tmp_path / "reports")
    past, future = utc_now() - timedelta(days=2), utc_now() + timedelta(days=1)
    with manager.get_session() as session:
        selected = seed(session, "past", past, past - timedelta(hours=1))
        seed(session, "future", future, future - timedelta(hours=1))
        seed(session, "no-cache", past, None)
        seed(session, "late-cache", past, past + timedelta(minutes=10))
        refreshed_id, untouched_id = seed_quotes(session, selected, past)

    rows = script.select_cached_rows()
    assert [(row[0], row[1], row[4]) for row in rows] == [(selected, "fx-past", "pinnacle")]

    received = []

    def fake_recover(batch):
        received.append(batch)
        with manager.get_session() as session:
            session.add(MarketChoiceSnapshot(quote_id=refreshed_id, odds_value=2.1, collected_at=utc_now()))

    monkeypatch.setattr(script, "recover_prematch_odds", fake_recover)
    script.main([])
    assert received == []
    script.main(["--commit"])
    assert [[tuple(row) for row in batch] for batch in received] == [[tuple(row) for row in rows]]

    [report] = (tmp_path / "reports").iterdir()
    lines = [json.loads(line) for line in report.read_text(encoding="utf-8").splitlines()]
    assert [(line["event_id"], line["fixture_id"], line["quote_id"], line["snapshots"]) for line in lines] == [
        (selected, "fx-past", untouched_id, 1),
    ]
