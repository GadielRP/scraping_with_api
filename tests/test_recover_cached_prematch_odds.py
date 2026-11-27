"""The cached-odds backfill selects past events with pre-kickoff cache and reuses the job flow."""

from datetime import timedelta

from infrastructure.persistence import database
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Event, EventSourceMapping, OddspapiMainlineOutcomeCache
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


def test_selects_past_cached_events_and_commits_through_job_flow(tmp_path, monkeypatch):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'backfill.db'}")
    manager.create_tables()
    monkeypatch.setattr(database, "db_manager", manager)
    past, future = utc_now() - timedelta(days=2), utc_now() + timedelta(days=1)
    with manager.get_session() as session:
        selected = seed(session, "past", past, past - timedelta(hours=1))
        seed(session, "future", future, future - timedelta(hours=1))
        seed(session, "no-cache", past, None)
        seed(session, "late-cache", past, past + timedelta(minutes=10))

    rows = script.select_cached_rows()
    assert [(row[0], row[1], row[4]) for row in rows] == [(selected, "fx-past", "pinnacle")]

    received = []
    monkeypatch.setattr(script, "recover_prematch_odds", received.append)
    script.main([])
    assert received == []
    script.main(["--commit"])
    assert [[tuple(row) for row in batch] for batch in received] == [[tuple(row) for row in rows]]
