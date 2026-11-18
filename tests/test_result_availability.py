"""A result counts only from the instant it was final, never merely from kickoff."""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from infrastructure.persistence.models import Event, Result
from modules.events.result_availability import (
    DEFAULT_SETTLEMENT_WINDOW, SETTLEMENT_WINDOWS, available_at, available_before, settled_at, settlement_window,
)

CUTOFF = datetime(2026, 5, 19, 19, 15, tzinfo=timezone.utc)


def test_recorded_final_instant_wins_over_the_estimate():
    starts_at = CUTOFF - timedelta(minutes=45)
    finished_at = CUTOFF + timedelta(minutes=70)
    assert available_at(starts_at, finished_at, "Football") == finished_at


def test_missing_final_instant_uses_the_sport_window_and_unknown_sports_the_default():
    starts_at = CUTOFF - timedelta(minutes=45)
    assert available_at(starts_at, None, "Football") == starts_at + SETTLEMENT_WINDOWS["Football"]
    assert available_at(starts_at, None, "Curling") == starts_at + DEFAULT_SETTLEMENT_WINDOW
    assert settlement_window(None) == DEFAULT_SETTLEMENT_WINDOW


def test_settled_instant_is_the_earlier_of_the_last_change_and_the_sport_window():
    starts_at = datetime(2026, 11, 17, 22, 0, tzinfo=timezone.utc)
    window_end = starts_at + timedelta(hours=3.5)

    assert settled_at(starts_at, "Basketball") == window_end
    assert settled_at(starts_at, "Basketball", starts_at + timedelta(seconds=10970)) == starts_at + timedelta(seconds=10970)
    assert settled_at(starts_at, "Basketball", starts_at + timedelta(days=1)) == window_end


def test_a_result_final_before_kickoff_counts_only_once_the_event_has_started():
    starts_at = CUTOFF + timedelta(hours=1)
    assert available_at(starts_at, CUTOFF - timedelta(hours=5), "Tennis") == starts_at


def _event(event_id, sport, starts_at, finished_at):
    return (
        Event(id=event_id, slug=str(event_id), starts_at=starts_at, sport=sport, home_team="H",
              away_team="A", competition="C"),
        Result(event_id=event_id, home_score=1, away_score=0, finished_at=finished_at),
    )


@pytest.fixture
def session():
    engine = create_engine("sqlite://")
    for model in (Event, Result):
        model.__table__.create(engine)
    with Session(engine) as db:
        yield db
    engine.dispose()


def test_sql_condition_selects_exactly_the_results_the_python_rule_makes_available(session):
    minutes = timedelta(minutes=1)
    cases = []
    for sport in (*SETTLEMENT_WINDOWS, "Curling"):
        for started_ago in (30, 100, 160, 200, 300, 400):
            for finished_offset in (None, -20, 0, 30, -600):
                starts_at = CUTOFF - started_ago * minutes
                finished_at = None if finished_offset is None else CUTOFF + finished_offset * minutes
                cases.append((sport, starts_at, finished_at))
    # Kickoff at or after the cutoff is never available, even when the result was final earlier.
    cases.append(("Football", CUTOFF, CUTOFF - 10 * minutes))
    cases.append(("Football", CUTOFF + 10 * minutes, CUTOFF - 10 * minutes))
    for event_id, (sport, starts_at, finished_at) in enumerate(cases, start=1):
        for row in _event(event_id, sport, starts_at, finished_at):
            session.add(row)
    session.flush()

    selected = set(session.scalars(
        select(Event.id).join(Result, Result.event_id == Event.id).where(
            available_before(CUTOFF, starts_at=Event.starts_at, finished_at=Result.finished_at, sport=Event.sport)
        )
    ))
    expected = {
        event_id for event_id, (sport, starts_at, finished_at) in enumerate(cases, start=1)
        if available_at(starts_at, finished_at, sport) < CUTOFF
    }
    assert selected == expected
    assert 0 < len(expected) < len(cases)
