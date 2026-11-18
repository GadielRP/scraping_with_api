"""The first recorded final instant is kept; later writes of the same result never move it."""

from datetime import datetime, timedelta, timezone

import pytest

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Event
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.repositories import result_repository
from infrastructure.persistence.repositories.result_repository import ResultRepository

KICKOFF = datetime(2026, 5, 19, 18, 30, tzinfo=timezone.utc)
FIRST = KICKOFF + timedelta(minutes=115)
LATER = KICKOFF + timedelta(hours=20)


@pytest.fixture
def database(tmp_path, monkeypatch):
    db = DatabaseManager(f"sqlite:///{tmp_path / 'results.db'}")
    Base.metadata.create_all(db.engine)
    monkeypatch.setattr(result_repository, "db_manager", db)
    with db.get_session() as session:
        session.add_all(
            Event(id=event_id, slug=str(event_id), starts_at=KICKOFF, sport="Football",
                  home_team="Home", away_team="Away", competition="League")
            for event_id in (1, 2)
        )
    yield db
    db.engine.dispose()


def _upsert(event_id, **fields):
    return ResultRepository.batch_upsert_results([(event_id, {"home_score": 1, "away_score": 1, "winner": "X", **fields})])


def test_final_instant_is_stored_with_the_result(database):
    _upsert(1, finished_at=FIRST)

    assert ResultRepository.get_result_by_event_id(1).finished_at == FIRST


def test_a_later_write_does_not_move_the_recorded_instant_but_updates_the_score(database):
    _upsert(1, finished_at=FIRST)
    _upsert(1, finished_at=LATER, home_score=2)

    stored = ResultRepository.get_result_by_event_id(1)
    assert stored.finished_at == FIRST
    assert stored.home_score == 2


def test_a_write_without_the_instant_keeps_the_recorded_one(database):
    _upsert(1, finished_at=FIRST)
    _upsert(1)

    assert ResultRepository.get_result_by_event_id(1).finished_at == FIRST


def test_without_a_final_instant_the_last_change_is_kept_while_inside_the_sport_window(database):
    inside = KICKOFF + timedelta(hours=2)

    _upsert(1, last_changed_at=inside)

    assert ResultRepository.get_result_by_event_id(1).finished_at == inside


def test_without_a_final_instant_a_late_last_change_is_capped_by_the_sport_window(database):
    _upsert(1, last_changed_at=LATER)
    _upsert(2)

    # Football allows 2.5 hours after kickoff.
    assert ResultRepository.get_result_by_event_id(1).finished_at == KICKOFF + timedelta(hours=2.5)
    assert ResultRepository.get_result_by_event_id(2).finished_at == KICKOFF + timedelta(hours=2.5)


def test_a_provider_final_instant_beats_the_last_change(database):
    _upsert(1, finished_at=FIRST, last_changed_at=KICKOFF + timedelta(hours=2))

    assert ResultRepository.get_result_by_event_id(1).finished_at == FIRST
