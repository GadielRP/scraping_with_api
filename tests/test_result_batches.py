"""Collection recovery and transaction boundaries on an isolated database."""
from datetime import date, timedelta
from importlib import import_module
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event as ThreadEvent

import pytest
from sqlalchemy import event as sql_event

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.models import Event, EventSourceMapping, Result
from infrastructure.persistence.repositories import event_repository, result_repository
from infrastructure.persistence.repositories.event_repository import EventRepository
from infrastructure.persistence.repositories.result_repository import ResultRepository
from infrastructure.settings import Config
from shared.temporal import local_day_bounds_utc

collection = import_module('modules.jobs.results_collection_job.run_results_collection_job')


@pytest.fixture
def database(tmp_path, monkeypatch):
    db = DatabaseManager(os.getenv('RESULTS_TEST_DATABASE_URL') or f'sqlite:///{tmp_path / "results.db"}')
    Base.metadata.create_all(db.engine)
    monkeypatch.setattr(event_repository, 'db_manager', db)
    monkeypatch.setattr(result_repository, 'db_manager', db)
    monkeypatch.setattr(Config, 'EVENT_WRITE_BATCH_SIZE', 2)
    yield db
    Base.metadata.drop_all(db.engine)
    db.engine.dispose()


def seed(db, count=5):
    start, _ = local_day_bounds_utc(date(2026, 10, 1), Config.TIMEZONE)
    with db.get_session() as session:
        for index in range(1, count + 1):
            session.add(Event(id=index, slug=str(index), starts_at=start + timedelta(hours=1), sport='Football',
                              home_team='Home', away_team='Away', competition='League'))
        session.flush()
        for index in range(1, count + 1):
            session.add(EventSourceMapping(event_id=index, source='sofascore', source_event_id=str(index)))


def response(event_id):
    return {'event': {'id': event_id,
                     'status': {'type': 'finished', 'code': 100, 'description': 'Ended'},
                     'homeScore': {'current': 2}, 'awayScore': {'current': 1},
                     'winnerCode': 1}}


def fake_normalize(raw, **kwargs):
    start, _ = local_day_bounds_utc(date(2026, 10, 1), Config.TIMEZONE)
    return {'event': {'id': raw['id'], 'startTimestamp': int(start.timestamp()),
                     'sport': 'Football', 'homeTeam': 'Home', 'awayTeam': 'Away'}}


def test_keyset_pages_exclude_results_and_do_not_skip_after_deletions(database):
    seed(database)
    ResultRepository.batch_upsert_results([(2, {'home_score': 1, 'away_score': 0})])
    pages = ResultRepository.pending_batches(date(2026, 10, 1))
    assert [event.id for event in next(pages)] == [1, 3]
    EventRepository.batch_delete_events([1, 3])
    assert [event.id for event in next(pages)] == [4, 5]
    assert list(pages) == []


def test_sql_upsert_has_bounded_commits_and_propagates_failure(database):
    seed(database)
    commits = []
    sql_event.listen(database.engine, 'commit', lambda connection: commits.append(1))
    assert ResultRepository.batch_upsert_results((i, {'home_score': i, 'away_score': 0}) for i in range(1, 6)) == 5
    assert len(commits) == 3
    assert ResultRepository.batch_upsert_results([(1, {'home_score': 9, 'away_score': 0})]) == 1
    assert ResultRepository.get_result_by_event_id(1).home_score == 9
    assert ResultRepository.batch_upsert_results([(999, {'home_score': 0})]) == 0


def test_collection_resumes_only_uncommitted_batches(database, monkeypatch):
    seed(database)
    calls = []
    fail = True
    original = ResultRepository.batch_upsert_results
    def upsert(rows):
        if fail and any(event_id == 3 for event_id, _ in rows):
            raise RuntimeError('database temporarily unavailable')
        return original(rows)
    monkeypatch.setattr(ResultRepository, 'batch_upsert_results', upsert)
    monkeypatch.setattr(collection, 'normalize_event_payload', fake_normalize)
    monkeypatch.setattr(collection, 'fetch_authoritative_event_response',
                        lambda client, sid, **kwargs: calls.append(sid) or response(sid))
    with pytest.raises(RuntimeError, match='temporarily unavailable'):
        collection.run_results_collection_for_date('2026-10-01')
    assert calls == [1, 2, 3, 4]
    assert ResultRepository.get_result_by_event_id(1) is not None
    assert ResultRepository.get_result_by_event_id(3) is None
    fail = False
    calls.clear()
    stats = collection.run_results_collection_for_date('2026-10-01')
    assert calls == [3, 4, 5]
    assert stats['updated'] == 3 and stats['failed'] == 0


def test_late_metadata_cannot_recreate_deleted_event(database):
    seed(database, 1)
    EventRepository.batch_delete_events([1])
    saved = EventRepository.batch_upsert_events([fake_normalize({'id': 1})], expected_event_ids={'1': 1})
    assert saved.events == {} and '1' in saved.errors
    with database.get_session() as session:
        assert session.query(Event).count() == 0


def test_collection_preserves_canceled_memory(database, monkeypatch):
    seed(database, 1)
    monkeypatch.setattr(Config, 'EVENT_DISCARD_MEMORY_ENABLED', True)
    monkeypatch.setattr(Config, 'EVENT_DISCARD_MEMORY_KINDS', ['canceled'])
    canceled = {'event': {'id': 1, 'status': {'code': 60, 'type': 'canceled'},
                          'homeScore': {}, 'awayScore': {}}}
    monkeypatch.setattr(collection, 'fetch_authoritative_event_response', lambda *a, **kw: canceled)
    stats = collection.run_results_collection_for_date('2026-10-01')
    assert stats['deleted'] == 1
    assert EventRepository.discarded_source_ids('sofascore', ['1']) == {'1'}


def test_selection_failure_is_not_a_successful_empty_run(monkeypatch):
    monkeypatch.setattr(ResultRepository, 'pending_batches', lambda *args: (_ for _ in ()).throw(RuntimeError('offline')))
    with pytest.raises(RuntimeError, match='offline'):
        collection.run_results_collection_previous_day()


def test_result_writer_waits_for_deletion_and_omits_removed_parent(database):
    if database.engine.dialect.name != 'postgresql':
        pytest.skip('Requires PostgreSQL row locks')
    seed(database, 1)
    started = ThreadEvent()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with database.get_session() as session:
            session.query(Event).filter_by(id=1).with_for_update().one()
            def write():
                started.set()
                return ResultRepository.batch_upsert_results([(1, {'home_score': 1, 'away_score': 0})])
            future = pool.submit(write)
            assert started.wait(2)
            with pytest.raises(TimeoutError):
                future.result(timeout=0.1)
            session.query(EventSourceMapping).filter_by(event_id=1).delete()
            session.query(Event).filter_by(id=1).delete()
        assert future.result(timeout=3) == 0


def test_missing_endpoint_cannot_delete_a_concurrently_completed_result(database):
    from modules.events.discards.contracts import DeletionBatch
    seed(database, 1)
    batch = DeletionBatch()
    batch.record_missing(1, 1, 'event_endpoint_not_found')
    ResultRepository.batch_upsert_results([(1, {'home_score': 1, 'away_score': 0})])
    assert EventRepository.batch_delete_events(batch) == 0
    assert ResultRepository.get_result_by_event_id(1) is not None


def test_ambiguous_mappings_do_not_duplicate_or_skip_page_events(database):
    seed(database, 3)
    with database.get_session() as session:
        session.add(EventSourceMapping(event_id=1, source='sofascore', source_event_id='alternate'))
    pages = list(ResultRepository.pending_batches(date(2026, 10, 1)))
    assert [[event.id for event in page] for page in pages] == [[1, 2], [3]]
    assert pages[0][0].source_event_id is None


def test_mismatched_canceled_response_cannot_delete_requested_event(database, monkeypatch):
    seed(database, 1)
    wrong_response = {'event': {'id': 999, 'status': {'code': 60, 'type': 'canceled'}}}
    monkeypatch.setattr(collection, 'fetch_authoritative_event_response', lambda *a, **kw: wrong_response)
    stats = collection.run_results_collection_for_date('2026-10-01')
    assert stats['deleted'] == 0 and stats['failed'] == 1
    with database.get_session() as session:
        assert session.get(Event, 1) is not None
