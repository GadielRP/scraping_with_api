"""Lifecycle invariants and batch query counts against an isolated database."""

from dataclasses import replace
from datetime import timedelta
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Event as ThreadEvent

import pytest
from sqlalchemy import event as sql_event, inspect
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.models import Event, EventSourceMapping, EventDiscardMemory, Result
from infrastructure.persistence.repositories import event_repository as repository_module
from infrastructure.persistence.repositories.event_repository import EventRepository
from infrastructure.persistence.repositories.event_discard_repository import EventDiscardRepository
from infrastructure.persistence.event_identity_lock import lock_event_identities
from modules.events.discards.contracts import DeletionBatch
from modules.events.discards.settings import DiscardSettings
from modules.sofascore.results_parser import parse_event_result
from shared.temporal import utc_now
from infrastructure.settings import Config


@pytest.fixture
def database(tmp_path, monkeypatch):
    url = os.getenv("DISCARD_TEST_DATABASE_URL")
    db = DatabaseManager(url or f'sqlite:///{tmp_path / "discard.db"}')
    # The supplied database MUST be disposable. Never default to Config.DATABASE_URL.
    Base.metadata.create_all(db.engine)
    monkeypatch.setattr(repository_module, "db_manager", db)
    monkeypatch.setattr(Config, "EVENT_DISCARD_MEMORY_ENABLED", True)
    monkeypatch.setattr(Config, "EVENT_DISCARD_MEMORY_KINDS", ["canceled", "finished_empty_score", "not_found"])
    monkeypatch.setattr(Config, "EVENT_WRITE_BATCH_SIZE", 100)
    yield db
    Base.metadata.drop_all(db.engine)
    db.engine.dispose()


def payload(sid):
    return {
        "event": {
            "id": sid,
            "startTimestamp": 1700000000,
            "sport": "Football",
            "homeTeam": "Home",
            "awayTeam": "Away",
            "season_id": 77,
            "season_name": "Season 2026",
            "season_year": 2026,
        },
        "home_participant": {"source_participant_id": 1, "name": "Home"},
        "away_participant": {"source_participant_id": 2, "name": "Away"},
        "competition_ref": {
            "source_tournament_id": 4,
            "source_unique_tournament_id": 4,
            "display_name": "League",
        },
    }


def deletion(event, sid, *, code=60, status_type="postponed"):
    parsed = parse_event_result(
        {
            "event": {
                "id": sid,
                "status": {"code": code, "type": status_type, "description": "Original CASE"},
                "homeScore": {},
                "awayScore": {"current": 0},
                "winnerCode": None,
            }
        }
    )
    batch = DeletionBatch(origin="test")
    batch.record(event.id, sid, parsed)
    return batch


def test_canceled_memory_survives_deletion_and_blocks_recreation(database):
    obj = EventRepository.upsert_event(payload(123))
    assert EventRepository.batch_delete_events(deletion(obj, 123)) == 1
    result = EventRepository.batch_upsert_events([payload(123), payload(124)])
    assert result.discarded == {"123"}
    assert set(result.events) == {"124"}
    with database.get_session() as s:
        row = s.get(EventDiscardMemory, ("sofascore", "123"))
        assert row.snapshot["status"]["description"] == "Original CASE"
        assert row.snapshot["homeScore"] == {}
        assert row.snapshot["awayScore"] == {"current": 0}
        assert row.snapshot["winnerCode"] is None
        assert "startTimestamp" not in row.snapshot
        assert s.query(EventSourceMapping).filter_by(source_event_id="123").count() == 0


def test_memory_and_deletion_rollback_together(database, monkeypatch, caplog):
    caplog.set_level("INFO")
    obj = EventRepository.upsert_event(payload(200))
    batch = deletion(obj, 200)
    original = EventDiscardRepository.remember

    def fail_after_insert(*args):
        original(*args)
        raise RuntimeError("after memory insert")

    monkeypatch.setattr(EventDiscardRepository, "remember", fail_after_insert)
    with pytest.raises(RuntimeError):
        EventRepository.batch_delete_events(batch)
    with database.get_session() as s:
        assert s.get(Event, obj.id) is not None
        assert s.query(EventDiscardMemory).count() == 0
    assert "Batch deletion committed:" not in caplog.text


def test_memory_counts_actual_inserts_not_conflicts(database):
    obj = EventRepository.upsert_event(payload(201))
    proof = deletion(obj, 201).evidence
    settings = DiscardSettings.current()
    with database.get_session() as s:
        assert EventDiscardRepository.remember(s, proof, settings) == 1
        assert EventDiscardRepository.remember(s, proof, settings) == 0
        assert EventDiscardRepository.remember(s, proof, replace(settings, enabled=False)) == 0


def test_committed_deletion_log_distinguishes_memory_policy(database, caplog):
    caplog.set_level("INFO")
    canceled = EventRepository.upsert_event(payload(202))
    pending = EventRepository.upsert_event(payload(203))
    batch = deletion(canceled, 202)
    other = deletion(pending, 203, code=0, status_type="notstarted")
    batch.update(other)
    batch.evidence.update(other.evidence)
    assert EventRepository.batch_delete_events(batch) == 2
    assert (
        "deleted=2 memory_eligible=1 memory_inserted=1 memory_existing=0 deleted_without_memory=1"
        in caplog.text
    )


def test_event_write_counts_new_existing_and_blocked(database):
    existing = EventRepository.upsert_event(payload(204))
    blocked = EventRepository.upsert_event(payload(205))
    EventRepository.batch_delete_events(deletion(blocked, 205))
    result = EventRepository.batch_upsert_events([payload(204), payload(205), payload(206)])
    assert result.inserted == result.updated == 1
    assert result.events["204"].id == existing.id
    assert result.discarded == {"205"}


def test_unselected_kind_deleted_without_memory(database):
    obj = EventRepository.upsert_event(payload(300))
    assert (
        EventRepository.batch_delete_events(deletion(obj, 300, code=0, status_type="notstarted"))
        == 1
    )
    assert EventRepository.upsert_event(payload(300)) is not None
    with database.get_session() as s:
        assert s.query(EventDiscardMemory).count() == 0


def test_cleanup_toggle_age_and_encounters_do_not_renew_memory(database):
    now = utc_now()
    for sid in (400, 401):
        obj = EventRepository.upsert_event(payload(sid))
        EventRepository.batch_delete_events(deletion(obj, sid))
    with database.get_session() as s:
        s.get(EventDiscardMemory, ("sofascore", "400")).discarded_at = now - timedelta(days=4)
        s.get(EventDiscardMemory, ("sofascore", "401")).discarded_at = now - timedelta(days=2)
    assert EventRepository.upsert_event(payload(400)) is None
    settings = replace(DiscardSettings.current(), cleanup_enabled=False)
    with database.get_session() as s:
        assert EventDiscardRepository.cleanup(s, settings, now=now) == 0
    assert EventRepository.upsert_event(payload(400)) is None
    with database.get_session() as s:
        assert (
            EventDiscardRepository.cleanup(s, replace(settings, cleanup_enabled=True), now=now) == 1
        )
    assert EventRepository.upsert_event(payload(400)) is not None
    assert EventRepository.upsert_event(payload(401)) is None


def test_memory_is_source_scoped_and_kind_toggle_is_explicit(database, monkeypatch):
    obj = EventRepository.upsert_event(payload(500))
    EventRepository.batch_delete_events(deletion(obj, 500))
    assert EventRepository.upsert_event(payload(500), source="oddspapi") is not None
    monkeypatch.setattr(Config, "EVENT_DISCARD_MEMORY_KINDS", [])
    assert EventRepository.upsert_event(payload(500)) is not None


def test_newer_event_update_invalidates_old_deletion(database):
    obj = EventRepository.upsert_event(payload(600))
    batch = deletion(obj, 600)
    EventRepository.upsert_event(payload(600))
    assert EventRepository.batch_delete_events(batch) == 0
    with database.get_session() as s:
        assert s.query(EventDiscardMemory).count() == 0


def test_new_result_invalidates_deletion(database):
    obj = EventRepository.upsert_event(payload(601))
    batch = deletion(obj, 601)
    with database.get_session() as s:
        s.add(Result(event_id=obj.id, home_score=1, away_score=0, winner="1"))
    assert EventRepository.batch_delete_events(batch) == 0


def test_batch_preloads_are_constant_and_single_wrapper_uses_same_rules(database):
    counts = {"selects": 0, "commits": 0}

    def record(_conn, _cursor, sql, *_args):
        if sql.lstrip().upper().startswith("SELECT"):
            counts["selects"] += 1

    def commit(_conn):
        counts["commits"] += 1

    sql_event.listen(database.engine, "before_cursor_execute", record)
    sql_event.listen(database.engine, "commit", commit)
    try:
        small = EventRepository.batch_upsert_events([payload(i) for i in range(700, 710)])
        small_counts = dict(counts)
        counts.update(selects=0, commits=0)
        large = EventRepository.batch_upsert_events([payload(i) for i in range(800, 900)])
        large_counts = dict(counts)
    finally:
        sql_event.remove(database.engine, "before_cursor_execute", record)
        sql_event.remove(database.engine, "commit", commit)
    assert len(small.events) == 10 and len(large.events) == 100
    assert small_counts["commits"] == large_counts["commits"] == 1
    assert large_counts["selects"] <= small_counts["selects"] + 1
    assert large_counts["selects"] < 15
    print("batch query counts:", small_counts, large_counts)


def test_invalid_event_does_not_rollback_other_events(database):
    result = EventRepository.batch_upsert_events([payload(901), {"id": 902}, payload(903)])
    assert set(result.events) == {"901", "903"}
    assert set(result.errors) == {"902"}


def test_concurrent_delete_then_upsert_cannot_resurrect(database):
    if database.engine.dialect.name != "postgresql":
        pytest.skip("Requires PostgreSQL transaction locks")
    obj = EventRepository.upsert_event(payload(999))
    started = ThreadEvent()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with database.get_session() as s:
            lock_event_identities(s, [("sofascore", "999")])
            proof = deletion(obj, 999).evidence
            EventDiscardRepository.remember(s, proof, DiscardSettings.current())
            s.query(EventSourceMapping).filter_by(event_id=obj.id).delete()
            s.query(Event).filter_by(id=obj.id).delete()

            def write():
                started.set()
                return EventRepository.batch_upsert_events([payload(999)])

            future = pool.submit(write)
            assert started.wait(5)
            with pytest.raises(TimeoutError):
                future.result(timeout=0.15)
        assert future.result(timeout=10).discarded == {"999"}


def test_policy_validation():
    with pytest.raises(ValueError):
        replace(DiscardSettings.current(), kinds=frozenset({"finished"}))
    with pytest.raises(ValueError):
        replace(DiscardSettings.current(), retention_days=0)


def test_batch_boundaries_and_sparse_reference_identity(database, monkeypatch):
    monkeypatch.setattr(Config, "EVENT_WRITE_BATCH_SIZE", 2)
    rows = [payload(sid) for sid in (1100, 1101, 1102)]
    rows[0]["competition_ref"]["slug"] = "league-slug"
    rows[1]["competition_ref"] = {"source_tournament_id": "4", "source_unique_tournament_id": "4"}
    commits = []
    record_commit = commits.append
    sql_event.listen(database.engine, "commit", record_commit)
    try:
        result = EventRepository.batch_upsert_events(iter(rows))
    finally:
        sql_event.remove(database.engine, "commit", record_commit)
    assert len(commits) == 2
    assert len(result.events) == 3
    assert len({event.competition_id for event in result.events.values()}) == 1
    with database.get_session() as session:
        from infrastructure.persistence.models import Competition

        league = session.get(Competition, result.events["1101"].competition_id)
        assert league.display_name == "League"
        assert league.slug == "league-slug"


def test_invalid_reference_does_not_abort_valid_batch(database):
    invalid = payload(1200)
    invalid["competition_ref"]["source_tournament_id"] = "bad-id"
    result = EventRepository.batch_upsert_events([invalid, payload(1201)])
    assert set(result.errors) == {"1200"}
    assert set(result.events) == {"1201"}


def test_daily_discard_is_successful_skip_before_normalization(database, monkeypatch):
    from modules.jobs.daily_discovery.persistence import persist_daily_events
    from types import SimpleNamespace

    obj = EventRepository.upsert_event(payload(1300))
    EventRepository.batch_delete_events(deletion(obj, 1300))
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])

    def unexpected(*args, **kwargs):
        pytest.fail("Discarded event reached normalization or odds persistence")

    result = persist_daily_events(
        SimpleNamespace(normalize_event_payload=unexpected),
        [{"id": 1300, "sport": "Football", "startTimestamp": 4_102_444_800}],
    )
    assert result.discarded == 1 and result.persisted == 0 and result.failed == 0


def test_results_collector_keeps_raw_evidence(database):
    from modules.sofascore.event_details import get_event_results
    from types import SimpleNamespace

    obj = EventRepository.upsert_event(payload(1400))
    response = {
        "event": {
            "id": 1400,
            "status": {"code": 60, "type": "postponed", "description": "Postponed"},
            "homeScore": {"current": 2},
            "awayScore": {},
        }
    }
    batch = DeletionBatch(origin="midnight-test")
    get_event_results(
        SimpleNamespace(request_json=lambda _: response),
        1400,
        canonical_event_id=obj.id,
        deferred_deletion_event_ids=batch,
        update_event_info=False,
    )
    assert batch.evidence[obj.id].snapshot["homeScore"] == {"current": 2}
    assert batch.evidence[obj.id].reason == "canceled_or_postponed"
    assert EventRepository.batch_delete_events(batch) == 1
    assert EventRepository.upsert_event(payload(1400)) is None


@pytest.mark.parametrize("kind", ["finished_empty_score", "not_found"])
def test_extended_memory_policy_blocks_recreation(database, caplog, kind):
    from types import SimpleNamespace
    from modules.sofascore.event_details import get_event_results
    from modules.sofascore.exceptions import SofaScoreNotFoundException

    obj = EventRepository.upsert_event(payload(1401))
    response = {"event": {
        "id": 1401, "status": {"code": 100, "type": "finished", "description": "Ended"},
        "homeScore": {}, "awayScore": {},
    }}

    def request_json(_):
        if kind == "not_found":
            raise SofaScoreNotFoundException("event endpoint not found")
        return response

    batch = DeletionBatch(origin="midnight-test")
    get_event_results(
        SimpleNamespace(request_json=request_json), 1401,
        canonical_event_id=obj.id, deferred_deletion_event_ids=batch, update_event_info=False,
    )
    caplog.set_level("INFO")
    assert EventRepository.batch_delete_events(batch) == 1
    assert EventRepository.upsert_event(payload(1401)) is None
    with database.get_session() as session:
        row = session.get(EventDiscardMemory, ("sofascore", "1401"))
        assert row.parser_kind == kind
        assert row.deletion_reason == ("event_endpoint_not_found" if kind == "not_found" else kind)
        if kind == "not_found":
            assert row.snapshot == {}
        else:
            assert row.snapshot["status"] == response["event"]["status"]
            assert row.snapshot["homeScore"] == row.snapshot["awayScore"] == {}
    assert "memory_eligible=1 memory_inserted=1 memory_existing=0 deleted_without_memory=0" in caplog.text


def test_postgres_bad_row_isolated_after_rollback(database):
    if database.engine.dialect.name != "postgresql":
        pytest.skip("PostgreSQL integer range enforcement")
    invalid = payload(1500)
    invalid["competition_ref"]["source_tournament_id"] = 2**100
    result = EventRepository.batch_upsert_events([payload(1501), invalid, payload(1502)])
    assert set(result.errors) == {"1500"}
    assert set(result.events) == {"1501", "1502"}


def test_migration_upgrade_and_downgrade(database):
    from importlib import import_module
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = import_module(
        "infrastructure.persistence.alembic.versions.20261001_01_event_discard_memory"
    )
    with database.engine.begin() as connection:
        EventDiscardMemory.__table__.drop(connection)
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
            assert "event_discard_memory" in inspect(connection).get_table_names()
            assert not inspect(connection).get_foreign_keys("event_discard_memory")
            migration.downgrade()
            assert "event_discard_memory" not in inspect(connection).get_table_names()
            migration.upgrade()
