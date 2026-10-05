"""Exercise incremental discovery with real SQL writes and a deterministic provider."""

from importlib import import_module
import json
import os
import pytest
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Event, DailyDiscoveryLog
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.repositories import event_repository, daily_discovery_repository
from infrastructure.settings import Config
from modules.jobs.daily_discovery.pipeline import discover_events_for_date
from modules.sofascore.event_normalizer import normalize_event_payload


class Provider:
    def __init__(self, count=205, truncated=False):
        self.count, self.truncated = count, truncated

    normalize_event_payload = staticmethod(normalize_event_payload)

    def request_json(self, endpoint, *, body_file):
        if "scheduled-tournaments" in endpoint:
            body_file.write(
                b'{"scheduled":[{"tournament":{"id":50,"uniqueTournament":{"id":5}}}],"hasNextPage":false}'
            )
        elif "/odds/" in endpoint:
            body_file.write(b'{"odds":{}}')
        else:
            body_file.write(b'{"events":[')
            for index in range(1, self.count + 1):
                if index > 1:
                    body_file.write(b",")
                raw = dict(
                    id=index,
                    startTimestamp=1791108000,
                    slug=f"event-{index}",
                    homeTeam=dict(id=1, name="Home", gender="M"),
                    awayTeam=dict(id=2, name="Away", gender="M"),
                    tournament=dict(
                        id=50,
                        name="League",
                        uniqueTournament=dict(id=5, name="League"),
                        category=dict(name="Country", sport=dict(name="Football", id=1)),
                    ),
                    status=dict(type="notstarted", code=0),
                )
                body_file.write(json.dumps(raw).encode())
            body_file.write(b"," if self.truncated else b"]}")
        body_file.seek(0)
        return body_file


@pytest.fixture
def database(tmp_path, monkeypatch):
    db = DatabaseManager(
        os.environ.get("DAILY_TEST_DATABASE_URL") or f'sqlite:///{tmp_path / "daily.db"}'
    )
    Base.metadata.create_all(db.engine)
    monkeypatch.setattr(event_repository, "db_manager", db)
    monkeypatch.setattr(daily_discovery_repository, "db_manager", db)
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])
    monkeypatch.setattr(Config, "EVENT_WRITE_BATCH_SIZE", 100)
    monkeypatch.setattr(
        import_module("modules.jobs.daily_discovery.pipeline"),
        "load_tracked_source_competitions",
        lambda _: None,
    )
    daily_discovery_repository.DailyDiscoveryRepository.initialize_sports_for_slot(
        "2026-10-03", "current_utc_day", ["football"]
    )
    yield db
    Base.metadata.drop_all(db.engine)
    db.engine.dispose()


def test_real_batches_persist_complete_source_and_mark_sport_complete(database):
    stats = discover_events_for_date("2026-10-03", ["football"], "current_utc_day", client=Provider())
    assert stats["events_inserted"] == stats["events_persisted"] == 205
    assert stats["events_failed"] == 0
    assert stats["sports_failed"] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == 205
        assert session.query(DailyDiscoveryLog).one().status == "completed"


def test_truncation_retains_commits_and_retry_is_idempotent(database):
    stats = discover_events_for_date(
        "2026-10-03", ["football"], "current_utc_day", client=Provider(truncated=True)
    )
    assert stats["events_failed"] == 0
    assert stats["sports_failed"] == 1
    with database.get_session() as session:
        assert session.query(Event).count() == 200
        assert session.query(DailyDiscoveryLog).one().status == "failed"
    stats = discover_events_for_date("2026-10-03", ["football"], "current_utc_day", client=Provider())
    assert stats["events_inserted"] == 5
    assert stats["events_updated"] == 200
    assert stats["events_failed"] == 0
    assert stats["sports_failed"] == 0


def test_event_write_failures_have_separate_event_and_sport_counts(database, monkeypatch):
    from types import SimpleNamespace

    pipeline = import_module("modules.jobs.daily_discovery.pipeline")
    monkeypatch.setattr(
        pipeline, "persist_daily_events",
        lambda *_args, **_kwargs: SimpleNamespace(
            persisted=0, inserted=0, updated=0, discarded=0, failed=2
        ),
    )
    stats = discover_events_for_date("2026-10-03", ["football"], "current_utc_day", client=Provider(count=2))
    assert stats["events_failed"] == 2
    assert stats["sports_failed"] == 1
    with database.get_session() as session:
        assert session.query(DailyDiscoveryLog).one().status == "failed"
