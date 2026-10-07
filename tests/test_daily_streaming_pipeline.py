"""Exercise incremental discovery with real SQL writes and a deterministic provider."""

from importlib import import_module
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
import pytest
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Event, EventSourceMapping, DailyDiscoveryLog
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.repositories import event_repository, daily_discovery_repository
from infrastructure.settings import Config
from infrastructure.settings import discovery as settings
from modules.jobs.daily_discovery.pipeline import discover_events_for_date
from modules.sofascore.event_normalizer import normalize_event_payload
from modules.sofascore.client import SofaScoreAPI


class Provider:
    def __init__(self, count=205, truncated=False):
        self.count, self.truncated = count, truncated

    normalize_event_payload = staticmethod(normalize_event_payload)
    open_scheduled_tournaments = SofaScoreAPI.open_scheduled_tournaments
    open_scheduled_events = SofaScoreAPI.open_scheduled_events
    open_scheduled_odds = SofaScoreAPI.open_scheduled_odds

    def download_json(self, endpoint, body_file, params=None):
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


@pytest.fixture
def database(tmp_path, monkeypatch):
    db = DatabaseManager(
        os.environ.get("DAILY_TEST_DATABASE_URL") or f'sqlite:///{tmp_path / "daily.db"}'
    )
    Base.metadata.create_all(db.engine)
    monkeypatch.setattr(event_repository, "db_manager", db)
    monkeypatch.setattr(daily_discovery_repository, "db_manager", db)
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["Football"])
    monkeypatch.setattr("modules.jobs.discovery.filters.utc_now", lambda: datetime(2026, 10, 2, tzinfo=timezone.utc))
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
            persisted=0, inserted=0, updated=0, discarded=0, failed=2, filtered=0
        ),
    )
    stats = discover_events_for_date("2026-10-03", ["football"], "current_utc_day", client=Provider(count=2))
    assert stats["events_failed"] == 2
    assert stats["sports_failed"] == 1
    with database.get_session() as session:
        assert session.query(DailyDiscoveryLog).one().status == "failed"


def test_daily_future_toggle_rejects_before_normalization_and_can_persist_backfills(database, monkeypatch):
    monkeypatch.setattr("modules.jobs.discovery.filters.utc_now", lambda: datetime(2026, 10, 6, tzinfo=timezone.utc))
    provider = Provider(count=2)
    stats = discover_events_for_date("2026-10-03", ["football"], "current_utc_day", client=provider)
    assert stats["events_processed"] == stats["events_persisted"] == 0
    assert stats["sports_failed"] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == 0
    monkeypatch.setattr(settings, "SOFASCORE", replace(settings.SOFASCORE,
        filters=replace(settings.SOFASCORE.filters, future_only=False)))
    stats = discover_events_for_date("2026-10-03", ["football"], "current_utc_day", client=provider)
    assert stats["events_persisted"] == 2


def test_excluded_calendar_category_does_not_download_or_normalize_events(database):
    class ExcludedProvider(Provider):
        def download_json(self, endpoint, body_file, params=None):
            assert "scheduled-events" not in endpoint
            if "/odds/" in endpoint:
                body_file.write(b'{"odds":{}}')
            else:
                assert "scheduled-tournaments" in endpoint
                body_file.write(b'{"scheduled":[{"tournament":{"id":50,"uniqueTournament":{"id":5},'
                                b'"category":{"name":"Romania Amateur"}}}],"hasNextPage":false}')
            body_file.seek(0)

        @staticmethod
        def normalize_event_payload(*args, **kwargs):
            pytest.fail("Excluded tournaments must be rejected before normalization")

    stats = discover_events_for_date("2026-10-03", ["football"], "current_utc_day", client=ExcludedProvider())
    assert stats["events_processed"] == stats["events_persisted"] == stats["sports_failed"] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == 0
        assert session.query(DailyDiscoveryLog).one().status == "completed"


@pytest.mark.parametrize("enabled,expected_ids", [(True, {102}), (False, {101, 102})])
def test_tennis_ranking_toggle_filters_before_normalization_and_writes(database, monkeypatch, enabled, expected_ids):
    monkeypatch.setattr(Config, "SUPPORTED_SPORTS", ["tennis"])
    monkeypatch.setattr(settings, "SOFASCORE", replace(
        settings.SOFASCORE, tennis_ranking_filter_enabled=enabled, tennis_ranking_cutoff=120,
    ))
    daily_discovery_repository.DailyDiscoveryRepository.initialize_sports_for_slot(
        "2026-10-03", "current_utc_day", ["tennis"]
    )
    normalized_ids = []

    class TennisProvider(Provider):
        def download_json(self, endpoint, body_file, params=None):
            if "scheduled-events" not in endpoint:
                return super().download_json(endpoint, body_file, params)
            events = []
            for source_id, ranking in [(101, 120), (102, 119)]:
                events.append(dict(
                    id=source_id, slug=f"tennis-{source_id}", startTimestamp=1791108000,
                    homeTeam=dict(id=source_id + 100, name=f"Home {source_id}", gender="M",
                                  playerTeamInfo=dict(currentRanking=ranking)),
                    awayTeam=dict(id=source_id + 200, name=f"Away {source_id}", gender="M"),
                    tournament=dict(id=50, name="ATP Tournament",
                                    uniqueTournament=dict(id=5, name="ATP Tournament"),
                                    category=dict(name="ATP", sport=dict(name="Tennis", id=5))),
                    status=dict(type="notstarted", code=0),
                ))
            body_file.write(json.dumps({"events": events}).encode())
            body_file.seek(0)

        @staticmethod
        def normalize_event_payload(raw, discovery_source):
            normalized_ids.append(raw["id"])
            return normalize_event_payload(raw, discovery_source)

    stats = discover_events_for_date("2026-10-03", ["tennis"], "current_utc_day", client=TennisProvider())
    assert set(normalized_ids) == expected_ids
    assert stats["events_processed"] == stats["events_persisted"] == len(expected_ids)
    assert stats["events_failed"] == stats["sports_failed"] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == len(expected_ids)
        assert {int(row.source_event_id) for row in session.query(EventSourceMapping)} == expected_ids
        assert session.query(DailyDiscoveryLog).filter_by(sport="tennis").one().status == "completed"
