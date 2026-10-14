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
from modules.jobs.daily_discovery.persistence import DailyWriteSummary
from modules.sofascore import event_metadata, event_normalizer
from modules.sofascore.event_normalizer import normalize_event_payload
from unittest.mock import Mock
from modules.sofascore.client import SofaScoreAPI
from modules.sofascore.exceptions import SofaScoreHttpError, SofaScoreNotFoundException


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
    monkeypatch.setattr("modules.jobs.daily_discovery.persistence.utc_now", lambda: datetime(2026, 10, 2, tzinfo=timezone.utc))
    monkeypatch.setattr(Config, "EVENT_WRITE_BATCH_SIZE", 100)
    monkeypatch.setattr(
        import_module("modules.jobs.daily_discovery.pipeline"),
        "load_tracked_source_competitions",
        lambda _: None,
    )
    daily_discovery_repository.DailyDiscoveryRepository.initialize_sports_for_slot(
        "2026-10-03", "actualizacion", ["football"]
    )
    yield db
    Base.metadata.drop_all(db.engine)
    db.engine.dispose()


def test_real_batches_persist_complete_source_and_mark_sport_complete(database):
    stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=Provider())
    assert stats["events_inserted"] == stats["events_persisted"] == 205
    assert stats["events_failed"] == 0
    assert stats["sports_failed"] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == 205
        assert session.query(DailyDiscoveryLog).one().status == "completed"


def test_truncation_retains_commits_and_retry_is_idempotent(database, caplog):
    caplog.set_level("INFO", logger="modules.jobs.daily_discovery.pipeline")
    stats = discover_events_for_date(
        "2026-10-03", ["football"], "actualizacion", client=Provider(truncated=True)
    )
    assert stats["events_failed"] == 0
    assert stats["sports_failed"] == 1
    assert stats["events_unprocessed"] == 5
    incomplete = next(
        record.args[-1] for record in caplog.records
        if record.msg.startswith("Daily discovery batch") and record.args[-2] == "incomplete"
    )
    assert incomplete["events_admitted"] == incomplete["events_unprocessed"] == 5
    assert incomplete["events_persisted"] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == 200
        assert session.query(DailyDiscoveryLog).one().status == "failed"
    stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=Provider())
    assert stats["events_inserted"] == 5
    assert stats["events_updated"] == 200
    assert stats["events_failed"] == 0
    assert stats["sports_failed"] == 0


@pytest.mark.parametrize("status_code", [404, 503])
def test_tournament_404_preserves_partial_batch_and_continues_but_503_fails(database, caplog, status_code):
    class CalendarProvider(Provider):
        def __init__(self):
            super().__init__(count=1)
            self.requested = []

        def download_json(self, endpoint, body_file, params=None):
            self.requested.append(endpoint)
            if "scheduled-tournaments" in endpoint:
                body_file.write(json.dumps({
                    "scheduled": [
                        {"tournament": {"uniqueTournament": {"id": tournament_id}}}
                        for tournament_id in (5, 22772, 6)
                    ],
                    "hasNextPage": False,
                }).encode())
                body_file.seek(0)
                return
            if "/unique-tournament/22772/" in endpoint:
                if status_code == 404:
                    raise SofaScoreNotFoundException(0, endpoint)
                raise SofaScoreHttpError(status_code, 0, endpoint, "Service unavailable")
            super().download_json(endpoint, body_file, params)
            if "/unique-tournament/6/" in endpoint:
                data = json.loads(body_file.read())
                raw = data["events"][0]
                raw.update(id=2, slug="event-2")
                raw["tournament"]["id"] = 60
                raw["tournament"]["uniqueTournament"]["id"] = 6
                body_file.reset()
                body_file.write(json.dumps(data).encode())
                body_file.seek(0)

    provider = CalendarProvider()
    with caplog.at_level("INFO", logger="modules.jobs.daily_discovery.event_source"):
        stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=provider)
    continued = status_code == 404
    expected = 2 if continued else 0
    assert stats["events_processed"] == stats["events_persisted"] == expected
    assert stats["events_failed"] == 0
    assert stats["sports_failed"] == int(not continued)
    assert stats["events_unprocessed"] == (0 if continued else 1)
    assert any("/unique-tournament/6/" in endpoint for endpoint in provider.requested) == continued
    if continued:
        assert "'tournament_schedule_not_found': 1" in caplog.text
        assert "tournament_id=22772" in caplog.text
    with database.get_session() as session:
        assert session.query(Event).count() == expected
        assert session.query(EventSourceMapping).count() == expected
        assert session.query(DailyDiscoveryLog).one().status == ("completed" if continued else "failed")


def test_event_write_failures_have_separate_event_and_sport_counts(database, monkeypatch):
    pipeline = import_module("modules.jobs.daily_discovery.pipeline")
    monkeypatch.setattr(
        pipeline, "persist_daily_events",
        lambda *_args, **_kwargs: DailyWriteSummary(failed=2),
    )
    stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=Provider(count=2))
    assert stats["events_failed"] == 2
    assert stats["sports_failed"] == 1
    with database.get_session() as session:
        assert session.query(DailyDiscoveryLog).one().status == "failed"


def test_batch_and_final_logs_include_filters_and_rejected_tail_without_more_writes(database, monkeypatch, caplog):
    from modules.jobs.daily_discovery import persistence

    monkeypatch.setattr(settings, "SOFASCORE", replace(
        settings.SOFASCORE, round_filter_enabled=True,
        allowed_rounds=frozenset({"regular-season"}),
        filters=replace(
            settings.SOFASCORE.filters, exclude_categories=True, exclude_competitions=True,
            excluded_categories=frozenset({("football", "Filtered Category")}),
            excluded_sofascore_unique_tournament_ids=frozenset({41}),
        ),
    ))

    class FilteredProvider(Provider):
        def download_json(self, endpoint, body_file, params=None):
            super().download_json(endpoint, body_file, params)
            if "scheduled-tournaments" in endpoint:
                data = json.loads(body_file.read())
                data["scheduled"].extend([
                    {"tournament": {"uniqueTournament": {"id": 40}, "category": {"name": "Filtered Category"}}},
                    {"tournament": {"uniqueTournament": {"id": 41}, "category": {"name": "Country"}}},
                ])
            elif "scheduled-events" in endpoint:
                data = json.loads(body_file.read())
                for raw in data["events"]:
                    raw["roundInfo"] = {"slug": "regular-season" if 3 <= raw["id"] <= 102 else "quarterfinal"}
            else:
                return
            body_file.reset()
            body_file.write(json.dumps(data).encode())
            body_file.seek(0)

    writer = Mock(wraps=persistence.EventRepository.batch_upsert_events)
    discard_lookup = Mock(wraps=persistence.EventRepository.discarded_source_ids)
    monkeypatch.setattr(persistence.EventRepository, "batch_upsert_events", writer)
    monkeypatch.setattr(persistence.EventRepository, "discarded_source_ids", discard_lookup)
    with caplog.at_level("INFO", logger="modules.jobs.daily_discovery.pipeline"):
        stats = discover_events_for_date(
            "2026-10-03", ["football"], "actualizacion", client=FilteredProvider(count=202),
        )
    batches = [record for record in caplog.records if record.msg.startswith("Daily discovery batch")]
    assert all("process_rss_mb=%s" in record.msg for record in batches)
    assert [record.args[-2] for record in batches] == ["persistence", "admission"]
    first, tail = [record.args[-1] for record in batches]
    assert (first["events_received"], first["events_admitted"], first["events_filtered"], first["events_persisted"]) == (102, 100, 2, 100)
    assert (tail["events_received"], tail["events_admitted"], tail["events_filtered"], tail["events_persisted"]) == (100, 0, 100, 0)
    assert stats["events_received"] == 202
    assert stats["events_persisted"] == 100
    assert stats["events_filtered"] == stats["events_filtered_at_source"] == 102
    assert stats["event_filter_reasons"] == {"round_not_allowed": 102}
    assert stats["tournaments_filtered"] == 2
    assert stats["tournament_filter_reasons"] == {"excluded_category": 1, "excluded_competition": 1}
    assert stats["events_blocked_by_discard_memory"] == stats["events_unprocessed"] == 0
    assert caplog.records[-1].msg.startswith("Daily discovery summary")
    assert caplog.records[-1].args[-1] == stats
    writer.assert_called_once()
    discard_lookup.assert_called_once()


def test_batch_and_final_logs_distinguish_discard_memory_from_filter_rejections(database, monkeypatch, caplog):
    from modules.jobs.daily_discovery import persistence

    discard_lookup = Mock(return_value={"1"})
    monkeypatch.setattr(persistence.EventRepository, "discarded_source_ids", discard_lookup)
    with caplog.at_level("INFO", logger="modules.jobs.daily_discovery.pipeline"):
        stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=Provider(count=2))
    batch = next(record.args[-1] for record in caplog.records if record.msg.startswith("Daily discovery batch"))
    for counts in (batch, stats):
        assert counts["events_received"] == counts["events_admitted"] == 2
        assert counts["events_persisted"] == counts["events_blocked_by_discard_memory"] == 1
        assert counts["events_filtered"] == 0
        assert counts["event_filter_reasons"] == {}
    discard_lookup.assert_called_once()


def test_daily_future_toggle_rejects_before_normalization_and_can_persist_backfills(database, monkeypatch):
    monkeypatch.setattr("modules.jobs.discovery.filters.utc_now", lambda: datetime(2026, 10, 6, tzinfo=timezone.utc))
    provider = Provider(count=2)
    stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=provider)
    assert stats["events_processed"] == stats["events_persisted"] == 0
    assert stats["events_received"] == stats["events_filtered"] == 2
    assert stats["event_filter_reasons"] == {"start_too_soon_or_started": 2}
    assert stats["sports_failed"] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == 0
    monkeypatch.setattr(settings, "SOFASCORE", replace(settings.SOFASCORE,
        filters=replace(settings.SOFASCORE.filters, future_only=False)))
    stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=provider)
    assert stats["events_persisted"] == 2


def test_daily_rechecks_only_time_when_an_admitted_batch_expires(database, monkeypatch):
    from modules.jobs.daily_discovery import persistence

    classifier = Mock(wraps=event_metadata.classify_event_round)
    monkeypatch.setattr(event_metadata, "classify_event_round", classifier)
    monkeypatch.setattr(persistence, "utc_now", lambda: datetime(2100, 1, 1, tzinfo=timezone.utc))
    normalizer = Mock(side_effect=AssertionError("Expired batches must not be normalized"))
    discarded = Mock(side_effect=AssertionError("Expired batches must not query discard memory"))
    provider = Provider(count=2)
    monkeypatch.setattr(provider, "normalize_event_payload", normalizer)
    monkeypatch.setattr(persistence.EventRepository, "discarded_source_ids", discarded)
    stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=provider)
    assert stats["events_processed"] == stats["events_filtered"] == 2
    assert stats["events_filtered_at_source"] == 0
    assert stats["events_filtered_before_write"] == 2
    assert stats["event_filter_reasons"] == {"start_too_soon_or_started": 2}
    assert stats["events_persisted"] == stats["events_failed"] == stats["sports_failed"] == 0
    assert classifier.call_count == 2
    normalizer.assert_not_called()
    discarded.assert_not_called()


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

    stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=ExcludedProvider())
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
        "2026-10-03", "actualizacion", ["tennis"]
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
        def normalize_event_payload(raw, discovery_source, **kwargs):
            normalized_ids.append(raw["id"])
            return normalize_event_payload(raw, discovery_source, **kwargs)

    stats = discover_events_for_date("2026-10-03", ["tennis"], "actualizacion", client=TennisProvider())
    assert set(normalized_ids) == expected_ids
    assert stats["events_processed"] == stats["events_persisted"] == len(expected_ids)
    assert stats["events_failed"] == stats["sports_failed"] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == len(expected_ids)
        assert {int(row.source_event_id) for row in session.query(EventSourceMapping)} == expected_ids
        assert session.query(DailyDiscoveryLog).filter_by(sport="tennis").one().status == "completed"


@pytest.mark.parametrize("enabled,expected_ids", [(True, {2}), (False, {1, 2})])
def test_round_toggle_rejects_before_normalization_and_event_mapping_writes(database, monkeypatch, enabled, expected_ids):
    monkeypatch.setattr(settings, "SOFASCORE", replace(
        settings.SOFASCORE, round_filter_enabled=enabled,
        allowed_rounds=frozenset({"regular-season"}),
    ))
    normalized_ids = []
    classifier = Mock(wraps=event_metadata.classify_event_round)
    monkeypatch.setattr(event_metadata, "classify_event_round", classifier)
    monkeypatch.setattr(event_normalizer, "classify_event_round", Mock(
        side_effect=AssertionError("Admission already resolved this round"),
    ))

    class RoundProvider(Provider):
        def download_json(self, endpoint, body_file, params=None):
            super().download_json(endpoint, body_file, params)
            if "scheduled-events" not in endpoint:
                return
            data = json.loads(body_file.read())
            for raw, slug in zip(data["events"], ("quarterfinal", "regular-season")):
                raw["roundInfo"] = {"slug": slug}
            body_file.reset()
            body_file.write(json.dumps(data).encode())
            body_file.seek(0)

        @staticmethod
        def normalize_event_payload(raw, discovery_source, **kwargs):
            normalized_ids.append(raw["id"])
            return normalize_event_payload(raw, discovery_source, **kwargs)

    stats = discover_events_for_date("2026-10-03", ["football"], "actualizacion", client=RoundProvider(count=2))
    assert set(normalized_ids) == expected_ids
    assert classifier.call_count == 2
    assert stats["events_processed"] == stats["events_persisted"] == len(expected_ids)
    assert stats["events_received"] == 2
    assert stats["events_filtered"] == (1 if enabled else 0)
    assert stats["event_filter_reasons"] == ({"round_not_allowed": 1} if enabled else {})
    assert stats["events_failed"] == stats["sports_failed"] == 0
    with database.get_session() as session:
        assert session.query(Event).count() == len(expected_ids)
        assert {int(row.source_event_id) for row in session.query(EventSourceMapping)} == expected_ids
        assert session.query(DailyDiscoveryLog).one().status == "completed"
