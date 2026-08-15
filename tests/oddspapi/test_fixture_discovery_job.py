from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from infrastructure.persistence.models import (
    Base,
    Event,
    EventSourceMapping,
    Participant,
)
from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)
from infrastructure.persistence.repositories.participant_repository import ParticipantRepository
from modules.jobs.oddspapi.fixture_discovery.fixture_batch_processor import (
    OddspapiFixtureBatchProcessor,
)
from modules.jobs.oddspapi.fixture_discovery.fixture_discovery_job import OddspapiFixtureDiscoveryJob
from modules.jobs.oddspapi.fixture_discovery.response_utils import (
    extract_fixture_list,
    split_time_window,
)
from modules.oddspapi.exceptions import OddsPapiHttpError
from modules.oddspapi.event_resolver import OddspapiEventResolver
from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity
from modules.oddspapi.fixture_persistence import (
    ResolvedFixtureWrite,
    participant_slug,
    persist_resolved_fixtures,
)
from modules.jobs.oddspapi.fixture_discovery.run_fixture_discovery import (
    _resolve_sports,
    build_parser,
)


def _fixture(fixture_id: str = "f-1") -> dict:
    return {
        "fixtureId": fixture_id,
        "sportId": 10,
        "sportName": "Soccer",
        "startTime": "2026-07-15T12:00:00Z",
        "participant1Name": "Home",
        "participant2Name": "Away",
    }


@pytest.mark.parametrize(
    "payload",
    [[_fixture()], {"fixtures": [_fixture()]}, {"data": [_fixture()]}, {"items": [_fixture()]}],
)
def test_extract_fixture_list_supports_raw_and_wrapped_shapes(payload):
    assert extract_fixture_list(payload) == [_fixture()]


def test_extract_fixture_list_unsupported_shape_returns_empty(caplog):
    assert extract_fixture_list({"unexpected": []}) == []
    assert "Unsupported Oddspapi" in caplog.text


def test_window_splitting_keeps_chunks_within_limit():
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    chunks = split_time_window(start, start + timedelta(hours=121), max_window_hours=48)
    assert len(chunks) == 3
    assert all((end - begin) <= timedelta(hours=48) for begin, end in chunks)
    assert chunks[0][0] == start
    assert chunks[-1][1] == start + timedelta(hours=121)


def test_cli_sport_subset_and_unknown_slug():
    assert _resolve_sports("soccer, baseball") == {"soccer": 10, "baseball": 13}
    with pytest.raises(ValueError, match="unknown sport"):
        _resolve_sports("soccer,quidditch")
    assert build_parser().parse_args(["--chunk-size", "25"]).chunk_size == 25


def test_fixture_summary_omits_missing_metadata():
    fixture = OddspapiFixtureIdentity.from_payload({"fixtureId": "fixture-1"})

    assert OddspapiEventResolver._fixture_summary(fixture) == "fixture_id=fixture-1"


def test_fixture_summary_keeps_available_ids_when_names_are_missing():
    fixture = OddspapiFixtureIdentity.from_payload(
        {
            "fixtureId": "fixture-1",
            "participant1Id": "home-1",
            "participant2Id": "away-1",
            "sportId": 10,
            "tournamentId": "tournament-1",
        }
    )

    assert (
        OddspapiEventResolver._fixture_summary(fixture)
        == "fixture_id=fixture-1 participants=home-1 vs away-1 tournament=tournament-1 sport=10"
    )


def test_chunk_size_is_validated_and_forwarded_to_batch_processor():
    job = OddspapiFixtureDiscoveryJob(client=_Client(), chunk_size=25)
    assert job.batch_processor.chunk_size == 25
    with pytest.raises(ValueError, match="chunk_size must be positive"):
        OddspapiFixtureBatchProcessor(chunk_size=0)


class _Client:
    def __init__(self):
        self.calls = []

    def get_fixtures(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["sport_id"] == 11:
            raise RuntimeError("basketball unavailable")
        return [_fixture(f"{kwargs['sport_id']}-fixture")]


class _BatchResult:
    fixtures_valid = 1
    fixtures_deduplicated = 0
    invalid_payloads = 0
    resolved_existing_oddspapi = 1
    resolved_external_sofascore = 0
    resolved_candidate_match = 0
    mappings_created = 0
    unresolved_no_candidates = 0
    needs_review = 0
    queue_rows_written = 0


class _BatchProcessor:
    def process_batch(self, **kwargs):
        return _BatchResult()


@contextmanager
def _session():
    yield object()


def test_api_error_for_one_sport_does_not_stop_other_sports(monkeypatch):
    client = _Client()
    monkeypatch.setattr(
        "modules.jobs.oddspapi.fixture_discovery.fixture_discovery_job.db_manager.get_session",
        _session,
    )
    job = OddspapiFixtureDiscoveryJob(
        client=client,
        sports={"soccer": 10, "basketball": 11},
        create_mappings=False,
        batch_processor=_BatchProcessor(),
    )
    summary = job.run(
        datetime(2026, 7, 15, tzinfo=timezone.utc),
        datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert [sport.sport_slug for sport in summary.sports] == ["soccer", "basketball"]
    assert summary.sports[0].fixtures_fetched == 1
    assert summary.sports[1].errors == 1
    assert summary.total_fixtures_fetched == 1


def test_fixture_not_found_is_treated_as_empty_success(monkeypatch):
    class _NoFixturesClient:
        def get_fixtures(self, **kwargs):
            raise OddsPapiHttpError(
                status_code=404,
                endpoint="/v4/fixtures",
                response_text="fixture not found",
                error_code="FIXTURE_NOT_FOUND",
            )

    job = OddspapiFixtureDiscoveryJob(
        client=_NoFixturesClient(),
        sports={"american-football": 14},
        create_mappings=False,
    )
    summary = job.run(
        datetime(2026, 7, 15, tzinfo=timezone.utc),
        datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert summary.sports[0].errors == 0
    assert summary.sports[0].fixtures_fetched == 0


def test_successful_oddspapi_mapping_persists_and_links_participants(monkeypatch):
    participant_calls = []
    mapping_calls = []

    def upsert_participants(session, participants_data):
        rows = list(participants_data)
        participant_calls.append((session, rows))
        return {
            ("oddspapi", 186328): SimpleNamespace(participant_id=701),
            ("oddspapi", 414379): SimpleNamespace(participant_id=702),
        }

    monkeypatch.setattr(
        ParticipantRepository,
        "upsert_participants",
        upsert_participants,
    )
    monkeypatch.setattr(
        EventSourceMappingRepository,
        "upsert_mappings",
        lambda **kwargs: mapping_calls.append(kwargs),
    )

    fixture = OddspapiFixtureIdentity.from_payload(
        {
            "fixtureId": "id1001460467877390",
            "participant1Id": 186328,
            "participant2Id": 414379,
            "participant1Name": "Ivory Coast",
            "participant1ShortName": "Ivory Coast",
            "participant1Abbr": "CIV",
            "participant2Name": "Burkina Faso",
            "participant2ShortName": "Burkina Faso",
            "participant2Abbr": "BUR",
            "sportId": 10,
            "tournamentId": 14604,
            "seasonId": 138746,
            "externalProviders": {"sofascoreId": None},
        }
    )
    session = object()

    persisted_sources = persist_resolved_fixtures(
        session,
        [
            ResolvedFixtureWrite(
                canonical_event_id=159317,
                fixture=fixture,
                match_method="deterministic_candidate_match",
                confidence=1.0,
            )
        ],
    )

    assert participant_calls == [
        (
            session,
            [
                {
                    "source": "oddspapi",
                    "source_participant_id": 186328,
                    "name": "Ivory Coast",
                    "slug": "ivory-coast",
                    "short_name": "Ivory Coast",
                    "code_name": "CIV",
                },
                {
                    "source": "oddspapi",
                    "source_participant_id": 414379,
                    "name": "Burkina Faso",
                    "slug": "burkina-faso",
                    "short_name": "Burkina Faso",
                    "code_name": "BUR",
                },
            ],
        ),
    ]
    assert mapping_calls == [
        {
            "session": session,
            "mappings_data": [
                {
                    "event_id": 159317,
                    "source": "oddspapi",
                    "source_event_id": "id1001460467877390",
                    "source_sport_id": "10",
                    "source_tournament_id": "14604",
                    "source_season_id": "138746",
                    "participant_home_id": 701,
                    "participant_away_id": 702,
                    "match_method": "deterministic_candidate_match",
                    "confidence": 1.0,
                    "raw_external_providers": {"sofascoreId": None},
                }
            ],
        }
    ]
    assert persisted_sources == {"id1001460467877390": ["oddspapi"]}


def test_oddspapi_participant_slug_normalizes_name():
    assert participant_slug("Atlético Nacional") == "atletico-nacional"


def test_existing_oddspapi_mapping_hydrates_participant_links(monkeypatch):
    persistence_calls = []
    fixture = OddspapiFixtureIdentity.from_payload(
        {
            "fixtureId": "existing-participant-link-fixture",
            "participant1Id": 186328,
            "participant2Id": 414379,
            "participant1Name": "Ivory Coast",
            "participant2Name": "Burkina Faso",
        }
    )

    monkeypatch.setattr(
        OddspapiEventResolver,
        "_persist_oddspapi_mapping",
        classmethod(lambda cls, **kwargs: persistence_calls.append(kwargs)),
    )
    session = object()

    resolution = OddspapiEventResolver.resolve_fixture_identity_in_session(
        fixture=fixture,
        session=session,
        create_mappings=True,
        existing_oddspapi={fixture.fixture_id: 159317},
        existing_sofascore={},
    )

    assert resolution.layer1_resolved is True
    assert persistence_calls == [
        {
            "canonical_event_id": 159317,
            "fixture": fixture,
            "match_method": None,
            "confidence": None,
            "session": session,
        }
    ]


def test_event_source_mapping_repository_stores_participant_links():
    # Isolated in-memory engine: this test must never write Event/Participant/
    # EventSourceMapping rows into the real configured database (doing so
    # previously leaked a permanent "events without SofaScore mapping" row
    # that broke the startup schema migration validation).
    engine = create_engine("sqlite:///:memory:")
    Event.__table__.create(bind=engine)
    Participant.__table__.create(bind=engine)
    EventSourceMapping.__table__.create(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)

    with Session() as session:
        event = Event(
            slug="oddspapi-participant-link-test",
            start_time_utc=datetime(2099, 1, 3, 12, 0),
            sport="Football",
            competition="Test Competition",
            home_team="Ivory Coast",
            away_team="Burkina Faso",
        )
        participant_home = Participant(
            source="oddspapi",
            source_participant_id=9186328,
            name="Ivory Coast",
            slug="ivory-coast",
            short_name="Ivory Coast",
            code_name="CIV",
        )
        participant_away = Participant(
            source="oddspapi",
            source_participant_id=9414379,
            name="Burkina Faso",
            slug="burkina-faso",
            short_name="Burkina Faso",
            code_name="BUR",
        )
        session.add_all((event, participant_home, participant_away))
        session.flush()

        mapping = EventSourceMappingRepository.upsert_mapping(
            event_id=event.id,
            source="oddspapi",
            source_event_id="participant-link-test-fixture",
            participant_home_id=participant_home.participant_id,
            participant_away_id=participant_away.participant_id,
            session=session,
        )

        assert mapping.participant_home_id == participant_home.participant_id
        assert mapping.participant_away_id == participant_away.participant_id


def test_participant_batch_deduplicates_and_skips_unchanged_updates():
    engine = create_engine("sqlite:///:memory:")
    Participant.__table__.create(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    statements: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def record_statement(_connection, _cursor, statement, _parameters, _context, _many):
        statements.append(statement.strip().upper())

    def participant(source_id: int, name: str) -> dict:
        return {
            "source": "oddspapi",
            "source_participant_id": source_id,
            "name": name,
            "slug": name.casefold().replace(" ", "-"),
            "short_name": name,
            "code_name": name[:3].upper(),
        }

    with Session() as session:
        inserted = ParticipantRepository.upsert_participants(
            session,
            [
                participant(101, "Home Team"),
                participant(102, "Away Team"),
                participant(101, "Home Team"),
            ],
        )
        session.commit()
        assert len(inserted) == 2

        statements.clear()
        unchanged = ParticipantRepository.upsert_participants(
            session,
            [participant(101, "Home Team"), participant(102, "Away Team")],
        )
        assert len(unchanged) == 2
        assert sum(statement.startswith("SELECT") for statement in statements) == 1
        assert not any(statement.startswith("UPDATE") for statement in statements)

        statements.clear()
        ParticipantRepository.upsert_participants(
            session,
            [participant(101, "Home Team Renamed"), participant(102, "Away Team")],
        )
        assert sum(statement.startswith("SELECT") for statement in statements) == 1
        assert sum(statement.startswith("UPDATE") for statement in statements) == 1


def test_mapping_batch_uses_bulk_preloads_and_skips_unchanged_updates():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    statements: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def record_statement(_connection, _cursor, statement, _parameters, _context, _many):
        statements.append(statement.strip().upper())

    with Session() as session:
        canonical_event = Event(
            slug="batch-mapping-test",
            start_time_utc=datetime(2099, 1, 1, 12, 0),
            sport="Football",
            competition="Test",
            home_team="Home",
            away_team="Away",
        )
        session.add(canonical_event)
        session.flush()
        rows = [
            {
                "event_id": canonical_event.id,
                "source": "oddspapi",
                "source_event_id": "fixture-1",
                "match_method": "deterministic_candidate_match",
                "confidence": 0.98,
            },
            {
                "event_id": canonical_event.id,
                "source": "oddspapi",
                "source_event_id": "fixture-2",
                "match_method": "deterministic_candidate_match",
                "confidence": 0.97,
            },
        ]

        inserted = EventSourceMappingRepository.upsert_mappings(session, rows)
        session.commit()
        assert len(inserted) == 2

        statements.clear()
        unchanged = EventSourceMappingRepository.upsert_mappings(session, rows)
        assert len(unchanged) == 2
        assert sum(statement.startswith("SELECT") for statement in statements) == 2
        assert not any(statement.startswith("UPDATE") for statement in statements)
