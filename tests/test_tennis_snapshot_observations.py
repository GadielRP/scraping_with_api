from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from infrastructure.persistence.models import Base, Event, EventObservation
from infrastructure.persistence.repositories.observation_repository import (
    ObservationRepository,
)
from modules.jobs.pre_start_check_job.providers.sofascore import tennis_observations
from modules.jobs.pre_start_check_job.run_pre_start_check_job import (
    run_pre_start_odds_moments,
)
from modules.observations.service import SportObservationService


def _event(**overrides):
    payload = {
        "slug": "a-b",
        "start_time_utc": datetime(2026, 8, 14, 10, 40),
        "sport": "Tennis",
        "competition": "ATP",
        "home_team": "A",
        "away_team": "B",
    }
    payload.update(overrides)
    return Event(**payload)


def _session_factory(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    class _SessionCM:
        def __enter__(self):
            self.session = SessionLocal()
            return self.session

        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
            self.session.close()
            return False

    monkeypatch.setattr(
        "infrastructure.persistence.repositories.observation_repository.db_manager.get_session",
        lambda: _SessionCM(),
    )
    return engine, SessionLocal


def _t30_candidate(event_id, sport="Tennis", minutes=30, observations=None):
    return {
        "event_id": event_id,
        "minutes_until_start": minutes,
        "event_data": {"sport": sport},
        "metadata_snapshot": {
            "observations": observations
            if observations is not None
            else [
                {"type": "ground_type", "value": "hard", "sport": sport},
                {
                    "type": "rankings",
                    "home_ranking": 10,
                    "away_ranking": 20,
                },
            ]
        },
    }


def test_upsert_observations_uses_one_session(monkeypatch):
    _, SessionLocal = _session_factory(monkeypatch)
    with SessionLocal() as session:
        session.add_all([_event(), _event()])
        session.commit()
        event_ids = [row.id for row in session.query(Event.id).all()]

    session_enters = {"count": 0}

    class _CountingSessionCM:
        def __enter__(self):
            session_enters["count"] += 1
            self.session = SessionLocal()
            return self.session

        def __exit__(self, exc_type, exc, tb):
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
            self.session.close()
            return False

    monkeypatch.setattr(
        "infrastructure.persistence.repositories.observation_repository.db_manager.get_session",
        lambda: _CountingSessionCM(),
    )
    saved = ObservationRepository.upsert_observations(
        [
            {
                "event_id": event_ids[0],
                "sport": "Tennis",
                "observation_type": "ground_type",
                "observation_value": "clay",
            },
            {
                "event_id": event_ids[1],
                "sport": "Tennis",
                "observation_type": "ground_type",
                "observation_value": "hard",
            },
        ]
    )

    assert saved == 2
    assert session_enters["count"] == 1

    with SessionLocal() as session:
        values = {
            row.event_id: row.observation_value
            for row in session.query(EventObservation).all()
        }
    assert values == {event_ids[0]: "clay", event_ids[1]: "hard"}


def test_save_observations_for_events_skips_rankings_without_value():
    repo = SimpleNamespace(upsert_observations=MagicMock(return_value=1))
    service = SportObservationService(observation_repo=repo)

    saved = service.save_observations_for_events(
        [
            (
                101,
                [
                    {"type": "ground_type", "value": "hard", "sport": "Tennis"},
                    {"type": "rankings", "home_ranking": 1, "away_ranking": 2},
                ],
                "Tennis",
            )
        ]
    )

    assert saved == 1
    repo.upsert_observations.assert_called_once_with(
        [
            {
                "event_id": 101,
                "sport": "Tennis",
                "observation_type": "ground_type",
                "observation_value": "hard",
            }
        ]
    )


def test_persist_snapshot_observations_saves_first_empty_tennis(monkeypatch):
    captured = {}

    def _save(events):
        captured["events"] = events
        return len(events)

    monkeypatch.setattr(
        tennis_observations.sport_observation_service,
        "save_observations_for_events",
        _save,
    )

    tennis_t120 = _t30_candidate(11, minutes=120)
    already_hydrated = _t30_candidate(15, minutes=120)
    already_hydrated["observations"] = [{"type": "ground_type", "value": "clay"}]
    basketball = _t30_candidate(12, sport="Basketball", minutes=120)
    tennis_t5 = _t30_candidate(13, minutes=5)
    tennis_empty = _t30_candidate(14, minutes=120, observations=[])

    saved = tennis_observations.persist_snapshot_observations(
        [tennis_t120, already_hydrated, basketball, tennis_t5, tennis_empty]
    )

    assert saved == 2
    assert captured["events"] == [
        (
            11,
            tennis_t120["metadata_snapshot"]["observations"],
            "Tennis",
        ),
        (
            13,
            tennis_t5["metadata_snapshot"]["observations"],
            "Tennis",
        ),
    ]
    assert tennis_t120["observations"] == tennis_t120["metadata_snapshot"]["observations"]
    assert tennis_t5["observations"] == tennis_t5["metadata_snapshot"]["observations"]
    assert already_hydrated["observations"] == [{"type": "ground_type", "value": "clay"}]
    assert "observations" not in basketball


def test_attach_stored_observations_loads_missing_tennis_in_one_query(monkeypatch):
    captured = {}

    def _load(event_ids):
        captured["event_ids"] = event_ids
        return {
            21: [{"type": "ground_type", "value": "clay", "sport": "Tennis"}],
        }

    monkeypatch.setattr(
        tennis_observations.sport_observation_service,
        "observations_for_events",
        _load,
    )

    already_set = _t30_candidate(20)
    already_set["observations"] = [{"type": "ground_type", "value": "hard"}]
    missing = {
        "event_id": 21,
        "minutes_until_start": 1,
        "event_data": {"sport": "Tennis"},
        "metadata_snapshot": None,
    }
    basketball = _t30_candidate(22, sport="Basketball")

    attached = tennis_observations.attach_stored_observations(
        [already_set, missing, basketball]
    )

    assert attached == 1
    assert captured["event_ids"] == [21]
    assert missing["observations"] == [
        {"type": "ground_type", "value": "clay", "sport": "Tennis"}
    ]
    assert already_set["observations"] == [{"type": "ground_type", "value": "hard"}]


def test_enrich_skips_api_when_candidate_already_has_observations(monkeypatch):
    monkeypatch.setattr(
        tennis_observations.sport_observation_service,
        "event_has_observations",
        lambda _event_id: (_ for _ in ()).throw(AssertionError("should not query DB")),
    )
    monkeypatch.setattr(
        tennis_observations.api_client,
        "get_event_results",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("should not fetch /event")
        ),
    )

    candidate = _t30_candidate(31)
    candidate["observations"] = [{"type": "ground_type", "value": "hard"}]
    tennis_observations.enrich_tennis_observations(candidate)

    assert candidate["observations"] == [{"type": "ground_type", "value": "hard"}]


def test_run_pre_start_odds_moments_persists_snapshots_before_ingest(monkeypatch):
    calls = []
    plan = SimpleNamespace(candidates=[{"event_id": 1}])

    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.load_pre_start_odds_source_states",
        lambda _events: {},
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.build_pre_start_event_candidates",
        lambda *_args, **_kwargs: plan,
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.persist_snapshot_observations",
        lambda candidates: calls.append(("persist", candidates)),
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.attach_stored_observations",
        lambda candidates: calls.append(("attach", candidates)),
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job._ingest_provider_odds",
        lambda *_args, **_kwargs: calls.append("ingest"),
    )
    monkeypatch.setattr(
        "modules.jobs.pre_start_check_job.run_pre_start_check_job.evaluate_pre_start_key_moments",
        lambda *_args, **_kwargs: calls.append("evaluate"),
    )

    run_pre_start_odds_moments(
        scheduler=SimpleNamespace(),
        upcoming_events=[],
        timings={},
        key_moments=(30,),
        oddsportal_context=SimpleNamespace(),
    )

    assert calls == [
        ("attach", plan.candidates),
        ("persist", plan.candidates),
        "ingest",
        "evaluate",
    ]
