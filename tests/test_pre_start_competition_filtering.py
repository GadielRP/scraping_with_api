from contextlib import contextmanager
from types import SimpleNamespace

from infrastructure.persistence.repositories import event_repository
from modules.competition.tracked_competitions import (
    TRACKED_COMPETITION_IDS,
    is_tracked_competition,
)
from modules.jobs.pre_start_check_job import key_moment_evaluation
from modules.jobs.pre_start_check_job import oddsportal_worker
from modules.jobs.pre_start_check_job import (
    run_pre_start_check_job as pre_start_job,
)


EXPECTED_TRACKED_COMPETITION_IDS = {
    176,
    318,
    129,
    167,
    88,
    168,
    50,
    171,
}


def test_tracked_competitions_are_canonical_and_season_independent():
    assert TRACKED_COMPETITION_IDS == EXPECTED_TRACKED_COMPETITION_IDS
    assert is_tracked_competition(176)
    assert is_tracked_competition("176")
    assert not is_tracked_competition(146)  # NBA playoffs
    assert not is_tracked_competition(None)


def test_pre_start_query_scope_uses_competition_ids(monkeypatch):
    monkeypatch.setattr(
        pre_start_job.Config,
        "PRE_START_TRACKED_COMPETITIONS_ONLY",
        True,
    )

    assert set(pre_start_job._tracked_competition_ids()) == (
        EXPECTED_TRACKED_COMPETITION_IDS
    )

    monkeypatch.setattr(
        pre_start_job.Config,
        "PRE_START_TRACKED_COMPETITIONS_ONLY",
        False,
    )
    assert pre_start_job._tracked_competition_ids() is None


def test_event_repository_pushes_competition_filter_into_query(monkeypatch):
    class RecordingQuery:
        def __init__(self):
            self.criteria = []

        def options(self, *_args):
            return self

        def filter(self, *criteria):
            self.criteria.extend(criteria)
            return self

        def all(self):
            return []

    query = RecordingQuery()
    session = SimpleNamespace(query=lambda *_args: query)

    @contextmanager
    def fake_session():
        yield session

    monkeypatch.setattr(
        event_repository.db_manager,
        "get_session",
        fake_session,
    )

    event_repository.EventRepository.get_events_starting_soon(
        competition_ids=[176, 168],
    )

    assert any(
        getattr(getattr(criterion, "left", None), "key", None)
        == "competition_id"
        for criterion in query.criteria
    )


def test_oddsportal_candidate_does_not_depend_on_season_id(monkeypatch):
    monkeypatch.setattr(
        oddsportal_worker.Config,
        "ODDSPORTAL_SCRAPING_ENABLED",
        True,
    )
    events = [
        {
            "id": 1,
            "competition_id": 176,
            "season_id": 999999,
        },
        {
            "id": 2,
            "competition_id": 146,
            "season_id": 80229,
        },
    ]

    candidates = oddsportal_worker.build_oddsportal_scrape_candidates(
        events,
        {1: -5, 2: -5},
    )

    assert [candidate["event_id"] for candidate in candidates] == [1]


def test_pipeline_gate_filters_before_alerts_and_pillars(monkeypatch):
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS",
        True,
    )
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "ENABLE_LEGACY_ALERT_PIPELINE",
        True,
    )
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "ENABLE_PILLAR_PIPELINE",
        True,
    )
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "PRE_START_ODDS_MOMENTS",
        [30],
    )
    event_plan = SimpleNamespace(
        candidates=[
            {
                "event_id": 1,
                "minutes_until_start": 30,
                "event_data": {"competition_id": 176},
            },
            {
                "event_id": 2,
                "minutes_until_start": 30,
                "event_data": {"competition_id": 146},
            },
        ],
        by_event_id={},
    )
    calls = []
    monkeypatch.setattr(
        key_moment_evaluation,
        "_hydrate_missing_tennis_metadata",
        lambda _scheduler, candidates, _moments: calls.append(
            ("hydrate", [candidate["event_id"] for candidate in candidates])
        ),
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "_load_trajectory_payloads",
        lambda event_ids, _moments: (
            calls.append(("trajectory", set(event_ids))) or {}
        ),
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "_build_evaluation_payloads",
        lambda _scheduler, _plan, event_ids, *_args: [
            {"event_id": event_id}
            for event_id in event_ids
        ],
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "evaluate_and_dispatch_alerts_batch",
        lambda payloads, *_args, **_kwargs: calls.append(
            ("alerts", [payload["event_id"] for payload in payloads])
        ),
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "evaluate_and_calculate_pillars_batch",
        lambda events_for_pillars, *_args, **_kwargs: calls.append(
            (
                "pillars",
                [payload["event_id"] for payload in events_for_pillars],
            )
        ),
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "flush_missing_standings_endpoints",
        lambda _competition_ids: None,
    )

    key_moment_evaluation.evaluate_pre_start_key_moments(
        SimpleNamespace(event_repo=SimpleNamespace()),
        event_plan,
        SimpleNamespace(event_states={}, event_ids=set(), data_cache={}),
    )

    assert calls == [
        ("hydrate", [1]),
        ("alerts", [1]),
        ("trajectory", {1}),
        ("pillars", [1]),
    ]


def test_legacy_alert_pipeline_does_not_load_pillar_trajectory(monkeypatch):
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS",
        True,
    )
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "ENABLE_LEGACY_ALERT_PIPELINE",
        True,
    )
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "ENABLE_PILLAR_PIPELINE",
        False,
    )
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "PRE_START_ODDS_MOMENTS",
        [30],
    )

    event_plan = SimpleNamespace(
        candidates=[
            {
                "event_id": 1,
                "minutes_until_start": 30,
                "event_data": {"competition_id": 176},
            }
        ],
        by_event_id={},
    )
    calls = []
    monkeypatch.setattr(
        key_moment_evaluation,
        "_hydrate_missing_tennis_metadata",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "_load_trajectory_payloads",
        lambda *_args: calls.append("trajectory") or {},
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "_build_evaluation_payloads",
        lambda _scheduler, _plan, event_ids, *_args: [
            {"event_id": event_id}
            for event_id in event_ids
        ],
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "evaluate_and_dispatch_alerts_batch",
        lambda *_args, **_kwargs: calls.append("alerts"),
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "flush_missing_standings_endpoints",
        lambda *_args: None,
    )

    key_moment_evaluation.evaluate_pre_start_key_moments(
        SimpleNamespace(event_repo=SimpleNamespace()),
        event_plan,
        SimpleNamespace(event_states={}, event_ids=set(), data_cache={}),
    )

    assert calls == ["alerts"]
