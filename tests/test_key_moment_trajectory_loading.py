from __future__ import annotations

from types import SimpleNamespace

from modules.jobs.pre_start_check_job import key_moment_evaluation


def _event_plan(*event_ids: int):
    return SimpleNamespace(
        candidates=[
            {
                "event_id": event_id,
                "minutes_until_start": 30,
                "event_data": {"competition_id": 176},
            }
            for event_id in event_ids
        ],
        by_event_id={},
    )


def _oddsportal_context():
    return SimpleNamespace(event_states={}, event_ids=set(), data_cache={})


def _configure_pipeline(monkeypatch, *, alerts: bool, pillars: bool) -> None:
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "FILTER_PIPELINES_BY_TRACKED_COMPETITIONS",
        False,
    )
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "ENABLE_LEGACY_ALERT_PIPELINE",
        alerts,
    )
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "ENABLE_PILLAR_PIPELINE",
        pillars,
    )
    monkeypatch.setattr(
        key_moment_evaluation.Config,
        "PRE_START_ODDS_MOMENTS",
        [30],
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "_hydrate_missing_tennis_metadata",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "flush_missing_standings_endpoints",
        lambda *_args: None,
    )


def test_trajectory_uses_validated_pillar_payloads_after_alerts(monkeypatch):
    _configure_pipeline(monkeypatch, alerts=True, pillars=True)
    calls = []

    def build_payloads(_scheduler, _plan, _event_ids, _missing_ids):
        # Event 2 represents a candidate rejected during normalized context
        # construction. It must never reach the trajectory repository.
        return [{"event_id": 1, "odds_trajectory": []}]

    def load_trajectory(event_ids, _moments):
        calls.append(("trajectory", set(event_ids)))
        return {1: [{"event_id": 1, "target_minute": 30}]}

    def run_alerts(payloads, *_args, **_kwargs):
        calls.append(("alerts", list(payloads[0]["odds_trajectory"])))

    def run_pillars(events_for_pillars, *_args, **_kwargs):
        calls.append(
            ("pillars", list(events_for_pillars[0]["odds_trajectory"]))
        )

    monkeypatch.setattr(
        key_moment_evaluation,
        "_build_evaluation_payloads",
        build_payloads,
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "_load_trajectory_payloads",
        load_trajectory,
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "evaluate_and_dispatch_alerts_batch",
        run_alerts,
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "evaluate_and_calculate_pillars_batch",
        run_pillars,
    )

    key_moment_evaluation.evaluate_pre_start_key_moments(
        SimpleNamespace(event_repo=SimpleNamespace()),
        _event_plan(1, 2),
        _oddsportal_context(),
    )

    assert calls == [
        ("alerts", []),
        ("trajectory", {1}),
        ("pillars", [{"event_id": 1, "target_minute": 30}]),
    ]


def test_alert_only_pipeline_does_not_load_trajectory(monkeypatch):
    _configure_pipeline(monkeypatch, alerts=True, pillars=False)
    calls = []

    monkeypatch.setattr(
        key_moment_evaluation,
        "_build_evaluation_payloads",
        lambda _scheduler, _plan, _event_ids, _missing_ids: [
            {"event_id": 1, "odds_trajectory": []}
        ],
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "_load_trajectory_payloads",
        lambda *_args: calls.append("trajectory") or {},
    )
    monkeypatch.setattr(
        key_moment_evaluation,
        "evaluate_and_dispatch_alerts_batch",
        lambda *_args, **_kwargs: calls.append("alerts"),
    )

    key_moment_evaluation.evaluate_pre_start_key_moments(
        SimpleNamespace(event_repo=SimpleNamespace()),
        _event_plan(1),
        _oddsportal_context(),
    )

    assert calls == ["alerts"]

