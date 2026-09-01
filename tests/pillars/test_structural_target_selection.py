from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import modules.jobs.pre_start_check_job.pillar_pipeline as pillar_pipeline
from modules.pillars import market_snapshot_extractor
from modules.pillars.context import CompetitionContext, EventContext, ParticipantContext
from modules.pillars.market_snapshot_extractor import TargetMinuteSelection


def _event_context() -> EventContext:
    event = EventContext(
        event_id=4004,
        custom_id="event-4004",
        sport="Football",
        season_id=2026,
        season_name="2026",
        season_year=2026,
        start_time_utc=datetime(2026, 8, 31, 18, 0, tzinfo=timezone.utc),
        minutes_until_start=5,
        discovery_source="test",
        home=ParticipantContext(
            participant_id=1,
            source="test",
            source_participant_id=101,
            name="Home",
            slug="home",
            short_name="H",
            source_status="normalized",
        ),
        away=ParticipantContext(
            participant_id=2,
            source="test",
            source_participant_id=202,
            name="Away",
            slug="away",
            short_name="A",
            source_status="normalized",
        ),
        competition=CompetitionContext(
            competition_id=99,
            source="test",
            source_tournament_id=99,
            source_unique_tournament_id=999,
            canonical_name="League",
            display_name="League",
            slug="league",
            unique_slug="league",
            category_id=1,
            category_name="Country",
            number_of_teams=20,
            number_of_teams_source="test",
            total_regular_season_games=38,
            standings_grouping="league",
            league_config_source="test",
            has_standings_source_endpoint=True,
            source_status="normalized",
        ),
        participants_label="Home vs Away",
        context_status="normalized",
        round="regular_season",
    )
    event.odds_trajectory = []
    return event


def _trajectory_context():
    return SimpleNamespace(
        available=True,
        event_id=4004,
        markets={},
        target_minutes_present=[30, 5],
        target_minutes_expected=[120, 30, 5, 1, 0, -5],
        missing_target_minutes=[120, 1, 0, -5],
    )


def _run_pipeline(monkeypatch, selection_spy=None, mining_service=None):
    captured = {}
    context = _trajectory_context()
    monkeypatch.setattr(
        pillar_pipeline,
        "_is_pillar_competition_in_scope",
        lambda _competition_id: True,
    )
    monkeypatch.setattr(
        pillar_pipeline,
        "build_odds_trajectory_context",
        lambda _rows: context,
    )
    if selection_spy is not None:
        monkeypatch.setattr(
            pillar_pipeline,
            "select_target_minute",
            selection_spy,
        )

    def fake_p2(*, target_selection, **_kwargs):
        captured["p2_selection"] = target_selection
        return {
            "event_id": 4004,
            "P2_STATUS": "ACTIVE",
            "P2_TARGET_MINUTE": target_selection.target_minute,
            "P2_SIGNAL_PROFILE": {},
            "status": "ACTIVE",
        }

    def fake_p3(*, target_selection, **_kwargs):
        captured["p3_selection"] = target_selection
        return {
            "event_id": 4004,
            "P3_STATUS": "ACTIVE",
            "P3_TARGET_MINUTE": target_selection.target_minute,
            "P3_SIGNAL_PROFILE": {},
            "status": "ACTIVE",
        }

    monkeypatch.setattr(pillar_pipeline, "calculate_pillar_2", fake_p2)
    monkeypatch.setattr(pillar_pipeline, "calculate_pillar_3", fake_p3)
    processor = pillar_pipeline.EventPillarProcessor(
        event_repo=None,
        mining_service=mining_service,
        enabled_pillars={
            "pillar_1": False,
            "pillar_2": True,
            "pillar_3": True,
            "pillar_4": False,
            "pillar_5": False,
        },
    )
    result = processor.process_event(_event_context())
    return result, captured


def test_pipeline_selects_target_once_and_injects_same_object(monkeypatch) -> None:
    calls = []

    def selection_spy(*args, **kwargs):
        calls.append((args, kwargs))
        return TargetMinuteSelection(
            target_minute=5,
            diagnostics={"selection": "test"},
        )

    result, captured = _run_pipeline(monkeypatch, selection_spy)

    assert len(calls) == 1
    assert calls[0][1]["flow_id"] == pillar_pipeline.CANONICAL_SIGNAL_FLOW_ID
    assert captured["p2_selection"] is captured["p3_selection"]
    assert result["pillar_2"]["P2_TARGET_MINUTE"] == 5
    assert result["pillar_3"]["P3_TARGET_MINUTE"] == 5


def test_shared_hardcoded_override_is_consumed_by_both_pillars(
    monkeypatch,
) -> None:
    monkeypatch.setitem(
        market_snapshot_extractor.HARDCODED_TARGET_MINUTE_BY_FLOW,
        pillar_pipeline.CANONICAL_SIGNAL_FLOW_ID,
        0,
    )
    result, captured = _run_pipeline(monkeypatch)

    assert captured["p2_selection"] is captured["p3_selection"]
    assert captured["p2_selection"].target_minute == 0
    assert result["pillar_2"]["P2_TARGET_MINUTE"] == 0
    assert result["pillar_3"]["P3_TARGET_MINUTE"] == 0


def test_pipeline_persists_both_structural_profiles(monkeypatch) -> None:
    persisted = []

    class _MiningService:
        def persist(self, pillar_id, event_context, result):
            profile_key = (
                "P2_SIGNAL_PROFILE"
                if pillar_id == "pillar_2_side_market"
                else "P3_SIGNAL_PROFILE"
            )
            persisted.append(
                (
                    pillar_id,
                    event_context.event_id,
                    result.get(profile_key),
                )
            )
            return True

    _run_pipeline(
        monkeypatch,
        selection_spy=lambda *_args, **_kwargs: TargetMinuteSelection(
            target_minute=5,
            diagnostics={"selection": "test"},
        ),
        mining_service=_MiningService(),
    )

    assert [(pillar_id, event_id) for pillar_id, event_id, _ in persisted] == [
        ("pillar_2_side_market", 4004),
        ("pillar_3_totals_market_context", 4004),
    ]
    assert all(profile == {} for _, _, profile in persisted)
