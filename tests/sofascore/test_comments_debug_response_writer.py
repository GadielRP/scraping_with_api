import json

from modules.jobs.pre_start_check_job.providers.sofascore.debug_response_writer import (
    SofaScoreDebugResponseWriter,
)


def test_comments_response_is_saved_under_sport_competition_and_event(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        SofaScoreDebugResponseWriter,
        "OUTPUT_ROOT",
        tmp_path / "debug",
    )
    payload = {"comments": [{"text": "Good game"}]}

    path = SofaScoreDebugResponseWriter.save_response(
        response_type="comments",
        event_id=16416349,
        payload=payload,
        sport="Football",
        competition_slug="La Liga",
        home_participant="Sevilla",
        away_participant="Barcelona",
    )

    assert path is not None
    assert path.parent == (
        tmp_path
        / "debug"
        / "sofascore"
        / "comments_responses"
        / "football"
        / "la_liga"
        / "16416349_Sevilla_Barcelona"
    )
    assert path.name == "16416349_comments.json"
    assert json.loads(path.read_text(encoding="utf-8")) == payload


def test_generic_writer_saves_statistics_responses_in_separate_tree(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(SofaScoreDebugResponseWriter, "OUTPUT_ROOT", tmp_path)

    path = SofaScoreDebugResponseWriter.save_response(
        response_type="statistics",
        event_id=9001,
        payload={"statistics": []},
    )

    assert path == (
        tmp_path
        / "sofascore"
        / "statistics_responses"
        / "unknown_sport"
        / "unknown_competition"
        / "9001"
        / "9001_statistics.json"
    )


def test_generic_writer_accepts_non_object_json_payloads(tmp_path, monkeypatch):
    monkeypatch.setattr(SofaScoreDebugResponseWriter, "OUTPUT_ROOT", tmp_path)

    path = SofaScoreDebugResponseWriter.save_response(
        response_type="timeline",
        event_id=9001,
        payload=[{"event": "start"}],
    )

    assert path is not None
    assert json.loads(path.read_text(encoding="utf-8")) == [{"event": "start"}]
