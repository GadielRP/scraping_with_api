import json

from modules.jobs.pre_start_check_job.providers.sofascore.comments_debug_response_writer import (
    SofaScoreCommentsDebugResponseWriter,
)


def test_comments_response_is_saved_under_sport_competition_and_event(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        SofaScoreCommentsDebugResponseWriter,
        "OUTPUT_DIRECTORY",
        tmp_path / "debug" / "sofascore_comments_responses",
    )
    payload = {"comments": [{"text": "Good game"}]}

    path = SofaScoreCommentsDebugResponseWriter.save(
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
        / "sofascore_comments_responses"
        / "football"
        / "la_liga"
        / "16416349_Sevilla_Barcelona"
    )
    assert path.name == "16416349_comments.json"
    assert json.loads(path.read_text(encoding="utf-8")) == payload
