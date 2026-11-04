import json
import sys
from unittest.mock import Mock

from modules.sofascore.client import SofaScoreAPI
from modules.sofascore.event_details import get_event_comments
from scripts import get_event_comments as get_event_comments_script


def test_get_event_comments_requests_event_comments_endpoint():
    response = {"comments": [{"text": "Good game"}]}
    client = Mock()
    client.request_json.return_value = response

    result = get_event_comments(client, 12345)

    client.request_json.assert_called_once_with("/event/12345/comments")
    assert result == response


def test_client_get_event_comments_delegates_to_endpoint_helper():
    response = {"comments": []}
    client = Mock()
    client.request_json.return_value = response

    result = SofaScoreAPI.get_event_comments(client, 12345)

    client.request_json.assert_called_once_with("/event/12345/comments")
    assert result == response


def test_script_prints_response_as_json(monkeypatch, capsys):
    response = {"comments": [{"text": "Good game"}]}
    event = {
        "slug": "sevilla-barcelona",
        "tournament": {
            "category": {"sport": {"name": "Football"}},
            "uniqueTournament": {"slug": "la-liga"},
        },
        "homeTeam": {"name": "Sevilla"},
        "awayTeam": {"name": "Barcelona"},
    }
    monkeypatch.setattr(sys, "argv", ["get_event_comments.py", "12345"])
    monkeypatch.setattr(
        get_event_comments_script.api_client,
        "get_event_comments",
        Mock(return_value=response),
    )
    monkeypatch.setattr(
        get_event_comments_script.api_client,
        "get_event_details",
        Mock(return_value=event),
    )
    save = Mock()
    monkeypatch.setattr(
        get_event_comments_script.SofaScoreCommentsDebugResponseWriter,
        "save",
        save,
    )

    get_event_comments_script.main()

    assert json.loads(capsys.readouterr().out) == response
    save.assert_called_once_with(
        event_id=12345,
        payload=response,
        competition_slug="la-liga",
        sport="Football",
        home_participant="Sevilla",
        away_participant="Barcelona",
        event_label="sevilla-barcelona",
    )
