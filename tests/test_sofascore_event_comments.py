import json
import sys
from unittest.mock import Mock

from modules.sofascore.client import SofaScoreAPI
from modules.sofascore.event_details import get_event_comments
from modules.sofascore.event_details import get_event_statistics
from scripts.sofascore import get_event_comments as get_event_comments_script


def test_get_event_comments_requests_event_comments_endpoint():
    response = {"comments": [{"text": "Good game"}]}
    client = Mock()
    client.request_json.return_value = response

    result = get_event_comments(client, 12345)

    client.request_json.assert_called_once_with("/event/12345/comments")
    assert result == response


def test_get_event_statistics_requests_event_statistics_endpoint():
    response = {"statistics": []}
    client = Mock()
    client.request_json.return_value = response

    result = get_event_statistics(client, 12345)

    client.request_json.assert_called_once_with("/event/12345/statistics")
    assert result == response


def test_client_get_event_comments_delegates_to_endpoint_helper():
    response = {"comments": []}
    client = Mock()
    client.request_json.return_value = response

    result = SofaScoreAPI.get_event_comments(client, 12345)

    client.request_json.assert_called_once_with("/event/12345/comments")
    assert result == response


def test_client_get_event_statistics_delegates_to_endpoint_helper():
    response = {"statistics": []}
    client = Mock()
    client.request_json.return_value = response

    result = SofaScoreAPI.get_event_statistics(client, 12345)

    client.request_json.assert_called_once_with("/event/12345/statistics")
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
    monkeypatch.setattr(
        sys,
        "argv",
        ["get_event_comments.py", "9001", "--source", "sofascore"],
    )
    identity = Mock(canonical_event_id=12345, sofascore_event_id=9001)
    resolve = Mock(return_value=identity)
    monkeypatch.setattr(
        get_event_comments_script,
        "resolve_sofascore_event_identity",
        resolve,
    )
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
        get_event_comments_script.SofaScoreDebugResponseWriter,
        "save_response",
        save,
    )

    get_event_comments_script.main()

    assert json.loads(capsys.readouterr().out) == response
    resolve.assert_called_once_with(9001, input_id_type="sofascore")
    get_event_comments_script.api_client.get_event_comments.assert_called_once_with(9001)
    get_event_comments_script.api_client.get_event_details.assert_called_once_with(9001)
    save.assert_called_once_with(
        response_type="comments",
        event_id=12345,
        payload=response,
        competition_slug="la-liga",
        sport="Football",
        home_participant="Sevilla",
        away_participant="Barcelona",
        event_label="sevilla-barcelona",
    )
