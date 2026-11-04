import json
import sys
from unittest.mock import Mock

from modules.sofascore import client as client_module
from modules.sofascore.event_details import get_event_statistics
from scripts.sofascore import get_event_statistics as statistics_script


def test_get_event_statistics_requests_event_statistics_endpoint():
    response = {"statistics": []}
    client = Mock()
    client.request_json.return_value = response

    result = get_event_statistics(client, 9001)

    client.request_json.assert_called_once_with("/event/9001/statistics")
    assert result == response


def test_statistics_script_accepts_sofascore_source_id_and_saves_response(
    monkeypatch, capsys
):
    response = {"statistics": []}
    event = {
        "slug": "home-away",
        "tournament": {
            "category": {"sport": {"name": "Football"}},
            "uniqueTournament": {"slug": "league"},
        },
        "homeTeam": {"name": "Home"},
        "awayTeam": {"name": "Away"},
    }
    monkeypatch.setattr(
        sys,
        "argv",
        ["get_event_statistics.py", "9001", "--source", "sofascore"],
    )
    identity = Mock(canonical_event_id=123, sofascore_event_id=9001)
    resolve = Mock(return_value=identity)
    monkeypatch.setattr(
        statistics_script,
        "resolve_sofascore_event_identity",
        resolve,
    )
    monkeypatch.setattr(
        statistics_script.api_client,
        "get_event_statistics",
        Mock(return_value=response),
    )
    monkeypatch.setattr(
        statistics_script.api_client,
        "get_event_details",
        Mock(return_value=event),
    )
    save = Mock()
    monkeypatch.setattr(
        statistics_script.SofaScoreDebugResponseWriter,
        "save_response",
        save,
    )

    statistics_script.main()

    resolve.assert_called_once_with(9001, input_id_type="sofascore")
    statistics_script.api_client.get_event_statistics.assert_called_once_with(9001)
    statistics_script.api_client.get_event_details.assert_called_once_with(9001)
    save.assert_called_once_with(
        response_type="statistics",
        event_id=123,
        payload=response,
        competition_slug="league",
        sport="Football",
        home_participant="Home",
        away_participant="Away",
        event_label="home-away",
    )
    assert json.loads(capsys.readouterr().out) == response


def test_client_statistics_method_uses_event_statistics_helper():
    client = Mock()
    client.request_json.return_value = {"statistics": []}

    result = client_module.SofaScoreAPI.get_event_statistics(client, 9001)

    client.request_json.assert_called_once_with("/event/9001/statistics")
    assert result == {"statistics": []}
