import json
import sys
from unittest.mock import Mock

import pytest

from modules.sofascore import client as client_module
from modules.sofascore.event_details import get_event_lineups
from scripts.sofascore import get_event_lineups as lineups_script


def test_get_event_lineups_requests_event_lineups_endpoint():
    response = {"home": {}, "away": {}}
    client = Mock()
    client.request_json.return_value = response

    result = get_event_lineups(client, 9001)

    client.request_json.assert_called_once_with("/event/9001/lineups")
    assert result == response


def test_client_lineups_method_uses_event_lineups_helper():
    client = Mock()
    client.request_json.return_value = {"home": {}, "away": {}}

    result = client_module.SofaScoreAPI.get_event_lineups(client, 9001)

    client.request_json.assert_called_once_with("/event/9001/lineups")
    assert result == {"home": {}, "away": {}}


@pytest.mark.parametrize(
    ("input_id", "input_id_type"),
    [(9001, "sofascore"), (123, "canonical")],
)
def test_lineups_script_resolves_event_and_saves_response(
    monkeypatch, capsys, input_id, input_id_type
):
    response = {"home": {}, "away": {}}
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
        ["get_event_lineups.py", str(input_id), "--source", input_id_type],
    )
    identity = Mock(canonical_event_id=123, sofascore_event_id=9001)
    resolve = Mock(return_value=identity)
    monkeypatch.setattr(lineups_script, "resolve_sofascore_event_identity", resolve)
    monkeypatch.setattr(
        lineups_script.api_client,
        "get_event_lineups",
        Mock(return_value=response),
    )
    monkeypatch.setattr(
        lineups_script.api_client,
        "get_event_details",
        Mock(return_value=event),
    )
    save = Mock()
    monkeypatch.setattr(
        lineups_script.SofaScoreDebugResponseWriter,
        "save_response",
        save,
    )

    lineups_script.main()

    resolve.assert_called_once_with(input_id, input_id_type=input_id_type)
    lineups_script.api_client.get_event_lineups.assert_called_once_with(9001)
    lineups_script.api_client.get_event_details.assert_called_once_with(9001)
    save.assert_called_once_with(
        response_type="lineups",
        event_id=123,
        payload=response,
        competition_slug="league",
        sport="Football",
        home_participant="Home",
        away_participant="Away",
        event_label="home-away",
    )
    assert json.loads(capsys.readouterr().out) == response
