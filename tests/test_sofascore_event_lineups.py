from unittest.mock import Mock


from modules.sofascore import client as client_module
from modules.sofascore.event_details import get_event_lineups


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
