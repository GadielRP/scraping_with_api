from unittest.mock import Mock

from modules.sofascore import client as client_module
from modules.sofascore.event_details import get_event_statistics


def test_get_event_statistics_requests_event_statistics_endpoint():
    response = {"statistics": []}
    client = Mock()
    client.request_json.return_value = response

    result = get_event_statistics(client, 9001)

    client.request_json.assert_called_once_with("/event/9001/statistics")
    assert result == response


def test_client_statistics_method_uses_event_statistics_helper():
    client = Mock()
    client.request_json.return_value = {"statistics": []}

    result = client_module.SofaScoreAPI.get_event_statistics(client, 9001)

    client.request_json.assert_called_once_with("/event/9001/statistics")
    assert result == {"statistics": []}
