from unittest.mock import Mock


from modules.sofascore.client import SofaScoreAPI
from modules.sofascore.event_details import get_event_comments
from modules.sofascore.event_details import get_event_statistics


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
