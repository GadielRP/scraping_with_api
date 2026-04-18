"""SofaScore domain exceptions."""

from __future__ import annotations


class SofaScoreNotFoundException(Exception):
    """Raised when a SofaScore endpoint returns 404."""

    def __init__(self, event_id: int | str, endpoint_type: str = "event"):
        self.event_id = event_id
        self.endpoint_type = endpoint_type
        super().__init__(f"{endpoint_type.capitalize()} endpoint returned 404 for event {event_id}")


class SofaScoreRateLimitException(Exception):
    """Raised when a SofaScore endpoint returns 403 or a hard rate limit."""

    def __init__(self, event_id: int | str, endpoint_type: str = "event"):
        self.event_id = event_id
        self.endpoint_type = endpoint_type
        super().__init__(f"Rate limited (403) on {endpoint_type} endpoint for event {event_id}")
