"""SofaScore domain exceptions."""

from __future__ import annotations


class SofaScoreNotFoundException(Exception):
    """Raised when a SofaScore endpoint returns 404."""

    def __init__(self, event_id: int | str, endpoint: str = "/event"):
        self.event_id = event_id
        self.endpoint = endpoint
        super().__init__(f"HTTP 404 on {endpoint} (event_id={event_id})")


class SofaScoreRateLimitException(Exception):
    """Raised when a SofaScore endpoint returns 403 or a hard rate limit."""

    def __init__(self, event_id: int | str, endpoint: str = "/event"):
        self.event_id = event_id
        self.endpoint = endpoint
        super().__init__(f"Rate limited (403) on {endpoint} (event_id={event_id})")
