"""SofaScore domain exceptions."""

from __future__ import annotations


class SofaScoreNotFoundException(Exception):
    """Raised when a SofaScore endpoint returns 404."""

    def __init__(self, event_id: int | str, endpoint: str = "/event"):
        self.event_id = event_id
        self.endpoint = endpoint
        super().__init__(f"HTTP 404 on {endpoint} (event_id={event_id})")


class SofaScoreRateLimitException(Exception):
    """Raised when a SofaScore endpoint returns a real rate limit such as HTTP 429."""

    def __init__(self, event_id: int | str, endpoint: str = "/event"):
        self.event_id = event_id
        self.endpoint = endpoint
        super().__init__(f"Rate limited on {endpoint} (event_id={event_id})")


class SofaScoreChallengeException(Exception):
    """Raised when SofaScore returns an explicit anti-bot/WAF challenge."""

    def __init__(
        self,
        event_id: int | str,
        endpoint: str = "/event",
        reason: str = "challenge",
        evidence: dict | None = None,
    ):
        self.event_id = event_id
        self.endpoint = endpoint
        self.reason = reason
        self.evidence = evidence or {}
        cf_ray = ""
        try:
            cf_ray = self.evidence.get("response_headers", {}).get("cf-ray", "")
        except Exception:
            cf_ray = ""
        suffix = f", cf-ray={cf_ray}" if cf_ray else ""
        super().__init__(
            f"SofaScore challenge ({reason}) on {endpoint} "
            f"(event_id={event_id}{suffix})"
        )
