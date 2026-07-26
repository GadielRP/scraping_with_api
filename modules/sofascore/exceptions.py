"""SofaScore domain exceptions."""

from __future__ import annotations


class SofaScoreError(RuntimeError):
    """Base exception for SofaScore transport failures."""


class SofaScoreHttpError(SofaScoreError):
    """SofaScore HTTP failure with structured request context."""

    def __init__(
        self,
        status_code: int,
        event_id: int | str,
        endpoint: str,
        message: str,
    ):
        self.status_code = int(status_code)
        self.event_id = event_id
        self.endpoint = endpoint
        super().__init__(message)


class SofaScoreNotFoundException(SofaScoreHttpError):
    """Raised when a SofaScore endpoint returns 404."""

    def __init__(self, event_id: int | str, endpoint: str = "/event"):
        super().__init__(
            status_code=404,
            event_id=event_id,
            endpoint=endpoint,
            message=f"HTTP 404 on {endpoint} (event_id={event_id})",
        )


class SofaScoreRateLimitException(SofaScoreHttpError):
    """Raised when a SofaScore endpoint returns a real rate limit such as HTTP 429."""

    def __init__(
        self,
        event_id: int | str,
        endpoint: str = "/event",
        status_code: int = 429,
    ):
        super().__init__(
            status_code=status_code,
            event_id=event_id,
            endpoint=endpoint,
            message=f"Rate limited on {endpoint} (event_id={event_id})",
        )


class SofaScoreChallengeException(SofaScoreError):
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
