"""Structured transport exceptions for the OddsPapi API."""

from __future__ import annotations


class OddsPapiError(RuntimeError):
    """Base exception for unusable OddsPapi responses."""


class OddsPapiQuotaExhaustedError(OddsPapiError):
    """No configured credential can call the requested endpoint."""

    def __init__(self, endpoint: str):
        self.endpoint = endpoint
        super().__init__(
            f"No eligible OddsPapi API key remains for endpoint={endpoint}"
        )


class OddsPapiHttpError(OddsPapiError):
    """Non-successful HTTP response with machine-readable context."""

    def __init__(
        self,
        status_code: int,
        endpoint: str,
        response_text: str = "",
        error_code: str | None = None,
    ):
        self.status_code = int(status_code)
        self.endpoint = endpoint
        self.response_text = response_text
        self.error_code = str(error_code).strip().upper() if error_code else None
        error_code_text = f" error_code={self.error_code}" if self.error_code else ""
        super().__init__(
            f"OddsPapi HTTP error status_code={self.status_code} "
            f"endpoint={self.endpoint}{error_code_text} response={self.response_text!r}"
        )
