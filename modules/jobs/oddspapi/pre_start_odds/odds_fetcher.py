"""HTTP adapter for the Oddspapi odds endpoint."""

from __future__ import annotations

from modules.oddspapi.client import OddsPapiClient, OddsPapiError


class OddspapiOddsFetcher:
    def __init__(self, client: OddsPapiClient | None = None):
        self.client = client or OddsPapiClient()

    @staticmethod
    def _is_no_odds_error(error: OddsPapiError) -> bool:
        message = str(error).lower()
        return "status_code=404" in message or "not found" in message or "no odds" in message

    def fetch_odds(
        self,
        fixture_id: str,
        bookmakers: list[str] | None = None,
        odds_format: str | None = None,
        language: str | None = None,
        verbosity: int | None = None,
    ) -> dict | None:
        try:
            payload = self.client.get_odds(
                fixture_id=fixture_id,
                bookmakers=bookmakers,
                odds_format=odds_format,
                language=language,
                verbosity=verbosity,
            )
        except OddsPapiError as exc:
            if self._is_no_odds_error(exc):
                return None
            raise
        return payload if isinstance(payload, dict) else None
