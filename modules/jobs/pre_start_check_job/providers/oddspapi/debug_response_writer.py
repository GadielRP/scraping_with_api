"""Debug-only persistence for raw OddsPAPI odds responses."""

from __future__ import annotations

import json
import logging
from pathlib import Path
import re
from typing import Iterable


logger = logging.getLogger(__name__)


class OddspapiDebugResponseWriter:
    """Write one deterministic raw-response artifact per regular request."""

    OUTPUT_DIRECTORY = Path("debug") / "oddspapi_odds_responses"
    ENDPOINT_FILENAME_LABELS = {
        "historical-odds": "historical",
        "odds": "odds",
    }

    @staticmethod
    def _filename_token(value: object, *, fallback: str) -> str:
        token = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
        return token.strip("._-") or fallback

    @classmethod
    def _endpoint_token(cls, endpoint: str | None) -> str | None:
        if endpoint is None:
            return None
        normalized = str(endpoint).strip().lower()
        if not normalized:
            return None
        label = cls.ENDPOINT_FILENAME_LABELS.get(normalized, normalized)
        return cls._filename_token(label, fallback="endpoint")

    @classmethod
    def save(
        cls,
        *,
        event_id: int,
        fixture_id: str,
        bookmakers: Iterable[str] | None,
        payload: dict,
        endpoint: str | None = None,
    ) -> Path | None:
        """Save the raw provider JSON without affecting ingestion on failure."""

        if not isinstance(payload, dict):
            return None

        bookmaker_tokens = [
            cls._filename_token(bookmaker, fallback="bookmaker")
            for bookmaker in bookmakers or []
        ]
        bookmakers_token = "_".join(bookmaker_tokens) or "all"
        endpoint_token = cls._endpoint_token(endpoint)
        filename_parts = [
            cls._filename_token(event_id, fallback="event"),
            cls._filename_token(fixture_id, fallback="fixture"),
        ]
        if endpoint_token:
            filename_parts.append(endpoint_token)
        filename_parts.append(bookmakers_token)
        filename = "_".join(filename_parts) + ".json"
        path = cls.OUTPUT_DIRECTORY / filename

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as file_handle:
                json.dump(
                    payload,
                    file_handle,
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
                file_handle.write("\n")
        except (OSError, TypeError, ValueError) as exc:
            # Debug observability must never turn a successful provider fetch
            # into a failed production ingestion.
            logger.warning(
                "Could not save raw OddsPAPI debug response event_id=%s "
                "fixture_id=%s: %s",
                event_id,
                fixture_id,
                exc,
            )
            return None

        logger.info(
            "Saved raw OddsPAPI response event_id=%s fixture_id=%s "
            "endpoint=%s bookmakers=%s path= %s",
            event_id,
            fixture_id,
            endpoint_token or "unspecified",
            ",".join(str(value) for value in bookmakers or []) or "all",
            path,
        )
        return path
