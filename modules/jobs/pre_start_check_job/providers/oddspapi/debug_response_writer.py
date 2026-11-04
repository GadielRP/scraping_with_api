"""OddsPAPI-specific filenames and preservation policy for debug responses."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Iterable

from ..debug_response_writer import DebugResponseWriter


class OddspapiDebugResponseWriter:
    """Adapt OddsPAPI request metadata to the shared response writer."""

    OUTPUT_DIRECTORY = Path("debug") / "oddspapi" / "odds_responses"
    ENDPOINT_FILENAME_LABELS = {
        "historical-odds": "historical",
        "odds": "odds",
    }

    @staticmethod
    def _filename_token(value: object, *, fallback: str) -> str:
        return DebugResponseWriter.filename_token(value, fallback=fallback)

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
        outcome_id: str | int | None = None,
        minutes_until_start: int | None = None,
        competition_slug: str | None = None,
        sport: str | None = None,
        home_participant: str | None = None,
        away_participant: str | None = None,
        event_label: str | None = None,
        event_folder: str | None = None,
    ) -> Path | None:
        if not isinstance(payload, dict):
            return None

        bookmaker_tokens = [
            cls._filename_token(bookmaker, fallback="bookmaker")
            for bookmaker in bookmakers or []
        ]
        filename_parts = [
            cls._filename_token(event_id, fallback="event"),
            cls._filename_token(fixture_id, fallback="fixture"),
        ]
        if minutes_until_start is not None:
            filename_parts.append(f"t_{minutes_until_start}")
        endpoint_token = cls._endpoint_token(endpoint)
        if endpoint_token:
            filename_parts.append(endpoint_token)
        if outcome_id is not None:
            filename_parts.append(
                f"outcome_{cls._filename_token(outcome_id, fallback='id')}"
            )
        filename_parts.append("_".join(bookmaker_tokens) or "all")

        preserve_existing = endpoint_token == "odds" and not any(
            isinstance(bookmaker, dict) and bookmaker.get("markets")
            for bookmaker in (payload.get("bookmakerOdds") or {}).values()
        )
        return DebugResponseWriter.save_response(
            provider="oddspapi",
            response_type=endpoint_token or "response",
            output_directory=cls.OUTPUT_DIRECTORY,
            event_id=event_id,
            filename="_".join(filename_parts) + ".json",
            payload=payload,
            competition_slug=competition_slug,
            sport=sport,
            home_participant=home_participant,
            away_participant=away_participant,
            event_label=event_label,
            event_folder=event_folder,
            source_event_id=fixture_id,
            preserve_existing=preserve_existing,
        )
