"""SofaScore-specific filenames for the shared debug response writer."""

from __future__ import annotations

import json
from pathlib import Path
import re

from ..debug_response_writer import DebugResponseWriter


class SofaScoreDebugResponseWriter:
    """Adapt SofaScore response names and folders to the shared writer."""

    OUTPUT_ROOT = Path("debug")
    OUTPUT_DIRECTORY = OUTPUT_ROOT / "sofascore" / "odds_responses"

    @staticmethod
    def _filename_token(value: object, *, fallback: str) -> str:
        return DebugResponseWriter.filename_token(value, fallback=fallback)

    @staticmethod
    def _moment_token(minutes_until_start: int | float | None) -> str:
        if minutes_until_start is None:
            return "unknown"
        token = re.sub(r"[^A-Za-z0-9._-]+", "_", str(minutes_until_start).strip())
        return token or "unknown"

    @classmethod
    def save_response(
        cls,
        *,
        response_type: str,
        event_id: int,
        payload: object,
        competition_slug: str | None = None,
        sport: str | None = None,
        home_participant: str | None = None,
        away_participant: str | None = None,
        event_label: str | None = None,
        event_folder: str | None = None,
    ) -> Path | None:
        response_token = cls._filename_token(response_type, fallback="response").lower()
        return DebugResponseWriter.save_response(
            provider="sofascore",
            response_type=response_token,
            output_directory=cls.OUTPUT_ROOT / "sofascore" / f"{response_token}_responses",
            event_id=event_id,
            filename=f"{event_id}_{response_token}.json",
            payload=payload,
            competition_slug=competition_slug,
            sport=sport,
            home_participant=home_participant,
            away_participant=away_participant,
            event_label=event_label,
            event_folder=event_folder,
        )

    @classmethod
    def save_general_response(
        cls,
        *,
        response_type: str,
        payload: object,
        filename: str | None = None,
        subfolder: str | Path | None = None,
    ) -> Path | None:
        """Save a non-event response directly under debug/sofascore/{response_type}_responses/."""
        response_token = cls._filename_token(response_type, fallback="response").lower()
        directory = cls.OUTPUT_ROOT / "sofascore" / f"{response_token}_responses"
        if subfolder:
            directory = directory / subfolder
        directory.mkdir(parents=True, exist_ok=True)

        if not filename:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{timestamp}.json"

        safe_filename = Path(filename).name
        target_path = directory / safe_filename
        with target_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
            handle.write("\n")
        return target_path

    @classmethod
    def save(
        cls,
        *,
        event_id: int,
        source_event_id: int,
        minutes_until_start: int | float | None,
        payload: dict,
        competition_slug: str | None = None,
        sport: str | None = None,
        home_participant: str | None = None,
        away_participant: str | None = None,
        event_label: str | None = None,
        event_folder: str | None = None,
    ) -> Path | None:
        if not isinstance(payload, dict):
            return None

        filename = "_".join(
            (
                cls._filename_token(event_id, fallback="event"),
                cls._filename_token(source_event_id, fallback="source_event"),
                f"t_{cls._moment_token(minutes_until_start)}",
            )
        ) + ".json"
        return DebugResponseWriter.save_response(
            provider="sofascore",
            response_type="odds",
            output_directory=cls.OUTPUT_DIRECTORY,
            event_id=event_id,
            filename=filename,
            payload=payload,
            competition_slug=competition_slug,
            sport=sport,
            home_participant=home_participant,
            away_participant=away_participant,
            event_label=event_label,
            event_folder=event_folder,
            source_event_id=source_event_id,
        )
