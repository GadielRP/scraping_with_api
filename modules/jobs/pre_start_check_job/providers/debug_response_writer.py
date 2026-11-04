"""Shared persistence for provider debug response artifacts."""

from __future__ import annotations

import json
import logging
from pathlib import Path
import re
import unicodedata

from .debug_paths import event_folder_name, sport_folder_name

logger = logging.getLogger(__name__)


class DebugResponseWriter:
    """Write a provider response below its provider and response-type folders."""

    @staticmethod
    def filename_token(value: object, *, fallback: str) -> str:
        normalized = (
            unicodedata.normalize("NFKD", str(value or ""))
            .encode("ascii", "ignore")
            .decode("ascii")
        )
        token = re.sub(r"[^A-Za-z0-9._-]+", "_", normalized.strip())
        return token.strip("._-") or fallback

    @staticmethod
    def response_path(
        *,
        output_directory: Path,
        filename: str,
        event_id: int,
        competition_slug: str | None = None,
        sport: str | None = None,
        home_participant: str | None = None,
        away_participant: str | None = None,
        event_label: str | None = None,
        event_folder: str | None = None,
    ) -> Path:
        competition_folder = re.sub(
            r"[^a-z0-9]+", "_", (competition_slug or "").lower()
        ).strip("_") or "unknown_competition"
        folder_name = event_folder or event_folder_name(
            event_id,
            home_participant=home_participant,
            away_participant=away_participant,
            event_label=event_label,
        )
        safe_filename = Path(filename).name
        return (
            output_directory
            / sport_folder_name(sport)
            / competition_folder
            / folder_name
            / safe_filename
        )

    @classmethod
    def save_response(
        cls,
        *,
        provider: str,
        response_type: str,
        output_directory: Path,
        event_id: int,
        filename: str,
        payload: object,
        competition_slug: str | None = None,
        sport: str | None = None,
        home_participant: str | None = None,
        away_participant: str | None = None,
        event_label: str | None = None,
        event_folder: str | None = None,
        source_event_id: str | int | None = None,
        preserve_existing: bool = False,
    ) -> Path | None:
        """Persist JSON, keeping debug I/O failures from affecting ingestion."""
        path = cls.response_path(
            output_directory=output_directory,
            filename=filename,
            event_id=event_id,
            competition_slug=competition_slug,
            sport=sport,
            home_participant=home_participant,
            away_participant=away_participant,
            event_label=event_label,
            event_folder=event_folder,
        )

        if preserve_existing and path.exists():
            logger.info(
                "Preserved existing %s %s debug response event_id=%s path=%s",
                provider,
                response_type,
                event_id,
                path,
            )
            return None

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
            logger.warning(
                "Could not save raw %s %s debug response event_id=%s "
                "source_event_id=%s: %s",
                provider,
                response_type,
                event_id,
                source_event_id,
                exc,
            )
            return None

        logger.info(
            "Saved raw %s %s response event_id=%s source_event_id=%s path=%s",
            provider,
            response_type,
            event_id,
            source_event_id,
            path,
        )
        return path
