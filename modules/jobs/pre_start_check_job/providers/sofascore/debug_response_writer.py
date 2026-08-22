"""Debug-only persistence for raw SofaScore odds responses."""

from __future__ import annotations

import json
import logging
from pathlib import Path
import re


logger = logging.getLogger(__name__)


class SofaScoreDebugResponseWriter:
    """Write the unmodified SofaScore odds body for one event/moment."""

    OUTPUT_DIRECTORY = Path("debug") / "sofascore_odds_responses"

    @staticmethod
    def _filename_token(value: object, *, fallback: str) -> str:
        token = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "").strip())
        return token.strip("._-") or fallback

    @staticmethod
    def _moment_token(minutes_until_start: int | float | None) -> str:
        if minutes_until_start is None:
            return "unknown"
        token = re.sub(r"[^A-Za-z0-9._-]+", "_", str(minutes_until_start).strip())
        return token or "unknown"

    @classmethod
    def save(
        cls,
        *,
        event_id: int,
        source_event_id: int,
        minutes_until_start: int | float | None,
        payload: dict,
    ) -> Path | None:
        """Save raw provider JSON without affecting successful ingestion."""

        if not isinstance(payload, dict):
            return None

        filename = "_".join(
            (
                cls._filename_token(event_id, fallback="event"),
                cls._filename_token(source_event_id, fallback="source_event"),
                f"t_{cls._moment_token(minutes_until_start)}",
            )
        ) + ".json"
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
                "Could not save raw SofaScore debug response event_id=%s "
                "source_event_id=%s minutes_until_start=%s: %s",
                event_id,
                source_event_id,
                minutes_until_start,
                exc,
            )
            return None

        logger.info(
            "Saved raw SofaScore response event_id=%s source_event_id=%s "
            "minutes_until_start=%s path=%s",
            event_id,
            source_event_id,
            minutes_until_start,
            path,
        )
        return path
