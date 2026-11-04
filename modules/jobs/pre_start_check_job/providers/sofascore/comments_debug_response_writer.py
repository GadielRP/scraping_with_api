"""Debug-only persistence for raw SofaScore event-comments responses."""

from __future__ import annotations

import json
import logging
from pathlib import Path
import re

from ..debug_paths import event_folder_name, sport_folder_name

logger = logging.getLogger(__name__)


class SofaScoreCommentsDebugResponseWriter:
    """Write one raw comments response under its sport/competition/event folder."""

    OUTPUT_DIRECTORY = Path("debug") / "sofascore_comments_responses"

    @classmethod
    def save(
        cls,
        *,
        event_id: int,
        payload: dict,
        competition_slug: str | None = None,
        sport: str | None = None,
        home_participant: str | None = None,
        away_participant: str | None = None,
        event_label: str | None = None,
    ) -> Path | None:
        if not isinstance(payload, dict):
            return None

        competition_folder = re.sub(
            r"[^a-z0-9]+", "_", (competition_slug or "").lower()
        ).strip("_") or "unknown_competition"
        event_folder = event_folder_name(
            event_id,
            home_participant=home_participant,
            away_participant=away_participant,
            event_label=event_label,
        )
        path = (
            cls.OUTPUT_DIRECTORY
            / sport_folder_name(sport)
            / competition_folder
            / event_folder
            / f"{event_id}_comments.json"
        )

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as file_handle:
                json.dump(payload, file_handle, ensure_ascii=False, indent=2, default=str)
                file_handle.write("\n")
        except (OSError, TypeError, ValueError) as exc:
            logger.warning(
                "Could not save raw SofaScore comments response event_id=%s: %s",
                event_id,
                exc,
            )
            return None

        logger.info(
            "Saved raw SofaScore comments response event_id=%s path=%s",
            event_id,
            path,
        )
        return path
