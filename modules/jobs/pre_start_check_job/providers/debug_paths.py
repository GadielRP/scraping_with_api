"""Folder naming shared by response writers and artifact relocation."""

import re
import unicodedata


def event_folder_name(
    event_id: int,
    *,
    home_participant: str | None = None,
    away_participant: str | None = None,
    event_label: str | None = None,
) -> str:
    def token(value):
        normalized = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^A-Za-z0-9._-]+", "_", normalized.strip()).strip("._-")

    home = token(home_participant)
    away = token(away_participant)
    if home and away:
        return f"{event_id}_{home}_{away}"
    if event_label:
        return f"{event_id}_{token(event_label) or 'event'}"
    return str(event_id)


def sport_folder_name(sport: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (sport or "").lower()).strip("_") or "unknown_sport"
