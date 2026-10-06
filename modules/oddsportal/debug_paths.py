"""Stable filesystem paths for OddsPortal debug artifacts."""

from __future__ import annotations

from pathlib import Path
import re
import unicodedata


def _slug(value: object) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "-", ascii_value.lower()).strip("-")


def event_debug_directory(
    root: str | Path,
    event_id: int | str | None,
    home_team: str | None = None,
    away_team: str | None = None,
) -> Path:
    """Build a Windows-safe event folder name under the OddsPortal debug root."""
    try:
        normalized_event_id = str(int(event_id))
    except (TypeError, ValueError):
        normalized_event_id = "unknown"

    home_slug = _slug(home_team)
    away_slug = _slug(away_team)
    if home_slug and away_slug:
        event_slug = f"{home_slug}-vs-{away_slug}"
    else:
        event_slug = home_slug or away_slug or "event"

    # Asterisks are invalid in Windows path components, so use a safe separator.
    return Path(root) / f"{normalized_event_id}-{event_slug}"
