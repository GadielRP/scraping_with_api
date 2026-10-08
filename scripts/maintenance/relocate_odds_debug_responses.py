#!/usr/bin/env python3
"""Group existing raw odds JSON files by sport and canonical competition slug.

Preview:
    python scripts/maintenance/relocate_odds_debug_responses.py
Move at most 100 response files across both providers:
    python scripts/maintenance/relocate_odds_debug_responses.py --apply --limit 100

The default is a dry run. Existing destination files are never overwritten.
Existing event-folder names are preserved inside sport/competition folders.
For loose files, event folders are rebuilt from database participant names.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import re
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy.orm import joinedload

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Competition, Event
from shared.batching import chunks
from modules.jobs.pre_start_check_job.providers.debug_paths import event_folder_name, sport_folder_name

logger = logging.getLogger(__name__)
PROVIDERS = ("oddspapi_odds_responses", "sofascore_odds_responses")
EVENT_DIRECTORY = re.compile(r"^([1-9]\d*)(?:_|$)")
# A single-ID legacy SofaScore filename may contain a provider ID, not events.id.
SOFASCORE_FILENAME = re.compile(r"^([1-9]\d*)_\d+_t_")


def collect_responses(debug_directory: Path) -> list[tuple[Path, Path, int]]:
    """Collect (provider root, JSON path, canonical event ID) before querying."""
    responses = []
    for provider in PROVIDERS:
        root = (debug_directory / provider).resolve()
        if not root.is_dir():
            logger.info("Directory absent: %s", root)
            continue
        for path in sorted(root.rglob("*.json")):
            if path.is_symlink() or not path.resolve().is_relative_to(root):
                logger.warning("Skipping linked file outside migration scope: %s", path)
                continue
            relative = path.relative_to(root)
            match = next(
                (match for part in reversed(relative.parts[:-1])
                 if (match := EVENT_DIRECTORY.match(part))),
                None,
            )
            if match is None:
                pattern = SOFASCORE_FILENAME if provider == "sofascore_odds_responses" else EVENT_DIRECTORY
                match = pattern.match(path.name)
            if match is None:
                logger.warning("Skipping filename without canonical event ID: %s", path)
                continue
            responses.append((root, path, int(match.group(1))))
    return responses


def load_event_debug_paths(event_ids: set[int]) -> dict[int, tuple[str | None, str, str | None]]:
    """Bulk-load sport, competition slugs and participant names for loose responses."""
    paths = {}
    if not event_ids:
        return paths
    with db_manager.get_session() as session:
        for batch in chunks(sorted(event_ids), 1000):
            rows = (
                session.query(Event, Competition.slug)
                .join(Competition, Competition.competition_id == Event.competition_id)
                .options(joinedload(Event.home_participant), joinedload(Event.away_participant))
                .filter(Event.id.in_(batch))
                .all()
            )
            for event, slug in rows:
                paths[event.id] = (slug, event_folder_name(
                    event.id,
                    home_participant=event.home_participant.name if event.home_participant else event.home_team,
                    away_participant=event.away_participant.name if event.away_participant else event.away_team,
                    event_label=event.slug,
                ), event.sport)
    return paths


def relocate_responses(
    responses, event_paths, *, apply: bool = False, limit: int | None = None,
) -> dict[str, int]:
    counts = {"relocated": 0, "already_grouped": 0, "unresolved": 0, "conflicts": 0}
    destinations = set()
    old_parents = set()
    for root, source, event_id in responses:
        if limit is not None and counts["relocated"] >= limit:
            break
        slug, default_event_folder, sport = event_paths.get(event_id, (None, str(event_id), None))
        folder = re.sub(r"[^a-z0-9]+", "_", (slug or "").lower()).strip("_")
        if not folder or not sport:
            counts["unresolved"] += 1
            logger.warning("Leaving unresolved event_id=%s: %s", event_id, source)
            continue
        existing_event_folder = next(
            (part for part in reversed(source.relative_to(root).parts[:-1])
             if (match := EVENT_DIRECTORY.match(part)) and int(match.group(1)) == event_id),
            None,
        )
        event_folder = existing_event_folder or default_event_folder
        destination = root / sport_folder_name(sport) / folder / event_folder / source.name
        if source == destination:
            counts["already_grouped"] += 1
            continue
        if destination.exists() or destination in destinations:
            counts["conflicts"] += 1
            logger.warning("Leaving source; destination conflict: %s -> %s", source, destination)
            continue
        destinations.add(destination)
        logger.info("%s %s -> %s", "MOVE" if apply else "WOULD MOVE", source, destination)
        if apply:
            destination.parent.mkdir(parents=True, exist_ok=True)
            source.rename(destination)
            parent = source.parent
            while parent != root:
                old_parents.add(parent)
                parent = parent.parent
        counts["relocated"] += 1
    if apply:
        for parent in sorted(old_parents, key=lambda path: len(path.parts), reverse=True):
            if parent.is_dir() and not any(parent.iterdir()):
                parent.rmdir()
    return counts


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--debug-dir", type=Path, default=PROJECT_ROOT / "debug")
    parser.add_argument("--apply", action="store_true", help="Move files; default only previews.")
    parser.add_argument("--limit", type=int, help="Maximum response files to relocate across both providers.")
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be a positive integer")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    responses = collect_responses(args.debug_dir.resolve())
    event_ids = {event_id for _, _, event_id in responses}
    logger.info("Collected %s responses for %s distinct events", len(responses), len(event_ids))
    event_paths = load_event_debug_paths(event_ids)
    counts = relocate_responses(responses, event_paths, apply=args.apply, limit=args.limit)
    logger.info("%s: %s", "Applied" if args.apply else "Dry run", counts)
    return 1 if counts["conflicts"] or counts["unresolved"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
