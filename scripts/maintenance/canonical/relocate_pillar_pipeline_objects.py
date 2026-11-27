#!/usr/bin/env python3
"""Group pillar pipeline objects by sport, competition slug and existing event folder.

Preview:
    python -m scripts.maintenance.relocate_pillar_pipeline_objects --limit 100
Move files:
    python -m scripts.maintenance.relocate_pillar_pipeline_objects --apply --limit 100

The default is a dry run. Only debug/pillar_pipeline_objects is scanned.
Existing event-folder names are preserved and destination files are not overwritten.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.maintenance.relocate_odds_debug_responses import (
    collect_responses as collect_debug_files,
    load_event_debug_paths,
    relocate_responses,
)

logger = logging.getLogger(__name__)


def collect_responses(debug_directory: Path) -> list[tuple[Path, Path, int]]:
    return collect_debug_files(debug_directory, directories=("pillar_pipeline_objects",))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--debug-dir", type=Path, default=PROJECT_ROOT / "debug")
    parser.add_argument("--apply", action="store_true", help="Move files; default only previews.")
    parser.add_argument("--limit", type=int, help="Maximum pillar snapshot files to relocate.")
    args = parser.parse_args(argv)
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be a positive integer")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    responses = collect_responses(args.debug_dir.resolve())
    event_ids = {event_id for _, _, event_id in responses}
    logger.info("Collected %s pillar snapshots for %s distinct events", len(responses), len(event_ids))
    event_paths = load_event_debug_paths(event_ids)
    counts = relocate_responses(responses, event_paths, apply=args.apply, limit=args.limit)
    logger.info("%s: %s", "Applied" if args.apply else "Dry run", counts)
    return 1 if counts["conflicts"] or counts["unresolved"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
