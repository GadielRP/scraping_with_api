"""Fill ``results.finished_at`` for results stored before that column existed.

Each stored result's event is refetched from SofaScore ``/event/{id}`` and persisted through the same
flow as the results collection job (``events`` and ``results``), so ``finished_at`` is the provider's
``changeTimestamp`` when that update changed ``status.type``, and otherwise the earlier of
``changeTimestamp`` and the sport's settlement window after kickoff.

Results are read by event id, one page of ``--batch-size`` rows at a time (the page size is also the
write size). Progress is saved after every batch, so an interrupted run resumes at the last event id
reviewed. Rows that already have ``finished_at`` are never read.

This script never deletes events. When the provider payload of a stored result would make the job
delete its event (canceled, not found, finished without score), the evidence is appended to
``deletion_candidates.jsonl`` next to the checkpoint for manual review.

Examples::

    python -m scripts.maintenance.backfill_result_finished_at --batch-size 50
    python -m scripts.maintenance.backfill_result_finished_at --season-id 123 --reset
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
from pathlib import Path

from infrastructure.persistence.backfill.checkpoint import read_json, utc_iso_now, write_json_atomic
from infrastructure.persistence.repositories import ResultRepository
from modules.jobs.results_collection_job.batch_processor import collect_batch
from shared.shutdown import is_shutdown_requested

logger = logging.getLogger("backfill_result_finished_at")

BACKFILL_DIRECTORY = Path("data/backfills/results_finished_at")
CHECKPOINT_FILE = "checkpoint.json"
DELETION_CANDIDATES_FILE = "deletion_candidates.jsonl"


def _record_deletion_candidates(path):
    def record(deletions):
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            for event_id in sorted(deletions):
                evidence = deletions.evidence.get(event_id)
                entry = {"event_id": event_id, **(dataclasses.asdict(evidence) if evidence else {})}
                handle.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        logger.warning("Events NOT deleted, logged for review: %s", sorted(deletions))

    return record


def run_backfill(batch_size, directory, season_id=None):
    """Process pages after the checkpoint until none are left; returns the final checkpoint."""
    if batch_size < 1:
        raise ValueError("--batch-size must be positive")
    directory.mkdir(parents=True, exist_ok=True)
    checkpoint_path = directory / CHECKPOINT_FILE
    on_deletions = _record_deletion_candidates(directory / DELETION_CANDIDATES_FILE)
    checkpoint = read_json(checkpoint_path) if checkpoint_path.exists() else {
        "last_event_id": 0, "reviewed": 0, "updated": 0,
    }
    logger.info("Starting batch_size=%s season_id=%s after_event_id=%s", batch_size, season_id, checkpoint["last_event_id"])
    while not is_shutdown_requested():
        batch = ResultRepository.missing_finished_at_page(checkpoint["last_event_id"], batch_size, season_id)
        if not batch:
            break
        stats = collect_batch(batch, "Backfill result finished_at", on_deletions=on_deletions)
        checkpoint = {
            "last_event_id": batch[-1].id,
            "reviewed": checkpoint["reviewed"] + len(batch),
            "updated": checkpoint["updated"] + stats["updated"],
            "saved_at": utc_iso_now(),
        }
        write_json_atomic(checkpoint_path, checkpoint)
        logger.info("Batch done batch=%s stats=%s totals=%s", len(batch), stats, checkpoint)
    return checkpoint


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--batch-size", type=int, default=100, help="Rows per page and per write")
    parser.add_argument("--season-id", type=int, help="Only events of this season (events.season_id)")
    parser.add_argument("--directory", type=Path, default=BACKFILL_DIRECTORY, help="Checkpoint and review log folder")
    parser.add_argument("--reset", action="store_true", help="Ignore saved progress and start at event id 1")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    if args.reset:
        (args.directory / CHECKPOINT_FILE).unlink(missing_ok=True)
    run_backfill(args.batch_size, args.directory, args.season_id)


if __name__ == "__main__":
    main()
