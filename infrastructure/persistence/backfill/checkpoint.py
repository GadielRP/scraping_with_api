"""Durable filesystem artifacts for resumable maintenance jobs.

The canonical-period backfill intentionally keeps its execution state outside
the application schema.  The manifest is immutable; the checkpoint and result
log are updated only after an event transaction commits.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def manifest_sha256(manifest: dict[str, Any]) -> str:
    """Hash a manifest while excluding its self-referential hash field."""
    payload = dict(manifest)
    payload.pop("manifest_sha256", None)
    # JSON object keys are strings on disk.  Normalize once before hashing so
    # an in-memory mapping such as {5: 8} hashes identically after round-trip
    # serialization as {"5": 8}.
    payload = json.loads(_json_bytes(payload).decode("utf-8"))
    return hashlib.sha256(_json_bytes(payload)).hexdigest()


def utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path: str | Path, value: Any) -> None:
    """Write JSON atomically and durably on the same filesystem."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=destination.parent,
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        replace_with_retry(temporary_name, destination)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def replace_with_retry(source: str | Path, destination: str | Path) -> None:
    """Atomically replace a file, tolerating short-lived Windows locks."""
    delays = (0.05, 0.1, 0.2, 0.4, 0.8, 1.6)
    source_path = Path(source)
    destination_path = Path(destination)
    for attempt, delay in enumerate(delays):
        try:
            os.replace(source_path, destination_path)
            return
        except PermissionError:
            if attempt == len(delays) - 1:
                raise
            time.sleep(delay)


def read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def iter_jsonl(path: str | Path):
    """Yield JSONL records one at a time without retaining the file in RAM."""
    for _, item, _ in iter_jsonl_with_offsets(path):
        yield item


def iter_jsonl_with_offsets(
    path: str | Path,
    *,
    start_offset: int = 0,
    start_index: int = 0,
):
    """Yield ``(index, record, end_offset)`` for resumable JSONL scans."""
    with Path(path).open("rb") as handle:
        handle.seek(int(start_offset))
        index = int(start_index)
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue
            try:
                item = json.loads(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_number}") from exc
            yield index, item, handle.tell()
            index += 1


class ResultLog:
    """Append-only event outcomes used to close the commit/checkpoint gap."""

    # A conflict is a deliberate fail-closed outcome and can be skipped on a
    # resume.  FAILED is retryable: the checkpoint must stop before advancing
    # past it so a transient DB/network failure is not silently lost.
    TERMINAL_STATUSES = frozenset({"APPLIED", "ALREADY_CORRECT", "CONFLICT", "SKIPPED"})

    def __init__(self, path: str | Path, *, manifest_sha256: str):
        self.path = Path(path)
        self.manifest_sha256 = manifest_sha256

    def latest(self) -> dict[int, dict[str, Any]]:
        if not self.path.exists():
            return {}
        latest: dict[int, dict[str, Any]] = {}
        for raw_line in self.path.read_text(encoding="utf-8").splitlines():
            if not raw_line.strip():
                continue
            item = json.loads(raw_line)
            if item.get("manifest_sha256") != self.manifest_sha256:
                raise ValueError(
                    "result log contains an outcome for a different manifest"
                )
            latest[int(item["event_id"])] = item
        return latest

    def latest_from_index(
        self, start_index: int, *, start_offset: int = 0
    ) -> dict[int, dict[str, Any]]:
        """Read only outcomes at or after a checkpoint index.

        A resumed run normally has at most one result beyond the checkpoint:
        the process may have committed and appended the result just before the
        checkpoint rename failed.  Keeping only this suffix avoids retaining a
        result dictionary for an entire very large migration.
        """
        if not self.path.exists():
            return {}
        latest: dict[int, dict[str, Any]] = {}
        for index, item, _ in iter_jsonl_with_offsets(
            self.path, start_offset=start_offset
        ):
            if item.get("manifest_sha256") != self.manifest_sha256:
                raise ValueError(
                    "result log contains an outcome for a different manifest"
                )
            if int(item.get("index", index)) >= int(start_index):
                latest[int(item["event_id"])] = item
        return latest

    def append(self, result: dict[str, Any]) -> int:
        if result.get("manifest_sha256") != self.manifest_sha256:
            raise ValueError("result does not belong to this manifest")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
            return handle.tell()


def build_manifest(
    *,
    strategy: str,
    scope: dict[str, Any],
    period_pairs: dict[int, int] | None = None,
    parameters: dict[str, Any] | None = None,
    events: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    manifest = {
        "format_version": 1,
        "strategy": strategy,
        "created_at": utc_iso_now(),
        "scope": scope,
        "parameters": parameters or {},
        "order_by": ["starts_at", "id"],
        "events": list(events),
    }
    if period_pairs is not None:
        # Kept as a top-level compatibility field because this is useful when
        # inspecting canonical-period manifests by hand.
        manifest["period_pairs"] = {
            str(k): int(v) for k, v in sorted(period_pairs.items())
        }
    manifest["manifest_sha256"] = manifest_sha256(manifest)
    return manifest


def manifest_v2_payload(manifest: dict[str, Any]) -> dict[str, Any]:
    """Return the metadata portion hashed by a streaming manifest."""
    payload = dict(manifest)
    payload.pop("manifest_sha256", None)
    return payload


def manifest_v2_sha256(
    manifest: dict[str, Any],
    events_path: str | Path,
    *,
    event_count: int | None = None,
) -> str:
    """Hash v2 metadata plus canonical event records without loading all events."""
    digest = hashlib.sha256()
    metadata = manifest_v2_payload(manifest)
    digest.update(_json_bytes(metadata))
    digest.update(b"\n")
    count = 0
    for event in iter_jsonl(events_path):
        digest.update(_json_bytes(event))
        digest.update(b"\n")
        count += 1
    if event_count is not None and count != int(event_count):
        raise ValueError(
            f"streaming manifest event_count mismatch: expected={event_count} actual={count}"
        )
    return digest.hexdigest()


def new_checkpoint(*, manifest: dict[str, Any], run_id: str) -> dict[str, Any]:
    return {
        "format_version": 1,
        "run_id": run_id,
        "strategy": manifest["strategy"],
        "manifest_sha256": manifest["manifest_sha256"],
        "next_index": 0,
        "events_offset": 0,
        "results_offset": 0,
        "status": "RUNNING",
        "updated_at": utc_iso_now(),
        "counts": {"APPLIED": 0, "ALREADY_CORRECT": 0, "CONFLICT": 0, "SKIPPED": 0, "FAILED": 0},
    }


def validate_checkpoint(
    checkpoint: dict[str, Any],
    manifest: dict[str, Any],
    *,
    base_path: str | Path | None = None,
) -> None:
    validate_manifest(manifest, base_path=base_path)
    expected = manifest["manifest_sha256"]
    if checkpoint.get("manifest_sha256") != expected:
        raise ValueError("checkpoint belongs to a different manifest")
    if checkpoint.get("strategy") != manifest.get("strategy"):
        raise ValueError("checkpoint belongs to a different backfill strategy")


def validate_manifest(manifest: dict[str, Any], *, base_path: str | Path | None = None) -> None:
    expected = manifest.get("manifest_sha256")
    if not expected:
        raise ValueError("manifest_sha256 is missing from the manifest")
    if int(manifest.get("format_version", 1)) >= 2:
        if base_path is None:
            raise ValueError("format v2 manifest validation requires base_path")
        events_path = Path(base_path) / manifest["events_file"]
        calculated = manifest_v2_sha256(
            manifest,
            events_path,
            event_count=manifest.get("event_count"),
        )
    else:
        calculated = manifest_sha256(manifest)
    if calculated != expected:
        raise ValueError(
            "manifest_sha256 does not match the manifest contents: "
            f"stored={expected} calculated={calculated}"
        )
