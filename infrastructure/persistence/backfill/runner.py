"""Generic execution engine for resumable persisted-data backfills.

Strategies decide *which* events qualify and *how* one event is changed.  The
runner owns the boring but safety-critical mechanics shared by every strategy:
immutable manifests, checkpoints, result logs, locks and transaction-level
resume behavior.
"""

from __future__ import annotations

import logging
import json
import os
import signal
import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import text

from infrastructure.persistence.backfill.checkpoint import (
    ResultLog,
    iter_jsonl_with_offsets,
    manifest_v2_sha256,
    new_checkpoint,
    read_json,
    replace_with_retry,
    utc_iso_now,
    validate_checkpoint,
    validate_manifest,
    write_json_atomic,
)
from infrastructure.persistence.backfill.strategy import BackfillConflict, BackfillStrategy
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.views.view_manager import refresh_materialized_views

logger = logging.getLogger("backfill_runner")


class _GracefulStop:
    """Turn the first Ctrl+C into a checkpointed stop request.

    The handler intentionally does not raise from inside a database operation.
    The current event/page is therefore allowed to finish, after which the
    runner persists ``PAUSED`` and releases its advisory lock. This keeps the
    transaction boundary and result/checkpoint ordering intact.
    """

    def __init__(self) -> None:
        self.requested = False
        self._previous = None
        self._installed = False

    def install(self) -> "_GracefulStop":
        try:
            self._previous = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, self._handle)
            self._installed = True
        except (AttributeError, OSError, ValueError):
            # Signals can only be installed from the main thread. The runner
            # remains usable from embedded callers; they can stop by limit.
            logger.debug("SIGINT handler could not be installed", exc_info=True)
        return self

    def restore(self) -> None:
        if self._installed:
            signal.signal(signal.SIGINT, self._previous)
            self._installed = False

    def _handle(self, _signum, _frame) -> None:
        if not self.requested:
            self.requested = True
            logger.warning(
                "Ctrl+C received; finishing the current unit and pausing safely"
            )
        else:
            logger.warning("Stop already requested; waiting for the current unit")


class BackfillRunner:
    """Run any strategy with one event as the atomic commit unit.

    The loop is intentionally a streaming operational batch of size one at
    the transaction boundary: the process advances many events, but each
    event commits independently.  A failure therefore rolls back one event,
    not the complete historical migration.
    """

    def __init__(
        self,
        *,
        strategy: BackfillStrategy,
        manifest_path: str | Path,
        checkpoint_path: str | Path | None = None,
        results_path: str | Path | None = None,
        lock_name: str = "historical_backfill",
        refresh_views: bool = True,
    ) -> None:
        self.strategy = strategy
        self.manifest_path = Path(manifest_path)
        root = self.manifest_path.parent
        self.checkpoint_path = Path(checkpoint_path or root / "checkpoint.json")
        self.results_path = Path(results_path or root / "results.jsonl")
        self.lock_name = lock_name
        self.refresh_views = refresh_views
        self._active_stop: _GracefulStop | None = None

    def _stop_requested(self) -> bool:
        return bool(self._active_stop and self._active_stop.requested)

    def audit(
        self,
        *,
        force: bool = False,
        page_size: int = 250,
        limit: int | None = None,
    ) -> dict[str, Any] | None:
        stop = _GracefulStop().install()
        self._active_stop = stop
        try:
            return self._audit_impl(force=force, page_size=page_size, limit=limit)
        finally:
            self._active_stop = None
            stop.restore()

    def _audit_impl(
        self,
        *,
        force: bool = False,
        page_size: int = 250,
        limit: int | None = None,
    ) -> dict[str, Any] | None:
        """Create/resume a v2 streaming manifest using bounded keyset pages."""
        if page_size < 1:
            raise ValueError("page_size must be greater than zero")
        if limit is not None and limit < 1:
            raise ValueError("limit must be greater than zero")
        events_path = self.manifest_path.with_name("events.jsonl")
        partial_events_path = self.manifest_path.with_name("events.jsonl.partial")
        audit_checkpoint_path = self.manifest_path.with_name("audit_checkpoint.json")
        if force:
            if self.manifest_path.exists() and (
                self.checkpoint_path.exists() or self.results_path.exists()
            ):
                raise FileExistsError(
                    "cannot replace a manifest that already has apply artifacts; "
                    "use a new manifest directory or explicit checkpoint/results paths"
                )
            for path in (
                self.manifest_path,
                events_path,
                partial_events_path,
                audit_checkpoint_path,
            ):
                path.unlink(missing_ok=True)
        elif self.manifest_path.exists():
            if (
                audit_checkpoint_path.exists()
                and not self.checkpoint_path.exists()
                and not self.results_path.exists()
                and self._recover_completed_audit(
                    events_path=events_path,
                    audit_checkpoint_path=audit_checkpoint_path,
                )
            ):
                return read_json(self.manifest_path)
            raise FileExistsError(
                f"manifest already exists: {self.manifest_path}; use --force to replace it"
            )

        metadata = self.strategy.audit_metadata()
        if metadata.get("strategy") != self.strategy.strategy_name:
            raise ValueError("strategy audit metadata has an unexpected strategy name")
        state = read_json(audit_checkpoint_path) if audit_checkpoint_path.exists() else None
        if state is None:
            state = {
                "format_version": 2,
                **metadata,
                "created_at": utc_iso_now(),
                "order_by": ["starts_at", "id"],
                "events_file": events_path.name,
                "upper_bound": self.strategy.audit_upper_bound(),
                "next_cursor": None,
                "events_bytes": 0,
                "event_count": 0,
                "status": "RUNNING",
                "updated_at": utc_iso_now(),
            }
            write_json_atomic(audit_checkpoint_path, state)
        else:
            self._validate_audit_state(state, metadata, events_path.name)

        partial_events_path.parent.mkdir(parents=True, exist_ok=True)
        expected_bytes = int(state.get("events_bytes", 0))
        if expected_bytes and not partial_events_path.exists():
            raise FileNotFoundError(
                f"audit checkpoint expects missing partial events file: {partial_events_path}"
            )
        if partial_events_path.exists() and partial_events_path.stat().st_size < expected_bytes:
            raise ValueError(
                "partial events file is shorter than audit checkpoint; refusing to resume"
            )
        state["status"] = "RUNNING"
        state["updated_at"] = utc_iso_now()
        write_json_atomic(audit_checkpoint_path, state)
        with partial_events_path.open("a+b") as events_handle:
            events_handle.truncate(expected_bytes)
            events_handle.seek(0, 2)
            remaining = limit
            while True:
                if self._stop_requested():
                    state["status"] = "PAUSED"
                    state["updated_at"] = utc_iso_now()
                    write_json_atomic(audit_checkpoint_path, state)
                    logger.info(
                        "Audit paused by Ctrl+C after %d events; rerun to resume",
                        state["event_count"],
                    )
                    return None
                requested = page_size if remaining is None else min(page_size, remaining)
                if requested <= 0:
                    state["status"] = "PAUSED"
                    state["updated_at"] = utc_iso_now()
                    write_json_atomic(audit_checkpoint_path, state)
                    logger.info(
                        "Audit paused after %d events; rerun without --force to resume",
                        state["event_count"],
                    )
                    return None
                page, next_cursor = self.strategy.audit_page(
                    page_size=requested,
                    cursor=state.get("next_cursor"),
                    upper_bound=state.get("upper_bound"),
                )
                if not page:
                    break
                if next_cursor == state.get("next_cursor"):
                    raise RuntimeError("audit strategy returned a non-advancing cursor")
                for event in page:
                    events_handle.write(
                        json.dumps(
                            event,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                        + b"\n"
                    )
                events_handle.flush()
                os.fsync(events_handle.fileno())
                state["events_bytes"] = events_handle.tell()
                state["event_count"] = int(state["event_count"]) + len(page)
                state["next_cursor"] = next_cursor
                state["updated_at"] = utc_iso_now()
                write_json_atomic(audit_checkpoint_path, state)
                if remaining is not None:
                    remaining -= len(page)
                logger.info("Audit events=%d", state["event_count"])
                if self._stop_requested() and state.get("next_cursor") != state.get(
                    "upper_bound"
                ):
                    state["status"] = "PAUSED"
                    state["updated_at"] = utc_iso_now()
                    write_json_atomic(audit_checkpoint_path, state)
                    logger.info(
                        "Audit paused by Ctrl+C after %d events; rerun to resume",
                        state["event_count"],
                    )
                    return None
                if len(page) < requested or remaining == 0:
                    if remaining == 0:
                        state["status"] = "PAUSED"
                        state["updated_at"] = utc_iso_now()
                        write_json_atomic(audit_checkpoint_path, state)
                        logger.info(
                            "Audit paused after %d events; rerun without --force to resume",
                            state["event_count"],
                        )
                        return None
                    break

        replace_with_retry(partial_events_path, events_path)
        manifest = {
            "format_version": 2,
            **metadata,
            "created_at": state["created_at"],
            "order_by": ["starts_at", "id"],
            "events_file": events_path.name,
            "event_count": int(state["event_count"]),
            "upper_bound": state.get("upper_bound"),
        }
        manifest["manifest_sha256"] = manifest_v2_sha256(
            manifest, events_path, event_count=manifest["event_count"]
        )
        write_json_atomic(self.manifest_path, manifest)
        persisted_manifest = read_json(self.manifest_path)
        validate_manifest(persisted_manifest, base_path=self.manifest_path.parent)
        audit_checkpoint_path.unlink(missing_ok=True)
        logger.info("Manifest written: %s", self.manifest_path)
        logger.info("Events: %d", manifest["event_count"])
        logger.info("Manifest SHA-256: %s", manifest["manifest_sha256"])
        return persisted_manifest

    def _recover_completed_audit(
        self,
        *,
        events_path: Path,
        audit_checkpoint_path: Path,
    ) -> bool:
        """Finalize a manifest whose audit finished before validation succeeded.

        Audit output is append-only and the checkpoint records both the byte
        count and the final keyset cursor. If those durable markers prove that
        the scan reached its captured upper bound, recompute the digest from
        the persisted JSONL and repair only the manifest hash. This avoids
        repeating a large read-only audit after a transient filesystem or
        synchronization race at the final rename/validation step.
        """
        try:
            state = read_json(audit_checkpoint_path)
            manifest = read_json(self.manifest_path)
            if state.get("status") not in {"RUNNING", "PAUSED"}:
                return False
            self._validate_audit_state(
                state, self.strategy.audit_metadata(), events_path.name
            )
            for key in ("strategy", "scope", "parameters"):
                state_value = json.loads(json.dumps(state.get(key), sort_keys=True))
                manifest_value = json.loads(
                    json.dumps(manifest.get(key), sort_keys=True)
                )
                if state_value != manifest_value:
                    return False
            if not events_path.exists():
                return False
            expected_bytes = int(state.get("events_bytes", -1))
            if expected_bytes != events_path.stat().st_size:
                return False
            if state.get("next_cursor") != state.get("upper_bound"):
                return False
            if int(manifest.get("event_count", -1)) != int(state.get("event_count", -2)):
                return False
            calculated = manifest_v2_sha256(
                manifest,
                events_path,
                event_count=manifest.get("event_count"),
            )
            if calculated != manifest.get("manifest_sha256"):
                logger.warning(
                    "Repairing completed audit manifest hash: stored=%s calculated=%s",
                    manifest.get("manifest_sha256"),
                    calculated,
                )
                manifest["manifest_sha256"] = calculated
                write_json_atomic(self.manifest_path, manifest)
            persisted_manifest = read_json(self.manifest_path)
            validate_manifest(persisted_manifest, base_path=self.manifest_path.parent)
            audit_checkpoint_path.unlink(missing_ok=True)
            logger.info("Recovered completed audit manifest: %s", self.manifest_path)
            return True
        except (OSError, KeyError, TypeError, ValueError):
            return False

    @staticmethod
    def _validate_audit_state(
        state: dict[str, Any], metadata: dict[str, Any], events_file: str
    ) -> None:
        for key in ("strategy", "scope", "parameters"):
            # JSON object keys are strings on disk (notably period-pair ids),
            # while strategy metadata may still contain integer keys in memory.
            state_value = json.loads(json.dumps(state.get(key), sort_keys=True))
            metadata_value = json.loads(json.dumps(metadata.get(key), sort_keys=True))
            if state_value != metadata_value:
                raise ValueError("audit checkpoint belongs to different strategy parameters")
        if state.get("events_file") != events_file:
            raise ValueError("audit checkpoint points to a different events file")

    def apply(self, *, limit: int | None = None) -> int:
        stop = _GracefulStop().install()
        self._active_stop = stop
        try:
            return self._apply_impl(limit=limit)
        finally:
            self._active_stop = None
            stop.restore()

    def _apply_impl(self, *, limit: int | None = None) -> int:
        if limit is not None and limit < 1:
            raise ValueError("limit must be greater than zero")
        manifest = read_json(self.manifest_path)
        validate_manifest(manifest, base_path=self.manifest_path.parent)
        if manifest.get("strategy") != self.strategy.strategy_name:
            raise ValueError("manifest belongs to a different backfill strategy")

        if self.checkpoint_path.exists():
            checkpoint = read_json(self.checkpoint_path)
            validate_checkpoint(
                checkpoint, manifest, base_path=self.manifest_path.parent
            )
        else:
            checkpoint = new_checkpoint(manifest=manifest, run_id=str(uuid.uuid4()))
            write_json_atomic(self.checkpoint_path, checkpoint)

        result_log = ResultLog(
            self.results_path,
            manifest_sha256=manifest["manifest_sha256"],
        )
        start = int(checkpoint.get("next_index", 0))
        results_offset = int(checkpoint.get("results_offset", 0))
        if result_log.path.exists() and result_log.path.stat().st_size < results_offset:
            raise ValueError("results log is shorter than checkpoint results_offset")
        latest_results = result_log.latest_from_index(
            start, start_offset=results_offset
        )
        checkpoint.setdefault(
            "counts",
            {"APPLIED": 0, "ALREADY_CORRECT": 0, "CONFLICT": 0, "SKIPPED": 0, "FAILED": 0},
        )
        for status, count in self._counts_from_results(latest_results).items():
            checkpoint["counts"][status] = checkpoint["counts"].get(status, 0) + count
        write_json_atomic(self.checkpoint_path, checkpoint)
        if int(manifest.get("format_version", 1)) >= 2:
            events_total = int(manifest["event_count"])
            events_path = self.manifest_path.parent / manifest["events_file"]
            events_offset = int(checkpoint.get("events_offset", 0))
            if events_path.stat().st_size < events_offset:
                raise ValueError("events file is shorter than checkpoint events_offset")
            event_iter = iter_jsonl_with_offsets(
                events_path,
                start_offset=events_offset,
                start_index=start,
            )
        else:
            events_total = len(manifest["events"])
            event_iter = iter(manifest["events"])
        lock_connection = self._acquire_lock()
        checkpoint["status"] = "RUNNING"
        write_json_atomic(self.checkpoint_path, checkpoint)
        try:
            processed = 0
            for streamed in event_iter:
                if self._stop_requested():
                    checkpoint["status"] = "PAUSED"
                    checkpoint["updated_at"] = utc_iso_now()
                    write_json_atomic(self.checkpoint_path, checkpoint)
                    logger.info(
                        "Apply paused by Ctrl+C after %d events; rerun to resume",
                        processed,
                    )
                    return 0
                if int(manifest.get("format_version", 1)) >= 2:
                    index, item, end_offset = streamed
                else:
                    index = start + processed
                    item = streamed
                    end_offset = None
                if limit is not None and processed >= limit:
                    break
                event_id = int(item["event_id"])
                prior = latest_results.get(event_id)
                prior_status = prior.get("status") if prior else None
                already_recorded = bool(
                    prior and prior.get("status") in ResultLog.TERMINAL_STATUSES
                )
                if already_recorded:
                    status_result = prior
                elif item.get("status") == "CONFLICT":
                    status_result = {
                        "event_id": event_id,
                        "status": "CONFLICT",
                        "detail": "manifest audit marked this event as conflicting",
                    }
                else:
                    try:
                        status_result = self.strategy.apply_event(
                            event_id,
                            expected_state=item,
                        )
                    except BackfillConflict as exc:
                        status_result = {
                            "event_id": event_id,
                            "status": "CONFLICT",
                            "detail": str(exc),
                        }
                    except Exception as exc:  # strategy decides conflicts; runner persists all failures
                        status_result = {
                            "event_id": event_id,
                            "status": "FAILED",
                            "detail": f"{type(exc).__name__}: {exc}",
                        }
                if not already_recorded:
                    status_result["manifest_sha256"] = manifest["manifest_sha256"]
                    status_result["index"] = index
                    status_result["completed_at"] = utc_iso_now()
                    results_end_offset = result_log.append(status_result)
                    latest_results[event_id] = status_result
                    checkpoint["results_offset"] = results_end_offset

                status = str(status_result["status"])
                checkpoint["next_index"] = index if status == "FAILED" else index + 1
                if status != "FAILED" and end_offset is not None:
                    checkpoint["events_offset"] = end_offset
                checkpoint["updated_at"] = utc_iso_now()
                checkpoint["last_event_id"] = event_id
                checkpoint["last_starts_at"] = item.get("starts_at")
                if not already_recorded:
                    if prior_status in checkpoint["counts"]:
                        checkpoint["counts"][prior_status] -= 1
                    checkpoint["counts"][status] = checkpoint["counts"].get(status, 0) + 1
                write_json_atomic(self.checkpoint_path, checkpoint)
                processed += 1
                logger.info("event=%s status=%s", event_id, status)
                if status == "FAILED":
                    checkpoint["status"] = "FAILED"
                    write_json_atomic(self.checkpoint_path, checkpoint)
                    return 1
                if self._stop_requested() and checkpoint["next_index"] < events_total:
                    checkpoint["status"] = "PAUSED"
                    checkpoint["updated_at"] = utc_iso_now()
                    write_json_atomic(self.checkpoint_path, checkpoint)
                    logger.info(
                        "Apply paused by Ctrl+C after event=%s; rerun to resume",
                        event_id,
                    )
                    return 0

            checkpoint["status"] = (
                "COMPLETED" if checkpoint["next_index"] >= events_total else "PAUSED"
            )
            checkpoint["updated_at"] = utc_iso_now()
            write_json_atomic(self.checkpoint_path, checkpoint)
            if checkpoint["status"] == "COMPLETED" and self.refresh_views:
                refresh_materialized_views(db_manager.engine)
                logger.info("Reporting materialized views refreshed")
            return 0
        finally:
            self._release_lock(lock_connection)

    @staticmethod
    def _counts_from_results(
        results: dict[int, dict[str, Any]],
    ) -> dict[str, int]:
        counts = {
            "APPLIED": 0,
            "ALREADY_CORRECT": 0,
            "CONFLICT": 0,
            "SKIPPED": 0,
            "FAILED": 0,
        }
        for result in results.values():
            status = str(result.get("status"))
            if status in counts:
                counts[status] += 1
        return counts

    def _acquire_lock(self):
        if db_manager.engine.dialect.name != "postgresql":
            return None
        connection = db_manager.engine.connect()
        acquired = connection.execute(
            text("SELECT pg_try_advisory_lock(hashtext(:lock_name))"),
            {"lock_name": self.lock_name},
        ).scalar()
        if not acquired:
            connection.close()
            raise RuntimeError("another historical backfill is already running")
        return connection

    def _release_lock(self, connection) -> None:
        if connection is None:
            return
        try:
            connection.execute(
                text("SELECT pg_advisory_unlock(hashtext(:lock_name))"),
                {"lock_name": self.lock_name},
            )
        finally:
            connection.close()
