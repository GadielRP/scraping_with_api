#!/usr/bin/env python3
"""
Server-side PostgreSQL backup script.

What it does:
- Executes pg_dump inside a running Postgres Docker container (no extra tools needed on host).
- Streams the dump to a gzip-compressed file (no large memory usage).
- Rotates old backups older than --retention-days.

Usage examples (on the server):
  python3 scripts/backup_server.py \
    --container sofascore-pg \
    --db-name sofascore_odds \
    --db-user sofascore \
    --output-dir /opt/sofascore/backups \
    --retention-days 14

Exit code is non-zero on failure.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import os
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PostgreSQL logical backup via Docker pg_dump")
    parser.add_argument("--container", default="sofascore-pg", help="Docker container name for Postgres")
    parser.add_argument("--db-name", default="sofascore_odds", help="Database name")
    parser.add_argument("--db-user", default="sofascore", help="Database user")
    parser.add_argument("--output-dir", default="/opt/sofascore/backups", help="Directory to write backups")
    parser.add_argument("--retention-days", type=int, default=14, help="Delete backups older than N days")
    parser.add_argument("--filename-prefix", default=None, help="Optional filename prefix. Default: <db-name>")
    return parser.parse_args()


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_output_path(output_dir: Path, db_name: str, prefix: str | None) -> Path:
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{prefix or db_name}_{timestamp}.dump.gz"
    return output_dir / base


def run_pg_dump(container: str, db_name: str, db_user: str, out_path: Path) -> None:
    """
    Run pg_dump inside the Postgres container and stream-compress to gzip locally.
    """
    cmd = [
        "docker",
        "exec",
        container,
        "pg_dump",
        "-U",
        db_user,
        "-d",
        db_name,
        "-Fc",  # custom format (recommended)
    ]

    # Stream stdout -> gzip file to avoid loading whole dump in memory
    with gzip.open(out_path, mode="wb", compresslevel=9) as gz_out:
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except FileNotFoundError as exc:
            raise RuntimeError("'docker' command not found. Ensure Docker is installed on server.") from exc

        assert process.stdout is not None  # for typing
        try:
            shutil.copyfileobj(process.stdout, gz_out)
        finally:
            process.stdout.close()
        return_code = process.wait()

    if return_code != 0:
        # Include stderr in error message for easier debugging
        stderr = b""
        if process.stderr:
            stderr = process.stderr.read()
        # Cleanup partial file
        try:
            if out_path.exists():
                out_path.unlink()
        except Exception:
            pass
        raise RuntimeError(f"pg_dump failed with code {return_code}: {stderr.decode(errors='ignore')}")


def validate_file_nonempty(path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f"Backup file not created: {path}")
    if path.stat().st_size <= 0:
        raise RuntimeError(f"Backup file is empty: {path}")


def rotate_old_backups(output_dir: Path, retention_days: int) -> int:
    if retention_days <= 0:
        return 0
    cutoff = dt.datetime.now() - dt.timedelta(days=retention_days)
    deleted = 0
    for item in output_dir.glob("*.dump.gz"):
        try:
            mtime = dt.datetime.fromtimestamp(item.stat().st_mtime)
        except OSError:
            continue
        if mtime < cutoff:
            try:
                item.unlink()
                deleted += 1
            except Exception:
                # best-effort; continue
                pass
    return deleted


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    ensure_directory(output_dir)

    out_path = build_output_path(output_dir, args.db_name, args.filename_prefix)
    print(f"[backup] Starting pg_dump from container '{args.container}' database '{args.db_name}'...")
    print(f"[backup] Writing to: {out_path}")
    try:
        run_pg_dump(args.container, args.db_name, args.db_user, out_path)
        validate_file_nonempty(out_path)
    except Exception as exc:
        print(f"[backup] ERROR: {exc}", file=sys.stderr)
        return 2

    removed = rotate_old_backups(output_dir, args.retention_days)
    print(f"[backup] OK: {out_path.name} ({out_path.stat().st_size} bytes). Rotated {removed} old file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


