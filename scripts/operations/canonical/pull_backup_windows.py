#!/usr/bin/env python3
"""
Windows-side helper to pull weekly backups from the server over SSH.

What it does:
1) Connects to the server via SSH and runs the server backup script to generate a fresh dump.
2) Finds the newest *.dump.gz file on the server.
3) Downloads it to a local directory using scp.

Prerequisites on Windows:
- OpenSSH Client installed (ssh, scp in PATH)
- A working SSH private key that can connect to the server (id_rsa or similar)
- Python 3 installed (to run this script)

Typical usage:
  python scripts/pull_backup_windows.py \
    --server-ip 203.0.113.10 \
    --server-user root \
    --ssh-key "%USERPROFILE%/.ssh/id_rsa" \
    --project-dir /opt/sofascore \
    --remote-backup-dir /opt/sofascore/backups \
    --local-backup-dir "%USERPROFILE%/Documents/sofascore/backups" \
    --container sofascore-pg \
    --db-name sofascore_odds \
    --db-user sofascore \
    --retention-days 30

You can schedule this script weekly with Windows Task Scheduler.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pull latest Postgres backup from server over SSH")
    parser.add_argument("--server-ip", required=True, help="Public IP or DNS of the server")
    parser.add_argument("--server-user", default="root", help="SSH user (default: root)")
    parser.add_argument("--ssh-key", default=str(Path.home() / ".ssh" / "id_rsa"), help="Path to SSH private key")
    parser.add_argument("--project-dir", default="/opt/sofascore", help="Server project dir containing scripts/")
    parser.add_argument("--remote-backup-dir", default="/opt/sofascore/backups", help="Server backups directory")
    parser.add_argument(
        "--local-backup-dir",
        default=str(Path.home() / "Documents" / "sofascore" / "backups"),
        help="Local directory to store downloaded backups",
    )
    parser.add_argument("--container", default="sofascore-pg", help="Docker container name for Postgres")
    parser.add_argument("--db-name", default="sofascore_odds", help="Database name")
    parser.add_argument("--db-user", default="sofascore", help="Database user")
    parser.add_argument("--retention-days", type=int, default=30, help="Server-side retention in days")
    return parser.parse_args()


def run_ssh(key: str, user: str, host: str, command: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["ssh", "-i", key, f"{user}@{host}", command],
        capture_output=True,
        text=True,
        check=False,
    )


def run_scp(key: str, src: str, dst_dir: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["scp", "-i", key, src, str(dst_dir)],
        capture_output=True,
        text=True,
        check=False,
    )


def main() -> int:
    args = parse_args()

    local_dir = Path(os.path.expandvars(os.path.expanduser(args.local_backup_dir)))
    local_dir.mkdir(parents=True, exist_ok=True)

    # 1) Trigger a fresh backup on the server
    backup_cmd = (
        f"python3 {args.project_dir}/scripts/backup_server.py "
        f"--container {args.container} "
        f"--db-name {args.db_name} "
        f"--db-user {args.db_user} "
        f"--output-dir {args.remote_backup_dir} "
        f"--retention-days {args.retention_days}"
    )
    print(f"[pull] Triggering server backup: {backup_cmd}")
    proc = run_ssh(args.ssh_key, args.server_user, args.server_ip, backup_cmd)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
        print("[pull] ERROR: Server backup script failed", file=sys.stderr)
        return 2
    else:
        print(proc.stdout)

    # 2) Query latest file on the server
    list_cmd = f"ls -1t {args.remote_backup_dir}/*.dump.gz 2>/dev/null | head -n 1"
    proc = run_ssh(args.ssh_key, args.server_user, args.server_ip, list_cmd)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
        print("[pull] ERROR: Could not list backups on server", file=sys.stderr)
        return 3

    latest = proc.stdout.strip()
    if not latest:
        print("[pull] ERROR: No backups found on server", file=sys.stderr)
        return 4
    print(f"[pull] Latest backup on server: {latest}")

    # 3) Download it via scp
    remote_spec = f"{args.server_user}@{args.server_ip}:{latest}"
    proc = run_scp(args.ssh_key, remote_spec, local_dir)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
        print("[pull] ERROR: scp download failed", file=sys.stderr)
        return 5

    filename = os.path.basename(latest)
    local_path = local_dir / filename
    if not local_path.exists() or local_path.stat().st_size <= 0:
        print("[pull] ERROR: Downloaded file missing or empty", file=sys.stderr)
        return 6

    print(f"[pull] OK: {local_path} ({local_path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


