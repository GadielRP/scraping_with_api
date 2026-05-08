"""Fail if active runtime jobs write old event_odds / odds_snapshot payloads."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ACTIVE_DIRS = [
    ROOT / "modules" / "jobs",
]
ALLOWED_FILES = {
    ROOT / "modules" / "jobs" / "pre_start_check_job" / "odds_extraction.py",
    ROOT / "modules" / "jobs" / "pre_start_check_job" / "__init__.py",
}
FORBIDDEN = [
    "OddsRepository.upsert_event_odds",
    "OddsRepository.create_odds_snapshot",
    "process_event_odds_from_dropping_odds",
]


def main() -> int:
    violations = []
    for directory in ACTIVE_DIRS:
        for path in directory.rglob("*.py"):
            if path in ALLOWED_FILES:
                continue
            source = path.read_text(encoding="utf-8", errors="ignore")
            for pattern in FORBIDDEN:
                if pattern in source:
                    violations.append((path, pattern))

    if violations:
        print("Active legacy odds write usages found:")
        for path, pattern in violations:
            print(f"- {path.relative_to(ROOT)}: {pattern}")
        return 1

    print("No active legacy odds writes found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
