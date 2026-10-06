"""Exact run membership and final calendar metadata without O(N) Python memory."""

import sqlite3
from .run_directory import RunDirectory
from zoneinfo import ZoneInfo
from infrastructure.settings import Config
from infrastructure.settings.job_execution import JobExecutionSettings
from shared.temporal import require_aware


class DiscoveryRunStore:
    def __init__(self):
        self._directory = RunDirectory()
        self.db = sqlite3.connect(self._directory.path / "members.db")
        self.db.execute(
            f"PRAGMA cache_size=-{JobExecutionSettings().temporary_cache_kib}"
        )
        self.db.execute("PRAGMA temp_store=FILE")
        self.db.execute("CREATE TABLE members (id INTEGER PRIMARY KEY, date TEXT, sport TEXT)")
        self.db.execute(
            "CREATE TABLE sources (source_id TEXT PRIMARY KEY, event_id INTEGER NOT NULL)"
        )
        self.db.execute("CREATE TABLE tournaments (sport TEXT, id INTEGER, PRIMARY KEY(sport,id))")
        self.db.execute("CREATE TABLE seen_ids (namespace TEXT, id TEXT, PRIMARY KEY(namespace,id))")
        self.zone = ZoneInfo(Config.TIMEZONE)

    def record_mapping(self, mapping):
        """Commit canonical calendar metadata and provider membership together."""
        values = []
        for event in mapping.values():
            try:
                kickoff = require_aware(event.starts_at)
                event_date = kickoff.astimezone(self.zone).date().isoformat()
            except (ValueError, TypeError, AttributeError):
                event_date = "unknown"
            values.append((event.id, event_date, event.sport or "Unknown"))
        with self.db:
            self.db.executemany(
                "INSERT INTO members VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "date=excluded.date, sport=excluded.sport",
                values,
            )
            self.db.executemany(
                "INSERT INTO sources VALUES (?,?) ON CONFLICT(source_id) DO UPDATE SET event_id=excluded.event_id",
                [(str(sid), event.id) for sid, event in mapping.items()],
            )

    def first_tournament(self, sport, tournament_id):
        cursor = self.db.execute(
            "INSERT OR IGNORE INTO tournaments VALUES (?,?)", (sport, tournament_id)
        )
        self.db.commit()
        return cursor.rowcount == 1

    def first_seen(self, namespace: str, source_id: str) -> bool:
        """Deduplicate streamed provider IDs using disk instead of an unbounded set."""
        cursor = self.db.execute(
            "INSERT OR IGNORE INTO seen_ids VALUES (?,?)", (namespace, source_id)
        )
        # This state is temporary and read only by this connection. A commit
        # per fixture would add disk work without providing useful durability.
        return cursor.rowcount == 1

    def source_members(self, source_ids):
        if not source_ids:
            return {}
        placeholders = ",".join("?" for _ in source_ids)
        return {
            row[0]: row[1]
            for row in self.db.execute(
                f"SELECT source_id,event_id FROM sources WHERE source_id IN ({placeholders})",
                list(map(str, source_ids)),
            )
        }

    def counts(self):
        return self.db.execute(
            "SELECT sport,date,count(*) FROM members GROUP BY sport,date ORDER BY sport,date"
        )

    def close(self):
        self.db.close()
        self._directory.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
