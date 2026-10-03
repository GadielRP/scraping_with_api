"""Calendar breakdown of committed events, deduplicated within one discovery run."""

from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

from infrastructure.settings import Config
from shared.temporal import require_aware


class DiscoveryPersistenceSummary:
    """Count canonical events using their final persisted kickoff and sport."""

    def __init__(self):
        self.timezone = Config.TIMEZONE
        self._zone = ZoneInfo(self.timezone)
        self._events = {}

    def record(self, events):
        for event in events:
            # Invalid calendar metadata must not turn a committed write into a failure.
            event_date = "unknown"
            starts_at = event.starts_at
            if isinstance(starts_at, datetime):
                try:
                    event_date = require_aware(starts_at).astimezone(self._zone).date().isoformat()
                except ValueError:
                    pass
            self._events[event.id] = (event_date, event.sport or "Unknown")

    def log(self, logger, *, job, requested_date=None, run_slot=None):
        context = (job, requested_date or "none", run_slot or "none", self.timezone)
        counts = Counter((sport, event_date) for event_date, sport in self._events.values())
        day_counts = Counter()
        lines = []
        previous_sport = None
        for (sport, event_date), count in sorted(counts.items()):
            if sport != previous_sport:
                lines.append(f"- {sport}:")
                previous_sport = sport
            lines.append(f"  - {event_date}: {count}")
            day_counts[event_date] += count
        if day_counts:
            lines.append("- Total by date:")
            lines.extend(f"  - {event_date}: {count}" for event_date, count in sorted(day_counts.items()))
        body = "\n" + "\n".join(lines) if lines else ""
        logger.info(
            "Discovery persistence summary: job=%s requested_date=%s slot=%s "
            "timezone=%s persisted_unique=%s%s",
            *context, len(self._events), body,
        )
