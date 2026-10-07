"""Two scheduled passes, with durable per-sport retries until the UTC day ends."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from infrastructure.settings import Config
from infrastructure.settings import discovery as settings
from modules.jobs.event_discard_cleanup import run_event_discard_cleanup
from modules.jobs.discovery.filters import sofascore_discovery_sport_slugs
from shared.temporal import UTC, in_timezone, now_in_timezone

from .pipeline import discover_events_for_date

logger = logging.getLogger(__name__)


def daily_discovery_passes(now: datetime) -> list[tuple[datetime, str, str]]:
    """Reconstruct due passes, including missed starts, with their original dates.

    The target comes from the opening occurrence, never the retry clock.
    Past UTC dates are excluded because discovery only admits future events.
    """
    local = in_timezone(now, Config.TIMEZONE)
    current_date = now.astimezone(UTC).date().isoformat()
    passes = []
    # Also covers zones where a local opening belongs to an adjacent UTC date.
    for days_ago in (2, 1, 0):
        day = local.date() - timedelta(days=days_ago)
        for slot, opening_time, offset in (
            ("anticipada", settings.SOFASCORE.daily_advance_time, 1),
            ("actualizacion", settings.SOFASCORE.daily_refresh_time, 0),
        ):
            occurrence = datetime.combine(
                day, datetime.strptime(opening_time, "%H:%M").time(), tzinfo=local.tzinfo,
            )
            target = (occurrence.astimezone(UTC).date() + timedelta(days=offset)).isoformat()
            if occurrence <= local and target >= current_date:
                passes.append((occurrence, target, slot))
    return sorted(passes)


def run_daily_discovery_job() -> dict | None:
    logger.info("Starting daily discovery heartbeat")
    run_event_discard_cleanup()
    try:
        DailyDiscoveryRepository.cleanup_old_logs(settings.SOFASCORE.daily_progress_retention_days)
    except Exception as exc:
        logger.warning("Failed to cleanup DailyDiscovery logs: %s", exc)

    stats = {}
    sports = sofascore_discovery_sport_slugs()
    for occurrence, target, slot in daily_discovery_passes(now_in_timezone(Config.TIMEZONE)):
        DailyDiscoveryRepository.initialize_sports_for_slot(target, slot, sports)
        pending = sofascore_discovery_sport_slugs(
            DailyDiscoveryRepository.get_pending_sports(target, slot)
        )
        if not pending:
            continue
        logger.info(
            "Daily discovery target_date=%s pass=%s scheduled_at=%s pending_sports=%s",
            target, slot, occurrence.isoformat(), pending,
        )
        result = discover_events_for_date(date=target, sports=pending, run_slot=slot)
        for name, count in result.items():
            stats[name] = stats.get(name, 0) + count
    return stats or None
