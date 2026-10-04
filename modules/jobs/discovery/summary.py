"""Calendar audit of confirmed discovery writes; the caller owns the run store."""

from infrastructure.settings import Config


def log_discovery_summary(store, logger, *, job, requested_date=None, run_slot=None):
    total = 0
    for sport, event_date, count in store.counts():
        total += count
        logger.info(
            "Discovery calendar job=%s requested_date=%s slot=%s timezone=%s "
            "sport=%s event_date=%s persisted_unique=%s",
            job,
            requested_date,
            run_slot,
            Config.TIMEZONE,
            sport,
            event_date,
            count,
        )
    logger.info("Discovery persistence summary job=%s persisted_unique=%s", job, total)
