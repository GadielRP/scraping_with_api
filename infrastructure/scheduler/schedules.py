"""Translate configured calendar occurrences into execution requests."""

from datetime import timezone
from infrastructure.settings import Config
from shared.execution_context import Priority


def configure_calendar(clock, settings, on_due=None):
    """Build the same calendar for execution and status without constructing workers."""

    def trigger(job, name, priority, closing):
        # Capture the occurrence before schedule advances its naive system-local next_run.
        if on_due is not None:
            on_due(name, job.next_run.astimezone(timezone.utc), priority, closing)

    def bind(job, name, priority=Priority.MAINTENANCE, closing=False):
        job.do(trigger, job, name, priority, closing)

    def daily(name, times):
        for time_str in times:
            bind(clock.every().day.at(time_str, Config.TIMEZONE), name)

    daily("discovery", Config.DISCOVERY_TIMES)
    daily("discovery2", Config.DISCOVERY2_TIMES)
    daily("midnight", ["04:00"])
    daily(
        "daily",
        sorted(
            set(Config.DAILY_DISCOVERY_FIXED_TIMES)
            | {
                f"{Config.DAILY_DISCOVERY_AM_OPEN_HOUR:02d}:00",
                f"{Config.DAILY_DISCOVERY_PM_OPEN_HOUR:02d}:00",
            }
        ),
    )
    daily("fixtures", Config.ODDSPAPI_FIXTURE_DISCOVERY_TIMES)
    bind(clock.every(Config.DAILY_DISCOVERY_CHECK_INTERVAL_MINUTES).minutes, "daily")
    bind(clock.every(settings.reporting_poll_seconds).seconds, "reporting")
    bind(clock.every(3).days.at("05:00", Config.TIMEZONE), "league_cache")
    bind(clock.every(Config.ODDSPAPI_ACCOUNT_USAGE_REFRESH_HOURS).hours, "account_usage")
    for minute in range(0, 60, Config.POLL_INTERVAL_MINUTES):
        bind(clock.every().hour.at(f":{minute:02d}"), "pre_start", Priority.PRE_START)
    if Config.ENABLE_PRE_START_T_MINUS_ONE_JOB:
        for minute in range(0, 60, Config.PRE_START_T_MINUS_ONE_INTERVAL_MINUTES):
            bind(clock.every().hour.at(f":{minute:02d}"), "closing", Priority.CLOSING, True)
