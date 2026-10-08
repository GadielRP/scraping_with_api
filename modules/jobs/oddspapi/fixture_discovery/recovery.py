from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from infrastructure.persistence.repositories import (
    DailyDiscoveryRepository,
    OddspapiFixtureDiscoveryRunRepository,
)
from infrastructure.settings import Config
from infrastructure.settings import discovery as settings
from modules.jobs.discovery.filters import oddspapi_discovery_sport_ids
from modules.sports.catalog import sofascore_sport_slugs
from modules.jobs.oddspapi.fixture_discovery.run_fixture_discovery import run_fixture_discovery_job
from modules.oddspapi.runtime import (
    oddspapi_account_usage_refresh_enabled,
    refresh_oddspapi_account_usage_if_due,
)
from shared.runtime_observability import observe_operation
from shared.execution_context import WorkDeferred
from shared.temporal import UTC, now_in_timezone

logger = logging.getLogger(__name__)


class FixtureDiscoveryService:
    def run(self, **kwargs):
        trigger = kwargs.pop("_trigger", "scheduled")
        scheduled_local_date = kwargs.pop("_scheduled_local_date", None)
        scheduled_time = kwargs.pop("_scheduled_time", None)
        create_mappings = bool(kwargs.get("create_mappings", True))
        local_now = now_in_timezone(Config.TIMEZONE)
        scheduled_local_date = scheduled_local_date or local_now.strftime("%Y-%m-%d")
        scheduled_time = scheduled_time or local_now.strftime("%H:%M")
        sport_scope = OddspapiFixtureDiscoveryRunRepository.normalize_sport_scope(
            kwargs.get("sports") if kwargs.get("sports") is not None else oddspapi_discovery_sport_ids()
        )

        # Anchor manual/recovery calls to the latest configured occurrence too.
        if kwargs.get("target_date") is None:
            kwargs["target_date"] = self.target_date_for_slot(local_now)

        target_date_str = kwargs.get("target_date")
        tracked_run = False
        if create_mappings:
            try:
                tracked_run = OddspapiFixtureDiscoveryRunRepository.begin(
                    target_date_str,
                    trigger=trigger,
                    sport_scope=sport_scope,
                    create_mappings=create_mappings,
                    scheduled_local_date=scheduled_local_date,
                    scheduled_time=scheduled_time,
                    discovery_completed_at=(
                        DailyDiscoveryRepository.latest_completed_at(
                            target_date_str,
                            sofascore_sport_slugs(
                                kwargs.get("sports") if kwargs.get("sports") is not None
                                else oddspapi_discovery_sport_ids()
                            ),
                        )
                        if settings.ODDSPAPI.reconciliation_enabled else None
                    ),
                )
                if not tracked_run:
                    logger.info(
                        "Skipping Oddspapi fixture discovery for UTC day %s "
                        "sport_scope=%s: "
                        "a successful or currently running durable run already exists",
                        target_date_str,
                        sport_scope,
                    )
                    return None
            except Exception as exc:
                # Discovery is more important than observability. Run fail-open if
                # the marker cannot be written, while making the durability loss loud.
                logger.exception(
                    "Could not claim durable Oddspapi fixture-discovery run for %s; "
                    "continuing without a marker: %s",
                    target_date_str,
                    exc,
                )
        else:
            logger.info(
                "Running Oddspapi fixture discovery for UTC day %s without a "
                "durable success marker because create_mappings=False",
                target_date_str,
            )

        self.refresh_usage()
        logger.info(
            "Starting Oddspapi fixture discovery for UTC day: %s sport_scope=%s trigger=%s "
            "scheduled_local_date=%s scheduled_time=%s",
            target_date_str,
            sport_scope,
            trigger,
            scheduled_local_date,
            scheduled_time,
        )
        try:
            with observe_operation(
                f"oddspapi_fixture_discovery:{target_date_str}:{sport_scope}:{trigger}"
            ):
                summary = run_fixture_discovery_job(**kwargs)
            errors = sum(sport.errors for sport in summary.sports)
            logger.info(
                "Oddspapi fixture discovery completed fixtures=%s mappings_created=%s errors=%s",
                summary.total_fixtures_fetched,
                summary.total_mappings_created,
                errors,
            )
            if tracked_run and errors == 0:
                summary_payload = json.loads(
                    json.dumps(
                        summary.to_dict(),
                        default=lambda value: value.isoformat(),
                    )
                )
                OddspapiFixtureDiscoveryRunRepository.finish_success(
                    target_date_str,
                    summary_payload,
                    sport_scope=sport_scope,
                )
            elif tracked_run:
                OddspapiFixtureDiscoveryRunRepository.finish_failed(
                    target_date_str,
                    f"Discovery completed with {errors} sport error(s)",
                    sport_scope=sport_scope,
                )
                logger.warning(
                    "Oddspapi fixture discovery target %s was not marked successful "
                    "because %s sport error(s) were reported",
                    target_date_str,
                    errors,
                )
                self._send_fixture_discovery_ops_alert(
                    target_date=target_date_str,
                    trigger=trigger,
                    detail=f"completed with {errors} sport error(s)",
                )
            return summary
        except WorkDeferred:
            if tracked_run:
                OddspapiFixtureDiscoveryRunRepository.finish_failed(
                    target_date_str, "Deferred by maintenance executor", sport_scope=sport_scope,
                )
            raise
        except Exception as exc:
            if tracked_run:
                try:
                    OddspapiFixtureDiscoveryRunRepository.finish_failed(
                        target_date_str,
                        repr(exc),
                        sport_scope=sport_scope,
                    )
                except Exception:
                    logger.exception(
                        "Could not mark failed Oddspapi fixture-discovery run for %s",
                        target_date_str,
                    )
            self._send_fixture_discovery_ops_alert(
                target_date=target_date_str,
                trigger=trigger,
                detail=f"failed: {type(exc).__name__}: {exc}",
            )
            logger.exception(f"Error in Oddspapi fixture discovery: {exc}")
            raise

    def refresh_usage(self):
        """Refresh key quotas only when the durable snapshot is due."""
        if not oddspapi_account_usage_refresh_enabled():
            return False
        try:
            return refresh_oddspapi_account_usage_if_due()
        except Exception:
            # Quota observation must never stop odds ingestion. The scheduler
            # continues from its persisted estimate and retries next time.
            logger.exception("Oddspapi account usage preflight failed")
            return False

    @staticmethod
    def _send_fixture_discovery_ops_alert(
        *,
        target_date: str,
        trigger: str,
        detail: str,
    ) -> None:
        """Best-effort alert using the already configured Telegram transport."""
        try:
            from modules.alerts import pre_start_notifier

            message = (
                "🚨 Oddspapi fixture discovery requires attention\n"
                f"UTC target: {target_date}\n"
                f"Trigger: {trigger}\n"
                f"Detail: {detail}"
            )
            if not pre_start_notifier.send_telegram_message(message):
                logger.warning(
                    "Oddspapi fixture-discovery ops alert was not delivered "
                    "target_date=%s trigger=%s",
                    target_date,
                    trigger,
                )
        except Exception:
            logger.exception(
                "Could not send Oddspapi fixture-discovery ops alert " "target_date=%s trigger=%s",
                target_date,
                trigger,
            )

    @staticmethod
    def target_date_for_slot(slot_local: datetime) -> str:
        """Use the last configured occurrence, even for a late manual retry."""
        if slot_local.tzinfo is None or slot_local.utcoffset() is None:
            slot_local = slot_local.replace(tzinfo=ZoneInfo(Config.TIMEZONE))
        slot_local = slot_local.astimezone(ZoneInfo(Config.TIMEZONE))
        occurrences = [
            datetime.combine(
                slot_local.date() - timedelta(days=days_ago),
                datetime.strptime(configured_time, "%H:%M").time(),
                tzinfo=slot_local.tzinfo,
            )
            for days_ago in (1, 0)
            for configured_time in settings.ODDSPAPI.scheduled_times
        ]
        slot_utc = max(occurrence for occurrence in occurrences if occurrence <= slot_local).astimezone(UTC)
        target = slot_utc + timedelta(days=1) if slot_utc.hour >= 12 else slot_utc
        return target.strftime("%Y-%m-%d")

    def _missed_fixture_discovery_slots(
        self,
        *,
        now_local: datetime | None = None,
    ) -> list[tuple[datetime, str, str]]:
        now_local = now_local or now_in_timezone(Config.TIMEZONE)
        if now_local.tzinfo is None or now_local.utcoffset() is None:
            now_local = now_local.replace(tzinfo=ZoneInfo(Config.TIMEZONE))
        lookback_hours = max(
            0,
            settings.ODDSPAPI.catchup_lookback_hours,
        )
        cutoff = now_local - timedelta(hours=lookback_hours)
        day_count = lookback_hours // 24 + 2
        slots: list[tuple[datetime, str, str]] = []
        seen_targets: set[str] = set()

        for days_ago in range(day_count, -1, -1):
            local_date = (now_local - timedelta(days=days_ago)).date()
            for configured_time in settings.ODDSPAPI.scheduled_times:
                try:
                    slot_time = datetime.strptime(configured_time, "%H:%M").time()
                except ValueError:
                    logger.error(
                        "Ignoring invalid ODDSPAPI.scheduled_times value: %s",
                        configured_time,
                    )
                    continue
                occurrence = datetime.combine(
                    local_date,
                    slot_time,
                    tzinfo=ZoneInfo(Config.TIMEZONE),
                )
                if occurrence < cutoff or occurrence > now_local:
                    continue
                target_date = self.target_date_for_slot(occurrence)
                if target_date < now_local.astimezone(UTC).date().isoformat():
                    continue
                if target_date in seen_targets:
                    continue
                seen_targets.add(target_date)
                slots.append((occurrence, configured_time, target_date))

        slots.sort(key=lambda item: item[0])
        max_runs = max(0, settings.ODDSPAPI.max_catchup_runs)
        return slots[-max_runs:] if max_runs else []

    def recover_interrupted(self) -> None:
        try:
            interrupted = OddspapiFixtureDiscoveryRunRepository.mark_running_as_interrupted()
            if interrupted:
                logger.critical(
                    "Recovered %s Oddspapi fixture-discovery run marker(s) left "
                    "running by an unclean process exit",
                    interrupted,
                )
        except Exception:
            logger.exception("Could not mark interrupted Oddspapi fixture-discovery runs")

    def retry_due(self) -> None:
        """Retry due occurrences after SofaScore, including newly completed passes."""
        for occurrence, configured_time, target_date in self._missed_fixture_discovery_slots():
            try:
                self.run(
                    target_date=target_date,
                    _trigger="catch_up",
                    _scheduled_local_date=occurrence.strftime("%Y-%m-%d"),
                    _scheduled_time=configured_time,
                )
            except WorkDeferred:
                raise
            except Exception:
                logger.exception(
                    "Catch-up Oddspapi fixture discovery failed for target UTC day %s",
                    target_date,
                )
