"""Orchestration for discovering and mapping Oddspapi fixtures."""

from __future__ import annotations

import logging
from contextlib import closing
from datetime import datetime, timezone
from time import monotonic

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.transient.discovery_run_store import DiscoveryRunStore
from infrastructure.settings import discovery as settings
from modules.jobs.discovery.filters import oddspapi_discovery_sport_ids, oddspapi_fixture_filter_reason
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.exceptions import OddsPapiError, OddsPapiHttpError
from shared.batching import chunks
from shared.execution_context import WorkDeferred, check_execution_budget

from .fixture_batch_processor import OddspapiFixtureBatchProcessor
from .response_utils import split_time_window, to_oddspapi_iso
from .summary import (
    OddspapiFixtureDiscoverySummary,
    SportFixtureDiscoverySummary, format_batch_metrics,
)

logger = logging.getLogger(__name__)


class OddspapiFixtureDiscoveryJob:
    def __init__(
        self,
        client: OddsPapiClient | None = None,
        sports: dict[str, int] | None = None,
        create_mappings: bool = True,
        persist_queue: bool = settings.ODDSPAPI.persist_queue,
        status_id: int = settings.ODDSPAPI.status_id,
        max_fixtures_per_sport: int | None = None,
        chunk_size: int = settings.ODDSPAPI.persistence_chunk_size,
        batch_processor: OddspapiFixtureBatchProcessor | None = None,
    ) -> None:
        self.client = client
        allowed_sports = oddspapi_discovery_sport_ids()
        requested_sports = allowed_sports if sports is None else sports
        self.sports = {
            sport_slug: sport_id
            for sport_slug, sport_id in requested_sports.items()
            if allowed_sports.get(sport_slug) == sport_id
        }
        self.create_mappings = create_mappings
        self.persist_queue = persist_queue
        self.status_id = int(status_id)
        if max_fixtures_per_sport is not None and max_fixtures_per_sport <= 0:
            raise ValueError("max_fixtures_per_sport must be positive")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.max_fixtures_per_sport = max_fixtures_per_sport
        self.chunk_size = int(chunk_size)
        self.batch_processor = batch_processor or OddspapiFixtureBatchProcessor(
            chunk_size=chunk_size,
        )

    @staticmethod
    def _validate_window(from_date: datetime, to_date: datetime) -> tuple[datetime, datetime]:
        if not isinstance(from_date, datetime) or not isinstance(to_date, datetime):
            raise TypeError("from_date and to_date must be datetime values")
        if from_date.tzinfo is None:
            from_date = from_date.replace(tzinfo=timezone.utc)
        if to_date.tzinfo is None:
            to_date = to_date.replace(tzinfo=timezone.utc)
        from_date = from_date.astimezone(timezone.utc)
        to_date = to_date.astimezone(timezone.utc)
        if to_date <= from_date:
            raise ValueError("to_date must be after from_date")
        return from_date, to_date

    @staticmethod
    def _is_fixture_not_found_error(error: OddsPapiError) -> bool:
        return (
            isinstance(error, OddsPapiHttpError)
            and error.status_code == 404
            and error.error_code == "FIXTURE_NOT_FOUND"
        )

    @staticmethod
    def _select_fixtures(fixtures, store, summary, remaining):
        selected = 0
        for fixture in fixtures:
            summary.fixtures_fetched += 1
            reason = oddspapi_fixture_filter_reason(fixture)
            if reason:
                summary.fixtures_skipped_policy += 1
                summary.rejected_by_reason[reason] = summary.rejected_by_reason.get(reason, 0) + 1
                continue
            fixture_id = str(fixture.get("fixtureId") or "").strip()
            if fixture_id and not store.first_seen(f"oddspapi:{summary.sport_id}", fixture_id):
                summary.fixtures_deduplicated += 1
                continue
            if remaining is None or selected < remaining:
                selected += 1
                yield fixture
            # Drain the current document even after a processing cap so a
            # truncated response cannot be reported as a successful run.

    def run(self, from_date: datetime, to_date: datetime) -> OddspapiFixtureDiscoverySummary:
        from_date, to_date = self._validate_window(from_date, to_date)
        started_at = datetime.now(timezone.utc)
        summary = OddspapiFixtureDiscoverySummary(
            started_at=started_at,
            finished_at=None,
            dry_run=not self.create_mappings,
            create_mappings=self.create_mappings,
            persist_queue=self.persist_queue,
        )
        request_windows = split_time_window(from_date, to_date, settings.ODDSPAPI.max_request_window_hours)
        runtime_client = self.client or OddsPapiClient()
        owns_runtime_client = self.client is None
        run_store = DiscoveryRunStore()

        try:
            for sport_slug, sport_id in self.sports.items():
                sport_started = monotonic()
                sport_summary = SportFixtureDiscoverySummary(
                    sport_slug=sport_slug,
                    sport_id=int(sport_id),
                    requested_from=to_oddspapi_iso(from_date),
                    requested_to=to_oddspapi_iso(to_date),
                )
                summary.sports.append(sport_summary)
                logger.info(
                    "Oddspapi fixture discovery started sport=%s sport_id=%s from=%s to=%s",
                    sport_slug,
                    sport_id,
                    sport_summary.requested_from,
                    sport_summary.requested_to,
                )

                processed_count = 0
                try:
                    for chunk_from, chunk_to in request_windows:
                        if (
                            self.max_fixtures_per_sport is not None
                            and processed_count >= self.max_fixtures_per_sport
                        ):
                            break
                        remaining = (
                            None if self.max_fixtures_per_sport is None
                            else self.max_fixtures_per_sport - processed_count
                        )
                        try:
                            with closing(runtime_client.iter_fixtures(
                                sport_id=sport_id,
                                from_date=to_oddspapi_iso(chunk_from),
                                to_date=to_oddspapi_iso(chunk_to),
                                status_id=self.status_id,
                                language=settings.ODDSPAPI.language,
                                has_odds=settings.ODDSPAPI.request_has_odds,
                            )) as fixtures:
                                selected = self._select_fixtures(
                                    fixtures, run_store, sport_summary, remaining
                                )
                                for batch in chunks(selected, self.chunk_size):
                                    check_execution_budget()
                                    with db_manager.get_session() as session:
                                        batch_result = self.batch_processor.process_batch(
                                            fixture_payloads=batch,
                                            create_mappings=self.create_mappings,
                                            persist_queue=self.persist_queue,
                                            session=session,
                                        )
                                    processed_count += len(batch)
                                    sport_summary.add_batch(batch_result)
                                    logger.info(
                                        "Oddspapi fixture batch sport=%s fixtures=%s mappings_created=%s %s",
                                        sport_slug, len(batch), batch_result.mappings_created,
                                        format_batch_metrics(batch_result),
                                    )
                        except OddsPapiError as exc:
                            if not self._is_fixture_not_found_error(exc):
                                raise
                            logger.info(
                                "No Oddspapi fixtures found sport=%s from=%s to=%s",
                                sport_slug,
                                to_oddspapi_iso(chunk_from),
                                to_oddspapi_iso(chunk_to),
                            )
                except WorkDeferred:
                    raise
                except Exception:
                    sport_summary.errors += 1
                    logger.exception("Oddspapi fixture discovery failed sport=%s", sport_slug)
                finally:
                    sport_summary.duration_seconds = round(monotonic() - sport_started, 3)
                    logger.info(
                        "Discovery admission source=oddspapi sport=%s rejected=%s",
                        sport_slug, sport_summary.rejected_by_reason,
                    )
                    logger.info(
                        "👨 Oddspapi fixture batch processed sport=%s resolved_existing=%s resolved_sofascore=%s "
                        "resolved_candidate=%s skipped_untracked=%s unresolved=%s "
                        "mappings_created=%s queue_rows=%s duration_s=%s",
                        sport_slug,
                        sport_summary.resolved_existing_oddspapi,
                        sport_summary.resolved_external_sofascore,
                        sport_summary.resolved_candidate_match,
                        sport_summary.fixtures_skipped_untracked_competition,
                        sport_summary.unresolved_no_candidates + sport_summary.needs_review,
                        sport_summary.mappings_created,
                        sport_summary.queue_rows_written,
                        sport_summary.duration_seconds,
                    )

        finally:
            run_store.close()
            if owns_runtime_client:
                close = getattr(runtime_client, "close", None)
                if callable(close):
                    close()

        summary.finished_at = datetime.now(timezone.utc)
        return summary
