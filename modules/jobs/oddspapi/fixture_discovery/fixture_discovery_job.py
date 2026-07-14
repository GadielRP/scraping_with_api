"""Orchestration for discovering and mapping Oddspapi fixtures."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import logging
from time import monotonic

from infrastructure.persistence.database import db_manager
from modules.oddspapi.client import OddsPapiClient, OddsPapiError

from .constants import (
    DEFAULT_HAS_ODDS,
    DEFAULT_LANGUAGE,
    DEFAULT_MAX_REQUEST_WINDOW_HOURS,
    DEFAULT_PERSIST_QUEUE,
    DEFAULT_STATUS_ID,
    DISCOVERY_SPORT_IDS,
)
from .fixture_batch_processor import OddspapiFixtureBatchProcessor
from .response_utils import extract_fixture_list, split_time_window, to_oddspapi_iso

logger = logging.getLogger(__name__)


@dataclass
class SportFixtureDiscoverySummary:
    sport_slug: str
    sport_id: int
    requested_from: str
    requested_to: str
    fixtures_fetched: int = 0
    fixtures_valid: int = 0
    fixtures_deduplicated: int = 0
    invalid_payloads: int = 0
    resolved_existing_oddspapi: int = 0
    resolved_external_sofascore: int = 0
    resolved_candidate_match: int = 0
    mappings_created: int = 0
    unresolved_no_candidates: int = 0
    needs_review: int = 0
    queue_rows_written: int = 0
    errors: int = 0
    duration_seconds: float = 0.0


@dataclass
class OddspapiFixtureDiscoverySummary:
    started_at: datetime
    finished_at: datetime | None
    dry_run: bool
    create_mappings: bool
    persist_queue: bool
    sports: list[SportFixtureDiscoverySummary] = field(default_factory=list)

    @property
    def total_fixtures_fetched(self) -> int:
        return sum(item.fixtures_fetched for item in self.sports)

    @property
    def total_mappings_created(self) -> int:
        return sum(item.mappings_created for item in self.sports)

    @property
    def total_resolved_existing_oddspapi(self) -> int:
        return sum(item.resolved_existing_oddspapi for item in self.sports)

    @property
    def total_resolved_external_sofascore(self) -> int:
        return sum(item.resolved_external_sofascore for item in self.sports)

    @property
    def total_resolved_candidate_match(self) -> int:
        return sum(item.resolved_candidate_match for item in self.sports)

    @property
    def total_needs_review(self) -> int:
        return sum(item.needs_review for item in self.sports)

    @property
    def total_unresolved_no_candidates(self) -> int:
        return sum(item.unresolved_no_candidates for item in self.sports)

    def to_dict(self) -> dict:
        result = asdict(self)
        result.update(
            total_fixtures_fetched=self.total_fixtures_fetched,
            total_mappings_created=self.total_mappings_created,
            resolved_existing_oddspapi=self.total_resolved_existing_oddspapi,
            resolved_external_sofascore=self.total_resolved_external_sofascore,
            resolved_candidate_match=self.total_resolved_candidate_match,
            needs_review=self.total_needs_review,
            unresolved_no_candidates=self.total_unresolved_no_candidates,
        )
        return result


class OddspapiFixtureDiscoveryJob:
    def __init__(
        self,
        client: OddsPapiClient | None = None,
        sports: dict[str, int] | None = None,
        create_mappings: bool = True,
        persist_queue: bool = DEFAULT_PERSIST_QUEUE,
        status_id: int = DEFAULT_STATUS_ID,
        max_fixtures_per_sport: int | None = None,
        batch_processor: OddspapiFixtureBatchProcessor | None = None,
    ) -> None:
        self.client = client or OddsPapiClient()
        self.sports = dict(sports or DISCOVERY_SPORT_IDS)
        self.create_mappings = create_mappings
        self.persist_queue = persist_queue
        self.status_id = int(status_id)
        if max_fixtures_per_sport is not None and max_fixtures_per_sport <= 0:
            raise ValueError("max_fixtures_per_sport must be positive")
        self.max_fixtures_per_sport = max_fixtures_per_sport
        self.batch_processor = batch_processor or OddspapiFixtureBatchProcessor()

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
        message = str(error)
        return (
            "status_code=404" in message
            and "FIXTURE_NOT_FOUND" in message
        )

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
        chunks = split_time_window(from_date, to_date, DEFAULT_MAX_REQUEST_WINDOW_HOURS)

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

            seen_fixture_ids: set[str] = set()
            processed_count = 0
            try:
                for chunk_from, chunk_to in chunks:
                    if (
                        self.max_fixtures_per_sport is not None
                        and processed_count >= self.max_fixtures_per_sport
                    ):
                        break
                    try:
                        payload = self.client.get_fixtures(
                            sport_id=sport_id,
                            from_date=to_oddspapi_iso(chunk_from),
                            to_date=to_oddspapi_iso(chunk_to),
                            status_id=self.status_id,
                            language=DEFAULT_LANGUAGE,
                            has_odds=DEFAULT_HAS_ODDS,
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
                        payload = []
                    fixtures = extract_fixture_list(payload)
                    sport_summary.fixtures_fetched += len(fixtures)
                    logger.info(
                        "Oddspapi fixtures fetched sport=%s count=%s",
                        sport_slug,
                        len(fixtures),
                    )

                    unique_fixtures: list[dict] = []
                    for fixture in fixtures:
                        fixture_id = str(fixture.get("fixtureId") or "").strip()
                        if fixture_id and fixture_id in seen_fixture_ids:
                            sport_summary.fixtures_deduplicated += 1
                            continue
                        if fixture_id:
                            seen_fixture_ids.add(fixture_id)
                        unique_fixtures.append(fixture)

                    if self.max_fixtures_per_sport is not None:
                        remaining = max(self.max_fixtures_per_sport - processed_count, 0)
                        unique_fixtures = unique_fixtures[:remaining]
                    if not unique_fixtures:
                        continue

                    with db_manager.get_session() as session:
                        batch_result = self.batch_processor.process_batch(
                            fixture_payloads=unique_fixtures,
                            create_mappings=self.create_mappings,
                            persist_queue=self.persist_queue,
                            session=session,
                        )
                    processed_count += len(unique_fixtures)
                    sport_summary.fixtures_valid += batch_result.fixtures_valid
                    sport_summary.fixtures_deduplicated += batch_result.fixtures_deduplicated
                    sport_summary.invalid_payloads += batch_result.invalid_payloads
                    sport_summary.resolved_existing_oddspapi += batch_result.resolved_existing_oddspapi
                    sport_summary.resolved_external_sofascore += batch_result.resolved_external_sofascore
                    sport_summary.resolved_candidate_match += batch_result.resolved_candidate_match
                    sport_summary.mappings_created += batch_result.mappings_created
                    sport_summary.unresolved_no_candidates += batch_result.unresolved_no_candidates
                    sport_summary.needs_review += batch_result.needs_review
                    sport_summary.queue_rows_written += batch_result.queue_rows_written

                logger.info(
                    "Oddspapi fixture batch processed sport=%s resolved_existing=%s resolved_sofascore=%s resolved_candidate=%s unresolved=%s mappings_created=%s",
                    sport_slug,
                    sport_summary.resolved_existing_oddspapi,
                    sport_summary.resolved_external_sofascore,
                    sport_summary.resolved_candidate_match,
                    sport_summary.unresolved_no_candidates + sport_summary.needs_review,
                    sport_summary.mappings_created,
                )
            except Exception:
                sport_summary.errors += 1
                logger.exception("Oddspapi fixture discovery failed sport=%s", sport_slug)
            finally:
                sport_summary.duration_seconds = round(monotonic() - sport_started, 3)

        summary.finished_at = datetime.now(timezone.utc)
        return summary
