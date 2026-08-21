"""Concurrent HTTP fan-out for OddsPapi exchange historical-odds requests.

Fetching opening/live exchange odds for one fixture can require one
`/historical-odds?outcomeId=...` call per selected outcome. Issued from a
single :class:`OddsPapiClient`, those calls are intentionally serialized by
that client's per-endpoint cooldown (see ``modules.oddspapi.client``).

This executor removes that serialization when more than one OddsPapi API key
is available by giving each worker its own HTTP session. Keys are leased for
each physical request, so workers do not own credentials and the scheduler
can balance both quota and endpoint cooldown availability.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Callable, Sequence

from modules.odds_ingestion.fetch_result import OddsFetchResult
from modules.oddspapi.client import OddsPapiClient
from modules.oddspapi.api_keys import parallel_worker_count
from modules.oddspapi.api_key_scheduler import OddsPapiApiKeyScheduler
from modules.oddspapi.runtime import get_oddspapi_key_scheduler

from .constants import ODDSPAPI_HISTORICAL_ODDS_ENDPOINT
from .exchange_outcome_selector import ExchangeHistoricalSelection
from .odds_fetcher import OddspapiOddsFetcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExchangeHistoricalFetchOutcome:
    """Result of fetching one exchange outcome's historical odds."""

    selection: ExchangeHistoricalSelection
    result: OddsFetchResult | None
    error: Exception | None = None


class OddspapiExchangeHistoricalFetchExecutor:
    """Fan out exchange historical-odds requests across the API key pool.

    Each worker owns one HTTP session, while every request gets a dynamic key
    lease. Worker count is ``min(max_workers, eligible keys, selections)``.
    """

    def __init__(
        self,
        *,
        api_keys: Sequence[str],
        max_workers: int | None = None,
        client_factory: Callable[..., OddsPapiClient] = OddsPapiClient,
        fetcher_factory: Callable[[OddsPapiClient], OddspapiOddsFetcher] = (
            lambda client: OddspapiOddsFetcher(client=client)
        ),
        key_scheduler: OddsPapiApiKeyScheduler | None = None,
    ) -> None:
        self._api_keys = [
            str(key).strip() for key in (api_keys or []) if str(key).strip()
        ]
        self._max_workers = max(1, int(max_workers)) if max_workers else None
        self._client_factory = client_factory
        self._fetcher_factory = fetcher_factory
        self._key_scheduler = key_scheduler

    def _scheduler(self) -> OddsPapiApiKeyScheduler:
        if self._key_scheduler is None:
            self._key_scheduler = get_oddspapi_key_scheduler()
        return self._key_scheduler

    def fetch_all(
        self,
        fixture_id: str,
        *,
        selections: list[ExchangeHistoricalSelection],
        source_sport_id: str | int | None,
        minimum_initial_span_minutes: float,
        require_active_quotes: bool = True,
        capture_raw_response: bool = False,
        as_of_targets: Sequence[tuple[int, datetime, datetime]] | None = None,
        current_cutoff_utc: datetime | None = None,
    ) -> list[ExchangeHistoricalFetchOutcome]:
        if not selections:
            return []

        eligible_key_count = min(
            len(self._api_keys),
            self._scheduler().available_key_count(
                ODDSPAPI_HISTORICAL_ODDS_ENDPOINT
            ),
        )
        worker_count = parallel_worker_count(
            max_workers=self._max_workers or len(selections),
            api_key_count=eligible_key_count,
            item_count=len(selections),
        )
        if worker_count <= 1 or len(self._api_keys) <= 1:
            return self._fetch_with_one_client(
                fixture_id,
                selections=selections,
                source_sport_id=source_sport_id,
                minimum_initial_span_minutes=minimum_initial_span_minutes,
                require_active_quotes=require_active_quotes,
                capture_raw_response=capture_raw_response,
                as_of_targets=as_of_targets,
                current_cutoff_utc=current_cutoff_utc,
            )
        return self._fetch_with_worker_pool(
            fixture_id,
            selections=selections,
            source_sport_id=source_sport_id,
            minimum_initial_span_minutes=minimum_initial_span_minutes,
            require_active_quotes=require_active_quotes,
            capture_raw_response=capture_raw_response,
            worker_count=worker_count,
            as_of_targets=as_of_targets,
            current_cutoff_utc=current_cutoff_utc,
        )

    def _fetch_one(
        self,
        fetcher: OddspapiOddsFetcher,
        fixture_id: str,
        selection: ExchangeHistoricalSelection,
        *,
        source_sport_id: str | int | None,
        minimum_initial_span_minutes: float,
        require_active_quotes: bool,
        capture_raw_response: bool = False,
        as_of_targets: Sequence[tuple[int, datetime, datetime]] | None = None,
        current_cutoff_utc: datetime | None = None,
    ) -> ExchangeHistoricalFetchOutcome:
        try:
            result = fetcher.fetch_odds(
                fixture_id,
                bookmakers=[selection.bookmaker_slug],
                endpoint=ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
                source_sport_id=source_sport_id,
                outcome_id=int(selection.source_outcome_id),
                minimum_initial_span_minutes=minimum_initial_span_minutes,
                require_active_quotes=require_active_quotes,
                capture_raw_response=capture_raw_response,
                as_of_targets=as_of_targets,
                current_cutoff_utc=current_cutoff_utc,
            )
            return ExchangeHistoricalFetchOutcome(selection=selection, result=result)
        except Exception as exc:  # noqa: BLE001 - surfaced for caller bookkeeping/logging
            return ExchangeHistoricalFetchOutcome(
                selection=selection, result=None, error=exc
            )

    def _fetch_with_one_client(
        self,
        fixture_id: str,
        *,
        selections: list[ExchangeHistoricalSelection],
        source_sport_id: str | int | None,
        minimum_initial_span_minutes: float,
        require_active_quotes: bool,
        capture_raw_response: bool = False,
        as_of_targets: Sequence[tuple[int, datetime, datetime]] | None = None,
        current_cutoff_utc: datetime | None = None,
    ) -> list[ExchangeHistoricalFetchOutcome]:
        client = self._client_factory(key_scheduler=self._scheduler())
        fetcher = self._fetcher_factory(client)
        try:
            return [
                self._fetch_one(
                    fetcher,
                    fixture_id,
                    selection,
                    source_sport_id=source_sport_id,
                    minimum_initial_span_minutes=minimum_initial_span_minutes,
                    require_active_quotes=require_active_quotes,
                    capture_raw_response=capture_raw_response,
                    as_of_targets=as_of_targets,
                    current_cutoff_utc=current_cutoff_utc,
                )
                for selection in selections
            ]
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def _fetch_with_worker_pool(
        self,
        fixture_id: str,
        *,
        selections: list[ExchangeHistoricalSelection],
        source_sport_id: str | int | None,
        minimum_initial_span_minutes: float,
        require_active_quotes: bool,
        worker_count: int,
        capture_raw_response: bool = False,
        as_of_targets: Sequence[tuple[int, datetime, datetime]] | None = None,
        current_cutoff_utc: datetime | None = None,
    ) -> list[ExchangeHistoricalFetchOutcome]:
        chunks = [selections[index::worker_count] for index in range(worker_count)]

        def run_worker(worker_index: int) -> list[ExchangeHistoricalFetchOutcome]:
            client = self._client_factory(key_scheduler=self._scheduler())
            fetcher = self._fetcher_factory(client)
            try:
                return [
                    self._fetch_one(
                        fetcher,
                        fixture_id,
                        selection,
                        source_sport_id=source_sport_id,
                        minimum_initial_span_minutes=minimum_initial_span_minutes,
                        require_active_quotes=require_active_quotes,
                        capture_raw_response=capture_raw_response,
                        as_of_targets=as_of_targets,
                        current_cutoff_utc=current_cutoff_utc,
                    )
                    for selection in chunks[worker_index]
                ]
            finally:
                close = getattr(client, "close", None)
                if callable(close):
                    close()

        outcomes_by_worker: list[list[ExchangeHistoricalFetchOutcome]] = [
            [] for _ in range(worker_count)
        ]
        logger.info(
            "Oddspapi exchange historical fan-out fixture_id=%s workers=%s selections=%s",
            fixture_id,
            worker_count,
            len(selections),
        )
        with ThreadPoolExecutor(
            max_workers=worker_count,
            thread_name_prefix="oddspapi-exchange-historical",
        ) as executor:
            future_to_index = {
                executor.submit(run_worker, index): index
                for index in range(worker_count)
            }
            for future in future_to_index:
                index = future_to_index[future]
                outcomes_by_worker[index] = future.result()

        outcomes: list[ExchangeHistoricalFetchOutcome] = []
        for chunk_outcomes in outcomes_by_worker:
            outcomes.extend(chunk_outcomes)
        return outcomes
