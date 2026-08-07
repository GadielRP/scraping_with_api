"""Concurrent HTTP fan-out for OddsPapi exchange historical-odds requests.

Fetching opening/live exchange odds for one fixture can require one
`/historical-odds?outcomeId=...` call per selected outcome. Issued from a
single :class:`OddsPapiClient`, those calls are intentionally serialized by
that client's per-endpoint cooldown (see ``modules.oddspapi.client``).

This executor removes that serialization when more than one OddsPapi API key
is available by giving each worker its own client, so each worker paces its
own requests against its own key's cooldown instead of all workers queueing
behind one client's lock. It never touches or bypasses the per-endpoint
cooldown itself; every worker still respects it independently.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import logging
from typing import Callable, Sequence

from modules.odds_ingestion.fetch_result import OddsFetchResult
from modules.oddspapi.client import OddsPapiClient

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
    """Fan out exchange historical-odds requests across a pool of API keys.

    Each worker owns one dedicated ``OddsPapiClient`` (and therefore its own
    cooldown state) so concurrency here can never make two requests race for
    the same key's cooldown slot.
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
    ) -> None:
        self._api_keys = [
            str(key).strip() for key in (api_keys or []) if str(key).strip()
        ]
        self._max_workers = max(1, int(max_workers)) if max_workers else None
        self._client_factory = client_factory
        self._fetcher_factory = fetcher_factory

    def fetch_all(
        self,
        fixture_id: str,
        *,
        selections: list[ExchangeHistoricalSelection],
        source_sport_id: str | int | None,
        minimum_initial_span_minutes: float,
    ) -> list[ExchangeHistoricalFetchOutcome]:
        if not selections:
            return []

        worker_count = min(
            len(self._api_keys) or 1,
            self._max_workers or len(selections),
            len(selections),
        )
        if worker_count <= 1 or len(self._api_keys) <= 1:
            return self._fetch_with_one_client(
                fixture_id,
                selections=selections,
                source_sport_id=source_sport_id,
                minimum_initial_span_minutes=minimum_initial_span_minutes,
            )
        return self._fetch_with_worker_pool(
            fixture_id,
            selections=selections,
            source_sport_id=source_sport_id,
            minimum_initial_span_minutes=minimum_initial_span_minutes,
            worker_count=worker_count,
        )

    def _fetch_one(
        self,
        fetcher: OddspapiOddsFetcher,
        fixture_id: str,
        selection: ExchangeHistoricalSelection,
        *,
        source_sport_id: str | int | None,
        minimum_initial_span_minutes: float,
    ) -> ExchangeHistoricalFetchOutcome:
        try:
            result = fetcher.fetch_odds(
                fixture_id,
                bookmakers=[selection.bookmaker_slug],
                endpoint=ODDSPAPI_HISTORICAL_ODDS_ENDPOINT,
                source_sport_id=source_sport_id,
                outcome_id=int(selection.source_outcome_id),
                minimum_initial_span_minutes=minimum_initial_span_minutes,
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
    ) -> list[ExchangeHistoricalFetchOutcome]:
        api_key = self._api_keys[0] if self._api_keys else None
        client = self._client_factory(api_key=api_key)
        fetcher = self._fetcher_factory(client)
        try:
            return [
                self._fetch_one(
                    fetcher,
                    fixture_id,
                    selection,
                    source_sport_id=source_sport_id,
                    minimum_initial_span_minutes=minimum_initial_span_minutes,
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
        worker_count: int,
    ) -> list[ExchangeHistoricalFetchOutcome]:
        chunks = [selections[index::worker_count] for index in range(worker_count)]

        def run_worker(worker_index: int) -> list[ExchangeHistoricalFetchOutcome]:
            client = self._client_factory(api_key=self._api_keys[worker_index])
            fetcher = self._fetcher_factory(client)
            try:
                return [
                    self._fetch_one(
                        fetcher,
                        fixture_id,
                        selection,
                        source_sport_id=source_sport_id,
                        minimum_initial_span_minutes=minimum_initial_span_minutes,
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
