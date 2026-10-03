# Daily Discovery

## Purpose and entrypoints

Daily Discovery fetches scheduled SofaScore events for the current local calendar date, persists eligible events, and optionally ingests available odds for upcoming events.

- [Scheduled entrypoint](../../modules/jobs/daily_discovery/run_daily_discovery.py): `run_daily_discovery_job()`.
- Retry entrypoint: `run_daily_discovery_retry_job()`, which delegates to the scheduled entrypoint.
- Direct entrypoint: `run_daily_discovery(sports=None, date_str=None, run_slot=None)`.
- [Extractor](../../modules/jobs/daily_discovery/extractor.py): `DailyDiscoveryExtractor.discover_events_for_date()`.
- [State repository](../../infrastructure/persistence/repositories/daily_discovery_repository.py): `DailyDiscoveryRepository`.
- [Scheduler](../../infrastructure/scheduler/job_scheduler.py): `JobScheduler`.

## Calendar and execution slots

The scheduled entrypoint resolves the target date and execution slot from a single clock reading in `Config.TIMEZONE`. Both daily passes request the same local calendar date. Neither slot carries over past local midnight.

AM and PM are persisted slot labels; their chronological order follows the configured opening hours.

| Slot | Opening setting | Default local window | Target date |
|---|---|---|---|
| PM | `DAILY_DISCOVERY_PM_OPEN_HOUR=8` | 08:00–16:59 | Current local date |
| AM | `DAILY_DISCOVERY_AM_OPEN_HOUR=17` | 17:00–23:59 | Current local date |

Before the earliest opening hour, the heartbeat skips discovery. Only the currently open slot is processed. Slots missed while the scheduler is stopped or blocked are not automatically recovered after their window closes.

The scheduler preserves `DAILY_DISCOVERY_FIXED_TIMES` and adds an opening-time trigger for each slot without a configured fixed trigger. The default fixed trigger is `17:10`; the scheduler also registers `08:00` for the morning slot.

`DAILY_DISCOVERY_CHECK_INTERVAL_MINUTES` controls periodic heartbeats. Its fallback is `DAILY_DISCOVERY_RETRY_INTERVAL_MINUTES`, whose default is 240 minutes. Heartbeats retry unfinished sports in the active slot. A completed sport is not repeated within that slot. The two daily passes are independent; failed passes may require additional attempts.

## Discovery scope

`SUPPORTED_SPORTS` determines eligible sports. The sport catalog translates canonical sport IDs into SofaScore routes, removes duplicate routes, and excludes sports without a provider route.

`DISCOVERY_TRACKED_COMPETITIONS_ONLY` defaults to `true`. When enabled, canonical IDs from [tracked_competitions.py](../../modules/competition/tracked_competitions.py) select competition rows. The job compares their SofaScore `source_tournament_id` and `source_unique_tournament_id` with provider payload IDs.

Tournament selection happens before requesting tournament events. Returned events are filtered again before normalization and persistence. An enabled filter with no mapped tracked competitions stops extraction without completing pending sports.

When the competition filter is disabled, all competitions remain eligible within the supported sports. Tracked competition IDs are still loaded to measure which payloads the filter would reject. Supported-sport filtering remains enforced. Odds parsing retains only eligible event IDs.

## Persistent state and retries

[DailyDiscoveryLog](../../infrastructure/persistence/models.py) has a unique key on `(date, run_slot, sport)`. `date` is the requested calendar date; `created_at` and `last_attempt_at` are UTC instants.

Each scheduled run performs these steps:

1. Delete state rows older than the local-date retention cutoff configured by `DAILY_DISCOVERY_DAYS_TO_KEEP`, defaulting to one day. A negative value disables cleanup.
2. Initialize missing rows for supported sports in the active date and slot with status `pending`. If initialization fails, stop the run.
3. Select unfinished sports. Completed rows with no attempt timestamp or with an attempt before the target local day are also eligible for processing.
4. Extract each pending sport and record `completed` or `failed`. Status updates increment `attempts` and set `last_attempt_at`.

A missing pagination response, failed tournament fetch, missing event ID, or failed persistence leaves the sport retryable. A valid empty response or a scope with no eligible events can complete the sport. Missing optional odds alone does not fail event discovery.

State is tracked at sport granularity: retrying a partially successful sport repeats extraction for that sport and upserts eligible events again. The repository does not atomically claim runs across multiple scheduler processes.

## Extraction and persistence

For each pending sport, the extractor executes the following sequence:

1. **Paginate scheduled tournaments.** Call `get_today_sport_events_response(date, sport, page)` starting at page 1. Inspect `timezoneEventCount`, apply the competition scope, and collect distinct unique-tournament IDs. Continue while `hasNextPage` is true. A missing page leaves the sport failed.
2. **Fetch tournament events.** Call `get_unique_tournament_scheduled_events(tournament_id, date)` for selected tournaments. Filter unsupported sports and out-of-scope competitions. Tournament failures leave the sport retryable while other tournament responses can still be persisted.
3. **Fetch optional odds.** If eligible events remain, call `get_today_sport_events_odds_response(date, sport)`. Parse fractional values into decimal odds keyed by eligible event ID. Continue without odds when the feed is unavailable.
4. **Select events eligible for odds.** `filter_upcoming_events(..., min_minutes_away=10)` uses aware timestamps to require at least ten minutes before kickoff. This threshold controls odds ingestion; past and imminent eligible events can still be persisted without odds.
5. **Persist events and optional odds.** `persist_event_and_optional_odds()` rechecks sport and competition scope, calls `normalize_event_payload(..., discovery_source="daily_discovery")`, and upserts the event through `EventRepository`. Available odds for eligible upcoming events are saved through `MarketOddsIngestionService.save_from_sofascore_response()`.
6. **Record sport status.** Mark the sport completed only if its event extraction and persistence had no recorded failures. Continue to the next sport after recording a failure.

The extractor checks `is_shutdown_requested()` between sports and inside event persistence loops. Each repository operation manages its own transaction; there is no transaction spanning the entire discovery pass.

## Direct execution and return values

`run_daily_discovery()` accepts an optional sport scope, explicit date string, and slot. Without a date, it uses the current date in `Config.TIMEZONE`. Explicit dates are forwarded to the provider. A missing or invalid extractor slot defaults to AM.

Direct execution invokes the extractor without the scheduled entrypoint's cleanup, state initialization, or pending-sport selection. Status updates require an existing state row.

The extractor returns `events_processed`, `events_persisted`, `events_inserted`, `events_updated`, `events_discarded`, `events_failed`, and `odds_inserted`. Persistence counters measure successful write operations; inserted and updated distinguish new rows from existing rows. Callers must inspect per-sport state to determine completeness; a returned statistics dictionary alone does not establish success for every sport.

## Scheduler follow-up and observability

Scheduled Daily Discovery runs on the bounded maintenance worker, outside the pre-start scheduler thread. After committed event writes, `JobScheduler.job_daily_discovery()` invokes `job_refresh_reporting_views()`. Skipped heartbeats still perform discard cleanup but normally skip refresh. A pending failed refresh is retried on a later heartbeat, and the first heartbeat after process startup refreshes once to recover interrupted work. A successful midnight refresh clears the same pending flag. Direct CLI calls remain synchronous.

Operational logging records the local clock reading, timezone, target date, slot, selected sport scope, filter mode, tournament and event filtering counts, persistence totals, and sport status. These fields describe each run; persistent completion and retry decisions use `DailyDiscoveryLog`.

At the end of extraction, the [discovery persistence calendar summary](discovery-persistence-logging.md) logs committed canonical events grouped by their local start date and persisted sport, with daily totals and a unique total for the run. The requested discovery date and slot are logged separately from the event start date.
