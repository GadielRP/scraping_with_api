# Daily Discovery

## Calendar and progress

`run_daily_discovery_job(target_date=None, run_slot=None)` is the heartbeat used by the CLI and scheduler. The scheduler binds its original local date and slot before admission. Delay cannot silently turn an old request into another calendar day. Slots are named for the UTC date they query: `current_utc_day` opens at 08:00 and `next_utc_day` at 17:00 in `Config.TIMEZONE` by default. The schedule thresholds are local wall-clock times; the names describe the provider date, not the local execution time.

### Provider date alignment

The target is a UTC calendar date, not simply the local date: the evening `next_utc_day` occurrence targets the next UTC date, while the morning `current_utc_day` occurrence targets the current UTC date. The scheduler derives that date from the original scheduled occurrence, so queue delays do not move a run to a different provider day. Existing `DAILY_DISCOVERY_AM_OPEN_HOUR` and `DAILY_DISCOVERY_PM_OPEN_HOUR` environment variables remain accepted as aliases for the new `DAILY_DISCOVERY_NEXT_UTC_DAY_OPEN_HOUR` and `DAILY_DISCOVERY_CURRENT_UTC_DAY_OPEN_HOUR` names.

This alignment is required by the scheduled Oddspapi Fixture Discovery run. That job fetches fixtures for a UTC date and maps each fixture to a canonical event that must already exist, either through its SofaScore source ID or through candidate matching against persisted events. Daily Discovery's evening `next_utc_day` pass therefore needs to persist the SofaScore events for the same UTC date before Fixture Discovery runs. If it queries the prior local date instead, the fixture can arrive while its canonical event is absent; fuzzy matching may then reject the wrong or low-confidence candidate, leaving no Oddspapi mapping for pre-start ingestion. This is why both jobs share a UTC target date and why the daily pass runs first.

For example, on 2026-10-04 at 17:10 in Mexico City, it is 23:10 UTC. The `next_utc_day` pass targets UTC date 2026-10-05; Fixture Discovery at 17:47 local (23:47 UTC) also targets 2026-10-05. The next morning's 08:00 local `current_utc_day` pass is 14:00 UTC on 2026-10-05, so it targets that same UTC date. If SofaScore only publishes an event after the evening fixture pass has completed, date alignment alone cannot link it; that case requires another fixture reconciliation for that date.

`DailyDiscoveryLog.run_slot` uses these semantic names as durable retry keys. Migration `20261005_01` converts historical `AM` rows to `next_utc_day` and `PM` rows to `current_utc_day`, preserving the progress records and their meanings.

Discard-memory cleanup runs first on **every heartbeat**, even when no slot is open or every sport is already completed. Its existing enabled toggle and three-day retention remain in `modules/events/discards/settings.py` / Config. There is no separate timed cleanup job.

The heartbeat cleans old progress rows, initializes missing `(date, run_slot, sport)` rows, reads unfinished sports and coordinates discovery. A completed sport is not replayed. A truncated response, failed write or failed odds response leaves that sport retryable. Valid empty collections can complete it. PostgreSQL/session errors propagate; they are not interpreted as an empty run. Progress is per sport, not per event, and does not provide a distributed lease for multiple application instances.

## Bounded ingestion

`event_source.py` yields eligible raw events from scheduled-tournament pages and tournament responses. Supported-sport and mapped-competition policies remain unchanged. Discovery still accepts past events as well as upcoming events.

`modules/sofascore/streaming.py` reuses the authenticated HTTP client and writes the response to a disk-backed temporary file through libcurl's callback. It parses one JSON member at a time, validates the complete document and bounds response bytes, individual elements/tokens and nesting depth, including ignored fields. It never retains a full sport payload in `Response.content`, `all_events` or a Python odds dictionary.

`pipeline.py` groups events with `shared/batching.chunks`. The existing identity/discard-aware event batch writer performs persistence; `modules/jobs/daily_discovery/persistence.py` owns normalization and confirmed-write counters; the incremental source owns sport and competition filtering. There is no second event upsert implementation.

Confirmed canonical IDs, source associations, final kickoff/sport metadata and tournament deduplication live in a temporary SQLite store with a bounded page cache. It accepts multiple source IDs for one canonical event. Calendar counts use SQL grouping and only confirmed writes. The store is closed at the end of the run. Startup removes abandoned discovery directories older than 24 hours only after acquiring their OS ownership lock; active runs are preserved.

After event batches commit, a second streamed pass reads the sport's odds. Each small group looks up run membership and checks current canonical identity/kickoff in one short database read. Odds are persisted only for events more than ten minutes from kickoff. Odds ingestion failures remain visible in the sport's retry state.

No HTTP call runs inside an event write transaction. A failed run can retain earlier committed batches; retry repeats that sport and safely updates existing events. Operation counters count writes; the calendar summary counts unique canonical events and cannot be added across runs to establish daily uniqueness.

The persistence summary separates `events_failed` (individual event persistence failures)
from `sports_failed` (sports left incomplete by an event failure or an exception during
calendar/odds processing). A truncated calendar can therefore report `events_failed=0`
and `sports_failed=1`: committed events remain valid, but the sport still needs a retry.

SofaScore transport timeouts use the HTTP client's existing `Config.MAX_RETRIES` attempt limit. Retry waits are interruptible and use the same capped exponential delay as server-error retries. Each streamed attempt resets the temporary response file before downloading again. Exhausted requests leave the sport failed and eligible for a later heartbeat; they do not discard already committed batches. The execution deadline and shutdown checks still limit requests and waits.

## Execution and configuration

Daily, other discoveries, midnight and reporting execute serially on the maintenance channel. Pre-start and T−1 have their own channels. Cooperative cancellation/available-memory checks occur at unit boundaries; unfinished daily work retains its date/slot when deferred by the executor.

Configure execution limits directly in `infrastructure/settings/job_execution.py`; there are no environment overrides. `event_read_batch_size` controls pipeline groups, `event_write_batch_size` controls database write chunks, `response_max_bytes` / `json_item_max_bytes` / `json_max_depth` control transport/parser limits and `temporary_cache_kib` controls SQLite's cache. Restart after editing. The default group/write size is 100, nesting depth is 128 and the cache is 2 MiB.

Direct `discover_events_for_date(date, sports, run_slot)` requires an explicit valid slot and initialized progress rows. It does not perform the heartbeat's cleanup/selection. `python main.py daily-discovery` invokes the heartbeat.

Event writes invalidate reporting in their own transaction. Daily does not synchronously refresh views. The independent `view_refresh` job checks durable pending generations every 15 minutes, including after a process restart, with a minimum of one hour between successful refreshes per view; see [results collection](results-collection.md).
