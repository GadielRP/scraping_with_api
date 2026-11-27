# Results collection and execution

## Scheduling and priorities

Midnight is scheduled at 04:00 in `Config.TIMEZONE` and collects the previous **local** calendar day. Application composition lives in `app/runtime.py`; manual CLI commands invoke the same use cases without constructing a scheduler.

A lightweight private `schedule.Scheduler` only dispatches descriptions of work. Three bounded, serial executors own pre-start, closing T−1 and maintenance. Pre-start keeps at most one active run and one latest pending tick. Closing preserves the exact original occurrence, expires at its target kickoff and uses a PostgreSQL advisory session lock for simultaneous exclusion across instances. The lock uses an autocommit connection, with guaranteed release and no transaction held during HTTP; it does not record historical slot consumption.

Maintenance admits eight active/queued identities by default. Date/slot are part of identity where relevant. A bounded retry registry handles full admission. A deferred maintenance job retries the same date/slot after 60 seconds. The in-process queue is lost on restart. Use `results-date --date YYYY-MM-DD` to collect a missed date and the separate recovery command to replay OddsPapi odds for it.

Provider calls share the existing SofaScore rate cap, with closing ahead of pre-start ahead of maintenance. Priority does not preempt requests already on the network. Provider pools have bounded pending futures and inherit execution/cancellation context. HTTP timeouts and retry waits respect remaining run budget. Pre-start processes acquisition/evaluation before intraday maintenance; its default cooperative budget is 240 seconds. Missing SofaScore odds endpoints use a bounded cooldown while permitting an attempt in each new configured critical moment, including T−1.

Shutdown stops admission, clears pending descriptions and signals active runs. Open SQL units finish or roll back normally. Total join time is bounded by `JobExecutionSettings.shutdown_grace_seconds` (30 seconds); noncooperating workers are logged. Existing browser/HTTP timeouts remain relevant; no thread is forcibly terminated.

## Selection and transactions

`run_results_collection(target_date=None)` is the single collection use case. An explicit date restricts collection to that local calendar day; omission selects sufficiently old incomplete events. The CLI `results` resolves the previous local day in `ApplicationRuntime`; `results-date --date YYYY-MM-DD` supplies its date and `results-all` omits it. Midnight binds yesterday to its original scheduled occurrence.

`contracts.result_selection` owns temporal/sport-duration policy. `ResultRepository.pending_batches(selection, read_size)` owns SQL projection and keyset pagination. Date bounds are UTC instants for the configured local day. A fixed initial maximum canonical ID bounds the invocation. Each read closes before HTTP begins. Complete rows require **both scores**; partial result rows remain eligible.

One mapping is resolved in the page query. Missing/ambiguous mappings are separate failures and cannot duplicate page entries. Provider response IDs must match the requested identity before parsing or deleting. No additional HTTP concurrency/rate is introduced into result collection.

`batch_processor.collect_batch` classifies each unit and calls the existing writers. Confirmed result IDs determine counters. Unresolved/in-progress events are `deferred`, separate from provider/parse errors, mapping errors and persistence conflicts. Resolved units are committed before a deadline defers the rest of the page. `ResultBatchDeferred` carries that page's confirmed counters to the run summary, with separate `deferred_budget` and `deferred_unresolved` outcomes.

Metadata, result and deletion writes are separate bounded transactions. Result rows and their observations are **one atomic transaction**, together with reporting invalidation. Observation failure rolls back its result. Parent row locks serialize result writes with event deletion, and removed parents are omitted. Expected canonical IDs prevent late metadata from recreating deleted/remapped parents. Guarded deletion checks identity/newer metadata/results and retains the existing discard categories/settings.

`JobExecutionSettings.event_read_batch_size` controls selection/pipeline groups. `JobExecutionSettings.event_write_batch_size` controls SQL write chunks. Configure both in `infrastructure/settings/job_execution.py`. Both default to 100 but are independent. `batch_upsert_results` returns a set of confirmed canonical IDs, not a speculative integer count. All internal consumers have been migrated.

Re-running a date selects only unresolved/incomplete rows; no cursor table is required. Infrastructure errors propagate. A provider failure remains pending and increments `failed`; a valid unfinished response increments `deferred`.

## OddsPapi prematch odds recovery

After the scheduled Midnight Sync call returns, `ApplicationRuntime` enqueues a separate `oddspapi_prematch_odds_recovery:YYYY-MM-DD` job for the same local target date if pre-start historical requests are disabled and `ODDSPAPI_HISTORY_RECOVERY_FROM_UTC` is set. The job runs on the maintenance executor, separate from pre-start and closing. It checks for critical work before each fixture and defers if any is pending.

Selection requires a tracked competition, an OddsPapi mapping and a row in `results`. Kickoff must fall on or after the configured recovery start and before the end of the target day. Events that started before the target day also qualify when `results.finished_at` falls within it. A null finish time is sufficient for events that started on the target day. Only configured bookmakers with pre-kickoff cache for that event and fixture are requested. When none qualify, the job logs `reason=no_cache`. See the [OddsPapi request modes and history rules](../providers/pre_start_odds_ingestion.md#oddspapi-request-modes).

Use the standalone command to replay a date. It requires `ODDSPAPI_HISTORY_RECOVERY_FROM_UTC` and writes to the configured database. It rejects `--dry-run`. The manual `midnight` command runs result sync alone.

```bash
python main.py oddspapi-prematch-odds-recovery --date 2026-11-25
```

The job replaces each existing quote instrument that `/historical-odds` returns with at least one tick through kickoff: its snapshots are deleted and rewritten in the same transaction, and its opening and current are overwritten. Instruments the response does not return keep their stored data. It selects qualifying fixtures again on a manual replay, and a replay rewrites the same state. If quota is exhausted, it stops the current run, and a replay of the target date can pick up the remaining work. Inspect `quotes_updated` and `snapshots_added` to see what each fixture wrote. `markets_saved=0` is expected because recovery works on existing markets. `no historical quote or snapshot changes` indicates a processed response with zero writes.

## Durable materialized-view refresh

Exactly two materialized views are refreshed: `mv_alert_events` and `mv_p5_price_memory`. SQL views are evaluated on demand and are not refreshed. Source transactions increment requested generations; successful refresh acknowledges only the captured generation. New invalidations arriving during execution remain pending. Completion/failure is independent per view.

The `view_refresh` maintenance job checks pending work every 15 minutes. Successful automatic refreshes are throttled to one per view per hour by default, coalescing repeated writes. The hour starts at successful completion; the next poll and maintenance queue can delay the following refresh further. Failure backoff starts at 60 seconds, doubles and caps at one hour. Daily, discovery, result collection and midnight leave refreshes to this job; an explicit manual refresh requests fresh generations and forces an attempt. Timing and limits are configured through `JobExecutionSettings.view_refresh_*` in `infrastructure/settings/job_execution.py`, without environment overrides; restart after editing. Skipped daily heartbeats do not gate recovery.

Within the application process, each view refresh transaction is mutually exclusive with pre-start, closing T−1 and background OddsPortal cycles. Before each view, `view_refresh` consults critical work that is due in the calendar, queued or active. If pre-start and view refresh coincide, pre-start runs first and view refresh defers through the existing 60-second retry path; pending generations are preserved. Critical work waits, within its execution budget, for an already active refresh; waiting critical work takes precedence over the next view. Other maintenance jobs retain their existing concurrency behavior.

Each view uses `REFRESH MATERIALIZED VIEW CONCURRENTLY`, its own advisory transaction lock, `work_mem=4MB`, zero parallel query workers, `lock_timeout=5s` and a 180-second statement timeout by default. Existing unique indexes support concurrent refresh. Views are independent snapshots; consumers in this code read one reporting view per calculation, rather than requiring an atomic paired snapshot.

Migration `20261003_01` adds `reporting_refresh_state` and a privileged allowlisted `public.refresh_reporting_view(text)`, grants the application role access, and retires the previous bulk/diagnostic functions. Historical migrations remain unchanged; downgrade restores the former callable surface without rebuilding views. The startup schema guard requires the new revision/function/table.

Apply `alembic upgrade head` with the normal migration role before starting the new app. Update runtime dependencies, including `ijson>=3.3,<4`. The app must not acquire DDL privileges.

## Measurement

Logs distinguish admission, queue/dispatch lag, run ID, deferred/failed completion, provider/parse time, confirmed writes, per-view duration and pending reporting. The process has one sampler and a registry of concurrent active operations. RSS belongs to the process; it is not attributed exclusively to a job.

See [validation and capacity](execution-validation.md) and the [single follow-up cleanup list](../maintenance/execution-legacy-cleanup.md). Disposable integration URLs (`RESULTS_TEST_DATABASE_URL`, `REPORTING_TEST_DATABASE_URL`, `DAILY_TEST_DATABASE_URL`) create/drop test tables and must never point at an application database.
