# Results collection and execution

## Scheduling and priorities

Midnight is scheduled at 04:00 in `Config.TIMEZONE` and collects the previous **local** calendar day. Application composition lives in `app/runtime.py`; manual CLI commands invoke the same use cases without constructing a scheduler.

A lightweight private `schedule.Scheduler` only dispatches descriptions of work. Three bounded, serial executors own pre-start, closing T−1 and maintenance. Pre-start keeps at most one active run and one latest pending tick. Closing preserves the exact original occurrence, expires at its target kickoff and uses a PostgreSQL advisory session lock for simultaneous exclusion across instances. The lock uses an autocommit connection, with guaranteed release and no transaction held during HTTP; it does not record historical slot consumption.

Maintenance admits eight active/queued identities by default. Date/slot are part of identity where relevant. A bounded retry registry handles full admission; maintenance deferred by resource pressure resumes the same date/slot after 60 seconds. This queue is in process. Missing midnight dates after a process crash still require `results-date` or a separately designed reconciliation job.

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

## Durable reporting

Exactly two materialized views are refreshed: `mv_alert_events` and `mv_p5_price_memory`. SQL views are evaluated on demand and are not refreshed. Source transactions increment requested generations; successful refresh acknowledges only the captured generation. New invalidations arriving during execution remain pending. Completion/failure is independent per view.

A maintenance poll checks pending work every 60 seconds. Successful automatic refreshes are throttled to one per view per 30 minutes by default, coalescing repeated writes. Failure backoff starts at 60 seconds, doubles and caps at one hour. Daily, discovery, result collection and midnight leave refreshes to this poll; an explicit manual refresh requests fresh generations and forces an attempt. Reporting timing and limits are configured only in `infrastructure/settings/job_execution.py`, without environment overrides; restart after editing. Skipped daily heartbeats do not gate recovery.

Within the application process, each view refresh transaction is mutually exclusive with pre-start, closing T−1 and background OddsPortal cycles. Reporting defers through the existing retry path while critical work is active. Critical work waits, within its execution budget, for an already active refresh; waiting critical work takes precedence over the next view. Other maintenance jobs retain their existing concurrency behavior.

Each view uses `REFRESH MATERIALIZED VIEW CONCURRENTLY`, its own advisory transaction lock, `work_mem=4MB`, zero parallel query workers, `lock_timeout=5s` and a 180-second statement timeout by default. Existing unique indexes support concurrent refresh. Views are independent snapshots; consumers in this code read one reporting view per calculation, rather than requiring an atomic paired snapshot.

Migration `20261003_01` adds `reporting_refresh_state` and a privileged allowlisted `public.refresh_reporting_view(text)`, grants the application role access, and retires the previous bulk/diagnostic functions. Historical migrations remain unchanged; downgrade restores the former callable surface without rebuilding views. The startup schema guard requires the new revision/function/table.

Apply `alembic upgrade head` with the normal migration role before starting the new app. Update runtime dependencies, including `ijson>=3.3,<4`. The app must not acquire DDL privileges.

## Measurement

Logs distinguish admission, queue/dispatch lag, run ID, deferred/failed completion, provider/parse time, confirmed writes, per-view duration and pending reporting. The process has one sampler and a registry of concurrent active operations. RSS belongs to the process; it is not attributed exclusively to a job.

See [validation and capacity](execution-validation.md) and the [single follow-up cleanup list](../maintenance/execution-legacy-cleanup.md). Disposable integration URLs (`RESULTS_TEST_DATABASE_URL`, `REPORTING_TEST_DATABASE_URL`, `DAILY_TEST_DATABASE_URL`) create/drop test tables and must never point at an application database.
